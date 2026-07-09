package disha

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const (
	defaultAPIBaseURL = "https://disha-ai.curelinktech.in"
	defaultAPITimeout = 10 * time.Second
)

type APIClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *log.Logger
}

func NewAPIClient(baseURL string, timeout time.Duration, logger *log.Logger) *APIClient {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = defaultAPIBaseURL
	}
	if timeout <= 0 {
		timeout = defaultAPITimeout
	}
	return &APIClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: timeout},
		logger:     logger,
	}
}

func (c *APIClient) UpdateConversation(ctx context.Context, req UpdateConversationRequest) error {
	return c.send(ctx, http.MethodPatch, "/bot/update_conversation", req)
}

func (c *APIClient) UpdateConversationWithFallback(ctx context.Context, req UpdateConversationRequest) error {
	err := c.UpdateConversation(ctx, req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("update_conversation", "bots.operations.voice_bot_operations", "update_conversation", req, err)
}

func (c *APIClient) RunPostCallOperations(ctx context.Context, req PostCallOperationsRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/run_post_call_operations", req)
}

func (c *APIClient) RunPostCallOperationsWithFallback(ctx context.Context, req PostCallOperationsRequest) error {
	err := c.RunPostCallOperations(ctx, req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("run_post_call_operations", "bots.operations.voice_bot_operations", "run_post_call_operations", req, err)
}

func (c *APIClient) SetUserCareplan(ctx context.Context, req SetUserCareplanRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/set_user_careplan", req)
}

func (c *APIClient) SetUserCareplanWithFallback(ctx context.Context, req SetUserCareplanRequest) error {
	err := c.SetUserCareplan(ctx, req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("set_user_careplan", "bots.operations.voice_bot_operations", "set_user_careplan", req, err)
}

func (c *APIClient) AddTagToUser(ctx context.Context, req AddTagToUserRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/add_tag_to_user", req)
}

func (c *APIClient) AddTagToUserWithFallback(ctx context.Context, req AddTagToUserRequest) error {
	err := c.AddTagToUser(ctx, req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("add_tag_to_user", "bots.operations.voice_bot_operations", "add_tag_to_user", req, err)
}

func (c *APIClient) EnqueueJob(ctx context.Context, req EnqueueJobRequest) error {
	return c.send(ctx, http.MethodPost, "/common/enqueue_job", req)
}

func (c *APIClient) send(ctx context.Context, method, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("disha: marshal %s %s request: %w", method, path, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("disha: build %s %s request: %w", method, path, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		wrapped := fmt.Errorf("disha: API %s %s failed: %w", method, path, err)
		sentryutil.Capture(sentryutil.Event{
			Err: wrapped,
			Tags: map[string]string{
				"component": "disha_api",
				"method":    method,
				"path":      path,
			},
		})
		return wrapped
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		wrapped := fmt.Errorf("disha: API %s %s returned %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
		sentryutil.Capture(sentryutil.Event{
			Err: wrapped,
			Tags: map[string]string{
				"component": "disha_api",
				"method":    method,
				"path":      path,
			},
			Details: map[string]any{
				"status": resp.StatusCode,
			},
		})
		return wrapped
	}
	return nil
}

func (c *APIClient) enqueueAPIFallback(operationName, moduleName, funcName string, req any, originalErr error) error {
	kwargs, err := requestAsMap(req)
	if err != nil {
		return fmt.Errorf("disha: build fallback kwargs for %s: %w", operationName, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultAPITimeout)
	defer cancel()
	fallbackReq := EnqueueJobRequest{
		ModuleName: moduleName,
		FuncName:   funcName,
		Kwargs:     kwargs,
		SQSQueue:   "p0-fast-l1",
	}
	if err := c.EnqueueJob(ctx, fallbackReq); err != nil {
		return fmt.Errorf("disha: %s API failed (%v) and fallback enqueue failed: %w", operationName, originalErr, err)
	}
	if c.logger != nil {
		c.logger.Printf("disha: %s API failed, queued fallback job: %v\n", operationName, originalErr)
	}
	return nil
}

func requestAsMap(req any) (map[string]any, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}
