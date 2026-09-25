package disha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const (
	defaultAPIBaseURL = "https://disha-ai.curelinktech.in"
	defaultAPITimeout = 10 * time.Second

	enqueueJobPath       = "/common/enqueue_job"
	idempotencyKeyHeader = "Idempotency-Key"

	// enqueue_job is the last line of defence: by the time we call it the direct
	// API call has already failed, so it retries transient failures instead of
	// dropping the work. Every attempt reuses one idempotency key, so a retry
	// after an ambiguous failure (timeout, 502) is deduped by Disha rather than
	// queueing the job twice.
	enqueueMaxAttempts    = 4 // 1 initial attempt + 3 retries
	enqueueRetryBaseDelay = time.Second

	// Budget for the detached context the fallback path builds: 4 attempts at
	// defaultAPITimeout each plus 1s+2s+4s of backoff does not fit, and it
	// should not — a post-call goroutine must not block for a minute. Attempts
	// that no longer fit are cut off by the context and reported as exhausted.
	enqueueFallbackBudget = 30 * time.Second
)

type APIClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *log.Logger
	// retryBaseDelay is the first enqueue_job backoff step; tests shrink it so
	// they do not sit through the real 1s+2s+4s ladder.
	retryBaseDelay time.Duration
}

// apiError is one failed attempt against the Disha API. Status is 0 when the
// request never produced a response (timeout, connection reset, DNS), which is
// exactly the ambiguous case the idempotency key exists for: the server may
// have processed the request anyway.
type apiError struct {
	Method string
	Path   string
	Status int
	Body   string
	Err    error
}

func (e *apiError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("disha: API %s %s failed: %v", e.Method, e.Path, e.Err)
	}
	return fmt.Sprintf("disha: API %s %s returned %d: %s", e.Method, e.Path, e.Status, e.Body)
}

func (e *apiError) Unwrap() error { return e.Err }

// transient reports whether retrying the identical request could plausibly
// succeed. 409 counts: Disha returns it while another attempt of the same
// idempotency key is still in flight, and that attempt may yet fail and release
// the key, so we keep asking until it resolves either way.
func (e *apiError) transient() bool {
	switch {
	case e.Status == 0:
		return true
	case e.Status == http.StatusConflict, e.Status == http.StatusTooManyRequests:
		return true
	case e.Status >= 500:
		return true
	default:
		return false
	}
}

func isTransient(err error) bool {
	var apiErr *apiError
	return errors.As(err, &apiErr) && apiErr.transient()
}

func statusOf(err error) int {
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
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
		baseURL:        baseURL,
		httpClient:     &http.Client{Timeout: timeout},
		logger:         logger,
		retryBaseDelay: enqueueRetryBaseDelay,
	}
}

func (c *APIClient) UpdateConversation(ctx context.Context, req UpdateConversationRequest) error {
	return c.send(ctx, http.MethodPatch, "/bot/update_conversation", req)
}

func (c *APIClient) UpdateConversationWithFallback(ctx context.Context, req UpdateConversationRequest) error {
	err := c.sendQuiet(ctx, http.MethodPatch, "/bot/update_conversation", req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("update_conversation", "bots.operations.voice_bot_operations", "update_conversation", req, err)
}

func (c *APIClient) RunPostCallOperations(ctx context.Context, req PostCallOperationsRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/run_post_call_operations", req)
}

func (c *APIClient) RunPostCallOperationsWithFallback(ctx context.Context, req PostCallOperationsRequest) error {
	err := c.sendQuiet(ctx, http.MethodPost, "/bot/run_post_call_operations", req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("run_post_call_operations", "bots.operations.voice_bot_operations", "run_post_call_operations", req, err)
}

func (c *APIClient) SetUserCareplan(ctx context.Context, req SetUserCareplanRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/set_user_careplan", req)
}

func (c *APIClient) SetUserCareplanWithFallback(ctx context.Context, req SetUserCareplanRequest) error {
	err := c.sendQuiet(ctx, http.MethodPost, "/bot/set_user_careplan", req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("set_user_careplan", "bots.operations.voice_bot_operations", "set_user_careplan", req, err)
}

func (c *APIClient) AddTagToUser(ctx context.Context, req AddTagToUserRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/add_tag_to_user", req)
}

func (c *APIClient) AddTagToUserWithFallback(ctx context.Context, req AddTagToUserRequest) error {
	err := c.sendQuiet(ctx, http.MethodPost, "/bot/add_tag_to_user", req)
	if err == nil {
		return nil
	}
	return c.enqueueAPIFallback("add_tag_to_user", "bots.operations.voice_bot_operations", "add_tag_to_user", req, err)
}

// EnqueueJob posts a job to Disha's queue endpoint, retrying transient failures
// up to enqueueMaxAttempts times. Every attempt carries the same
// Idempotency-Key, so a retry after an ambiguous failure is deduped server-side
// instead of queueing the job twice. Sentry is fired exactly once, and only
// when the job could not be handed over at all.
func (c *APIClient) EnqueueJob(ctx context.Context, req EnqueueJobRequest) error {
	return c.enqueueJob(ctx, req, nil)
}

// enqueueJob carries primaryErr, the direct-API failure that triggered the
// fallback, so the Sentry event raised on exhaustion names both failures
// instead of only the queue one.
func (c *APIClient) enqueueJob(ctx context.Context, req EnqueueJobRequest, primaryErr error) error {
	key, err := req.IdempotencyKey()
	if err != nil {
		// The job can never be sent — do would fail to marshal the same payload —
		// so this is dropped work and deserves the same alert as an exhausted retry.
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "disha_api",
				"operation": "enqueue_job",
				"module":    req.ModuleName,
				"func":      req.FuncName,
			},
		})
		return err
	}
	headers := map[string]string{idempotencyKeyHeader: key}

	var lastErr error
	attempts := 0
	for attempt := 1; attempt <= enqueueMaxAttempts; attempt++ {
		attempts = attempt
		lastErr = c.do(ctx, http.MethodPost, enqueueJobPath, req, headers)
		if lastErr == nil {
			if attempt > 1 && c.logger != nil {
				c.logger.Printf("disha: enqueue_job %s.%s succeeded on attempt %d/%d key=%s\n",
					req.ModuleName, req.FuncName, attempt, enqueueMaxAttempts, key)
			}
			return nil
		}
		if !isTransient(lastErr) || attempt == enqueueMaxAttempts {
			break
		}
		if err := sleepWithContext(ctx, c.retryDelay(attempt)); err != nil {
			// The budget ran out mid-backoff; stop here and report what we have.
			break
		}
	}

	wrapped := fmt.Errorf("disha: enqueue_job %s.%s failed after %d attempt(s): %w",
		req.ModuleName, req.FuncName, attempts, lastErr)
	details := map[string]any{
		"attempts":        attempts,
		"max_attempts":    enqueueMaxAttempts,
		"idempotency_key": key,
		"module_name":     req.ModuleName,
		"func_name":       req.FuncName,
		"sqs_queue":       req.SQSQueue,
		"last_status":     statusOf(lastErr),
	}
	if primaryErr != nil {
		details["primary_error"] = primaryErr.Error()
	}
	sentryutil.Capture(sentryutil.Event{
		Err: wrapped,
		Tags: map[string]string{
			"component": "disha_api",
			"operation": "enqueue_job",
			"module":    req.ModuleName,
			"func":      req.FuncName,
		},
		Details: details,
	})
	return wrapped
}

// retryDelay doubles from retryBaseDelay (1s, 2s, 4s) and adds up to 25% jitter
// so workers that failed against the same outage do not retry in lockstep.
func (c *APIClient) retryDelay(attempt int) time.Duration {
	base := c.retryBaseDelay
	if base <= 0 {
		base = enqueueRetryBaseDelay
	}
	base <<= attempt - 1
	return base + rand.N(base/4+1)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// send reports failures to Sentry itself. Call sites that have a queue fallback
// use sendQuiet instead: a direct call that fails but is rescued by the fallback
// is not an incident, so only an exhausted fallback raises an event.
func (c *APIClient) send(ctx context.Context, method, path string, body any) error {
	err := c.do(ctx, method, path, body, nil)
	if err == nil {
		return nil
	}
	sentryutil.Capture(sentryutil.Event{
		Err: err,
		Tags: map[string]string{
			"component": "disha_api",
			"method":    method,
			"path":      path,
		},
		Details: map[string]any{
			"status": statusOf(err),
		},
	})
	return err
}

func (c *APIClient) sendQuiet(ctx context.Context, method, path string, body any) error {
	return c.do(ctx, method, path, body, nil)
}

// do performs a single attempt and classifies the outcome; it never reports to
// Sentry so that retry loops decide when a failure is worth an alert.
func (c *APIClient) do(ctx context.Context, method, path string, body any, headers map[string]string) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("disha: marshal %s %s request: %w", method, path, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("disha: build %s %s request: %w", method, path, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		httpReq.Header.Set(name, value)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return &apiError{Method: method, Path: path, Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &apiError{
			Method: method,
			Path:   path,
			Status: resp.StatusCode,
			Body:   strings.TrimSpace(string(raw)),
		}
	}
	return nil
}

func (c *APIClient) enqueueAPIFallback(operationName, moduleName, funcName string, req any, originalErr error) error {
	kwargs, err := requestAsMap(req)
	if err != nil {
		return fmt.Errorf("disha: build fallback kwargs for %s: %w", operationName, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), enqueueFallbackBudget)
	defer cancel()
	fallbackReq := EnqueueJobRequest{
		ModuleName: moduleName,
		FuncName:   funcName,
		Kwargs:     kwargs,
		SQSQueue:   "p0-fast-l1",
	}
	if err := c.enqueueJob(ctx, fallbackReq, originalErr); err != nil {
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
