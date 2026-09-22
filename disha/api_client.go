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

	enqueueJobPath = "/common/enqueue_job"

	// idempotencyHeader carries the envelope key on the direct HTTP
	// routes. The job path carries the same key inside
	// EnqueueJobRequest instead, so disha-backend can honour one notion
	// of "already did this" from either direction.
	idempotencyHeader = "Idempotency-Key"
)

// Operation names. These become the Sentry `operation` tag, so they are
// what separates one actionable issue from another — keep them stable.
const (
	opUpdateConversation     = "update_conversation"
	opRunPostCallOperations  = "run_post_call_operations"
	opSetUserCareplan        = "set_user_careplan"
	opAddTagToUser           = "add_tag_to_user"
	opSyncConversationChunks = "sync_conversation_chunks_to_db"
)

// IdempotencyContext is the per-operation metadata a caller supplies:
// the deterministic key that lets disha-backend recognise a replay, and
// the call identity to tag a failure report with.
//
// vago does not retry (decided 2026-09-21). Durability lives in
// disha-backend, which is co-located with its Redis and SQS; this side
// makes exactly one attempt and reports the ones that do not land.
type IdempotencyContext struct {
	IdempotencyKey string
	SentryTags     map[string]string
}

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

func (c *APIClient) UpdateConversation(ctx context.Context, req UpdateConversationRequest, ic IdempotencyContext) error {
	return c.call(ctx, opUpdateConversation, http.MethodPatch, "/bot/update_conversation", req, ic)
}

func (c *APIClient) RunPostCallOperations(ctx context.Context, req PostCallOperationsRequest, ic IdempotencyContext) error {
	return c.call(ctx, opRunPostCallOperations, http.MethodPost, "/bot/run_post_call_operations", req, ic)
}

func (c *APIClient) SetUserCareplan(ctx context.Context, req SetUserCareplanRequest, ic IdempotencyContext) error {
	return c.call(ctx, opSetUserCareplan, http.MethodPost, "/bot/set_user_careplan", req, ic)
}

func (c *APIClient) AddTagToUser(ctx context.Context, req AddTagToUserRequest, ic IdempotencyContext) error {
	return c.call(ctx, opAddTagToUser, http.MethodPost, "/bot/add_tag_to_user", req, ic)
}

// EnqueueJob posts a background job with no idempotency key. Use it only
// for best-effort telemetry (LLM logs, Daily metrics) where a duplicate
// is harmless and a loss is acceptable.
func (c *APIClient) EnqueueJob(ctx context.Context, req EnqueueJobRequest) error {
	return c.send(ctx, http.MethodPost, enqueueJobPath, req, "")
}

// EnqueueJobKeyed posts a job whose replay must be suppressed. The key
// travels inside the envelope, where disha-backend's worker dispatcher
// reads it.
func (c *APIClient) EnqueueJobKeyed(ctx context.Context, operation string, req EnqueueJobRequest, ic IdempotencyContext) error {
	req.IdempotencyKey = ic.IdempotencyKey
	return c.call(ctx, operation, http.MethodPost, enqueueJobPath, req, ic)
}

// call makes ONE attempt and reports a failure that nothing else will
// recover.
//
// There is no retry here by design: vago's worker is a cross-region hop
// from Disha, and holding operations on this side meant either paying
// that latency to persist them (the Redis outbox) or keeping them in
// memory where a pod deletion loses them. disha-backend owns durability
// instead — it is co-located with its Redis and SQS, and its idempotency
// claim makes a replay safe.
//
// The consequence, stated plainly: a request that never reaches the
// backend is gone. That is what the report below is for.
func (c *APIClient) call(ctx context.Context, operation, method, path string, body any, ic IdempotencyContext) error {
	start := time.Now()
	err := c.send(ctx, method, path, body, ic.IdempotencyKey)
	if err == nil {
		// TODO: remove this when merging PR
		apiLogf(c.logger, "delivered operation=%s %s %s duration_ms=%d idempotency_key=%s conversation=%s",
			operation, method, path, time.Since(start).Milliseconds(), ic.IdempotencyKey, ic.SentryTags["conversation_id"])
		return nil
	}
	apiLogf(c.logger, "NOT DELIVERED operation=%s %s %s duration_ms=%d idempotency_key=%s conversation=%s: %v",
		operation, method, path, time.Since(start).Milliseconds(), ic.IdempotencyKey, ic.SentryTags["conversation_id"], err)
	c.reportUndelivered(operation, ic, err)
	return err
}

// reportUndelivered captures one event per operation per minute. Nothing
// retries these, so each is real loss and worth seeing — but a Disha
// outage would otherwise produce one event per operation per call, which
// is how VAGO-6 and VAGO-7 became unreadable.
func (c *APIClient) reportUndelivered(operation string, ic IdempotencyContext, cause error) {
	if !allowSentryReport("api_undelivered:"+operation, time.Now()) {
		return
	}
	details := map[string]any{"idempotency_key": ic.IdempotencyKey}
	for k, v := range ic.SentryTags {
		details[k] = v
	}
	captureOutboxSentry(sentryutil.Event{
		Hub: sentryutil.NewTaskHub(ic.SentryTags),
		Err: cause,
		Tags: map[string]string{
			"component": "disha_api",
			"operation": operation,
			"reason":    "not_delivered",
		},
		Details: details,
	})
}

// apiLogf mirrors the outbox line format that preceded it.
// TODO: remove this when merging PR
func apiLogf(logger *log.Logger, format string, v ...any) {
	if logger == nil {
		return
	}
	logger.Printf("disha: api "+format+"\n", v...)
}

func (c *APIClient) send(ctx context.Context, method, path string, body any, idempotencyKey string) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("disha: marshal %s %s request: %w", method, path, err)
	}
	return c.sendRaw(ctx, method, path, payload, idempotencyKey)
}

// sendRaw performs exactly one HTTP attempt.
// It deliberately does NOT capture to Sentry. It used to, which is what
// made VAGO-6 and VAGO-7 fire on every transient blip — including ones
// the fallback then recovered from — and collapsed nine unrelated job
// call sites into a single ungroupable issue. Reporting is now the
// responsibility of whoever exhausts the retry budget.
func (c *APIClient) sendRaw(ctx context.Context, method, path string, payload []byte, idempotencyKey string) error {
	httpReq, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("disha: build %s %s request: %w", method, path, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		httpReq.Header.Set(idempotencyHeader, idempotencyKey)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("disha: API %s %s failed: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &APIStatusError{
			Method: method,
			Path:   path,
			Status: resp.StatusCode,
			Body:   strings.TrimSpace(string(raw)),
		}
	}
	return nil
}
