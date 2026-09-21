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

// OutboxContext is the per-operation durability metadata a caller
// supplies: the deterministic idempotency key that makes a replay safe,
// and the call identity to tag a give-up event with.
type OutboxContext struct {
	IdempotencyKey string
	SentryTags     map[string]string
}

type APIClient struct {
	baseURL    string
	httpClient *http.Client
	logger     *log.Logger
	outbox     *Outbox
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

// SetOutbox enables durable delivery. Until it is called (or when the
// outbox is disabled) every ...Durable method degrades to exactly the
// pre-outbox behaviour: one attempt, error returned to the caller.
func (c *APIClient) SetOutbox(o *Outbox) {
	c.outbox = o
}

func (c *APIClient) UpdateConversation(ctx context.Context, req UpdateConversationRequest) error {
	return c.send(ctx, http.MethodPatch, "/bot/update_conversation", req, "")
}

func (c *APIClient) UpdateConversationDurable(ctx context.Context, req UpdateConversationRequest, oc OutboxContext) error {
	return c.durable(ctx, opUpdateConversation, http.MethodPatch, "/bot/update_conversation", req, oc)
}

func (c *APIClient) RunPostCallOperations(ctx context.Context, req PostCallOperationsRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/run_post_call_operations", req, "")
}

func (c *APIClient) RunPostCallOperationsDurable(ctx context.Context, req PostCallOperationsRequest, oc OutboxContext) error {
	return c.durable(ctx, opRunPostCallOperations, http.MethodPost, "/bot/run_post_call_operations", req, oc)
}

func (c *APIClient) SetUserCareplan(ctx context.Context, req SetUserCareplanRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/set_user_careplan", req, "")
}

func (c *APIClient) SetUserCareplanDurable(ctx context.Context, req SetUserCareplanRequest, oc OutboxContext) error {
	return c.durable(ctx, opSetUserCareplan, http.MethodPost, "/bot/set_user_careplan", req, oc)
}

func (c *APIClient) AddTagToUser(ctx context.Context, req AddTagToUserRequest) error {
	return c.send(ctx, http.MethodPost, "/bot/add_tag_to_user", req, "")
}

func (c *APIClient) AddTagToUserDurable(ctx context.Context, req AddTagToUserRequest, oc OutboxContext) error {
	return c.durable(ctx, opAddTagToUser, http.MethodPost, "/bot/add_tag_to_user", req, oc)
}

func (c *APIClient) EnqueueJob(ctx context.Context, req EnqueueJobRequest) error {
	return c.send(ctx, http.MethodPost, enqueueJobPath, req, "")
}

// EnqueueJobDurable persists the job before attempting it, so a failed
// enqueue is retried rather than dropped. The idempotency key travels
// inside the envelope, where disha-backend's worker dispatcher reads it.
func (c *APIClient) EnqueueJobDurable(ctx context.Context, operation string, req EnqueueJobRequest, oc OutboxContext) error {
	req.IdempotencyKey = oc.IdempotencyKey
	return c.durable(ctx, operation, http.MethodPost, enqueueJobPath, req, oc)
}

// durable implements outbox-first delivery: persist, then attempt, then
// delete on success. Persisting first is what makes "nothing is
// dropped" true — run_post_call_operations fires at call end, exactly
// when KEDA is most likely to SIGTERM the pod mid-attempt. The cost is
// that a crash between a successful attempt and the delete replays the
// operation, which the backend idempotency key absorbs.
func (c *APIClient) durable(ctx context.Context, operation, method, path string, body any, oc OutboxContext) error {
	if !c.outbox.Enabled() {
		// TODO: remove this when merging PR
		outboxLogf(c.logger, "disabled operation=%s direct_send=%s %s idempotency_key=%s", operation, method, path, oc.IdempotencyKey)
		return c.send(ctx, method, path, body, oc.IdempotencyKey)
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("disha: marshal %s request: %w", operation, err)
	}
	item := &OutboxItem{
		Kind:           OutboxKindAPI,
		Operation:      operation,
		Method:         method,
		Path:           path,
		Payload:        payload,
		IdempotencyKey: oc.IdempotencyKey,
		SentryTags:     oc.SentryTags,
	}
	if path == enqueueJobPath {
		item.Kind = OutboxKindJob
	}

	if err := c.outbox.Enqueue(ctx, item); err != nil {
		// Redis is unavailable. Degrade to a plain attempt rather than
		// refusing to do the work at all — but with no durable copy
		// there is nothing to retry, so a failure here IS terminal and
		// is the one Redis-related condition worth reporting.
		if c.logger != nil {
			c.logger.Printf("disha: outbox enqueue failed operation=%s, attempting inline: %v\n", operation, err)
		}
		start := time.Now()
		sendErr := c.send(ctx, method, path, body, oc.IdempotencyKey)
		if sendErr != nil {
			outboxLogf(c.logger, "DROPPED operation=%s attempt=1/1 source=inline_no_outbox duration_ms=%d reason=not_durable: %v",
				operation, time.Since(start).Milliseconds(), sendErr)
			c.reportDropped(operation, item, sendErr, "not_durable")
			return sendErr
		}
		outboxLogf(c.logger, "delivered operation=%s attempts=1 source=inline_no_outbox duration_ms=%d (no durable copy)",
			operation, time.Since(start).Milliseconds())
		return nil
	}

	item.Attempts = 1
	// TODO: remove this when merging PR
	outboxLogf(c.logger, "attempt operation=%s id=%s attempt=%d/%d source=inline %s %s",
		operation, item.ID, item.Attempts, outboxMaxAttempts, method, path)
	start := time.Now()
	sendErr := c.sendRaw(ctx, method, path, payload, oc.IdempotencyKey)
	logOutboxAttempt(c.logger, item, "inline", time.Since(start), sendErr)
	if sendErr != nil {
		if !outboxRetryable(sendErr) {
			// A permanent client error will never succeed, so retrying
			// it eight times only delays the alert. Terminal.
			_ = c.outbox.Park(ctx, item, sendErr)
			outboxLogf(c.logger, "GAVE UP operation=%s id=%s attempts=%d reason=permanent source=inline: %v",
				operation, item.ID, item.Attempts, sendErr)
			c.reportDropped(operation, item, sendErr, "permanent")
			return sendErr
		}
		// Durably queued — the drainer owns it from here, so this is
		// not a failure from the caller's point of view. Outbox.Retry
		// logs when it comes due.
		if rerr := c.outbox.Retry(ctx, item, sendErr); rerr != nil && c.logger != nil {
			c.logger.Printf("disha: outbox reschedule failed operation=%s id=%s: %v\n", operation, item.ID, rerr)
		}
		return nil
	}

	if err := c.outbox.Complete(ctx, item.ID); err != nil && c.logger != nil {
		c.logger.Printf("disha: outbox complete failed operation=%s id=%s: %v\n", operation, item.ID, err)
	}
	outboxLogf(c.logger, "delivered operation=%s id=%s attempts=%d source=inline", operation, item.ID, item.Attempts)
	return nil
}

// reportDropped fires the single Sentry event for an operation that is
// definitively lost on this path — no durable copy and no further
// attempt will be made. Everything recoverable is logged instead, so an
// event from this component always means real data loss.
func (c *APIClient) reportDropped(operation string, item *OutboxItem, cause error, reason string) {
	details := map[string]any{"reason": reason, "attempts": item.Attempts}
	for k, v := range item.SentryTags {
		details[k] = v
	}
	captureOutboxSentry(sentryutil.Event{
		Hub: sentryutil.NewTaskHub(item.SentryTags),
		Err: cause,
		Tags: map[string]string{
			"component": "disha_outbox",
			"operation": operation,
			"reason":    reason,
		},
		Details: details,
	})
}

// attemptOutbox replays a claimed item. It satisfies outboxAttempter.
func (c *APIClient) attemptOutbox(ctx context.Context, item *OutboxItem) error {
	return c.sendRaw(ctx, item.Method, item.Path, item.Payload, item.IdempotencyKey)
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
