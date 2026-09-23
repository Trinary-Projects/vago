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

	enqueueJobPath = "/common/enqueue_job"

	// fallbackJobModule holds the module-level Python equivalents of the
	// four bot routes; fallbackJobQueue is the same queue Disha's own
	// callers use for them.
	fallbackJobModule = "bots.operations.voice_bot_operations"
	fallbackJobQueue  = "p0-fast-l1"

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

// apiFallbackRetryDelays caps the wait before each fallback retry, so
// the fallback makes len+1 attempts in total. Exponential full jitter,
// mirroring the S3 uploader's policy. A package var so tests can
// shorten it.
var apiFallbackRetryDelays = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}

// jobFallbackFunc maps an operation to the module-level Python function
// that performs the same work off SQS. An operation absent from this map
// has no fallback: opSyncConversationChunks IS an enqueue_job, so
// queueing it again would just repeat the call that has already failed.
var jobFallbackFunc = map[string]string{
	opUpdateConversation:    "update_conversation",
	opRunPostCallOperations: "run_post_call_operations",
	opSetUserCareplan:       "set_user_careplan",
	opAddTagToUser:          "add_tag_to_user",
}

// IdempotencyContext is the per-operation metadata a caller supplies:
// the deterministic key that lets disha-backend recognise a replay, and
// the call identity to tag a failure report with.
//
// The key travels on both delivery hops — the route's Idempotency-Key
// header and the fallback job's envelope — so a backend that honours it
// cannot run the same operation twice when vago falls back after a
// request whose response was lost rather than whose work never ran.
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

// call sends the operation to its route and, if that fails, queues the
// same work as a background job. Sentry hears about it only when BOTH
// have failed — that is the point at which the operation is genuinely
// lost, and one incident should produce one alert, not one per hop.
//
// The fallback is API-first rather than enqueue-always because the happy
// path is virtually every call: queueing all of them would add four SQS
// messages per call for the update_conversation lifecycle alone.
//
// The deterministic idempotency key travels on both hops — as the
// Idempotency-Key header on the route, and inside the job envelope — so
// a backend that honours it runs the work at most once even when the
// route in fact succeeded and only its response was lost.
func (c *APIClient) call(ctx context.Context, operation, method, path string, body any, ic IdempotencyContext) error {
	start := time.Now()
	err := c.send(ctx, method, path, body, ic.IdempotencyKey)
	if err == nil {
		// TODO: remove this when merging PR
		apiLogf(c.logger, "delivered operation=%s %s %s duration_ms=%d idempotency_key=%s conversation=%s",
			operation, method, path, time.Since(start).Milliseconds(), ic.IdempotencyKey, ic.SentryTags["conversation_id"])
		return nil
	}

	fallbackErr := c.enqueueAPIFallback(operation, body, ic, err)
	if fallbackErr == nil {
		apiLogf(c.logger, "queued fallback job operation=%s %s %s duration_ms=%d idempotency_key=%s conversation=%s after: %v",
			operation, method, path, time.Since(start).Milliseconds(), ic.IdempotencyKey, ic.SentryTags["conversation_id"], err)
		return nil
	}

	apiLogf(c.logger, "NOT DELIVERED operation=%s %s %s duration_ms=%d idempotency_key=%s conversation=%s: %v",
		operation, method, path, time.Since(start).Milliseconds(), ic.IdempotencyKey, ic.SentryTags["conversation_id"], fallbackErr)
	c.reportUndelivered(operation, ic, fallbackErr)
	return fallbackErr
}

// enqueueAPIFallback queues the failed operation as a background job.
// It returns nil once the job is accepted, and otherwise the error that
// describes the ultimate failure — the one worth reporting.
func (c *APIClient) enqueueAPIFallback(operation string, body any, ic IdempotencyContext, cause error) error {
	funcName, ok := jobFallbackFunc[operation]
	if !ok {
		// Nothing to fall back to; the route error is already final.
		return cause
	}
	// A permanent 4xx will fail exactly the same way inside the worker,
	// so queueing it only moves the failure to the DLQ. Only a response
	// the backend may yet recover from — or no response at all — is
	// worth a second hop.
	if !retryableAPIError(cause) {
		return cause
	}

	kwargs, err := requestAsMap(body)
	if err != nil {
		return fmt.Errorf("disha: %s API failed (%v) and building fallback kwargs failed: %w", operation, cause, err)
	}

	job := EnqueueJobRequest{
		ModuleName:     fallbackJobModule,
		FuncName:       funcName,
		Kwargs:         kwargs,
		SQSQueue:       fallbackJobQueue,
		IdempotencyKey: ic.IdempotencyKey,
	}

	// This is the last hop the operation has, so a transient failure here
	// is worth another try before declaring the work lost. Attempts are
	// safe to repeat: the envelope carries the idempotency key, so a
	// request that in fact landed and only lost its response cannot run
	// the work twice on a backend that honours it.
	maxAttempts := len(apiFallbackRetryDelays) + 1
	var enqueueErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// The caller's context is normally already dead here — a timeout
		// is exactly when this path runs — so each attempt gets its own
		// budget.
		ctx, cancel := context.WithTimeout(context.Background(), defaultAPITimeout)
		enqueueErr = c.EnqueueJob(ctx, job)
		cancel()
		if enqueueErr == nil {
			return nil
		}
		if !retryableAPIError(enqueueErr) || attempt == maxAttempts {
			break
		}
		delay := fullJitter(apiFallbackRetryDelays[attempt-1])
		apiLogf(c.logger, "transient fallback enqueue failure operation=%s attempt=%d/%d retrying_in=%s idempotency_key=%s conversation=%s: %v",
			operation, attempt, maxAttempts, delay, ic.IdempotencyKey, ic.SentryTags["conversation_id"], enqueueErr)
		time.Sleep(delay)
	}
	return fmt.Errorf("disha: %s API failed (%v) and fallback enqueue failed after %d attempts: %w",
		operation, cause, maxAttempts, enqueueErr)
}

// retryableAPIError reports whether another identical attempt could
// plausibly succeed: a transport failure or timeout (no response at
// all), or a status the backend may yet recover from. A permanent 4xx
// would be rejected the same way every time.
func retryableAPIError(err error) bool {
	var status *APIStatusError
	if errors.As(err, &status) {
		return status.IsServerSide()
	}
	return true
}

// fullJitter picks a delay uniformly in [0, max], matching the S3
// uploader's backoff so two retrying callers cannot resynchronise.
func fullJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(max) + 1))
}

// requestAsMap turns a typed request into the job kwargs. The JSON tags
// already match the Python functions' parameter names, and those have
// strict signatures, so this must stay a plain round-trip: an extra key
// TypeErrors the job in the worker.
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

// reportUndelivered captures one event per operation per minute. It runs
// only after the route AND the fallback job have both failed, so every
// event here is real loss — but a Disha outage would otherwise produce
// one per operation per call, which is how VAGO-6 and VAGO-7 became
// unreadable.
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
// call sites into a single ungroupable issue. Reporting belongs to
// whoever runs out of ways to deliver the work.
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
