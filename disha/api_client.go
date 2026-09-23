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

	fallbackJobModule = "bots.operations.voice_bot_operations"
	fallbackJobQueue  = "p0-fast-l1"

	idempotencyHeader = "Idempotency-Key"
)

const (
	opUpdateConversation    = "update_conversation"
	opRunPostCallOperations = "run_post_call_operations"
	opSetUserCareplan       = "set_user_careplan"
	opAddTagToUser          = "add_tag_to_user"
)

var apiFallbackRetryDelays = []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}

var jobFallbackFunc = map[string]string{
	opUpdateConversation:    "update_conversation",
	opRunPostCallOperations: "run_post_call_operations",
	opSetUserCareplan:       "set_user_careplan",
	opAddTagToUser:          "add_tag_to_user",
}

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
	err := c.send(ctx, method, path, body, ic.IdempotencyKey)
	if err == nil {
		return nil
	}

	fallbackErr := c.enqueueAPIFallback(operation, body, ic, err)
	if fallbackErr == nil {
		if c.logger != nil {
			c.logger.Printf("disha: api queued fallback job operation=%s conversation=%s after: %v\n",
				operation, ic.SentryTags["conversation_id"], err)
		}
		return nil
	}

	if c.logger != nil {
		c.logger.Printf("disha: api NOT DELIVERED operation=%s conversation=%s: %v\n",
			operation, ic.SentryTags["conversation_id"], fallbackErr)
	}
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
	//
	// The caller's context is normally already dead here — a timeout is
	// exactly when this path runs — so the whole loop runs on a fresh
	// budget of its own rather than inheriting it. That budget is the
	// only thing bounding this hop, so the backoff waits on it too: a
	// bare sleep would let the last wait run past the deadline and
	// nothing could cut it.
	maxAttempts := len(apiFallbackRetryDelays) + 1
	budget, cancelBudget := context.WithTimeout(context.Background(), fallbackBudget())
	defer cancelBudget()

	var enqueueErr error
	attempts := 0
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attempts = attempt
		ctx, cancel := context.WithTimeout(budget, defaultAPITimeout)
		enqueueErr = c.EnqueueJob(ctx, job)
		cancel()
		if enqueueErr == nil {
			return nil
		}
		if !retryableAPIError(enqueueErr) || attempt == maxAttempts {
			break
		}
		delay := fullJitter(apiFallbackRetryDelays[attempt-1])
		if c.logger != nil {
			c.logger.Printf("disha: api transient fallback enqueue failure operation=%s attempt=%d/%d retrying_in=%s conversation=%s: %v\n",
				operation, attempt, maxAttempts, delay, ic.SentryTags["conversation_id"], enqueueErr)
		}
		if !sleepCtx(budget, delay) {
			break
		}
	}
	return fmt.Errorf("disha: %s API failed (%v) and fallback enqueue failed after %d attempts: %w",
		operation, cause, attempts, enqueueErr)
}

// fallbackBudget is what the whole fallback hop is allowed to take:
// every attempt's request timeout plus every backoff wait. Derived from
// the retry plan rather than hardcoded so shortening the delays in a
// test shortens the budget with them.
func fallbackBudget() time.Duration {
	budget := time.Duration(len(apiFallbackRetryDelays)+1) * defaultAPITimeout
	for _, delay := range apiFallbackRetryDelays {
		budget += delay
	}
	return budget
}

// sleepCtx waits for d and reports whether it completed. It returns
// false as soon as ctx is done, so a backoff wait cannot outlive the
// budget it is backing off inside.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
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
	// The call identity rides the hub as scope tags, so it is searchable;
	// Details carries only what is not a tag.
	captureSentry(sentryutil.Event{
		Hub: sentryutil.NewTaskHub(ic.SentryTags),
		Err: cause,
		Tags: map[string]string{
			"component": "disha_api",
			"operation": operation,
			"reason":    "not_delivered",
		},
		Details: map[string]any{"idempotency_key": ic.IdempotencyKey},
	})
}

// send performs exactly one HTTP attempt.
// It deliberately does NOT capture to Sentry. It used to, which is what
// made VAGO-6 and VAGO-7 fire on every transient blip — including ones
// the fallback then recovered from — and collapsed nine unrelated job
// call sites into a single ungroupable issue. Reporting belongs to
// whoever runs out of ways to deliver the work.
func (c *APIClient) send(ctx context.Context, method, path string, body any, idempotencyKey string) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("disha: marshal %s %s request: %w", method, path, err)
	}

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
