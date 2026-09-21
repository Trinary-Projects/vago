package disha

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// The outbox is Disha's durability layer for operations that must not be
// dropped when the Disha API is unreachable. It deliberately lives in
// disha/ rather than voicepipelinecore/: it knows Disha job names, HTTP
// paths and conversation IDs, all of which core must stay free of.
//
// Shape: an item is persisted BEFORE the HTTP attempt and deleted after
// it succeeds. A pod that dies mid-attempt therefore loses nothing — the
// item is already durable and another pod's drainer picks it up when the
// lease expires. The cost of that ordering is that an operation can run
// twice (attempt succeeded, delete did not), which is exactly what the
// disha-backend envelope idempotency key absorbs.
const (
	// outboxMaxAttempts and outboxBackoffSchedule together give a ~68
	// minute budget before an item is parked and reported once.
	outboxMaxAttempts = 8

	// outboxItemTTL is a leak backstop only. The drainer deletes items
	// on success and parks them on exhaustion; this TTL just guarantees
	// nothing survives forever if both paths are somehow missed.
	outboxItemTTL = 48 * time.Hour

	// outboxDeadListCap bounds the parked-item list so a sustained
	// outage cannot grow it without limit.
	outboxDeadListCap = 1000
)

// outboxBackoffSchedule is the delay before attempt N+1, indexed by the
// number of attempts already made. Mirrors the S3 retry policy's
// exponential full-jitter shape (AGENTS.md, 2026-07-24).

// TODO: confirm the number of retries once
var outboxBackoffSchedule = []time.Duration{
	5 * time.Second,
	15 * time.Second,
	45 * time.Second,
}

// idempotencyKeyPrefix marks a key as vago-origin when eyeballing Redis.
// Everything after it is a fixed-width digest, so every key this package
// produces is exactly len(prefix)+32 characters regardless of how long
// the inputs were or what characters they contained (the stage-threshold
// tag name, for instance, has spaces in it).
const (
	idempotencyKeyPrefix = "vago:"
	idempotencyKeyDigest = 32 // hex chars; 128 bits, collision risk negligible
)

// idempotencyKey builds the deterministic dedupe key for one logical
// operation.
//
// Deterministic, not random: the whole mechanism is that a retry
// presents the SAME key so SETNX rejects it the second time. It also has
// to match across independent producers — Disha's own room_finished
// recovery path derives post-call work from its own state, not from a
// vago outbox item, and only a recomputable key lets those two collide
// on purpose.
//
// Parts are separated by a NUL byte so that ("ab", "c") and ("a", "bc")
// cannot hash to the same key.
func idempotencyKey(operation string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(operation))
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	return idempotencyKeyPrefix + hex.EncodeToString(h.Sum(nil))[:idempotencyKeyDigest]
}

type OutboxKind string

const (
	OutboxKindAPI OutboxKind = "api"
	OutboxKindJob OutboxKind = "job"
)

// OutboxItem is the persisted unit of work. Payload holds the request
// body verbatim so a replay sends byte-identical bytes to the original
// attempt.
type OutboxItem struct {
	ID        string     `json:"id"`
	Kind      OutboxKind `json:"kind"`
	Operation string     `json:"operation"`

	// Kind == OutboxKindAPI
	Method string `json:"method,omitempty"`
	Path   string `json:"path,omitempty"`

	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`

	Attempts      int       `json:"attempts"`
	FirstFailedAt time.Time `json:"first_failed_at,omitempty"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
	LastError     string    `json:"last_error,omitempty"`

	// SentryTags carries the call identity (conversation_id, user_id,
	// bot_type) so the drainer — which runs outside any PipelineTask and
	// therefore has no task hub — can rebuild one via
	// sentryutil.NewTaskHub when it finally gives up.
	SentryTags map[string]string `json:"sentry_tags,omitempty"`
}

// OutboxRecord is one claimed item: its id plus the raw JSON to decode.
type OutboxRecord struct {
	ID      string
	Payload []byte
}

// APIStatusError is returned for a non-2xx Disha API response. It keeps
// the status code so the drainer can tell a retryable 503 from a
// permanent 422 rather than burning the whole budget on a bad request.
type APIStatusError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *APIStatusError) Error() string {
	return fmt.Sprintf("disha: API %s %s returned %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// outboxRetryable reports whether another attempt could plausibly
// succeed. Transport failures (timeouts, resets — the VAGO-6/VAGO-7
// population) always retry. HTTP mirrors the S3 policy: 408/429/5xx
// retry, every other 4xx is a permanent client error.
func outboxRetryable(err error) bool {
	if err == nil {
		return false
	}
	var statusErr *APIStatusError
	if errors.As(err, &statusErr) {
		switch {
		case statusErr.Status == http.StatusRequestTimeout,
			statusErr.Status == http.StatusTooManyRequests,
			statusErr.Status >= 500:
			return true
		default:
			return false
		}
	}
	return true
}

// outboxBackoff returns the delay before the next attempt, with full
// jitter so pods that failed together do not retry together.
func outboxBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	idx := attempts - 1
	if idx >= len(outboxBackoffSchedule) {
		idx = len(outboxBackoffSchedule) - 1
	}
	base := outboxBackoffSchedule[idx]
	return time.Duration(rand.Int64N(int64(base))) + base/2
}

// Outbox owns persistence. It is a thin policy layer over the Redis
// primitives so the drainer and the API client share one notion of
// "queued", "done" and "parked".
type Outbox struct {
	store   RedisClient
	logger  *log.Logger
	enabled bool
}

func NewOutbox(store RedisClient, logger *log.Logger, enabled bool) *Outbox {
	return &Outbox{store: store, logger: logger, enabled: enabled}
}

// Enabled reports whether operations should be persisted. When false the
// callers fall back to exactly the pre-outbox behaviour (one attempt,
// log on failure), which is the rollback path for VAGO_OUTBOX_ENABLED=0.
func (o *Outbox) Enabled() bool {
	return o != nil && o.enabled && o.store != nil
}

// Enqueue persists item and schedules it for immediate pickup, returning
// the assigned id.
func (o *Outbox) Enqueue(ctx context.Context, item *OutboxItem) error {
	if !o.Enabled() {
		return errors.New("disha: outbox disabled")
	}
	if item.ID == "" {
		item.ID = uuid.NewString()
	}
	if item.NextAttemptAt.IsZero() {
		item.NextAttemptAt = time.Now()
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("disha: marshal outbox item: %w", err)
	}
	if err := o.store.EnqueueOutboxItem(ctx, item.ID, payload, item.NextAttemptAt); err != nil {
		return err
	}
	// Log the assigned id so a call's persisted work can be found in
	// Redis (GET vago_outbox:item:{id}) from the app log alone.
	// TODO: remove this when merging PR
	if o.logger != nil {
		o.logger.Printf("disha: outbox enqueued operation=%s id=%s redis_key=%s idempotency_key=%s conversation=%s\n",
			item.Operation, item.ID, outboxItemKey(item.ID), item.IdempotencyKey, item.SentryTags["conversation_id"])
	}
	return nil
}

// Complete removes a finished item.
func (o *Outbox) Complete(ctx context.Context, id string) error {
	if !o.Enabled() {
		return nil
	}
	return o.store.DeleteOutboxItem(ctx, id)
}

// Retry records the failure and reschedules the item for its next
// attempt.
func (o *Outbox) Retry(ctx context.Context, item *OutboxItem, cause error) error {
	if !o.Enabled() {
		return nil
	}
	if item.FirstFailedAt.IsZero() {
		item.FirstFailedAt = time.Now()
	}
	if cause != nil {
		item.LastError = truncateOutboxError(cause.Error())
	}
	item.NextAttemptAt = time.Now().Add(outboxBackoff(item.Attempts))
	payload, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("disha: marshal outbox item: %w", err)
	}
	return o.store.RescheduleOutboxItem(ctx, item.ID, payload, item.NextAttemptAt)
}

// Park moves an exhausted or permanently-failed item to the dead list so
// it survives for manual inspection instead of vanishing.
func (o *Outbox) Park(ctx context.Context, item *OutboxItem, cause error) error {
	if !o.Enabled() {
		return nil
	}
	if cause != nil {
		item.LastError = truncateOutboxError(cause.Error())
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("disha: marshal outbox item: %w", err)
	}
	return o.store.ParkOutboxItem(ctx, item.ID, payload)
}

func truncateOutboxError(s string) string {
	const max = 512
	if len(s) <= max {
		return s
	}
	return s[:max]
}
