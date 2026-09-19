package disha

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

func newTestOutbox(t *testing.T) (*Outbox, RedisClient) {
	t.Helper()
	_, client := newRedisTestClient(t)
	return NewOutbox(client, nil, true), client
}

func TestOutboxEnqueueThenClaimRoundTrips(t *testing.T) {
	outbox, store := newTestOutbox(t)
	ctx := context.Background()

	item := &OutboxItem{
		Kind:           OutboxKindAPI,
		Operation:      opRunPostCallOperations,
		Method:         http.MethodPost,
		Path:           "/bot/run_post_call_operations",
		Payload:        json.RawMessage(`{"conversation_id":"conv-1"}`),
		IdempotencyKey: "vago:postcall:conv-1",
		SentryTags:     map[string]string{"conversation_id": "conv-1"},
	}
	if err := outbox.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if item.ID == "" {
		t.Fatal("Enqueue did not assign an id")
	}

	records, err := store.ClaimOutboxItems(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimOutboxItems: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("claimed %d items, want 1", len(records))
	}

	var got OutboxItem
	if err := json.Unmarshal(records[0].Payload, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Operation != opRunPostCallOperations || got.IdempotencyKey != "vago:postcall:conv-1" {
		t.Fatalf("round-tripped item = %+v", got)
	}
	if got.SentryTags["conversation_id"] != "conv-1" {
		t.Fatalf("sentry tags lost: %+v", got.SentryTags)
	}
}

// A claim must be exclusive: a ZRANGEBYSCORE followed by a separate ZADD
// would let two pods pick up the same item and run it twice.
func TestOutboxClaimIsExclusiveAcrossPods(t *testing.T) {
	outbox, store := newTestOutbox(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		item := &OutboxItem{Operation: "op", Payload: json.RawMessage(`{}`)}
		if err := outbox.Enqueue(ctx, item); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	now := time.Now()
	first, err := store.ClaimOutboxItems(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("first claim got %d, want 3", len(first))
	}

	// A second pod claiming immediately must see nothing: the lease
	// pushed every item's score into the future.
	second, err := store.ClaimOutboxItems(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second claim got %d items, want 0 (lease not honoured)", len(second))
	}

	// Once the lease expires the item is due again, so a pod that died
	// mid-attempt cannot strand work.
	afterLease, err := store.ClaimOutboxItems(ctx, now.Add(2*time.Minute), time.Minute, 10)
	if err != nil {
		t.Fatalf("post-lease claim: %v", err)
	}
	if len(afterLease) != 3 {
		t.Fatalf("post-lease claim got %d, want 3", len(afterLease))
	}
}

func TestOutboxClaimSkipsItemsNotYetDue(t *testing.T) {
	outbox, store := newTestOutbox(t)
	ctx := context.Background()

	item := &OutboxItem{Operation: "op", Payload: json.RawMessage(`{}`), NextAttemptAt: time.Now().Add(time.Hour)}
	if err := outbox.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	records, err := store.ClaimOutboxItems(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimOutboxItems: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("claimed %d items, want 0", len(records))
	}
}

func TestOutboxCompleteRemovesItem(t *testing.T) {
	outbox, store := newTestOutbox(t)
	ctx := context.Background()

	item := &OutboxItem{Operation: "op", Payload: json.RawMessage(`{}`)}
	if err := outbox.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := outbox.Complete(ctx, item.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	records, err := store.ClaimOutboxItems(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimOutboxItems: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("claimed %d items after Complete, want 0", len(records))
	}
}

func TestOutboxRetryAdvancesSchedule(t *testing.T) {
	outbox, store := newTestOutbox(t)
	ctx := context.Background()

	item := &OutboxItem{Operation: "op", Payload: json.RawMessage(`{}`)}
	if err := outbox.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	item.Attempts = 1
	if err := outbox.Retry(ctx, item, errors.New("boom")); err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if item.FirstFailedAt.IsZero() {
		t.Fatal("Retry did not stamp FirstFailedAt")
	}
	if item.LastError != "boom" {
		t.Fatalf("LastError = %q, want boom", item.LastError)
	}
	if !item.NextAttemptAt.After(time.Now()) {
		t.Fatalf("NextAttemptAt = %s, want future", item.NextAttemptAt)
	}

	// Not yet due, so a drain right now must not pick it up.
	records, err := store.ClaimOutboxItems(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimOutboxItems: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("claimed %d items, want 0", len(records))
	}
}

func TestOutboxParkRemovesFromSchedule(t *testing.T) {
	outbox, store := newTestOutbox(t)
	ctx := context.Background()

	item := &OutboxItem{Operation: "op", Payload: json.RawMessage(`{}`)}
	if err := outbox.Enqueue(ctx, item); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := outbox.Park(ctx, item, errors.New("dead")); err != nil {
		t.Fatalf("Park: %v", err)
	}

	records, err := store.ClaimOutboxItems(ctx, time.Now().Add(time.Hour), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimOutboxItems: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("claimed %d parked items, want 0", len(records))
	}
}

func TestOutboxDisabledIsInert(t *testing.T) {
	_, client := newRedisTestClient(t)
	outbox := NewOutbox(client, nil, false)
	if outbox.Enabled() {
		t.Fatal("Enabled() = true for a disabled outbox")
	}
	if err := outbox.Enqueue(context.Background(), &OutboxItem{}); err == nil {
		t.Fatal("Enqueue on a disabled outbox should fail so callers fall back to a direct send")
	}
}

func TestOutboxRetryableClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"transport", errors.New("connection reset by peer"), true},
		{"500", &APIStatusError{Status: 500}, true},
		{"503", &APIStatusError{Status: 503}, true},
		{"429", &APIStatusError{Status: http.StatusTooManyRequests}, true},
		{"408", &APIStatusError{Status: http.StatusRequestTimeout}, true},
		{"422", &APIStatusError{Status: 422}, false},
		{"400", &APIStatusError{Status: 400}, false},
		{"404", &APIStatusError{Status: 404}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := outboxRetryable(tc.err); got != tc.want {
				t.Fatalf("outboxRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestOutboxBackoffGrowsAndIsBounded(t *testing.T) {
	var prev time.Duration
	for attempts := 1; attempts <= outboxMaxAttempts; attempts++ {
		got := outboxBackoff(attempts)
		if got <= 0 {
			t.Fatalf("attempt %d: backoff = %s, want > 0", attempts, got)
		}
		if attempts > 1 && got < prev/4 {
			t.Fatalf("attempt %d: backoff %s collapsed vs previous %s", attempts, got, prev)
		}
		prev = got
	}
	// Past the schedule the delay must not keep growing unbounded.
	if got := outboxBackoff(outboxMaxAttempts + 50); got > 2*outboxBackoffSchedule[len(outboxBackoffSchedule)-1] {
		t.Fatalf("backoff past schedule = %s, want capped", got)
	}
}

func TestIdempotencyKeyIsDeterministic(t *testing.T) {
	// The entire mechanism depends on a retry recomputing the same key.
	a := idempotencyKey(opRunPostCallOperations, "conv-1")
	b := idempotencyKey(opRunPostCallOperations, "conv-1")
	if a != b {
		t.Fatalf("same inputs produced different keys: %q vs %q", a, b)
	}
}

func TestIdempotencyKeyIsFixedLength(t *testing.T) {
	want := len(idempotencyKeyPrefix) + idempotencyKeyDigest
	cases := [][]string{
		{},
		{"conv-1"},
		{"conv-1", "bot_joined"},
		// The stage-threshold tag has spaces; the digest must absorb that.
		{"user-1", "Stage Transition Failure"},
		{"user-1", "a-very-long-care-plan-name-that-goes-on-and-on-and-on-forever"},
		{"", ""},
	}
	for _, parts := range cases {
		got := idempotencyKey(opAddTagToUser, parts...)
		if len(got) != want {
			t.Fatalf("idempotencyKey(%q) = %q (len %d), want len %d", parts, got, len(got), want)
		}
		for _, r := range got[len(idempotencyKeyPrefix):] {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
				t.Fatalf("key %q contains non-hex digest character %q", got, r)
			}
		}
	}
}

func TestIdempotencyKeyDistinguishesInputs(t *testing.T) {
	seen := map[string]string{}
	cases := map[string]string{
		"different conversation":   idempotencyKey(opRunPostCallOperations, "conv-2"),
		"same conversation":        idempotencyKey(opRunPostCallOperations, "conv-1"),
		"different operation":      idempotencyKey(opUpdateConversation, "conv-1"),
		"update bot_joined":        idempotencyKey(opUpdateConversation, "conv-1", "bot_joined"),
		"update user_first_speech": idempotencyKey(opUpdateConversation, "conv-1", "user_first_speech"),
		"chunk sync same conv":     idempotencyKey(opSyncConversationChunks, "conv-1"),
	}
	for name, key := range cases {
		if prev, ok := seen[key]; ok {
			t.Fatalf("%q and %q collided on key %q", name, prev, key)
		}
		seen[key] = name
	}
}

// A naive concatenation would hash ("ab","c") and ("a","bc") identically,
// letting two unrelated operations suppress each other.
func TestIdempotencyKeySeparatesParts(t *testing.T) {
	if idempotencyKey("op", "ab", "c") == idempotencyKey("op", "a", "bc") {
		t.Fatal("part boundaries are not encoded; ambiguous inputs collide")
	}
	if idempotencyKey("op", "a") == idempotencyKey("op", "a", "") {
		t.Fatal("a trailing empty part must still change the key")
	}
}
