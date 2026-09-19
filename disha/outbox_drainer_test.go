package disha

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

type stubAttempter struct {
	mu       sync.Mutex
	calls    int
	failWith []error
}

func (s *stubAttempter) attemptOutbox(context.Context, *OutboxItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.failWith) == 0 {
		return nil
	}
	err := s.failWith[0]
	s.failWith = s.failWith[1:]
	return err
}

func (s *stubAttempter) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// captureOutboxSentry is a package var so a test can count exactly how
// many events a flow produces — the whole point of this change.
func recordSentryEvents(t *testing.T) *[]sentryutil.Event {
	t.Helper()
	var mu sync.Mutex
	events := []sentryutil.Event{}
	original := captureOutboxSentry
	captureOutboxSentry = func(e sentryutil.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	t.Cleanup(func() { captureOutboxSentry = original })
	return &events
}

func enqueueTestItem(t *testing.T, outbox *Outbox, op string) *OutboxItem {
	t.Helper()
	item := &OutboxItem{
		Kind:       OutboxKindAPI,
		Operation:  op,
		Method:     "POST",
		Path:       "/bot/run_post_call_operations",
		Payload:    json.RawMessage(`{"conversation_id":"conv-1"}`),
		SentryTags: map[string]string{"conversation_id": "conv-1", "user_id": "user-1"},
	}
	if err := outbox.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return item
}

// A transient failure must stay silent: the retry is the recovery, and
// reporting it is exactly what made VAGO-6 fire 2,434 times.
func TestDrainerRetriesTransientFailureWithoutReporting(t *testing.T) {
	events := recordSentryEvents(t)
	outbox, store := newTestOutbox(t)
	attempter := &stubAttempter{failWith: []error{errors.New("connection reset by peer")}}
	drainer := NewOutboxDrainer(outbox, attempter, nil)

	enqueueTestItem(t, outbox, opRunPostCallOperations)

	records, err := store.ClaimOutboxItems(context.Background(), time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	drainer.processRecord(context.Background(), records[0])

	if len(*events) != 0 {
		t.Fatalf("captured %d Sentry events on a retryable failure, want 0", len(*events))
	}
	// Still scheduled, just later.
	later, err := store.ClaimOutboxItems(context.Background(), time.Now().Add(time.Hour), time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(later) != 1 {
		t.Fatalf("item not rescheduled: claimed %d, want 1", len(later))
	}
}

func TestDrainerDeletesItemOnSuccess(t *testing.T) {
	events := recordSentryEvents(t)
	outbox, store := newTestOutbox(t)
	drainer := NewOutboxDrainer(outbox, &stubAttempter{}, nil)

	enqueueTestItem(t, outbox, opRunPostCallOperations)
	records, _ := store.ClaimOutboxItems(context.Background(), time.Now(), time.Minute, 10)
	drainer.processRecord(context.Background(), records[0])

	if len(*events) != 0 {
		t.Fatalf("captured %d Sentry events on success, want 0", len(*events))
	}
	remaining, _ := store.ClaimOutboxItems(context.Background(), time.Now().Add(time.Hour), time.Minute, 10)
	if len(remaining) != 0 {
		t.Fatalf("item still queued after success: %d", len(remaining))
	}
}

// The whole budget must produce exactly one event, carrying the call
// identity that the old disha_api captures lacked.
func TestDrainerReportsOnceAfterBudgetExhausted(t *testing.T) {
	events := recordSentryEvents(t)
	outbox, store := newTestOutbox(t)
	attempter := &stubAttempter{}
	for i := 0; i < outboxMaxAttempts+2; i++ {
		attempter.failWith = append(attempter.failWith, errors.New("connection reset by peer"))
	}
	drainer := NewOutboxDrainer(outbox, attempter, nil)

	enqueueTestItem(t, outbox, opRunPostCallOperations)

	at := time.Now()
	for i := 0; i < outboxMaxAttempts; i++ {
		records, err := store.ClaimOutboxItems(context.Background(), at, time.Minute, 10)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if len(records) == 0 {
			t.Fatalf("nothing due on attempt %d", i+1)
		}
		drainer.processRecord(context.Background(), records[0])
		at = at.Add(time.Hour)
	}

	if len(*events) != 1 {
		t.Fatalf("captured %d Sentry events, want exactly 1", len(*events))
	}
	ev := (*events)[0]
	if ev.Tags["component"] != "disha_outbox" {
		t.Fatalf("component tag = %q, want disha_outbox", ev.Tags["component"])
	}
	if ev.Tags["operation"] != opRunPostCallOperations {
		t.Fatalf("operation tag = %q, want %q", ev.Tags["operation"], opRunPostCallOperations)
	}
	if ev.Details["conversation_id"] != "conv-1" {
		t.Fatalf("details missing conversation_id: %+v", ev.Details)
	}
	if ev.Details["attempts"] != outboxMaxAttempts {
		t.Fatalf("attempts = %v, want %d", ev.Details["attempts"], outboxMaxAttempts)
	}
	if ev.Details["reason"] != "exhausted" {
		t.Fatalf("reason = %v, want exhausted", ev.Details["reason"])
	}
	if ev.Hub == nil {
		t.Fatal("give-up event has no hub; conversation tags would be lost")
	}

	remaining, _ := store.ClaimOutboxItems(context.Background(), at.Add(time.Hour), time.Minute, 10)
	if len(remaining) != 0 {
		t.Fatalf("exhausted item still scheduled: %d", len(remaining))
	}
}

// A 422 can never succeed, so burning eight attempts on it would only
// delay the alert.
func TestDrainerParksPermanentFailureImmediately(t *testing.T) {
	events := recordSentryEvents(t)
	outbox, store := newTestOutbox(t)
	attempter := &stubAttempter{failWith: []error{&APIStatusError{Method: "POST", Path: "/bot/x", Status: 422, Body: "bad"}}}
	drainer := NewOutboxDrainer(outbox, attempter, nil)

	enqueueTestItem(t, outbox, opUpdateConversation)
	records, _ := store.ClaimOutboxItems(context.Background(), time.Now(), time.Minute, 10)
	drainer.processRecord(context.Background(), records[0])

	if len(*events) != 1 {
		t.Fatalf("captured %d Sentry events, want 1", len(*events))
	}
	if (*events)[0].Details["reason"] != "permanent" {
		t.Fatalf("reason = %v, want permanent", (*events)[0].Details["reason"])
	}
	if attempter.callCount() != 1 {
		t.Fatalf("attempted %d times, want 1", attempter.callCount())
	}
}

func TestDrainerParksCorruptPayload(t *testing.T) {
	events := recordSentryEvents(t)
	outbox, _ := newTestOutbox(t)
	drainer := NewOutboxDrainer(outbox, &stubAttempter{}, nil)

	drainer.processRecord(context.Background(), OutboxRecord{ID: "bad-1", Payload: []byte("{not json")})

	if len(*events) != 1 {
		t.Fatalf("captured %d Sentry events, want 1", len(*events))
	}
	if (*events)[0].Details["reason"] != "corrupt_payload" {
		t.Fatalf("reason = %v, want corrupt_payload", (*events)[0].Details["reason"])
	}
}

func TestDrainerDrainOnceProcessesBatch(t *testing.T) {
	recordSentryEvents(t)
	outbox, _ := newTestOutbox(t)
	attempter := &stubAttempter{}
	drainer := NewOutboxDrainer(outbox, attempter, nil)

	for i := 0; i < 5; i++ {
		enqueueTestItem(t, outbox, opRunPostCallOperations)
	}
	drainer.drainOnce(context.Background())

	if got := attempter.callCount(); got != 5 {
		t.Fatalf("attempted %d items, want 5", got)
	}
}

func TestDrainerStartIsNoOpWhenDisabled(t *testing.T) {
	_, client := newRedisTestClient(t)
	drainer := NewOutboxDrainer(NewOutbox(client, nil, false), &stubAttempter{}, nil)
	drainer.Start(context.Background())
	drainer.Stop() // must not hang waiting on a goroutine that never started
}

// The governing rule: an event from component=disha_outbox always means
// the operation is dead. Anything still recoverable stays silent.
func TestOnlyTerminalFailuresAreReported(t *testing.T) {
	recoverable := []struct {
		name string
		run  func(t *testing.T, outbox *Outbox, store RedisClient, drainer *OutboxDrainer)
	}{
		{"successful first attempt", func(t *testing.T, o *Outbox, s RedisClient, d *OutboxDrainer) {
			enqueueTestItem(t, o, opRunPostCallOperations)
			recs, _ := s.ClaimOutboxItems(context.Background(), time.Now(), time.Minute, 10)
			d.processRecord(context.Background(), recs[0])
		}},
		{"retryable failure mid-budget", func(t *testing.T, o *Outbox, s RedisClient, d *OutboxDrainer) {
			enqueueTestItem(t, o, opRunPostCallOperations)
			recs, _ := s.ClaimOutboxItems(context.Background(), time.Now(), time.Minute, 10)
			d.processRecord(context.Background(), recs[0])
		}},
		{"nothing due", func(t *testing.T, o *Outbox, s RedisClient, d *OutboxDrainer) {
			d.drainOnce(context.Background())
		}},
	}

	for i, tc := range recoverable {
		t.Run(tc.name, func(t *testing.T) {
			events := recordSentryEvents(t)
			outbox, store := newTestOutbox(t)
			att := &stubAttempter{}
			if i == 1 {
				att.failWith = []error{errors.New("connection reset by peer")}
			}
			tc.run(t, outbox, store, NewOutboxDrainer(outbox, att, nil))
			if len(*events) != 0 {
				t.Fatalf("recoverable case reported %d events, want 0", len(*events))
			}
		})
	}
}

// Every terminal path must carry reason as a TAG, so exhausted /
// permanent / corrupt_payload group into separate Sentry issues and can
// be alerted on independently.
func TestTerminalReportsCarryReasonTag(t *testing.T) {
	t.Run("permanent", func(t *testing.T) {
		events := recordSentryEvents(t)
		outbox, store := newTestOutbox(t)
		att := &stubAttempter{failWith: []error{&APIStatusError{Status: 422}}}
		drainer := NewOutboxDrainer(outbox, att, nil)
		enqueueTestItem(t, outbox, opUpdateConversation)
		recs, _ := store.ClaimOutboxItems(context.Background(), time.Now(), time.Minute, 10)
		drainer.processRecord(context.Background(), recs[0])

		if len(*events) != 1 {
			t.Fatalf("got %d events, want 1", len(*events))
		}
		if got := (*events)[0].Tags["reason"]; got != "permanent" {
			t.Fatalf("reason tag = %q, want permanent", got)
		}
	})

	t.Run("corrupt_payload", func(t *testing.T) {
		events := recordSentryEvents(t)
		outbox, _ := newTestOutbox(t)
		drainer := NewOutboxDrainer(outbox, &stubAttempter{}, nil)
		drainer.processRecord(context.Background(), OutboxRecord{ID: "x", Payload: []byte("{bad")})

		if len(*events) != 1 {
			t.Fatalf("got %d events, want 1", len(*events))
		}
		if got := (*events)[0].Tags["reason"]; got != "corrupt_payload" {
			t.Fatalf("reason tag = %q, want corrupt_payload", got)
		}
	})
}
