package disha

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// flakyBackend hangs for the first failUntil requests so the client's own
// timeout fires (a real "context deadline exceeded", the VAGO-6 shape),
// then serves normally. It records every body it received so a replay can
// be proven byte-identical to the original.
type flakyBackend struct {
	mu        sync.Mutex
	failUntil int
	hangFor   time.Duration
	bodies    [][]byte
	keys      []string
	paths     []string
}

func (f *flakyBackend) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)

		f.mu.Lock()
		n := len(f.bodies) + 1
		f.bodies = append(f.bodies, raw)
		f.keys = append(f.keys, r.Header.Get(idempotencyHeader))
		f.paths = append(f.paths, r.URL.Path)
		shouldHang := n <= f.failUntil
		hang := f.hangFor
		f.mu.Unlock()

		if shouldHang {
			time.Sleep(hang)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true,"message":"Success!"}`))
	}
}

func (f *flakyBackend) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bodies)
}

// The scenario asked for: the enqueue_job call times out twice, then the
// third attempt gets the original response through. Nothing was lost and
// NOTHING is reported, because at no point did the operation die.
func TestEnqueueJobRecoversAfterTwoTimeoutsWithoutSentry(t *testing.T) {
	events := recordSentryEvents(t)

	backend := &flakyBackend{failUntil: 2, hangFor: 400 * time.Millisecond}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	_, store := newRedisTestClient(t)
	outbox := NewOutbox(store, nil, true)
	// Client timeout well under the server's hang, so attempts 1 and 2
	// fail the way a real overloaded backend fails.
	client := NewAPIClient(server.URL, 100*time.Millisecond, nil)
	client.SetOutbox(outbox)
	drainer := NewOutboxDrainer(outbox, client, nil)

	req := EnqueueJobRequest{
		ModuleName: "services.conversation_chunk_manager",
		FuncName:   "sync_conversation_chunks_to_db",
		Kwargs:     map[string]any{"conversation_id": "conv-1", "user_id": "user-1", "bot_type": "sales_call"},
		SQSQueue:   "p1-fast-l1",
	}
	oc := OutboxContext{
		IdempotencyKey: idempotencyKey(opSyncConversationChunks, "conv-1"),
		SentryTags:     map[string]string{"conversation_id": "conv-1"},
	}

	// --- attempt 1: inline, times out, gets queued ---
	if err := client.EnqueueJobDurable(context.Background(), opSyncConversationChunks, req, oc); err != nil {
		t.Fatalf("attempt 1 should report success to the caller (queued), got %v", err)
	}
	if len(*events) != 0 {
		t.Fatalf("attempt 1 reported %d Sentry events, want 0", len(*events))
	}

	// --- attempt 2: drainer, times out, rescheduled ---
	at := time.Now().Add(time.Hour)
	recs, err := store.ClaimOutboxItems(context.Background(), at, time.Minute, 10)
	if err != nil || len(recs) != 1 {
		t.Fatalf("expected 1 queued item before attempt 2, got %d (err=%v)", len(recs), err)
	}
	drainer.processRecord(context.Background(), recs[0])
	if len(*events) != 0 {
		t.Fatalf("attempt 2 reported %d Sentry events, want 0", len(*events))
	}

	// --- attempt 3: drainer, backend healthy again, delivered ---
	at = at.Add(time.Hour)
	recs, err = store.ClaimOutboxItems(context.Background(), at, time.Minute, 10)
	if err != nil || len(recs) != 1 {
		t.Fatalf("expected 1 queued item before attempt 3, got %d (err=%v)", len(recs), err)
	}
	drainer.processRecord(context.Background(), recs[0])

	// The headline assertion.
	if len(*events) != 0 {
		t.Fatalf("a recovered operation reported %d Sentry events, want 0: %+v", len(*events), (*events))
	}

	if got := backend.calls(); got != 3 {
		t.Fatalf("backend saw %d calls, want 3", got)
	}
	remaining, _ := store.ClaimOutboxItems(context.Background(), at.Add(time.Hour), time.Minute, 10)
	if len(remaining) != 0 {
		t.Fatalf("%d items still queued after delivery, want 0", len(remaining))
	}

	// Every attempt must be a faithful replay: same path, same key, and a
	// byte-identical body, so the job the worker finally runs is the one
	// the call originally produced.
	for i := range backend.paths {
		if backend.paths[i] != enqueueJobPath {
			t.Fatalf("attempt %d hit %q, want %q", i+1, backend.paths[i], enqueueJobPath)
		}
		if backend.keys[i] != oc.IdempotencyKey {
			t.Fatalf("attempt %d key = %q, want %q", i+1, backend.keys[i], oc.IdempotencyKey)
		}
		if i > 0 && !bytes.Equal(backend.bodies[i], backend.bodies[0]) {
			t.Fatalf("attempt %d body differs from the original:\n%s\n%s", i+1, backend.bodies[0], backend.bodies[i])
		}
	}

	// And the envelope carries the dedupe key, so the two timed-out
	// attempts cannot cause a double-run if they landed server-side.
	var envelope map[string]any
	if err := json.Unmarshal(backend.bodies[0], &envelope); err != nil {
		t.Fatalf("Unmarshal envelope: %v", err)
	}
	if envelope["idempotency_key"] != oc.IdempotencyKey {
		t.Fatalf("envelope idempotency_key = %v, want %q", envelope["idempotency_key"], oc.IdempotencyKey)
	}
}

// The same recovery shape on the direct HTTP path.
func TestPostCallRecoversAfterTimeoutsWithoutSentry(t *testing.T) {
	events := recordSentryEvents(t)

	backend := &flakyBackend{failUntil: 2, hangFor: 400 * time.Millisecond}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	_, store := newRedisTestClient(t)
	outbox := NewOutbox(store, nil, true)
	client := NewAPIClient(server.URL, 100*time.Millisecond, nil)
	client.SetOutbox(outbox)
	drainer := NewOutboxDrainer(outbox, client, nil)

	oc := OutboxContext{
		IdempotencyKey: idempotencyKey(opRunPostCallOperations, "conv-1"),
		SentryTags:     map[string]string{"conversation_id": "conv-1", "user_id": "user-1"},
	}
	if err := client.RunPostCallOperationsDurable(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"}, oc); err != nil {
		t.Fatalf("caller saw an error for a queued operation: %v", err)
	}

	at := time.Now()
	for attempt := 2; attempt <= 3; attempt++ {
		at = at.Add(time.Hour)
		recs, err := store.ClaimOutboxItems(context.Background(), at, time.Minute, 10)
		if err != nil || len(recs) != 1 {
			t.Fatalf("attempt %d: expected 1 due item, got %d (err=%v)", attempt, len(recs), err)
		}
		drainer.processRecord(context.Background(), recs[0])
	}

	if len(*events) != 0 {
		t.Fatalf("recovered post-call reported %d Sentry events, want 0", len(*events))
	}
	if got := backend.calls(); got != 3 {
		t.Fatalf("backend saw %d calls, want 3", got)
	}
	if backend.paths[2] != "/bot/run_post_call_operations" {
		t.Fatalf("final attempt hit %q", backend.paths[2])
	}
}

// Confirms attempts 1 and 2 really failed on a TIMEOUT, not some other
// error — this is the exact VAGO-6 failure mode.
func TestTimeoutIsRecordedAsTheRetryCause(t *testing.T) {
	recordSentryEvents(t)

	backend := &flakyBackend{failUntil: 1, hangFor: 400 * time.Millisecond}
	server := httptest.NewServer(backend.handler())
	t.Cleanup(server.Close)

	_, store := newRedisTestClient(t)
	outbox := NewOutbox(store, nil, true)
	client := NewAPIClient(server.URL, 100*time.Millisecond, nil)
	client.SetOutbox(outbox)

	oc := OutboxContext{IdempotencyKey: idempotencyKey(opRunPostCallOperations, "conv-1")}
	if err := client.RunPostCallOperationsDurable(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"}, oc); err != nil {
		t.Fatalf("queued operation returned %v", err)
	}

	recs, _ := store.ClaimOutboxItems(context.Background(), time.Now().Add(time.Hour), time.Minute, 10)
	if len(recs) != 1 {
		t.Fatalf("expected the timed-out operation to be queued, got %d items", len(recs))
	}
	var item OutboxItem
	if err := json.Unmarshal(recs[0].Payload, &item); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !strings.Contains(item.LastError, "Client.Timeout") && !strings.Contains(item.LastError, "deadline exceeded") {
		t.Fatalf("LastError = %q, want a timeout", item.LastError)
	}
	if item.Attempts != 1 {
		t.Fatalf("Attempts = %d, want 1", item.Attempts)
	}
	if item.NextAttemptAt.Before(time.Now()) {
		t.Fatalf("NextAttemptAt = %s, want a future backoff", item.NextAttemptAt)
	}
}
