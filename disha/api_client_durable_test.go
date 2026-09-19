package disha

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type recordedRequest struct {
	Method         string
	Path           string
	IdempotencyKey string
	Body           map[string]any
}

func durableTestServer(t *testing.T, status int) (*httptest.Server, <-chan recordedRequest) {
	t.Helper()
	requests := make(chan recordedRequest, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
		requests <- recordedRequest{
			Method:         r.Method,
			Path:           r.URL.Path,
			IdempotencyKey: r.Header.Get(idempotencyHeader),
			Body:           body,
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func durableTestClient(t *testing.T, serverURL string) (*APIClient, RedisClient) {
	t.Helper()
	_, store := newRedisTestClient(t)
	client := NewAPIClient(serverURL, 2*time.Second, nil)
	client.SetOutbox(NewOutbox(store, nil, true))
	return client, store
}

// The single most important assertion in this change: the shared HTTP
// helper must not report. It used to, which is what produced VAGO-6 and
// VAGO-7 — including on failures a retry then recovered from.
func TestAPIClientSendDoesNotCaptureSentry(t *testing.T) {
	transport := bindMockSentry(t)
	server, _ := durableTestServer(t, http.StatusInternalServerError)
	client := NewAPIClient(server.URL, 2*time.Second, nil)

	if err := client.UpdateConversation(context.Background(), UpdateConversationRequest{ConversationID: "conv-1"}); err == nil {
		t.Fatal("expected an error from a 500 response")
	}
	if got := len(transport.Events()); got != 0 {
		t.Fatalf("send captured %d Sentry events, want 0", got)
	}
}

func TestAPIClientDurableQueuesTransientFailure(t *testing.T) {
	transport := bindMockSentry(t)
	server, requests := durableTestServer(t, http.StatusInternalServerError)
	client, store := durableTestClient(t, server.URL)

	err := client.RunPostCallOperationsDurable(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"},
		OutboxContext{
			IdempotencyKey: "vago:postcall:conv-1",
			SentryTags:     map[string]string{"conversation_id": "conv-1"},
		})
	// Durably queued is not a caller-visible failure.
	if err != nil {
		t.Fatalf("RunPostCallOperationsDurable returned %v, want nil (queued)", err)
	}
	if got := len(transport.Events()); got != 0 {
		t.Fatalf("captured %d Sentry events while queued, want 0", got)
	}

	<-requests // the inline attempt happened
	records, err := store.ClaimOutboxItems(context.Background(), time.Now().Add(time.Hour), time.Minute, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("queued %d items, want 1", len(records))
	}
	var item OutboxItem
	if err := json.Unmarshal(records[0].Payload, &item); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if item.Operation != opRunPostCallOperations {
		t.Fatalf("operation = %q", item.Operation)
	}
	if item.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", item.Attempts)
	}
	if item.SentryTags["conversation_id"] != "conv-1" {
		t.Fatalf("sentry tags not persisted: %+v", item.SentryTags)
	}
}

func TestAPIClientDurableLeavesNothingQueuedOnSuccess(t *testing.T) {
	server, requests := durableTestServer(t, http.StatusOK)
	client, store := durableTestClient(t, server.URL)

	err := client.RunPostCallOperationsDurable(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"},
		OutboxContext{IdempotencyKey: "vago:postcall:conv-1"})
	if err != nil {
		t.Fatalf("RunPostCallOperationsDurable: %v", err)
	}

	got := <-requests
	if got.IdempotencyKey != "vago:postcall:conv-1" {
		t.Fatalf("Idempotency-Key header = %q", got.IdempotencyKey)
	}
	records, _ := store.ClaimOutboxItems(context.Background(), time.Now().Add(time.Hour), time.Minute, 10)
	if len(records) != 0 {
		t.Fatalf("%d items left queued after success, want 0", len(records))
	}
}

// A permanent client error is surfaced immediately rather than hidden
// behind an hour of pointless retries.
func TestAPIClientDurableSurfacesPermanentFailure(t *testing.T) {
	server, _ := durableTestServer(t, http.StatusUnprocessableEntity)
	client, store := durableTestClient(t, server.URL)

	err := client.UpdateConversationDurable(context.Background(),
		UpdateConversationRequest{ConversationID: "conv-1"},
		OutboxContext{IdempotencyKey: "vago:updateconv:conv-1:bot_joined"})
	if err == nil {
		t.Fatal("expected a permanent failure to be returned")
	}
	var statusErr *APIStatusError
	if !asStatusError(err, &statusErr) || statusErr.Status != http.StatusUnprocessableEntity {
		t.Fatalf("err = %v, want an APIStatusError with 422", err)
	}
	records, _ := store.ClaimOutboxItems(context.Background(), time.Now().Add(time.Hour), time.Minute, 10)
	if len(records) != 0 {
		t.Fatalf("%d items queued for a permanent failure, want 0 (parked)", len(records))
	}
}

func TestAPIClientEnqueueJobDurableCarriesKeyInEnvelope(t *testing.T) {
	server, requests := durableTestServer(t, http.StatusOK)
	client, _ := durableTestClient(t, server.URL)

	err := client.EnqueueJobDurable(context.Background(), opSyncConversationChunks, EnqueueJobRequest{
		ModuleName: "services.conversation_chunk_manager",
		FuncName:   "sync_conversation_chunks_to_db",
		Kwargs:     map[string]any{"conversation_id": "conv-1"},
		SQSQueue:   "p1-fast-l1",
	}, OutboxContext{IdempotencyKey: "vago:chunksync:conv-1"})
	if err != nil {
		t.Fatalf("EnqueueJobDurable: %v", err)
	}

	got := <-requests
	if got.Path != enqueueJobPath {
		t.Fatalf("path = %q, want %q", got.Path, enqueueJobPath)
	}
	if got.Body["idempotency_key"] != "vago:chunksync:conv-1" {
		t.Fatalf("envelope idempotency_key = %v", got.Body["idempotency_key"])
	}
}

// Without an outbox the durable methods degrade to exactly the previous
// one-shot behaviour, which is the VAGO_OUTBOX_ENABLED=0 rollback path.
func TestAPIClientDurableDegradesWhenOutboxDisabled(t *testing.T) {
	server, _ := durableTestServer(t, http.StatusInternalServerError)
	client := NewAPIClient(server.URL, 2*time.Second, nil)

	err := client.RunPostCallOperationsDurable(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"},
		OutboxContext{IdempotencyKey: "vago:postcall:conv-1"})
	if err == nil {
		t.Fatal("expected the error to be returned when the outbox is disabled")
	}
}

func asStatusError(err error, target **APIStatusError) bool {
	for err != nil {
		if se, ok := err.(*APIStatusError); ok {
			*target = se
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

type brokenOutboxStore struct{ RedisClient }

func (brokenOutboxStore) EnqueueOutboxItem(context.Context, string, []byte, time.Time) error {
	return errors.New("redis down")
}

// Redis down + API down = the operation is gone with nothing queued.
// That is the one Redis-adjacent condition that is genuinely terminal,
// and it used to be completely silent.
func TestDurableReportsWhenNeitherOutboxNorSendSucceeds(t *testing.T) {
	events := recordSentryEvents(t)
	server, _ := durableTestServer(t, http.StatusInternalServerError)
	_, store := newRedisTestClient(t)

	client := NewAPIClient(server.URL, 2*time.Second, nil)
	client.SetOutbox(NewOutbox(brokenOutboxStore{RedisClient: store}, nil, true))

	err := client.RunPostCallOperationsDurable(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"},
		OutboxContext{
			IdempotencyKey: "vago:deadbeef",
			SentryTags:     map[string]string{"conversation_id": "conv-1"},
		})
	if err == nil {
		t.Fatal("expected the error to surface when nothing could be queued")
	}
	if len(*events) != 1 {
		t.Fatalf("got %d events, want exactly 1", len(*events))
	}
	ev := (*events)[0]
	if ev.Tags["reason"] != "not_durable" {
		t.Fatalf("reason tag = %q, want not_durable", ev.Tags["reason"])
	}
	if ev.Details["conversation_id"] != "conv-1" {
		t.Fatalf("details missing conversation_id: %+v", ev.Details)
	}
}

// Redis down but the API healthy is fully recoverable, so it must stay silent.
func TestDurableStaysSilentWhenOutboxFailsButSendSucceeds(t *testing.T) {
	events := recordSentryEvents(t)
	server, _ := durableTestServer(t, http.StatusOK)
	_, store := newRedisTestClient(t)

	client := NewAPIClient(server.URL, 2*time.Second, nil)
	client.SetOutbox(NewOutbox(brokenOutboxStore{RedisClient: store}, nil, true))

	err := client.RunPostCallOperationsDurable(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"},
		OutboxContext{IdempotencyKey: "vago:deadbeef"})
	if err != nil {
		t.Fatalf("inline send succeeded, want nil error, got %v", err)
	}
	if len(*events) != 0 {
		t.Fatalf("got %d events for a recovered operation, want 0", len(*events))
	}
}
