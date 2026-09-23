package disha

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// recordSentryEvents swaps the capture seam so a test can count exactly
// how many events a flow produces.
func recordSentryEvents(t *testing.T) *[]sentryutil.Event {
	t.Helper()
	var mu sync.Mutex
	events := []sentryutil.Event{}
	original := captureSentry
	captureSentry = func(e sentryutil.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	t.Cleanup(func() { captureSentry = original })
	return &events
}

// hubTags returns the scope tags a captured event would carry, so a test
// can assert the identity that makes a report searchable in Sentry.
func hubTags(t *testing.T, hub *sentry.Hub) map[string]string {
	t.Helper()
	if hub == nil {
		t.Fatal("no hub; the event would lose its call identity")
	}
	return hub.Scope().ApplyToEvent(sentry.NewEvent(), nil, nil).Tags
}

// resetSentryRateLimiter clears the per-subject window so one test's
// report cannot suppress the next one's.
func resetSentryRateLimiter(t *testing.T) {
	t.Helper()
	sentryReportMu.Lock()
	sentryReportLast = map[string]time.Time{}
	sentryReportMu.Unlock()
	t.Cleanup(func() {
		sentryReportMu.Lock()
		sentryReportLast = map[string]time.Time{}
		sentryReportMu.Unlock()
	})
}

// When neither the route nor the fallback job takes the work, that IS
// the loss and must be reported — with the conversation identity
// attached. The stub 503s both hops.
func TestAPIClientReportsUndeliveredOperation(t *testing.T) {
	resetSentryRateLimiter(t)
	shortenFallbackRetries(t)
	events := recordSentryEvents(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := NewAPIClient(server.URL, time.Second, nil)

	err := client.RunPostCallOperations(context.Background(),
		PostCallOperationsRequest{ConversationID: "conv-1"},
		IdempotencyContext{
			IdempotencyKey: "vago:postcall:conv-1",
			SentryTags:     map[string]string{"conversation_id": "conv-1", "user_id": "user-1"},
		})
	if err == nil {
		t.Fatal("expected the 503 to surface to the caller")
	}
	if len(*events) != 1 {
		t.Fatalf("captured %d events, want exactly 1", len(*events))
	}
	ev := (*events)[0]
	if ev.Tags["reason"] != "not_delivered" {
		t.Fatalf("reason tag = %q, want not_delivered", ev.Tags["reason"])
	}
	if ev.Tags["operation"] != opRunPostCallOperations {
		t.Fatalf("operation tag = %q", ev.Tags["operation"])
	}
	if ev.Details["idempotency_key"] != "vago:postcall:conv-1" {
		t.Fatalf("details missing the key: %+v", ev.Details)
	}
	if got := hubTags(t, ev.Hub); got["conversation_id"] != "conv-1" || got["user_id"] != "user-1" {
		t.Fatalf("report lost its call identity: %+v", got)
	}
}

// A Disha outage takes both hops down together and must not reproduce
// VAGO-7: one event per operation per minute, however many calls end
// during it.
func TestAPIClientRateLimitsUndeliveredReports(t *testing.T) {
	resetSentryRateLimiter(t)
	shortenFallbackRetries(t)
	events := recordSentryEvents(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	client := NewAPIClient(server.URL, time.Second, nil)

	for i := 0; i < 20; i++ {
		_ = client.RunPostCallOperations(context.Background(),
			PostCallOperationsRequest{ConversationID: "conv-1"}, IdempotencyContext{})
	}
	if len(*events) != 1 {
		t.Fatalf("captured %d events for one outage, want 1", len(*events))
	}
}

// A successful call is silent, and carries the key the backend dedupes on.
func TestAPIClientSuccessIsSilentAndCarriesTheKey(t *testing.T) {
	resetSentryRateLimiter(t)
	events := recordSentryEvents(t)
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, time.Second, nil)

	err := client.UpdateConversation(context.Background(),
		UpdateConversationRequest{ConversationID: "conv-1"},
		IdempotencyContext{IdempotencyKey: "vago:updateconv:conv-1:bot_joined"})
	if err != nil {
		t.Fatalf("UpdateConversation: %v", err)
	}
	if got := <-requests; got.IdempotencyKey != "vago:updateconv:conv-1:bot_joined" {
		t.Fatalf("Idempotency-Key header = %q", got.IdempotencyKey)
	}
	if len(*events) != 0 {
		t.Fatalf("a successful call reported %d events, want 0", len(*events))
	}
}

// Best-effort telemetry deliberately carries no key: a duplicate LLM log
// is harmless and dedupe would cost the backend a Redis round trip.
func TestAPIClientEnqueueJobSendsNoKey(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, time.Second, nil)

	if err := client.EnqueueJob(context.Background(), EnqueueJobRequest{
		ModuleName: "services.llm_logging_service",
		FuncName:   "log_llm_call_job",
		SQSQueue:   "llm-logs-parallel",
	}); err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	got := <-requests
	if got.IdempotencyKey != "" {
		t.Fatalf("Idempotency-Key header = %q, want empty", got.IdempotencyKey)
	}
	if _, ok := got.Body["idempotency_key"]; ok {
		t.Fatalf("envelope carried a key: %+v", got.Body)
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
		"careplan same conv":       idempotencyKey(opSetUserCareplan, "conv-1"),
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
