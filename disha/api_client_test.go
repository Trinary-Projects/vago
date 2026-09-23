package disha

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type capturedAPIRequest struct {
	Method         string
	Path           string
	ContentType    string
	Authorization  string
	IdempotencyKey string
	Body           map[string]any
}

func captureAPIRequest(t *testing.T, status int) (*httptest.Server, <-chan capturedAPIRequest) {
	t.Helper()
	requests := make(chan capturedAPIRequest, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var body map[string]any
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("Unmarshal request: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		requests <- capturedAPIRequest{
			Method:         r.Method,
			Path:           r.URL.Path,
			ContentType:    r.Header.Get("Content-Type"),
			Authorization:  r.Header.Get("Authorization"),
			IdempotencyKey: r.Header.Get(idempotencyHeader),
			Body:           body,
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func TestAPIClientUpdateConversation(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL+"/", 0, nil)
	at := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)

	err := client.UpdateConversation(context.Background(), UpdateConversationRequest{
		ConversationID: "conv-1",
		BotJoinedAt:    &at,
	}, IdempotencyContext{})
	if err != nil {
		t.Fatalf("UpdateConversation: %v", err)
	}
	got := <-requests
	if got.Method != http.MethodPatch || got.Path != "/bot/update_conversation" {
		t.Fatalf("request = %s %s, want PATCH /bot/update_conversation", got.Method, got.Path)
	}
	if got.Authorization != "" {
		t.Fatalf("Authorization = %q, want absent", got.Authorization)
	}
	if got.ContentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got.ContentType)
	}
	if got.Body["conversation_id"] != "conv-1" || got.Body["bot_joined_at"] != at.Format(time.RFC3339) {
		t.Fatalf("body mismatch: %+v", got.Body)
	}
	if _, ok := got.Body["user_joined_at"]; ok {
		t.Fatalf("user_joined_at should be omitted when nil: %+v", got.Body)
	}
}

func TestAPIClientRunPostCallOperationsIncludesNulls(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, 10*time.Second, nil)
	endedAt := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)

	err := client.RunPostCallOperations(context.Background(), PostCallOperationsRequest{
		ConversationID:     "conv-1",
		TotalUserDuration:  13,
		EndedAt:            endedAt,
		LogDataS3Key:       "",
		OnboardingCallDone: false,
	}, IdempotencyContext{})
	if err != nil {
		t.Fatalf("RunPostCallOperations: %v", err)
	}
	got := <-requests
	if got.Method != http.MethodPost || got.Path != "/bot/run_post_call_operations" {
		t.Fatalf("request = %s %s, want POST /bot/run_post_call_operations", got.Method, got.Path)
	}
	if got.Authorization != "" {
		t.Fatalf("Authorization = %q, want absent", got.Authorization)
	}
	for _, key := range []string{
		"end_reason",
		"first_user_audio_frames_received_at",
		"diet_plan_intensity_level",
		"fitness_plan_intensity_level",
		"latest_onboarding_call_stage",
		"conversation_variables",
	} {
		value, ok := got.Body[key]
		if !ok || value != nil {
			t.Fatalf("%s = %v (present=%v), want explicit null", key, value, ok)
		}
	}
	if got.Body["total_user_duration"] != float64(13) || got.Body["ended_at"] != endedAt.Format(time.RFC3339) {
		t.Fatalf("body mismatch: %+v", got.Body)
	}
}

// A route failure queues the same work as a job. The caller sees success
// because the operation is still going to run.
func TestAPIClientFailureQueuesFallbackJob(t *testing.T) {
	resetSentryRateLimiter(t)
	events := recordSentryEvents(t)
	requests := make(chan capturedAPIRequest, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		requests <- capturedAPIRequest{Method: r.Method, Path: r.URL.Path, Body: body}
		if r.URL.Path == enqueueJobPath {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":true}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("db down"))
	}))
	t.Cleanup(server.Close)

	client := NewAPIClient(server.URL, 10*time.Second, nil)
	at := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)
	err := client.UpdateConversation(context.Background(), UpdateConversationRequest{
		ConversationID: "conv-1",
		BotJoinedAt:    &at,
	}, IdempotencyContext{IdempotencyKey: "vago:updateconv:conv-1:bot_joined"})
	if err != nil {
		t.Fatalf("a queued fallback must read as delivered, got %v", err)
	}

	first := <-requests
	if first.Method != http.MethodPatch || first.Path != "/bot/update_conversation" {
		t.Fatalf("request = %s %s, want PATCH /bot/update_conversation", first.Method, first.Path)
	}
	second := <-requests
	if second.Method != http.MethodPost || second.Path != enqueueJobPath {
		t.Fatalf("fallback = %s %s, want POST %s", second.Method, second.Path, enqueueJobPath)
	}
	if second.Body["module_name"] != fallbackJobModule || second.Body["func_name"] != opUpdateConversation {
		t.Fatalf("fallback job = %v.%v", second.Body["module_name"], second.Body["func_name"])
	}
	if second.Body["sqs_queue"] != fallbackJobQueue {
		t.Fatalf("fallback queue = %v, want %q", second.Body["sqs_queue"], fallbackJobQueue)
	}
	if second.Body["idempotency_key"] != "vago:updateconv:conv-1:bot_joined" {
		t.Fatalf("fallback lost the key: %v", second.Body["idempotency_key"])
	}
	// The kwargs must be exactly the route's body: bots.operations.
	// voice_bot_operations.update_conversation has a strict signature.
	kwargs, _ := second.Body["kwargs"].(map[string]any)
	if kwargs["conversation_id"] != "conv-1" || kwargs["bot_joined_at"] == nil {
		t.Fatalf("fallback kwargs = %+v", kwargs)
	}
	if len(*events) != 0 {
		t.Fatalf("a recovered failure reported %d events, want 0", len(*events))
	}
}

// Both hops failing is the ultimate failure, and reports exactly once —
// after the fallback has exhausted its retries, not on the first one.
func TestAPIClientQueuedFallbackFailureReportsOnce(t *testing.T) {
	resetSentryRateLimiter(t)
	shortenFallbackRetries(t)
	events := recordSentryEvents(t)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)

	client := NewAPIClient(server.URL, time.Second, nil)
	err := client.UpdateConversation(context.Background(),
		UpdateConversationRequest{ConversationID: "conv-1"},
		IdempotencyContext{SentryTags: map[string]string{"conversation_id": "conv-1"}})
	if err == nil {
		t.Fatal("expected the loss to surface once both hops failed")
	}
	wantPaths := 1 + len(apiFallbackRetryDelays) + 1
	if len(paths) != wantPaths {
		t.Fatalf("paths = %v, want the route then %d fallback attempts", paths, wantPaths-1)
	}
	for _, path := range paths[1:] {
		if path != enqueueJobPath {
			t.Fatalf("paths = %v, want every retry on %s", paths, enqueueJobPath)
		}
	}
	if len(*events) != 1 {
		t.Fatalf("captured %d events for one lost operation, want 1", len(*events))
	}
	if (*events)[0].Tags["operation"] != opUpdateConversation {
		t.Fatalf("operation tag = %q, want the operation, not the enqueue", (*events)[0].Tags["operation"])
	}
}

// A permanent 4xx would fail identically in the worker, so it is not
// worth a second hop — it reports straight away.
func TestAPIClientPermanentStatusSkipsFallback(t *testing.T) {
	resetSentryRateLimiter(t)
	events := recordSentryEvents(t)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	t.Cleanup(server.Close)

	client := NewAPIClient(server.URL, time.Second, nil)
	err := client.AddTagToUser(context.Background(),
		AddTagToUserRequest{UserID: "user-1", TagName: "Stage Transition Failure"},
		IdempotencyContext{})
	if err == nil {
		t.Fatal("expected the 422 to surface")
	}
	if len(paths) != 1 {
		t.Fatalf("paths = %v, want the route only", paths)
	}
	if len(*events) != 1 {
		t.Fatalf("captured %d events, want 1", len(*events))
	}
}

// ended_at must keep sub-second precision on the wire; run_post_call_
// operations is matched against this same value backend-side.
func TestAPIClientUpdateConversationSendsNanosecondEndedAt(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, 10*time.Second, nil)
	endedAt := time.Date(2026, 5, 22, 1, 9, 3, 123456789, time.UTC)

	err := client.UpdateConversation(context.Background(), UpdateConversationRequest{
		ConversationID: "conv-1",
		EndedAt:        &endedAt,
	}, IdempotencyContext{})
	if err != nil {
		t.Fatalf("UpdateConversation: %v", err)
	}
	got := <-requests
	if got.Body["ended_at"] != endedAt.Format(time.RFC3339Nano) {
		t.Fatalf("ended_at = %#v, want %s", got.Body["ended_at"], endedAt.Format(time.RFC3339Nano))
	}
	if _, ok := got.Body["user_joined_at"]; ok {
		t.Fatalf("nil fields should be omitted: %+v", got.Body)
	}
}

func TestAPIClientEnqueueJob(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, 10*time.Second, nil)

	err := client.EnqueueJob(context.Background(), EnqueueJobRequest{
		ModuleName: "services.conversation_chunk_manager",
		FuncName:   "sync_conversation_chunks_to_db",
		Kwargs: map[string]any{
			"user_id":         "user-1",
			"conversation_id": "conv-1",
			"bot_type":        "sales_call",
		},
		SQSQueue: "p1-fast-l1",
	})
	if err != nil {
		t.Fatalf("EnqueueJob: %v", err)
	}
	got := <-requests
	if got.Method != http.MethodPost || got.Path != "/common/enqueue_job" {
		t.Fatalf("request = %s %s, want POST /common/enqueue_job", got.Method, got.Path)
	}
	if got.Body["module_name"] != "services.conversation_chunk_manager" ||
		got.Body["func_name"] != "sync_conversation_chunks_to_db" ||
		got.Body["sqs_queue"] != "p1-fast-l1" {
		t.Fatalf("body mismatch: %+v", got.Body)
	}
}

func TestAPIClientNon2xxReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("nope"))
	}))
	t.Cleanup(server.Close)
	client := NewAPIClient(server.URL, 10*time.Second, nil)

	err := client.UpdateConversation(context.Background(), UpdateConversationRequest{ConversationID: "conv-1"}, IdempotencyContext{})
	if err == nil || !strings.Contains(err.Error(), "418") || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("error = %v, want status/body", err)
	}
}

// A dead caller context is the normal shape of this failure — the 10s
// budget expiring is a deadline, and a call ending mid-transition is a
// cancel. The work is still wanted either way, so the fallback runs on
// its own budget rather than inheriting the context that just died.
func TestAPIClientContextCancellationStillQueuesFallback(t *testing.T) {
	resetSentryRateLimiter(t)
	events := recordSentryEvents(t)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)
	client := NewAPIClient(server.URL, 10*time.Second, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.UpdateConversation(ctx, UpdateConversationRequest{ConversationID: "conv-1"}, IdempotencyContext{})
	if err != nil {
		t.Fatalf("a queued fallback must read as delivered, got %v", err)
	}
	if len(paths) != 1 || paths[0] != enqueueJobPath {
		t.Fatalf("paths = %v, want only the fallback (the route never left)", paths)
	}
	if len(*events) != 0 {
		t.Fatalf("captured %d events, want 0", len(*events))
	}
}

func TestAPIClientSetUserCareplan(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, 0, nil)

	detected := "hair_loss"
	err := client.SetUserCareplan(context.Background(), SetUserCareplanRequest{
		UserID:             "user-1",
		OnboardingCarePlan: "general",
		DetectedCarePlan:   &detected,
	}, IdempotencyContext{IdempotencyKey: "vago:careplan:user-1:general"})
	if err != nil {
		t.Fatalf("SetUserCareplan: %v", err)
	}
	got := <-requests
	if got.Method != http.MethodPost || got.Path != "/bot/set_user_careplan" {
		t.Fatalf("request = %s %s, want POST /bot/set_user_careplan", got.Method, got.Path)
	}
	if got.Body["user_id"] != "user-1" || got.Body["onboarding_care_plan"] != "general" || got.Body["detected_care_plan"] != "hair_loss" {
		t.Fatalf("body mismatch: %+v", got.Body)
	}
}

func TestAPIClientSetUserCareplanNullDetected(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, 0, nil)

	err := client.SetUserCareplan(context.Background(), SetUserCareplanRequest{
		UserID:             "user-1",
		OnboardingCarePlan: "general",
	}, IdempotencyContext{})
	if err != nil {
		t.Fatalf("SetUserCareplan: %v", err)
	}
	got := <-requests
	// Python sends detected_care_plan: null explicitly.
	if v, ok := got.Body["detected_care_plan"]; !ok || v != nil {
		t.Fatalf("detected_care_plan = %#v, want present null", v)
	}
}

func TestAPIClientAddTagToUser(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, 0, nil)

	err := client.AddTagToUser(context.Background(), AddTagToUserRequest{
		UserID:  "user-1",
		TagName: "Stage Transition Failure",
	}, IdempotencyContext{IdempotencyKey: "vago:tag:user-1:Stage Transition Failure"})
	if err != nil {
		t.Fatalf("AddTagToUser: %v", err)
	}
	got := <-requests
	if got.Method != http.MethodPost || got.Path != "/bot/add_tag_to_user" {
		t.Fatalf("request = %s %s, want POST /bot/add_tag_to_user", got.Method, got.Path)
	}
	if got.Body["user_id"] != "user-1" || got.Body["tag_name"] != "Stage Transition Failure" {
		t.Fatalf("body mismatch: %+v", got.Body)
	}
}

func TestNewAPIClientDefaults(t *testing.T) {
	client := NewAPIClient("  ", 0, nil)
	if client.baseURL != defaultAPIBaseURL {
		t.Fatalf("baseURL = %q, want %q", client.baseURL, defaultAPIBaseURL)
	}
	if client.httpClient.Timeout != defaultAPITimeout {
		t.Fatalf("timeout = %s, want %s", client.httpClient.Timeout, defaultAPITimeout)
	}
}

// shortenFallbackRetries keeps the retry shape but removes the wall
// time, so tests exercise the loop without sleeping for it.
func shortenFallbackRetries(t *testing.T) {
	t.Helper()
	original := apiFallbackRetryDelays
	apiFallbackRetryDelays = []time.Duration{0, 0}
	t.Cleanup(func() { apiFallbackRetryDelays = original })
}

// A transient failure on the fallback hop is not the ultimate failure:
// the retry takes the work, so nothing is lost and nothing is reported.
func TestAPIClientFallbackRetrySucceeds(t *testing.T) {
	resetSentryRateLimiter(t)
	shortenFallbackRetries(t)
	events := recordSentryEvents(t)
	var paths []string
	enqueueAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == enqueueJobPath {
			enqueueAttempts++
			if enqueueAttempts < 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":true}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	client := NewAPIClient(server.URL, time.Second, nil)
	err := client.UpdateConversation(context.Background(),
		UpdateConversationRequest{ConversationID: "conv-1"},
		IdempotencyContext{IdempotencyKey: "vago:updateconv:conv-1"})
	if err != nil {
		t.Fatalf("a retried fallback must read as delivered, got %v", err)
	}
	if enqueueAttempts != 2 {
		t.Fatalf("enqueue attempts = %d, want 2", enqueueAttempts)
	}
	if len(*events) != 0 {
		t.Fatalf("a recovered failure reported %d events, want 0", len(*events))
	}
}

// A permanent status on the fallback hop would be rejected identically
// every time, so it is not retried.
func TestAPIClientFallbackPermanentStatusIsNotRetried(t *testing.T) {
	resetSentryRateLimiter(t)
	shortenFallbackRetries(t)
	events := recordSentryEvents(t)
	enqueueAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == enqueueJobPath {
			enqueueAttempts++
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	client := NewAPIClient(server.URL, time.Second, nil)
	err := client.UpdateConversation(context.Background(),
		UpdateConversationRequest{ConversationID: "conv-1"},
		IdempotencyContext{
			IdempotencyKey: "vago:updateconv:conv-1",
			SentryTags:     map[string]string{"conversation_id": "conv-1"},
		})
	if err == nil {
		t.Fatal("expected the loss to surface")
	}
	if enqueueAttempts != 1 {
		t.Fatalf("enqueue attempts = %d, want 1", enqueueAttempts)
	}
	if len(*events) != 1 {
		t.Fatalf("captured %d events, want exactly 1", len(*events))
	}
	ev := (*events)[0]
	if ev.Details["idempotency_key"] != "vago:updateconv:conv-1" {
		t.Fatalf("report lost the key: %+v", ev.Details)
	}
	if got := hubTags(t, ev.Hub); got["conversation_id"] != "conv-1" {
		t.Fatalf("report lost its call identity: %+v", got)
	}
}

// The backoff wait is what the fallback budget has to be able to cut:
// a bare sleep would let the last wait run past the deadline with
// nothing able to stop it.
func TestSleepCtxCompletesAndCancels(t *testing.T) {
	if !sleepCtx(context.Background(), time.Millisecond) {
		t.Fatal("a wait that fits should complete")
	}

	done, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepCtx(done, time.Hour) {
		t.Fatal("an expired budget must cut the wait")
	}

	mid, cancelMid := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancelMid()
	}()
	start := time.Now()
	if sleepCtx(mid, time.Hour) {
		t.Fatal("a budget expiring mid-wait must cut it")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited %s after cancellation; the wait is not selecting on the budget", elapsed)
	}
}

// The budget has to cover the whole plan, or it would silently drop the
// retries it is supposed to be bounding.
func TestFallbackBudgetCoversEveryAttempt(t *testing.T) {
	want := time.Duration(len(apiFallbackRetryDelays)+1) * defaultAPITimeout
	for _, delay := range apiFallbackRetryDelays {
		want += delay
	}
	if got := fallbackBudget(); got != want {
		t.Fatalf("fallbackBudget() = %s, want %s", got, want)
	}
	if fallbackBudget() <= defaultAPITimeout {
		t.Fatal("budget must leave room for more than one attempt")
	}
}
