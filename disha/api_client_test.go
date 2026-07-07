package disha

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type capturedAPIRequest struct {
	Method        string
	Path          string
	ContentType   string
	Authorization string
	Body          map[string]any
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
			Method:        r.Method,
			Path:          r.URL.Path,
			ContentType:   r.Header.Get("Content-Type"),
			Authorization: r.Header.Get("Authorization"),
			Body:          body,
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
	})
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
	})
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

func TestAPIClientUpdateConversationFallbackQueuesJob(t *testing.T) {
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
		requests <- capturedAPIRequest{Method: r.Method, Path: r.URL.Path, Body: body}
		if r.URL.Path == "/bot/update_conversation" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("db down"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)

	client := NewAPIClient(server.URL, 10*time.Second, nil)
	at := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)
	err := client.UpdateConversationWithFallback(context.Background(), UpdateConversationRequest{
		ConversationID: "conv-1",
		BotJoinedAt:    &at,
	})
	if err != nil {
		t.Fatalf("UpdateConversationWithFallback: %v", err)
	}

	first := <-requests
	if first.Method != http.MethodPatch || first.Path != "/bot/update_conversation" {
		t.Fatalf("first request = %s %s, want PATCH /bot/update_conversation", first.Method, first.Path)
	}
	second := <-requests
	if second.Method != http.MethodPost || second.Path != "/common/enqueue_job" {
		t.Fatalf("fallback request = %s %s, want POST /common/enqueue_job", second.Method, second.Path)
	}
	kwargs, ok := second.Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", second.Body["kwargs"])
	}
	if second.Body["module_name"] != "bots.operations.voice_bot_operations" ||
		second.Body["func_name"] != "update_conversation" ||
		second.Body["sqs_queue"] != "p0-fast-l1" ||
		kwargs["conversation_id"] != "conv-1" ||
		kwargs["bot_joined_at"] != at.Format(time.RFC3339) {
		t.Fatalf("fallback body mismatch: %+v", second.Body)
	}
	if _, ok := kwargs["user_joined_at"]; ok {
		t.Fatalf("fallback kwargs should omit nil fields: %+v", kwargs)
	}
}

func TestAPIClientRunPostCallFallbackQueuesJob(t *testing.T) {
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
		requests <- capturedAPIRequest{Method: r.Method, Path: r.URL.Path, Body: body}
		if r.URL.Path == "/bot/run_post_call_operations" {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("temporary outage"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)

	client := NewAPIClient(server.URL, 10*time.Second, nil)
	endedAt := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)
	err := client.RunPostCallOperationsWithFallback(context.Background(), PostCallOperationsRequest{
		ConversationID:     "conv-1",
		TotalUserDuration:  13,
		EndedAt:            endedAt,
		LogDataS3Key:       "debug_log_data/conv-1/log_data.json",
		OnboardingCallDone: false,
	})
	if err != nil {
		t.Fatalf("RunPostCallOperationsWithFallback: %v", err)
	}

	<-requests
	second := <-requests
	if second.Method != http.MethodPost || second.Path != "/common/enqueue_job" {
		t.Fatalf("fallback request = %s %s, want POST /common/enqueue_job", second.Method, second.Path)
	}
	kwargs, ok := second.Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", second.Body["kwargs"])
	}
	if second.Body["module_name"] != "bots.operations.voice_bot_operations" ||
		second.Body["func_name"] != "run_post_call_operations" ||
		second.Body["sqs_queue"] != "p0-fast-l1" ||
		kwargs["conversation_id"] != "conv-1" ||
		kwargs["end_reason"] != nil ||
		kwargs["ended_at"] != endedAt.Format(time.RFC3339) {
		t.Fatalf("fallback body mismatch: %+v", second.Body)
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

	err := client.UpdateConversation(context.Background(), UpdateConversationRequest{ConversationID: "conv-1"})
	if err == nil || !strings.Contains(err.Error(), "418") || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("error = %v, want status/body", err)
	}
}

func TestAPIClientContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	client := NewAPIClient(server.URL, 10*time.Second, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.UpdateConversation(ctx, UpdateConversationRequest{ConversationID: "conv-1"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestAPIClientSetUserCareplan(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	client := NewAPIClient(server.URL, 0, nil)

	detected := "hair_loss"
	err := client.SetUserCareplanWithFallback(context.Background(), SetUserCareplanRequest{
		UserID:             "user-1",
		OnboardingCarePlan: "general",
		DetectedCarePlan:   &detected,
	})
	if err != nil {
		t.Fatalf("SetUserCareplanWithFallback: %v", err)
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
	})
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

	err := client.AddTagToUserWithFallback(context.Background(), AddTagToUserRequest{
		UserID:  "user-1",
		TagName: "Stage Transition Failure",
	})
	if err != nil {
		t.Fatalf("AddTagToUserWithFallback: %v", err)
	}
	got := <-requests
	if got.Method != http.MethodPost || got.Path != "/bot/add_tag_to_user" {
		t.Fatalf("request = %s %s, want POST /bot/add_tag_to_user", got.Method, got.Path)
	}
	if got.Body["user_id"] != "user-1" || got.Body["tag_name"] != "Stage Transition Failure" {
		t.Fatalf("body mismatch: %+v", got.Body)
	}
}

func TestAPIClientCareplanAndTagFallbacksQueueJobs(t *testing.T) {
	requests := make(chan capturedAPIRequest, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &body)
		}
		requests <- capturedAPIRequest{Method: r.Method, Path: r.URL.Path, Body: body}
		if strings.HasPrefix(r.URL.Path, "/bot/") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)
	client := NewAPIClient(server.URL, 10*time.Second, nil)

	if err := client.SetUserCareplanWithFallback(context.Background(), SetUserCareplanRequest{
		UserID:             "user-1",
		OnboardingCarePlan: "general",
	}); err != nil {
		t.Fatalf("SetUserCareplanWithFallback: %v", err)
	}
	<-requests // failed API call
	fallback := <-requests
	if fallback.Path != "/common/enqueue_job" ||
		fallback.Body["module_name"] != "bots.operations.voice_bot_operations" ||
		fallback.Body["func_name"] != "set_user_careplan" ||
		fallback.Body["sqs_queue"] != "p0-fast-l1" {
		t.Fatalf("careplan fallback mismatch: %+v", fallback.Body)
	}
	kwargs, _ := fallback.Body["kwargs"].(map[string]any)
	if kwargs["user_id"] != "user-1" || kwargs["onboarding_care_plan"] != "general" {
		t.Fatalf("careplan fallback kwargs mismatch: %+v", kwargs)
	}

	if err := client.AddTagToUserWithFallback(context.Background(), AddTagToUserRequest{
		UserID:  "user-1",
		TagName: "Stage Transition Failure",
	}); err != nil {
		t.Fatalf("AddTagToUserWithFallback: %v", err)
	}
	<-requests // failed API call
	fallback = <-requests
	if fallback.Body["func_name"] != "add_tag_to_user" || fallback.Body["sqs_queue"] != "p0-fast-l1" {
		t.Fatalf("tag fallback mismatch: %+v", fallback.Body)
	}
	kwargs, _ = fallback.Body["kwargs"].(map[string]any)
	if kwargs["tag_name"] != "Stage Transition Failure" {
		t.Fatalf("tag fallback kwargs mismatch: %+v", kwargs)
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
