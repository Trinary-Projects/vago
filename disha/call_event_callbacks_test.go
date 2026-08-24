package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// onboardingPostCallCallbacks builds a CallEventCallbacks wired to a
// captureAPIRequest fake server, for exercising runPostCallOperations'
// onboarding-only fields directly.
func onboardingPostCallCallbacks(t *testing.T, userID string) (*CallEventCallbacks, <-chan capturedAPIRequest) {
	t.Helper()
	server, requests := captureAPIRequest(t, 200)
	api := NewAPIClient(server.URL, 10*time.Second, nil)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         userID,
		BotType:        OnboardingCallBotType,
	}, nil, api, nil)
	return callbacks, requests
}

// fakeJSONUploader is a JSONUploader test double that records every
// upload call (key + decoded payload) in order. If err is set, UploadJSON
// fails after recording the call so ordering assertions still see it.
type fakeJSONUploader struct {
	mu       sync.Mutex
	calls    []fakeUploadCall
	err      error
	onUpload func(objectKey string, value any)
}

type fakeUploadCall struct {
	objectKey string
	value     map[string]any
}

func (f *fakeJSONUploader) UploadJSON(_ context.Context, objectKey string, value any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	stateDict, _ := value.(map[string]any)
	f.calls = append(f.calls, fakeUploadCall{objectKey: objectKey, value: stateDict})
	if f.onUpload != nil {
		f.onUpload(objectKey, value)
	}
	return f.err
}

func (f *fakeJSONUploader) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func assertConversationStatePayloadShape(t *testing.T, payload map[string]any, userID, conversationID string) {
	t.Helper()
	wantKeys := []string{
		"variant", "agenda", "stage_turn_count", "variable_store",
		"care_plan_name", "stage_threshold_reminded", "stage_threshold_alerted",
		"user_id", "conversation_id",
	}
	if len(payload) != len(wantKeys) {
		t.Fatalf("payload keys = %+v, want exactly %v", payload, wantKeys)
	}
	for _, k := range wantKeys {
		if _, ok := payload[k]; !ok {
			t.Fatalf("payload missing key %q: %+v", k, payload)
		}
	}
	if payload["user_id"] != userID {
		t.Fatalf("payload user_id = %v, want %v", payload["user_id"], userID)
	}
	if payload["conversation_id"] != conversationID {
		t.Fatalf("payload conversation_id = %v, want %v", payload["conversation_id"], conversationID)
	}
}

// TestCallEventCallbacksConversationStateUploadOnCommittedTurns verifies
// user and assistant committed turns each carry a
// conversation_state_s3_key matching conversation_state/{conv}/{chunk_id}
// .json, and the uploaded payload has the exact 9-key Python shape. The
// decorator under test is the real onboarding one
// (newOnboardingChunkDecorator), wired through the generic
// SetChunkDecorator seam.
func TestCallEventCallbacksConversationStateUploadOnCommittedTurns(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        OnboardingCallBotType,
	}, redisClient, nil, nil)

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	uploader := &fakeJSONUploader{}
	callbacks.SetChunkDecorator(newOnboardingChunkDecorator(state, uploader, "user-1", "conv-1", nil))

	events := callbacks.Events()
	at := time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC)
	events.OnUserTurnCommitted("hello doctor", at, "")
	events.OnAssistantTurnCommitted("hi, let's begin", at.Add(time.Second), voicepipelinecore.TurnMetrics{}, "")

	chunkItems, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List chunks: %v", err)
	}
	if len(chunkItems) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunkItems))
	}
	if uploader.callCount() != 2 {
		t.Fatalf("upload call count = %d, want 2", uploader.callCount())
	}

	for i, raw := range chunkItems {
		var chunk ConversationChunk
		if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
			t.Fatalf("Unmarshal chunk %d: %v", i, err)
		}
		if chunk.ConversationStateS3Key == nil {
			t.Fatalf("chunk %d ConversationStateS3Key is nil", i)
		}
		wantKey := fmt.Sprintf("conversation_state/conv-1/%s.json", chunk.ID)
		if *chunk.ConversationStateS3Key != wantKey {
			t.Fatalf("chunk %d ConversationStateS3Key = %q, want %q", i, *chunk.ConversationStateS3Key, wantKey)
		}
		if chunk.CurrentAgenda == nil || *chunk.CurrentAgenda != "introduction" {
			t.Fatalf("chunk %d CurrentAgenda = %v, want introduction", i, chunk.CurrentAgenda)
		}

		call := uploader.calls[i]
		if call.objectKey != wantKey {
			t.Fatalf("upload %d key = %q, want %q", i, call.objectKey, wantKey)
		}
		assertConversationStatePayloadShape(t, call.value, "user-1", "conv-1")
	}
}

func TestCallEventCallbacksReplacesConsecutiveUserChunk(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        OnboardingCallBotType,
	}, redisClient, nil, nil)

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	uploader := &fakeJSONUploader{}
	callbacks.SetChunkDecorator(newOnboardingChunkDecorator(state, uploader, "user-1", "conv-1", nil))

	events := callbacks.Events()
	at := time.Date(2026, 8, 24, 16, 19, 46, 0, time.UTC)
	events.OnUserTurnCommitted("first", at, "prompt-v1")

	initialItems, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List initial chunks: %v", err)
	}
	if len(initialItems) != 1 {
		t.Fatalf("initial chunk count = %d, want 1", len(initialItems))
	}
	var initial ConversationChunk
	if err := json.Unmarshal([]byte(initialItems[0]), &initial); err != nil {
		t.Fatalf("Unmarshal initial chunk: %v", err)
	}

	state.AdvanceStage(cfg.ResolveStage("closing_and_assurance", nil))
	callbacks.AppendDebugLogChunk("stage changed", at.Add(time.Second), "", nil)
	events.OnUserTurnCommitted("first second", at.Add(2*time.Second), "prompt-v2")

	items, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List replaced chunks: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("chunk count after replacement = %d, want 2 (one user + one debug)", len(items))
	}
	var userChunk, debugChunk ConversationChunk
	if err := json.Unmarshal([]byte(items[0]), &userChunk); err != nil {
		t.Fatalf("Unmarshal user chunk: %v", err)
	}
	if err := json.Unmarshal([]byte(items[1]), &debugChunk); err != nil {
		t.Fatalf("Unmarshal debug chunk: %v", err)
	}
	if userChunk.ID != initial.ID || userChunk.Created != initial.Created {
		t.Fatalf("replacement identity changed: initial=%+v replaced=%+v", initial, userChunk)
	}
	if userChunk.Text != "first second" {
		t.Fatalf("replacement text = %q, want combined text", userChunk.Text)
	}
	if userChunk.MainAgentSystemPromptLangfuseKey == nil || *userChunk.MainAgentSystemPromptLangfuseKey != "prompt-v2" {
		t.Fatalf("replacement prompt key = %v, want prompt-v2", userChunk.MainAgentSystemPromptLangfuseKey)
	}
	if userChunk.CurrentAgenda == nil || *userChunk.CurrentAgenda != "closing_and_assurance" {
		t.Fatalf("replacement agenda = %v, want closing_and_assurance", userChunk.CurrentAgenda)
	}
	if !debugChunk.IsDebugLog || debugChunk.Text != "stage changed" {
		t.Fatalf("debug chunk changed: %+v", debugChunk)
	}
	if uploader.callCount() != 3 {
		t.Fatalf("upload count = %d, want initial user + debug + replacement", uploader.callCount())
	}
	wantStateKey := fmt.Sprintf("conversation_state/conv-1/%s.json", initial.ID)
	if uploader.calls[0].objectKey != wantStateKey || uploader.calls[2].objectKey != wantStateKey {
		t.Fatalf("replacement state uploads = %q and %q, want same key %q", uploader.calls[0].objectKey, uploader.calls[2].objectKey, wantStateKey)
	}
	if uploader.calls[2].value["agenda"] != "closing_and_assurance" {
		t.Fatalf("replacement state agenda = %v, want closing_and_assurance", uploader.calls[2].value["agenda"])
	}

	events.OnAssistantTurnCommitted("assistant boundary", at.Add(3*time.Second), voicepipelinecore.TurnMetrics{}, "prompt-v2")
	events.OnUserTurnCommitted("third", at.Add(4*time.Second), "prompt-v2")
	items, err = redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List chunks after assistant boundary: %v", err)
	}
	if len(items) != 4 {
		t.Fatalf("chunk count after assistant boundary = %d, want 4", len(items))
	}
	var finalUser ConversationChunk
	if err := json.Unmarshal([]byte(items[3]), &finalUser); err != nil {
		t.Fatalf("Unmarshal final user chunk: %v", err)
	}
	if finalUser.ID == initial.ID || finalUser.Text != "third" {
		t.Fatalf("post-assistant user chunk = %+v, want separate third turn", finalUser)
	}
}

// TestCallEventCallbacksConversationStateUploadEveryChunkRole verifies
// debug-log chunks (AppendDebugLogChunk) and tool-context chunks
// (OnToolResultCommitted) also get a conversation_state_s3_key — every
// chunk role gets the upload, matching Python's
// ConversationPersistenceProcessor.
func TestCallEventCallbacksConversationStateUploadEveryChunkRole(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        OnboardingCallBotType,
	}, redisClient, nil, nil)

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	uploader := &fakeJSONUploader{}
	callbacks.SetChunkDecorator(newOnboardingChunkDecorator(state, uploader, "user-1", "conv-1", nil))

	events := callbacks.Events()
	at := time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC)

	assistantToolCall := voicepipelinecore.Message{
		Role: "assistant",
		ToolCalls: []voicepipelinecore.ToolCall{{
			ID:   "call_1",
			Type: "function",
			Function: voicepipelinecore.ToolCallFunction{
				Name:      "end_call",
				Arguments: `{}`,
			},
		}},
	}
	toolResult := voicepipelinecore.Message{Role: "tool", Content: "ok", ToolCallID: "call_1"}
	events.OnToolResultCommitted(assistantToolCall, toolResult, at)
	callbacks.AppendDebugLogChunk("stage transition", at.Add(time.Second), "", nil)

	chunkItems, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List chunks: %v", err)
	}
	if len(chunkItems) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(chunkItems))
	}
	if uploader.callCount() != 3 {
		t.Fatalf("upload call count = %d, want 3", uploader.callCount())
	}
	for i, raw := range chunkItems {
		var chunk ConversationChunk
		if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
			t.Fatalf("Unmarshal chunk %d: %v", i, err)
		}
		if chunk.ConversationStateS3Key == nil {
			t.Fatalf("chunk %d (role=%s, is_debug_log=%v) ConversationStateS3Key is nil", i, chunk.Role, chunk.IsDebugLog)
		}
		wantKey := fmt.Sprintf("conversation_state/conv-1/%s.json", chunk.ID)
		if *chunk.ConversationStateS3Key != wantKey {
			t.Fatalf("chunk %d ConversationStateS3Key = %q, want %q", i, *chunk.ConversationStateS3Key, wantKey)
		}
	}
}

// TestCallEventCallbacksConversationStateUploadErrorStillWritesChunk
// verifies an S3 upload failure never blocks the chunk write: the chunk
// still lands in Redis with conversation_state_s3_key explicitly null.
func TestCallEventCallbacksConversationStateUploadErrorStillWritesChunk(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        OnboardingCallBotType,
	}, redisClient, nil, nil)

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	uploader := &fakeJSONUploader{err: errors.New("s3 unavailable")}
	callbacks.SetChunkDecorator(newOnboardingChunkDecorator(state, uploader, "user-1", "conv-1", nil))

	events := callbacks.Events()
	at := time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC)
	events.OnUserTurnCommitted("hello doctor", at, "")

	chunkItems, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List chunks: %v", err)
	}
	if len(chunkItems) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(chunkItems))
	}
	if uploader.callCount() != 1 {
		t.Fatalf("upload call count = %d, want 1", uploader.callCount())
	}

	raw := chunkItems[0]
	if !containsNullConversationStateKey(raw) {
		t.Fatalf("raw chunk JSON does not have null conversation_state_s3_key: %s", raw)
	}
	var chunk ConversationChunk
	if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
		t.Fatalf("Unmarshal chunk: %v", err)
	}
	if chunk.ConversationStateS3Key != nil {
		t.Fatalf("ConversationStateS3Key = %v, want nil", *chunk.ConversationStateS3Key)
	}
	// current_agenda is unaffected by the upload failure — it's set before
	// the upload attempt.
	if chunk.CurrentAgenda == nil || *chunk.CurrentAgenda != "introduction" {
		t.Fatalf("CurrentAgenda = %v, want introduction", chunk.CurrentAgenda)
	}
}

func containsNullConversationStateKey(raw string) bool {
	var generic map[string]any
	if err := json.Unmarshal([]byte(raw), &generic); err != nil {
		return false
	}
	value, ok := generic["conversation_state_s3_key"]
	return ok && value == nil
}

// TestCallEventCallbacksConversationStateUploadHappensBeforeRedisWrite
// verifies the S3 upload completes before the chunk is written to Redis
// (so the key never points to a missing object): at upload time, the
// chunk list for this conversation must still be empty.
func TestCallEventCallbacksConversationStateUploadHappensBeforeRedisWrite(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        OnboardingCallBotType,
	}, redisClient, nil, nil)

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")

	var chunksAtUploadTime int
	var listErrAtUploadTime error
	uploader := &fakeJSONUploader{
		onUpload: func(string, any) {
			items, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
			chunksAtUploadTime = len(items)
			listErrAtUploadTime = err
		},
	}
	callbacks.SetChunkDecorator(newOnboardingChunkDecorator(state, uploader, "user-1", "conv-1", nil))

	events := callbacks.Events()
	events.OnUserTurnCommitted("hello doctor", time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC), "")

	// miniredis returns an error for a List on a key that does not exist
	// yet; either that or an empty slice proves no chunk had been written
	// when the upload ran.
	if listErrAtUploadTime == nil && chunksAtUploadTime != 0 {
		t.Fatalf("chunks present at upload time = %d, want 0 (upload must precede the Redis write)", chunksAtUploadTime)
	}

	chunkItems, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List chunks after commit: %v", err)
	}
	if len(chunkItems) != 1 {
		t.Fatalf("chunk count after commit = %d, want 1", len(chunkItems))
	}
}

// TestCallEventCallbacksConversationStateUploadNilForNonOnboardingBots
// verifies sales/follow-up-shaped callbacks (no SetChunkDecorator call)
// are a strict no-op: no uploader is ever invoked and every chunk carries
// a null conversation_state_s3_key (and a null current_agenda).
func TestCallEventCallbacksConversationStateUploadNilForNonOnboardingBots(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        SalesCallBotType,
	}, redisClient, nil, nil)
	// Deliberately not calling SetChunkDecorator.

	events := callbacks.Events()
	events.OnUserTurnCommitted("hello", time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC), "")
	events.OnAssistantTurnCommitted("hi", time.Date(2026, 7, 8, 10, 0, 1, 0, time.UTC), voicepipelinecore.TurnMetrics{}, "")

	chunkItems, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List chunks: %v", err)
	}
	if len(chunkItems) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunkItems))
	}
	for i, raw := range chunkItems {
		if !containsNullConversationStateKey(raw) {
			t.Fatalf("chunk %d raw JSON does not have null conversation_state_s3_key: %s", i, raw)
		}
		var chunk ConversationChunk
		if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
			t.Fatalf("Unmarshal chunk %d: %v", i, err)
		}
		if chunk.CurrentAgenda != nil {
			t.Fatalf("chunk %d CurrentAgenda = %v, want nil", i, *chunk.CurrentAgenda)
		}
	}
}

func TestCallEventCallbacksPersistToolContextChunks(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        FollowUpBotType,
	}, redisClient, nil, nil)
	events := callbacks.Events()

	assistantToolCall := voicepipelinecore.Message{
		Role: "assistant",
		ToolCalls: []voicepipelinecore.ToolCall{{
			ID:   "call_1",
			Type: "function",
			Function: voicepipelinecore.ToolCallFunction{
				Name:      "get_guidance",
				Arguments: `{"situation":"pain"}`,
			},
		}},
	}
	toolResult := voicepipelinecore.Message{Role: "tool", Content: "guidance text", ToolCallID: "call_1"}
	at := time.Date(2026, 6, 10, 11, 39, 22, 0, time.UTC)

	events.OnToolResultCommitted(assistantToolCall, toolResult, at)

	chunkItems, err := redisServer.List(conversationChunksKey("user-1", "conv-1"))
	if err != nil {
		t.Fatalf("List chunks: %v", err)
	}
	if len(chunkItems) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunkItems))
	}

	var assistantChunk, toolChunk ConversationChunk
	if err := json.Unmarshal([]byte(chunkItems[0]), &assistantChunk); err != nil {
		t.Fatalf("Unmarshal assistant chunk: %v", err)
	}
	if err := json.Unmarshal([]byte(chunkItems[1]), &toolChunk); err != nil {
		t.Fatalf("Unmarshal tool chunk: %v", err)
	}
	if assistantChunk.Role != "assistant" || assistantChunk.Text != "" {
		t.Fatalf("assistant chunk = %+v", assistantChunk)
	}
	if toolChunk.Role != "tool" || toolChunk.Text != "guidance text" {
		t.Fatalf("tool chunk = %+v", toolChunk)
	}

	assistantMsg, ok := messageFromChunkTuple([]any{
		assistantChunk.ID,
		assistantChunk.Role,
		assistantChunk.Text,
		assistantChunk.IsDebugLog,
		assistantChunk.AdditionalData,
	})
	if !ok {
		t.Fatal("assistant tool chunk did not reconstruct")
	}
	if assistantMsg.Role != "assistant" ||
		len(assistantMsg.ToolCalls) != 1 ||
		assistantMsg.ToolCalls[0].ID != "call_1" ||
		assistantMsg.ToolCalls[0].Function.Name != "get_guidance" ||
		assistantMsg.ToolCalls[0].Function.Arguments != `{"situation":"pain"}` {
		t.Fatalf("assistant reconstructed message = %+v", assistantMsg)
	}

	toolMsg, ok := messageFromChunkTuple([]any{
		toolChunk.ID,
		toolChunk.Role,
		toolChunk.Text,
		toolChunk.IsDebugLog,
		toolChunk.AdditionalData,
	})
	if !ok {
		t.Fatal("tool result chunk did not reconstruct")
	}
	if toolMsg.Role != "tool" || toolMsg.ToolCallID != "call_1" || toolMsg.Content != "guidance text" {
		t.Fatalf("tool reconstructed message = %+v", toolMsg)
	}
}

// TestCallEventCallbacksLLMCallCompletedDelegation verifies the
// OnLLMCallCompleted entry in the Events() mapping: without a registered
// handler it no-ops safely, and with SetLLMCallCompletedHandler the
// text/interrupted values reach the handler through the mapping (the path
// the onboarding stage tracker is wired into).
func TestCallEventCallbacksLLMCallCompletedDelegation(t *testing.T) {
	_, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        OnboardingCallBotType,
	}, redisClient, nil, nil)

	events := callbacks.Events()
	if events.OnLLMCallCompleted == nil {
		t.Fatal("Events().OnLLMCallCompleted must be registered")
	}

	// No handler set: must be a safe no-op.
	events.OnLLMCallCompleted("ignored text", false)

	type call struct {
		text        string
		interrupted bool
	}
	var got []call
	callbacks.SetLLMCallCompletedHandler(func(text string, interrupted bool) {
		got = append(got, call{text, interrupted})
	})

	// Fire through the Events() mapping, not the handler directly, so the
	// delegation path is what's exercised.
	events.OnLLMCallCompleted("namaste, kaise hain aap", false)
	events.OnLLMCallCompleted("half a sen", true)

	want := []call{
		{"namaste, kaise hain aap", false},
		{"half a sen", true},
	}
	if len(got) != len(want) {
		t.Fatalf("handler calls = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("handler call %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestCallEventCallbacksOnboardingPostCallEndStageDone verifies the happy
// path: an end stage with positive talk time reports onboarding_call_done
// = true, plus the stage name, intensity levels, and full variable store.
// The decorator under test is the real onboarding one
// (newOnboardingPostCallDecorator), wired through the generic
// SetPostCallDecorator seam.
func TestCallEventCallbacksOnboardingPostCallEndStageDone(t *testing.T) {
	callbacks, requests := onboardingPostCallCallbacks(t, "user-1")

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	state.AdvanceStage(cfg.ResolveStage("closing_and_assurance", nil))
	state.MergeVariables(map[string]any{
		"diet_intensity_level":    "medium",
		"fitness_intensity_level": "high",
		"height_cm":               "170",
	})
	callbacks.SetPostCallDecorator(newOnboardingPostCallDecorator(state, "user-1"))

	callbacks.runPostCallOperations(voicepipelinecore.EndReasonUnspecified, voicepipelinecore.CallStats{
		TotalUserDurationSec: 42,
		EndedAt:              time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC),
	}, "")

	got := <-requests
	if got.Body["onboarding_call_done"] != true {
		t.Fatalf("onboarding_call_done = %v, want true", got.Body["onboarding_call_done"])
	}
	if got.Body["latest_onboarding_call_stage"] != "closing_and_assurance" {
		t.Fatalf("latest_onboarding_call_stage = %v", got.Body["latest_onboarding_call_stage"])
	}
	if got.Body["diet_plan_intensity_level"] != "medium" || got.Body["fitness_plan_intensity_level"] != "high" {
		t.Fatalf("intensity fields = %+v", got.Body)
	}
	wantVars := map[string]any{
		"diet_intensity_level":    "medium",
		"fitness_intensity_level": "high",
		"height_cm":               "170",
	}
	gotVars, _ := got.Body["conversation_variables"].(map[string]any)
	if len(gotVars) != len(wantVars) {
		t.Fatalf("conversation_variables = %+v, want %+v", gotVars, wantVars)
	}
	for k, v := range wantVars {
		if gotVars[k] != v {
			t.Fatalf("conversation_variables[%q] = %v, want %v", k, gotVars[k], v)
		}
	}
}

// TestCallEventCallbacksOnboardingPostCallNonEndStage verifies the stage
// name and variables are still sent (unconditional) even when the current
// stage is not an end stage, but onboarding_call_done is false.
func TestCallEventCallbacksOnboardingPostCallNonEndStage(t *testing.T) {
	callbacks, requests := onboardingPostCallCallbacks(t, "user-1")

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	state.AdvanceStage(cfg.ResolveStage("diet_information", nil))
	state.MergeVariables(map[string]any{"height_cm": "170"})
	callbacks.SetPostCallDecorator(newOnboardingPostCallDecorator(state, "user-1"))

	callbacks.runPostCallOperations(voicepipelinecore.EndReasonUnspecified, voicepipelinecore.CallStats{
		TotalUserDurationSec: 30,
	}, "")

	got := <-requests
	if got.Body["onboarding_call_done"] != false {
		t.Fatalf("onboarding_call_done = %v, want false", got.Body["onboarding_call_done"])
	}
	if got.Body["latest_onboarding_call_stage"] != "diet_information" {
		t.Fatalf("latest_onboarding_call_stage = %v, want diet_information (sent unconditionally)", got.Body["latest_onboarding_call_stage"])
	}
	gotVars, _ := got.Body["conversation_variables"].(map[string]any)
	if gotVars["height_cm"] != "170" {
		t.Fatalf("conversation_variables = %+v", gotVars)
	}
	for _, key := range []string{"diet_plan_intensity_level", "fitness_plan_intensity_level"} {
		if v, ok := got.Body[key]; ok && v != nil {
			t.Fatalf("%s = %v, want absent/null when intensity missing", key, v)
		}
	}
}

// TestCallEventCallbacksOnboardingPostCallZeroDuration verifies zero total
// user duration keeps onboarding_call_done false even on an end stage.
func TestCallEventCallbacksOnboardingPostCallZeroDuration(t *testing.T) {
	callbacks, requests := onboardingPostCallCallbacks(t, "user-1")

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	state.AdvanceStage(cfg.ResolveStage("end_call", nil))
	callbacks.SetPostCallDecorator(newOnboardingPostCallDecorator(state, "user-1"))

	callbacks.runPostCallOperations(voicepipelinecore.EndReasonUnspecified, voicepipelinecore.CallStats{
		TotalUserDurationSec: 0,
	}, "")

	got := <-requests
	if got.Body["onboarding_call_done"] != false {
		t.Fatalf("onboarding_call_done = %v, want false (zero duration)", got.Body["onboarding_call_done"])
	}
}

// TestCallEventCallbacksOnboardingPostCallTestUserExcluded verifies the
// hardcoded onboarding_pipeline_manager._is_call_completed test-user
// exclusion keeps onboarding_call_done false even on a completed end
// stage with positive duration.
func TestCallEventCallbacksOnboardingPostCallTestUserExcluded(t *testing.T) {
	callbacks, requests := onboardingPostCallCallbacks(t, onboardingTestUserExclusionID)

	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	state.AdvanceStage(cfg.ResolveStage("end_call", nil))
	callbacks.SetPostCallDecorator(newOnboardingPostCallDecorator(state, onboardingTestUserExclusionID))

	callbacks.runPostCallOperations(voicepipelinecore.EndReasonUnspecified, voicepipelinecore.CallStats{
		TotalUserDurationSec: 100,
	}, "")

	got := <-requests
	if got.Body["onboarding_call_done"] != false {
		t.Fatalf("onboarding_call_done = %v, want false (excluded test user)", got.Body["onboarding_call_done"])
	}
}

// TestCallEventCallbacksOnboardingPostCallNilProviderUnaffected verifies
// sales/follow-up bots (no post-call decorator wired) get the exact same
// zero-value onboarding fields as before this change: onboarding_call_done
// = false and the onboarding-only fields serialize as explicit null.
func TestCallEventCallbacksOnboardingPostCallNilProviderUnaffected(t *testing.T) {
	callbacks, requests := onboardingPostCallCallbacks(t, "user-1")
	// No SetPostCallDecorator call — mirrors sales/follow-up.

	callbacks.runPostCallOperations(voicepipelinecore.EndReasonUnspecified, voicepipelinecore.CallStats{
		TotalUserDurationSec: 99,
	}, "")

	got := <-requests
	if got.Body["onboarding_call_done"] != false {
		t.Fatalf("onboarding_call_done = %v, want false", got.Body["onboarding_call_done"])
	}
	for _, key := range []string{
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
}
