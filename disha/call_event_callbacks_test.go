package disha

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

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
