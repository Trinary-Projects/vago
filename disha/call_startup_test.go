package disha

import (
	"testing"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// TestBuildResumeSystemMessage pins fetch_conversation.py: a resume nudge
// is emitted only when resume_gracefully is explicitly true (False/None
// mean an explicit-chunkId rebuild — continue silently), picking the
// within-window or after-window text by the resumed chunk's age. The
// non-empty results are wrapped in <system_message> — buildResumeSystemMessage
// is the shared sales/follow-up nudge builder and now returns the
// fully-composed text ready to append verbatim via buildInitialMessages.
func TestBuildResumeSystemMessage(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	chunkID := "chunk-1"
	boolPtr := func(v bool) *bool { return &v }
	resumedChunk := func(age time.Duration) map[string]any {
		return map[string]any{"created": now.Add(-age).Format(time.RFC3339Nano)}
	}
	wrap := func(text string) string { return "<system_message>" + text + "</system_message>" }
	cases := []struct {
		name string
		data *ConversationData
		want string
	}{
		{"nil data", nil, ""},
		{"no resumed chunk id", &ConversationData{
			ResumedChunk: resumedChunk(time.Minute),
		}, ""},
		{"gracefully nil emits nothing", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID},
			ResumedChunk: resumedChunk(time.Minute),
		}, ""},
		{"gracefully false emits nothing", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(false)},
			ResumedChunk: resumedChunk(time.Minute),
		}, ""},
		{"gracefully true within window", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(true)},
			ResumedChunk: resumedChunk(time.Minute),
		}, wrap(resumeMessageGracefulWithinWindow)},
		{"gracefully true after window", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(true)},
			ResumedChunk: resumedChunk(10 * time.Minute),
		}, wrap(resumeMessageAfterWindow)},
		{"missing created timestamp emits nothing", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(true)},
			ResumedChunk: map[string]any{},
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildResumeSystemMessage(tc.data, now); got != tc.want {
				t.Fatalf("buildResumeSystemMessage = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestMessageFromChunkTuple pins the filter to sales_call.py: replay a
// chunk iff it is not a debug log and has no (truthy) additional_data;
// role and text are taken verbatim with no further filtering.
func TestMessageFromChunkTuple(t *testing.T) {
	cases := []struct {
		name     string
		tuple    []any
		wantOK   bool
		wantRole string
		wantText string
	}{
		{"user turn", []any{"id", "user", "hello", false, nil}, true, "user", "hello"},
		{"assistant turn", []any{"id", "assistant", "hi", false, nil}, true, "assistant", "hi"},
		{"debug dropped", []any{"id", "assistant", "x", true, nil}, false, "", ""},
		{"additional_data dropped", []any{"id", "assistant", "x", false, map[string]any{"k": "v"}}, false, "", ""},
		{"empty additional_data kept", []any{"id", "assistant", "x", false, map[string]any{}}, true, "assistant", "x"},
		{"non-standard role kept", []any{"id", "tool", "tool turn", false, nil}, true, "tool", "tool turn"},
		{"empty text kept", []any{"id", "user", "", false, nil}, true, "user", ""},
		{"four-element tuple kept", []any{"id", "user", "hi", false}, true, "user", "hi"},
		{"too short dropped", []any{"id", "user", "hi"}, false, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, ok := messageFromChunkTuple(tc.tuple)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && (msg.Role != tc.wantRole || msg.Content != tc.wantText) {
				t.Fatalf("msg = %+v, want {Role:%q Content:%q}", msg, tc.wantRole, tc.wantText)
			}
		})
	}
}

func TestIsTruthyJSON(t *testing.T) {
	truthy := []any{true, float64(1), "x", map[string]any{"k": 1}, []any{1}, struct{}{}}
	falsy := []any{nil, false, float64(0), "", map[string]any{}, []any{}}
	for _, v := range truthy {
		if !isTruthyJSON(v) {
			t.Errorf("isTruthyJSON(%#v) = false, want true", v)
		}
	}
	for _, v := range falsy {
		if isTruthyJSON(v) {
			t.Errorf("isTruthyJSON(%#v) = true, want false", v)
		}
	}
}

func TestMessageFromChunkTupleReplaysToolContextAdditionalData(t *testing.T) {
	assistantMsg := voicepipelinecore.Message{
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
	msg, ok := messageFromChunkTuple([]any{
		"tool-call-chunk",
		"assistant",
		"",
		false,
		toolContextAdditionalData(assistantMsg),
	})
	if !ok {
		t.Fatal("assistant tool-call chunk was not replayed")
	}
	if msg.Role != "assistant" || len(msg.ToolCalls) != 1 {
		t.Fatalf("assistant tool message = %+v", msg)
	}
	if msg.ToolCalls[0].ID != "call_1" ||
		msg.ToolCalls[0].Function.Name != "get_guidance" ||
		msg.ToolCalls[0].Function.Arguments != `{"situation":"pain"}` {
		t.Fatalf("tool call = %+v", msg.ToolCalls[0])
	}

	toolMsg := voicepipelinecore.Message{Role: "tool", Content: "guidance text", ToolCallID: "call_1"}
	msg, ok = messageFromChunkTuple([]any{
		"tool-result-chunk",
		"tool",
		"guidance text",
		false,
		toolContextAdditionalData(toolMsg),
	})
	if !ok {
		t.Fatal("tool result chunk was not replayed")
	}
	if msg.Role != "tool" || msg.ToolCallID != "call_1" || msg.Content != "guidance text" {
		t.Fatalf("tool result message = %+v", msg)
	}
}

func TestBuildInitialMessagesReordersToolResultAfterAssistantToolCall(t *testing.T) {
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
	toolResult := voicepipelinecore.Message{
		Role:       "tool",
		Content:    "guidance text",
		ToolCallID: "call_1",
	}

	msgs := buildInitialMessages("system prompt", [][]any{
		{"user-1", "user", "hello", false, nil},
		{"tool-result", "tool", toolResult.Content, false, toolContextAdditionalData(toolResult)},
		{"tool-call", "assistant", "", false, toolContextAdditionalData(assistantToolCall)},
		{"assistant-1", "assistant", "ok", false, nil},
	}, "")

	if len(msgs) != 5 {
		t.Fatalf("len(msgs) = %d, want 5: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != "system" || msgs[1].Role != "user" {
		t.Fatalf("unexpected prefix: %+v", msgs[:2])
	}
	if msgs[2].Role != "assistant" || len(msgs[2].ToolCalls) != 1 || msgs[2].ToolCalls[0].ID != "call_1" {
		t.Fatalf("assistant tool-call message = %+v", msgs[2])
	}
	if msgs[3].Role != "tool" || msgs[3].ToolCallID != "call_1" || msgs[3].Content != toolResult.Content {
		t.Fatalf("tool result message = %+v", msgs[3])
	}
	if msgs[4].Role != "assistant" || msgs[4].Content != "ok" {
		t.Fatalf("trailing assistant message = %+v", msgs[4])
	}
}

func TestBuildInitialMessagesAppendsUnmatchedToolResultsAtEnd(t *testing.T) {
	toolResult := voicepipelinecore.Message{
		Role:       "tool",
		Content:    "orphan tool result",
		ToolCallID: "missing_call",
	}

	msgs := buildInitialMessages("system prompt", [][]any{
		{"tool-result", "tool", toolResult.Content, false, toolContextAdditionalData(toolResult)},
		{"user-1", "user", "hello", false, nil},
	}, "")

	if len(msgs) != 3 {
		t.Fatalf("len(msgs) = %d, want 3: %+v", len(msgs), msgs)
	}
	if msgs[1].Role != "user" || msgs[1].Content != "hello" {
		t.Fatalf("non-tool message order changed: %+v", msgs)
	}
	if msgs[2].Role != "tool" || msgs[2].ToolCallID != "missing_call" {
		t.Fatalf("unmatched tool result = %+v", msgs[2])
	}
}
