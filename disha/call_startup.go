package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	// Resume-message branches from
	// bots/followup_call/fetch_conversation.py. These are shared by any
	// call flow that can resume a prior conversation, not just sales.
	resumeWindowGraceful = 5 * time.Minute

	resumeMessageGracefulWithinWindow = "The conversation might have interrupted a few mins ago. Here's how to resume, follow carefully:\n" +
		"1. If the interruption was user initiated (like \"ill call you back, give me a min\") then say something like 'hanji to aap keh rhe the' and resume. Make sure to not acknowledge their interrupt request (this already happened), just continue.\n" +
		"2. If not, make sure to first acknowledge the call being disconnected and then continue."

	resumeMessageAfterWindow = "This conversation was interrupted because the call ended. Now you have to resume this conversation by saying hi and acknowledge the things that have been discussed very briefly and inform the next agenda. Then ask the user if we should continue further"
)

type CallStartup struct {
	ConversationID string
	UserID         string
	BotType        string
	Logger         *log.Logger
	Data           *ConversationData
}

func collectCallStartup(ctx context.Context, conversationID, expectedBotType string, deps Deps) (CallStartup, error) {
	if conversationID == "" {
		return CallStartup{}, errors.New("disha: conversation_id is required")
	}
	if deps.Redis == nil {
		return CallStartup{}, errors.New("disha: Redis dependency is required")
	}
	if deps.API == nil {
		return CallStartup{}, errors.New("disha: API dependency is required")
	}

	data, err := deps.Redis.GetConversationData(ctx, conversationID)
	if err != nil {
		return CallStartup{}, fmt.Errorf("disha: load conversation_data: %w", err)
	}
	if expectedBotType != "" && data.Conversation.BotType != expectedBotType {
		return CallStartup{}, fmt.Errorf("disha: unsupported bot_type %q", data.Conversation.BotType)
	}

	userID := data.Conversation.UserID
	if userID == "" {
		userID = data.UserProfile.UserID
	}
	if userID == "" {
		return CallStartup{}, errors.New("disha: user_id is missing from conversation_data")
	}

	logger := deps.Logger
	if logger == nil {
		logger = log.Default()
	}
	callLogger := log.New(logger.Writer(), fmt.Sprintf("[conv=%s] ", conversationID), logger.Flags())

	return CallStartup{
		ConversationID: conversationID,
		UserID:         userID,
		BotType:        data.Conversation.BotType,
		Logger:         callLogger,
		Data:           data,
	}, nil
}

// messageFromChunkTuple mirrors Python's chunk replay behavior for normal
// transcript chunks: unknown additional_data is skipped. Tool context chunks
// use the same additional_data shape as Disha's Python onboarding pipeline:
// assistant chunks carry `tool_calls`; tool chunks carry `tool_call_id`.
func messageFromChunkTuple(tuple []any) (voicepipelinecore.Message, bool) {
	if len(tuple) < 4 {
		return voicepipelinecore.Message{}, false
	}
	role, _ := tuple[1].(string)
	text, _ := tuple[2].(string)
	isDebug, _ := tuple[3].(bool)
	if isDebug {
		return voicepipelinecore.Message{}, false
	}
	// Python's `if additional_data: continue` — skip unknown truthy metadata,
	// but replay the explicit OpenAI-format tool context chunks that Go writes
	// for cross-call resume.
	if len(tuple) >= 5 && isTruthyJSON(tuple[4]) {
		if msg, ok := messageFromToolContextChunk(role, text, tuple[4]); ok {
			return msg, true
		}
		return voicepipelinecore.Message{}, false
	}
	return voicepipelinecore.Message{Role: role, Content: text}, true
}

func messageFromToolContextChunk(role, text string, additionalData any) (voicepipelinecore.Message, bool) {
	data, ok := additionalData.(map[string]any)
	if !ok {
		return voicepipelinecore.Message{}, false
	}
	if toolCallsData, ok := data["tool_calls"]; ok {
		raw, err := json.Marshal(toolCallsData)
		if err != nil {
			return voicepipelinecore.Message{}, false
		}
		var toolCalls []voicepipelinecore.ToolCall
		if err := json.Unmarshal(raw, &toolCalls); err != nil || len(toolCalls) == 0 {
			return voicepipelinecore.Message{}, false
		}
		return voicepipelinecore.Message{Role: role, ToolCalls: toolCalls}, true
	}
	if toolCallID, ok := data["tool_call_id"].(string); ok && toolCallID != "" {
		return voicepipelinecore.Message{Role: role, Content: text, ToolCallID: toolCallID}, true
	}
	return voicepipelinecore.Message{}, false
}

// isTruthyJSON reports whether a JSON-decoded value is "truthy" in the
// Python sense, so additional_data filtering matches sales_call.py
// across the shapes encoding/json produces (nil, bool, float64, string,
// map, slice).
func isTruthyJSON(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case float64:
		return t != 0
	case string:
		return t != ""
	case map[string]any:
		return len(t) > 0
	case []any:
		return len(t) > 0
	default:
		return true
	}
}

// buildInitialMessages mirrors the Python "build messages" block and is
// shared by every Disha call flow:
//
//   - If there are prior chunks, prepend the system prompt and replay
//     each non-debug chunk in role/content form.
//   - When resuming, append a system message describing how to
//     reconnect with the user.
//   - On a fresh call, seed with `{user: "hello?"}` so the bot speaks
//     first.
func buildInitialMessages(systemPrompt string, chunks [][]any, resumeMessage string) []voicepipelinecore.Message {
	msgs := []voicepipelinecore.Message{{Role: "system", Content: systemPrompt}}
	for _, tuple := range chunks {
		msg, ok := messageFromChunkTuple(tuple)
		if ok {
			msgs = append(msgs, msg)
		}
	}
	msgs = reorderToolResultMessages(msgs)
	hasPriorTurns := len(msgs) > 1
	if hasPriorTurns && resumeMessage != "" {
		msgs = append(msgs, voicepipelinecore.Message{Role: "user", Content: "<system_message>" + resumeMessage + "</system_message>"})
	}
	if !hasPriorTurns {
		msgs = append(msgs, voicepipelinecore.Message{Role: "user", Content: "hello?"})
	}
	return msgs
}

// reorderToolResultMessages mirrors onboarding_call/conversation_context_manager.py:
// tool responses are replayed immediately after the assistant message that
// requested them, even if storage returns those chunks in a different order.
func reorderToolResultMessages(msgs []voicepipelinecore.Message) []voicepipelinecore.Message {
	if len(msgs) == 0 {
		return nil
	}
	nonToolMessages := make([]voicepipelinecore.Message, 0, len(msgs))
	toolMessagesByID := make(map[string][]voicepipelinecore.Message)
	toolIDOrder := make([]string, 0)
	for _, msg := range msgs {
		if msg.Role != "tool" || msg.ToolCallID == "" {
			nonToolMessages = append(nonToolMessages, msg)
			continue
		}
		if _, exists := toolMessagesByID[msg.ToolCallID]; !exists {
			toolIDOrder = append(toolIDOrder, msg.ToolCallID)
		}
		toolMessagesByID[msg.ToolCallID] = append(toolMessagesByID[msg.ToolCallID], msg)
	}
	if len(toolMessagesByID) == 0 {
		return msgs
	}

	ordered := make([]voicepipelinecore.Message, 0, len(msgs))
	for _, msg := range nonToolMessages {
		ordered = append(ordered, msg)
		for _, toolCall := range msg.ToolCalls {
			if toolCall.ID == "" {
				continue
			}
			if toolMessages, ok := toolMessagesByID[toolCall.ID]; ok {
				ordered = append(ordered, toolMessages...)
				delete(toolMessagesByID, toolCall.ID)
			}
		}
	}
	for _, toolCallID := range toolIDOrder {
		if toolMessages, ok := toolMessagesByID[toolCallID]; ok {
			ordered = append(ordered, toolMessages...)
		}
	}
	return ordered
}

// buildResumeSystemMessage reproduces fetch_conversation.py's
// resume-message logic, but driven by the cached `resumed_chunk`
// payload that Disha already writes to conversation_data in Redis.
// Returns "" when no resume nudge is needed (no chunk to resume from,
// resume_gracefully=false, etc).
func buildResumeSystemMessage(data *ConversationData, now time.Time) string {
	if data == nil {
		return ""
	}
	if data.Conversation.ResumedFromChunkID == nil || strings.TrimSpace(*data.Conversation.ResumedFromChunkID) == "" {
		return ""
	}
	if len(data.ResumedChunk) == 0 {
		return ""
	}
	chunkCreated, ok := parseResumedChunkCreated(data.ResumedChunk)
	if !ok {
		// Without a creation timestamp we can't pick a branch — match
		// Python's behaviour, which silently skips when the chunk lookup
		// fails.
		return ""
	}
	// Python gates on `if conversation.resume_gracefully:` — False and
	// missing both mean the thread was deliberately rebuilt from an
	// explicit chunkId (bot_session_manager sets resume_gracefully =
	// not resumed_from_specific_chunk), so no resume nudge is emitted.
	if data.Conversation.ResumeGracefully == nil || !*data.Conversation.ResumeGracefully {
		return ""
	}
	if now.Sub(chunkCreated) < resumeWindowGraceful {
		return resumeMessageGracefulWithinWindow
	}
	return resumeMessageAfterWindow
}

func parseResumedChunkCreated(payload map[string]any) (time.Time, bool) {
	raw, ok := payload["created"]
	if !ok {
		return time.Time{}, false
	}
	switch v := raw.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t, true
		}
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, true
		}
		if t, err := time.Parse("2006-01-02T15:04:05.999999", v); err == nil {
			return t, true
		}
	case float64:
		return time.Unix(int64(v), 0), true
	}
	return time.Time{}, false
}

// mergeShortTermMemory replicates Python's fetch_conversation.py:
// short_term_memory is concatenated with unprocessed_chat_context
// using a double newline separator. Either side may be empty. This is
// part of the shared resume/initial-flow memory, independent of bot
// type.
func mergeShortTermMemory(shortTerm, unprocessed string) string {
	shortTerm = strings.TrimSpace(shortTerm)
	unprocessed = strings.TrimSpace(unprocessed)
	switch {
	case shortTerm == "" && unprocessed == "":
		return ""
	case shortTerm == "":
		return unprocessed
	case unprocessed == "":
		return shortTerm
	default:
		return shortTerm + "\n\n" + unprocessed
	}
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func istLocation() *time.Location {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return time.FixedZone("IST", 5*3600+30*60)
	}
	return loc
}
