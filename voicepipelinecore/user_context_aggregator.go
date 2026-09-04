package voicepipelinecore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

type UserContextAggregator struct {
	*BaseProcessor
	mu                sync.Mutex
	taskCtx           *TaskContext
	state             *aggregatorSharedState
	currentTranscript string
	interimTranscript string
	interimResponseID int
	interruptSent     bool
	botSpeaking       bool
}

func NewUserContextAggregator(taskCtx *TaskContext, initialMessages []Message, mainAgentSystemPromptLangfuseKey string) *UserContextAggregator {
	return newUserContextAggregatorWithState(taskCtx, newAggregatorSharedState(taskCtx, initialMessages, mainAgentSystemPromptLangfuseKey))
}

func newUserContextAggregatorWithState(taskCtx *TaskContext, state *aggregatorSharedState) *UserContextAggregator {
	a := &UserContextAggregator{
		taskCtx: taskCtx,
		state:   state,
	}
	a.BaseProcessor = NewBaseProcessor("UserContextAggregator", a, taskCtx)
	return a
}

func (a *UserContextAggregator) messagesForTest() []Message {
	return a.state.messagesForTest()
}

func (a *UserContextAggregator) resetFinalTranscript() {
	a.currentTranscript = ""
}

func (a *UserContextAggregator) resetInterimTranscript() {
	a.interimTranscript = ""
	a.interimResponseID = 0
}

func (a *UserContextAggregator) sendLiveTranscript(text string) {
	// Interim user transcription events are intentionally suppressed.
	// They are high-frequency diagnostics/UI traffic and final RTVI
	// user-transcription events are still emitted from addUserMessage.
}

func (a *UserContextAggregator) updateInterimTranscript(f TranscriptFrame) string {
	if f.IsFinal && f.Text == "<end>" {
		a.sendLiveTranscript("")
		a.resetInterimTranscript()
		return ""
	}
	if f.IsFinal {
		return a.interimTranscript
	}

	if f.ResponseID != 0 && f.ResponseID != a.interimResponseID {
		a.interimResponseID = f.ResponseID
		a.interimTranscript = ""
	}

	a.interimTranscript += f.Text
	if a.interimTranscript != "" {
		a.sendLiveTranscript(a.interimTranscript)
	}
	return a.interimTranscript
}

func (a *UserContextAggregator) updateFinalTranscript(f TranscriptFrame) (string, bool) {
	if !f.IsFinal {
		return "", false
	}
	if f.Text == "<end>" {
		text := a.currentTranscript
		a.resetFinalTranscript()
		return text, true
	}

	a.currentTranscript += f.Text
	return "", false
}

func (a *UserContextAggregator) appendMessages(messages []Message) {
	added := messagesFromInitial(messages)
	if len(added) == 0 {
		return
	}
	a.state.mu.Lock()
	a.state.messages = append(a.state.messages, added...)
	a.state.mu.Unlock()
}

func (a *UserContextAggregator) snapshotMessages() []Message {
	a.state.mu.Lock()
	defer a.state.mu.Unlock()
	return cloneMessages(a.state.messages)
}

func (a *UserContextAggregator) recordUserMessage(text string) (snapshot []Message, promptKey string, concatenated string) {
	a.state.mu.Lock()
	defer a.state.mu.Unlock()

	if len(a.state.messages) > 0 && a.state.messages[len(a.state.messages)-1].Role == "user" {
		last := &a.state.messages[len(a.state.messages)-1]
		last.Content += " " + text
		concatenated = last.Content
	} else {
		a.state.messages = append(a.state.messages, Message{Role: "user", Content: text})
	}
	promptKey = a.state.mainAgentSystemPromptLangfuseKey
	snapshot = cloneMessages(a.state.messages)
	return snapshot, promptKey, concatenated
}

func (a *UserContextAggregator) addUserMessage(text string) {
	at := time.Now()
	_, promptKey, concatenated := a.recordUserMessage(text)
	committed := text
	if concatenated != "" {
		a.taskCtx.Logger.Printf("Concatenated user message: %s\n", concatenated)
		committed = concatenated
	}
	a.taskCtx.UIEvents.UserTranscription(text, true, at)
	if a.taskCtx.callEvents != nil {
		a.taskCtx.callEvents.fireUserTurnCommitted(committed, at, promptKey)
	}
}

func toolCallFromFunctionFrame(functionName, toolCallID string, arguments map[string]any, rawArguments string) ToolCall {
	rawArgs := rawArguments
	if rawArgs == "" {
		rawArgs = "{}"
		if len(arguments) > 0 {
			if encoded, err := json.Marshal(arguments); err == nil {
				rawArgs = string(encoded)
			}
		}
	}
	return ToolCall{
		ID:   toolCallID,
		Type: "function",
		Function: ToolCallFunction{
			Name:      functionName,
			Arguments: rawArgs,
		},
	}
}

func assistantToolCallMessageFromFrame(functionName, toolCallID string, arguments map[string]any, rawArguments string) Message {
	toolCall := toolCallFromFunctionFrame(functionName, toolCallID, arguments, rawArguments)
	return Message{
		Role:      "assistant",
		ToolCalls: []ToolCall{toolCall},
	}
}

func (a *UserContextAggregator) addFunctionCallInProgress(f FunctionCallInProgressFrame) {
	assistantToolCall := assistantToolCallMessageFromFrame(f.FunctionName, f.ToolCallID, f.Arguments, f.RawArguments)
	toolMessage := Message{
		Role:       "tool",
		Content:    "IN_PROGRESS",
		ToolCallID: f.ToolCallID,
	}
	a.state.mu.Lock()
	a.state.messages = append(a.state.messages, assistantToolCall, toolMessage)
	a.state.mu.Unlock()
}

func (a *UserContextAggregator) applyFunctionCallResult(f FunctionCallResultFrame) (Message, Message) {
	result := strings.TrimSpace(f.Result)
	if result == "" {
		err := errors.New("empty tool result")
		a.PushError("empty tool result", false)
		sentryutil.Capture(sentryutil.Event{
			Hub: a.taskCtx.SentryHub(),
			Err: err,
			Tags: map[string]string{
				"component": "user_context_aggregator",
				"operation": "tool_result",
			},
			Details: map[string]any{
				"function_name": f.FunctionName,
				"tool_call_id":  f.ToolCallID,
			},
		})
		result = toolErrorResultString(err.Error())
	}
	assistantToolCall := assistantToolCallMessageFromFrame(f.FunctionName, f.ToolCallID, f.Arguments, f.RawArguments)
	toolResult := Message{
		Role:       "tool",
		Content:    result,
		ToolCallID: f.ToolCallID,
	}
	a.state.mu.Lock()
	defer a.state.mu.Unlock()
	for i := len(a.state.messages) - 1; i >= 0; i-- {
		msg := &a.state.messages[i]
		if msg.Role == "tool" && msg.ToolCallID == f.ToolCallID {
			msg.Content = result
			return assistantToolCall, toolResult
		}
	}
	a.state.messages = append(a.state.messages, toolResult)
	return assistantToolCall, toolResult
}

func (a *UserContextAggregator) submitUserMessage(text string) {
	a.taskCtx.Logger.Printf("Final transcript received: %s\n", text)
	if a.taskCtx.callEvents != nil {
		a.taskCtx.callEvents.fireUserFirstSpeech(time.Now())
	}
	at := time.Now()
	messages, promptKey, concatenated := a.recordUserMessage(text)
	committed := text
	if concatenated != "" {
		a.taskCtx.Logger.Printf("Concatenated user message: %s\n", concatenated)
		committed = concatenated
	}
	a.taskCtx.UIEvents.UserTranscription(text, true, at)
	if a.taskCtx.callEvents != nil {
		a.taskCtx.callEvents.fireUserTurnCommitted(committed, at, promptKey)
	}
	a.interruptSent = false
	a.resetInterimTranscript()
	a.resetFinalTranscript()
	a.PushFrame(NewLLMMessagesFrame(messages), Downstream)
}

func (a *UserContextAggregator) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch f := frame.(type) {
	case EndFrame:
		a.taskCtx.Logger.Printf("EndFrame at UserContextAggregator: reason=%q\n", f.Reason)
		a.PushFrame(f, dir)
	case LLMMessagesAppendFrame:
		// Append any provided messages to the context, then (if RunLLM)
		// run a turn on the current context. Pushed on user-join with no
		// messages + RunLLM to make the bot greet first from the initial
		// context (system prompt + "hello?" for a fresh call, or prior
		// chunks + resume note). Consumed here, not forwarded.
		if len(f.Messages) > 0 {
			a.appendMessages(f.Messages)
		}
		if f.RunLLM {
			messages := a.snapshotMessages()
			if len(messages) == 0 {
				a.taskCtx.Logger.Println("LLMMessagesAppend run skipped: empty context")
				return
			}
			a.taskCtx.Logger.Println("Running LLM turn from appended context (greet-first / injected)")
			a.PushFrame(NewLLMMessagesFrame(messages), Downstream)
		}
	case FunctionCallInProgressFrame:
		a.taskCtx.Logger.Printf("Function call in progress: %s tool_call_id=%s\n", f.FunctionName, f.ToolCallID)
		a.addFunctionCallInProgress(f)
		a.PushFrame(f, Upstream)
	case FunctionCallResultFrame:
		a.taskCtx.Logger.Printf("Function call result: %s tool_call_id=%s run_llm=%v\n", f.FunctionName, f.ToolCallID, f.RunLLM)
		assistantToolCall, toolResult := a.applyFunctionCallResult(f)
		if a.taskCtx.callEvents != nil {
			a.taskCtx.callEvents.fireToolResultCommitted(assistantToolCall, toolResult, time.Now())
		}
		a.PushFrame(f, Upstream)
		if f.RunLLM {
			a.PushFrame(NewLLMMessagesFrame(a.snapshotMessages()), Downstream)
		}
	case TranscriptFrame:
		interimTranscript := a.updateInterimTranscript(f)

		// Barge-in uses the latest non-final response snapshot only.
		// Turn-taking waits for final tokens ending with <end>.
		if a.botSpeaking && !a.interruptSent && !f.IsFinal {
			if len(strings.Fields(interimTranscript)) >= minBargeInWords {
				a.taskCtx.Logger.Println("Barge-in detected")
				a.taskCtx.UIEvents.ServerMessage("Interruption received while bot is speaking", time.Now())
				// Mirrors Pipecat's MinWordsUserTurnStartStrategy firing
				// on_user_turn_started as soon as the (bot-speaking) 3-word
				// threshold is crossed (base_pipeline_manager.py:425-431).
				a.taskCtx.UIEvents.ServerMessage("User turn started", time.Now())
				a.PushFrame(NewInterruptFrame(), Downstream)
				a.interruptSent = true
				a.botSpeaking = false
			}
		}
		if text, finished := a.updateFinalTranscript(f); finished {
			if text != "" {
				if a.botSpeaking && !a.interruptSent {
					// Bot speaking, below barge-in threshold — discard.
					// Matches Pipecat: short utterances during bot speech are
					// acknowledgments, not intentional turns.
					a.taskCtx.Logger.Printf("Discarding below-threshold transcript (bot speaking): %s\n", text)
					a.resetInterimTranscript()
				} else {
					if !a.interruptSent {
						// Bot was silent for this whole turn (not a barge-in
						// continuation, which already signaled turn-start
						// above) — this is Go's equivalent of Pipecat's
						// min_words=1-when-bot-silent threshold crossing.
						a.taskCtx.UIEvents.ServerMessage("User turn started", time.Now())
					}
					a.submitUserMessage(text)
				}
			}
		}
	case TTSDoneFrame:
		a.PushFrame(f, dir)
	case BotStartedSpeakingFrame:
		a.botSpeaking = true
		a.PushFrame(f, dir) // continue upstream to UserIdle
	case BotStoppedSpeakingFrame:
		a.botSpeaking = false
		// Mirror Pipecat's reset_aggregation behavior at the bot-turn
		// boundary: any user speech that didn't trigger barge-in
		// during this turn was back-channeling and must not become a
		// user message. Without this, a Soniox <end> arriving a few
		// hundred ms AFTER the bot stops would fall into the
		// submitUserMessage branch and make the next LLM turn respond
		// to a short unrelated acknowledgment.
		if !a.interruptSent {
			if a.interimTranscript != "" || a.currentTranscript != "" {
				a.taskCtx.Logger.Printf("Discarding back-channel speech after bot turn: interim=%q final=%q\n", a.interimTranscript, a.currentTranscript)
				a.resetInterimTranscript()
				a.resetFinalTranscript()
				a.sendLiveTranscript("")
			}
		}
		a.PushFrame(f, dir) // continue upstream to UserIdle
	default:
		a.PushFrame(frame, dir)
	}
}
