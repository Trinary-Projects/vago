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
	mu                   sync.Mutex
	taskCtx              *TaskContext
	state                *aggregatorSharedState
	currentTranscript    string
	interimTranscript    string
	interimResponseID    int
	interruptSent        bool
	botSpeaking          bool
	speakingResponseID   int64
	generatingResponseID int64
	pendingRuns          []LLMMessagesAppendFrame
	awaitingInterrupt    bool
	pendingUserText      string
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

func (a *UserContextAggregator) snapshotMessages() []Message {
	a.state.mu.Lock()
	defer a.state.mu.Unlock()
	return cloneMessages(a.state.messages)
}

func (a *UserContextAggregator) recordUserMessage(text string) (snapshot []Message, promptKey string, concatenated string) {
	a.state.mu.Lock()
	defer a.state.mu.Unlock()

	a.state.responseID = 0
	a.state.completedResponseID = 0
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
	assistantToolCall.ResponseID = f.ResponseID
	toolMessage := Message{
		Role:       "tool",
		Content:    "IN_PROGRESS",
		ResponseID: f.ResponseID,
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
	// A replacement user turn waits only for interruption reconciliation, never
	// for pending audio to play. This keeps unheard generated text out of its request.
	if a.awaitingInterrupt {
		a.pendingUserText = strings.TrimSpace(a.pendingUserText + " " + text)
		return
	}
	a.state.mu.Lock()
	pendingSpeech := false
	for _, message := range a.state.messages {
		if message.pendingPlayback {
			pendingSpeech = true
			break
		}
	}
	a.state.mu.Unlock()
	if a.generatingResponseID != 0 || pendingSpeech {
		a.beginInterruption()
		a.PushFrame(NewInterruptFrame(), Downstream)
		a.pendingUserText = text
		return
	}
	a.commitUserMessage(text)
}

func (a *UserContextAggregator) commitUserMessage(text string) {
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
	a.pushLLMMessages(messages)
}

// Called under a.mu, so response admission and incoming user/interrupt/end
// frames are serialized. Generated and played history use the shared lock.
func (a *UserContextAggregator) pushLLMMessages(messages []Message) {
	frame := NewLLMMessagesFrame(messages)
	a.state.mu.Lock()
	if a.state.ending {
		a.state.mu.Unlock()
		return
	}
	a.state.responseID = frame.ID()
	// Reserve its position before independent speech or tool updates arrive.
	a.state.messages = append(a.state.messages, Message{Role: "assistant", ResponseID: frame.ID(), pendingPlayback: true})
	a.generatingResponseID = frame.ID()
	a.state.completedResponseID = 0
	a.state.mu.Unlock()
	a.PushFrame(frame, Downstream)
}

func (a *UserContextAggregator) invalidateResponse() {
	a.state.mu.Lock()
	a.state.responseID = 0
	a.state.completedResponseID = 0
	a.state.mu.Unlock()
}

func (a *UserContextAggregator) beginInterruption() {
	a.invalidateResponse()
	a.generatingResponseID = 0
	a.pendingRuns = nil
	a.awaitingInterrupt = true
}

func (a *UserContextAggregator) handleMessagesAppend(f LLMMessagesAppendFrame) {
	a.state.mu.Lock()
	if a.state.ending || a.ctx.Err() != nil || (f.AfterResponseID != 0 && a.state.responseID != f.AfterResponseID) {
		a.state.mu.Unlock()
		return
	}
	if f.RunLLM && (a.generatingResponseID != 0 || a.awaitingInterrupt) {
		a.pendingRuns = append(a.pendingRuns, f)
		a.state.mu.Unlock()
		return
	}
	a.state.messages = append(a.state.messages, messagesFromInitial(f.Messages)...)
	messages := cloneMessages(a.state.messages)
	a.state.mu.Unlock()
	if f.RunLLM && len(messages) > 0 {
		a.pushLLMMessages(messages)
	}
}

func (a *UserContextAggregator) flushPendingRuns() {
	for len(a.pendingRuns) > 0 && a.generatingResponseID == 0 && !a.awaitingInterrupt {
		f := a.pendingRuns[0]
		a.pendingRuns = a.pendingRuns[1:]
		a.handleMessagesAppend(f)
	}
}

func (a *UserContextAggregator) generationCompleted(f LLMResponseEndFrame) {
	if f.ResponseID == 0 || f.ResponseID != a.generatingResponseID {
		return
	}
	a.generatingResponseID = 0
	a.state.mu.Lock()
	// Playback can win this race. Never replace its committed text with the
	// longer generated version. Otherwise retain generated context provisionally
	// so another inference can run while earlier speech is still playing.
	found := false
	insertAt := len(a.state.messages)
	for i, message := range a.state.messages {
		if message.ResponseID != f.ResponseID {
			continue
		}
		if message.Role == "assistant" && len(message.ToolCalls) == 0 {
			found = true
			if message.pendingPlayback {
				if strings.TrimSpace(f.Text) == "" {
					a.state.messages = append(a.state.messages[:i], a.state.messages[i+1:]...)
				} else {
					a.state.messages[i].Content = f.Text
				}
			}
			break
		}
		if i < insertAt {
			insertAt = i
		}
	}
	if !found && a.state.completedResponseID != f.ResponseID && strings.TrimSpace(f.Text) != "" {
		message := Message{Role: "assistant", Content: f.Text, ResponseID: f.ResponseID, pendingPlayback: true}
		a.state.messages = append(a.state.messages, Message{})
		copy(a.state.messages[insertAt+1:], a.state.messages[insertAt:])
		a.state.messages[insertAt] = message
	}
	a.state.mu.Unlock()
	a.flushPendingRuns()
}

func (a *UserContextAggregator) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch f := frame.(type) {
	case EndFrame:
		a.pendingRuns = nil
		a.pendingUserText = ""
		a.generatingResponseID = 0
		a.state.mu.Lock()
		a.state.ending = true
		a.state.responseID = 0
		a.state.completedResponseID = 0
		a.state.mu.Unlock()
		a.taskCtx.Logger.Printf("EndFrame at UserContextAggregator: reason=%q\n", f.Reason)
		a.PushFrame(f, dir)
	case LLMMessagesAppendFrame:
		// Append any provided messages to the context, then (if RunLLM)
		// run a turn on the current context. Pushed on user-join with no
		// messages + RunLLM to make the bot greet first from the initial
		// context (system prompt + "hello?" for a fresh call, or prior
		// chunks + resume note). Consumed here, not forwarded.
		a.handleMessagesAppend(f)
	case LLMResponseEndFrame:
		if dir == Upstream {
			a.generationCompleted(f)
		} else {
			a.PushFrame(f, dir)
		}
	case InterruptFrame:
		a.beginInterruption()
		a.PushFrame(f, dir)
	case TTSSpeakFrame:
		a.invalidateResponse()
		a.PushFrame(f, dir)
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
			request := NewLLMMessagesAppendFrame(nil, true)
			request.AfterResponseID = f.ResponseID
			a.handleMessagesAppend(request)
		}
	case TranscriptFrame:
		// Once playback has finished, even partial new input supersedes a
		// delayed continuation. During playback, short backchannels keep
		// their existing behavior; an actual barge-in invalidates below.
		if f.Text != "<end>" && strings.TrimSpace(f.Text) != "" {
			a.state.mu.Lock()
			if !a.botSpeaking || (a.state.responseID != 0 && a.state.completedResponseID == a.state.responseID) {
				a.state.responseID = 0
				a.state.completedResponseID = 0
			}
			a.state.mu.Unlock()
		}
		interimTranscript := a.updateInterimTranscript(f)

		// Barge-in uses the latest non-final response snapshot only.
		// Turn-taking waits for final tokens ending with <end>.
		if a.botSpeaking && !a.interruptSent && !f.IsFinal {
			if len(strings.Fields(interimTranscript)) >= minBargeInWords {
				a.beginInterruption()
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
		a.speakingResponseID = f.ResponseID
		a.PushFrame(f, dir) // continue upstream to UserIdle
	case BotStoppedSpeakingFrame:
		if f.Interrupted {
			if !a.awaitingInterrupt {
				a.beginInterruption()
			}
			if a.awaitingInterrupt {
				a.awaitingInterrupt = false
				text := a.pendingUserText
				a.pendingUserText = ""
				if text != "" {
					a.commitUserMessage(text)
				}
				a.flushPendingRuns()
			}
		} else if f.ResponseID != 0 && a.speakingResponseID != 0 && f.ResponseID != a.speakingResponseID {
			// A's commit may arrive after B has already started playing.
			return
		}
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
