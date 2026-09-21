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

type AssistantContextAggregator struct {
	*BaseProcessor
	mu                      sync.Mutex
	taskCtx                 *TaskContext
	state                   *LLMContext
	playedWords             []string
	functionCallsInProgress map[string]*FunctionCallInProgressFrame
}

func newAssistantContextAggregatorWithState(taskCtx *TaskContext, state *LLMContext) *AssistantContextAggregator {
	a := &AssistantContextAggregator{
		taskCtx:                 taskCtx,
		functionCallsInProgress: make(map[string]*FunctionCallInProgressFrame),
		state:                   state,
	}
	a.BaseProcessor = NewBaseProcessor("AssistantContextAggregator", a, taskCtx)
	return a
}

func (a *AssistantContextAggregator) messagesForTest() []Message {
	return a.state.messagesForTest()
}

func (a *AssistantContextAggregator) appendPlayedAssistantWords(words []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, w := range words {
		if len(a.playedWords) > 0 && len(w) > 0 && w[0] != '.' && w[0] != ',' && w[0] != '!' && w[0] != '?' && w[0] != ';' && w[0] != ':' {
			a.playedWords = append(a.playedWords, " "+w)
		} else {
			a.playedWords = append(a.playedWords, w)
		}
	}
}

func (a *AssistantContextAggregator) playedTextLocked() string {
	var spoken string
	for _, w := range a.playedWords {
		spoken += w
	}
	a.playedWords = nil
	return spoken
}

func (a *AssistantContextAggregator) commitPlayedAssistantText(turn AssistantTurnCompletion) {
	interrupted := turn.Reason == AssistantTurnInterrupted
	a.mu.Lock()
	spoken := a.playedTextLocked()
	a.mu.Unlock()

	var promptKey string
	if spoken != "" {
		a.state.mu.Lock()
		a.state.messages = append(a.state.messages, Message{Role: "assistant", Content: spoken})
		promptKey = a.state.mainAgentSystemPromptLangfuseKey
		a.state.mu.Unlock()
	}

	if spoken != "" {
		a.taskCtx.Logger.Printf("Committing to history (interrupted=%v): %s\n", interrupted, spoken)
		metrics := TurnMetrics{}
		if a.taskCtx.metrics != nil {
			metrics = a.taskCtx.metrics.snapshotAndReset()
		}
		if a.taskCtx.callEvents != nil {
			a.taskCtx.callEvents.fireAssistantTurnCommitted(spoken, time.Now(), metrics, promptKey, turn)
		}
		a.PushFrame(NewLLMContextFrame(a.state), Downstream)
	}
}

func (a *AssistantContextAggregator) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	if dir == Upstream {
		a.PushFrame(frame, dir)
		return
	}
	switch f := frame.(type) {
	case FunctionCallsStartedFrame:
		a.mu.Lock()
		for _, call := range f.ToolCalls {
			a.functionCallsInProgress[call.ID] = nil
		}
		a.mu.Unlock()
	case FunctionCallInProgressFrame:
		a.mu.Lock()
		a.addFunctionCallInProgress(f)
		a.functionCallsInProgress[f.ToolCallID] = &f
		a.mu.Unlock()
	case FunctionCallResultFrame:
		a.mu.Lock()
		_, running := a.functionCallsInProgress[f.ToolCallID]
		if !running {
			a.mu.Unlock()
			return
		}
		delete(a.functionCallsInProgress, f.ToolCallID)
		assistant, result := a.applyFunctionCallResult(f)
		a.mu.Unlock()
		if a.taskCtx.callEvents != nil {
			a.taskCtx.callEvents.fireToolResultCommitted(assistant, result, time.Now())
		}
		if f.RunLLM {
			a.PushFrame(NewLLMContextFrame(a.state), Upstream)
		}
	case FunctionCallCancelFrame:
		a.mu.Lock()
		if call := a.functionCallsInProgress[f.ToolCallID]; call != nil && call.CancelOnInterruption {
			a.applyFunctionCallResult(NewFunctionCallResultFrame(call.FunctionName, call.ToolCallID, call.Arguments, call.RawArguments, "CANCELLED", false))
			delete(a.functionCallsInProgress, f.ToolCallID)
		}
		a.mu.Unlock()
	case LLMMessagesAppendFrame:
		a.state.AddMessages(f.Messages)
		if f.RunLLM {
			a.PushFrame(NewLLMContextFrame(a.state), Upstream)
		}

	case TextFrame:
		// Without a TTS service, plain LLM text is the downstream aggregation source.
		a.mu.Lock()
		a.playedWords = append(a.playedWords, f.Text)
		a.mu.Unlock()
		a.PushFrame(f, dir)
	case WordTimestampFrame:
		// Downstream from PlaybackSink after the audio frame for these words
		// has actually been played.
		a.appendPlayedAssistantWords(f.Words)
		a.PushFrame(f, dir)
	case LLMResponseEndFrame, LLMAssistantPushAggregationFrame:
		a.commitPlayedAssistantText(AssistantTurnCompletion{Reason: AssistantTurnPlaybackCompleted})
		a.PushFrame(f, dir)
	case InterruptFrame:
		a.commitPlayedAssistantText(AssistantTurnCompletion{Reason: AssistantTurnInterrupted})
		a.PushFrame(f, dir)
	case EndFrame:
		a.taskCtx.Logger.Printf("EndFrame at AssistantContextAggregator: reason=%q\n", f.Reason)
		a.commitPlayedAssistantText(AssistantTurnCompletion{Reason: AssistantTurnEnding})
		a.PushFrame(f, dir)
	default:
		a.PushFrame(frame, dir)
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

func (a *AssistantContextAggregator) addFunctionCallInProgress(f FunctionCallInProgressFrame) {
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

func (a *AssistantContextAggregator) applyFunctionCallResult(f FunctionCallResultFrame) (Message, Message) {
	result := strings.TrimSpace(f.Result)
	if result == "" {
		err := errors.New("empty tool result")
		a.PushError("empty tool result", false)
		sentryutil.Capture(sentryutil.Event{
			Hub: a.taskCtx.SentryHub(),
			Err: err,
			Tags: map[string]string{
				"component": "assistant_context_aggregator",
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
