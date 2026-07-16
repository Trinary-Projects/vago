package voicepipelinecore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// LLMResult summarizes a single streaming completion. Implementations
// fill Model with the model that actually served the request (which may
// differ per call when a router selects among endpoints). TTFB/Total
// are best-effort timings and Interrupted reports whether the stream
// ended because its context was cancelled.
type LLMResult struct {
	Model       string
	TTFB        time.Duration
	Total       time.Duration
	Interrupted bool
	ToolCalls   []ToolCall
}

// LLMClient performs one streaming chat completion. It must invoke
// onToken for every non-empty text delta as it arrives, and return the
// model that served the request. The implementation owns provider and
// transport concerns (endpoint selection, auth, health write-backs);
// the LLMProcessor owns pipeline framing, metrics, and cancellation.
//
// Inputs are plain stdlib types; the only core type in the contract is
// the small LLMResult return value, so an implementer (e.g. the
// llmrouter package) imports voicepipelinecore just for that. The
// dependency runs one way — the core never imports the implementer — so
// the pipeline package stays free of any business/transport dependency.
type LLMClient interface {
	Stream(ctx context.Context, req LLMRequest, onToken func(string)) (LLMResult, error)
}

type ToolCallRequest struct {
	FunctionName string
	ToolCallID   string
	Arguments    map[string]any
	RawArguments string
}

type ToolCallResponse struct {
	Result any
	RunLLM bool
}

type ToolHandler func(ctx context.Context, req ToolCallRequest) (ToolCallResponse, error)

type ToolOptions struct {
	CancelOnInterruption bool
	Timeout              time.Duration
}

type registeredTool struct {
	definition ToolDefinition
	handler    ToolHandler
	options    ToolOptions
}

type LLMProcessor struct {
	*BaseProcessor
	taskCtx   *TaskContext
	client    LLMClient
	metrics   *ProcessorMetrics
	cancelMu  sync.Mutex
	cancelLLM context.CancelFunc

	tools      []ToolDefinition
	toolChoice any
	toolMap    map[string]registeredTool
}

// NewLLMProcessorWithClient builds an LLM processor backed by the injected
// client (e.g. the health-based llmrouter for sales calls).
func NewLLMProcessorWithClient(taskCtx *TaskContext, client LLMClient) *LLMProcessor {
	if client == nil {
		panic("voicepipelinecore: LLMClient is required")
	}
	p := &LLMProcessor{
		taskCtx: taskCtx,
		client:  client,
		metrics: NewProcessorMetrics("llm"),
		toolMap: make(map[string]registeredTool),
	}
	p.BaseProcessor = NewBaseProcessor("LLM", p, taskCtx)
	return p
}

func (p *LLMProcessor) RegisterTool(def ToolDefinition, handler ToolHandler, opts ToolOptions) {
	if def.Type == "" {
		def.Type = "function"
	}
	name := strings.TrimSpace(def.Function.Name)
	if name == "" {
		return
	}
	p.tools = appendOrReplaceToolDefinition(p.tools, def)
	p.toolMap[name] = registeredTool{definition: def, handler: handler, options: opts}
}

func (p *LLMProcessor) SetToolChoice(toolChoice any) {
	p.toolChoice = toolChoice
}

func appendOrReplaceToolDefinition(tools []ToolDefinition, def ToolDefinition) []ToolDefinition {
	for i, existing := range tools {
		if existing.Function.Name == def.Function.Name {
			tools[i] = def
			return tools
		}
	}
	return append(tools, def)
}

func (p *LLMProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case EndFrame:
		p.taskCtx.Logger.Printf("EndFrame at LLMProcessor, cancelling LLM: reason=%q\n", f.Reason)
		p.cancelInFlight()
		p.metrics.Reset()
		p.PushFrame(f, dir)
	case LLMMessagesFrame:
		runCtx, cancel := context.WithCancel(ctx)
		p.cancelMu.Lock()
		if p.cancelLLM != nil {
			p.cancelLLM()
		}
		p.cancelLLM = cancel
		p.cancelMu.Unlock()
		p.Go(func() { p.runLLM(runCtx, f.Messages) })
	case InterruptFrame:
		// Base has already cancelled the previous procCtx, which cancels any
		// in-flight runLLM transitively. Clearing the stored cancel func is
		// just bookkeeping.
		p.cancelInFlight()
		p.metrics.Reset()
		p.PushFrame(frame, dir)
	default:
		p.PushFrame(frame, dir)
	}
}

func (p *LLMProcessor) cancelInFlight() {
	p.cancelMu.Lock()
	defer p.cancelMu.Unlock()
	if p.cancelLLM != nil {
		p.cancelLLM()
		p.cancelLLM = nil
	}
}

func (p *LLMProcessor) runLLM(ctx context.Context, messages []Message) {
	p.metrics.Start(MetricTTFB)
	p.metrics.Start(MetricProcessing)
	startedAt := time.Now()
	p.PushFrame(NewLLMResponseStartFrame(startedAt), Downstream)

	var ttfbMs *float64
	var responseText strings.Builder
	firstToken := true
	onToken := func(content string) {
		if content == "" {
			return
		}
		if firstToken {
			firstToken = false
			if mf := p.metrics.Stop(MetricTTFB); mf != nil {
				ttfbMs = float64Ptr(mf.Data[0].ValueMs)
				p.PushFrame(*mf, Downstream)
			}
		}
		responseText.WriteString(content)
		p.PushFrame(NewTextFrame(content), Downstream)
	}

	req := LLMRequest{
		Messages:   cloneMessages(messages),
		Tools:      append([]ToolDefinition(nil), p.tools...),
		ToolChoice: p.toolChoice,
	}
	result, err := p.client.Stream(ctx, req, onToken)
	if firstToken && result.TTFB > 0 {
		ttfbMs = float64Ptr(float64(result.TTFB.Microseconds()) / 1000.0)
		_ = p.metrics.Stop(MetricTTFB)
	}
	totalMs := float64Ptr(millisecondsSince(startedAt))

	// Cancellation (barge-in / EndFrame) takes precedence: the interrupt/
	// end path already reset TTS + playback, so we must NOT emit terminal
	// frames here (they'd be processed after the reset).
	if ctx.Err() != nil {
		p.emitLLMCallResult(result.Model, ttfbMs, totalMs, "interrupted")
		p.fireLLMCallCompleted(responseText.String(), true)
		return
	}
	if err != nil {
		_ = p.metrics.Stop(MetricTTFB)
		_ = p.metrics.Stop(MetricProcessing)
		// Live endpoint error (the router has already blacklisted +
		// triggered a re-poll). The turn fails but the call continues
		// (Python parity: the next turn re-selects). Close the turn so
		// the pipeline returns to a terminal state — LLMResponseEndFrame
		// makes TTS flush any partial text and emit TTSDoneFrame, which
		// PlaybackSink turns into BotStoppedSpeaking so UserIdleProcessor
		// arms its prompt loop. Otherwise the caller sits in silence
		// until the 120s watchdog.
		p.taskCtx.Logger.Println("LLM stream failed:", err)
		// Python parity: pipecat logs the failed call at ERROR level and
		// the bots' loguru sentry_sink forwards every ERROR to Sentry, so
		// a live LLM failure always raises a Sentry issue there. Capture
		// here (never on cancellation — handled above) with the task hub
		// so the event carries the call identity.
		sentryutil.Capture(sentryutil.Event{
			Hub: p.taskCtx.SentryHub(),
			Err: err,
			Tags: map[string]string{
				"component": "llm",
				"operation": "live_stream",
			},
			Details: map[string]any{
				"model":           result.Model,
				"partial_chars":   responseText.Len(),
				"had_first_token": ttfbMs != nil,
			},
		})
		p.PushFrame(NewLLMResponseEndFrame(), Downstream)
		p.emitLLMCallResult(result.Model, ttfbMs, totalMs, "interrupted")
		p.fireLLMCallCompleted(responseText.String(), true)
		return
	}

	if mf := p.metrics.Stop(MetricProcessing); mf != nil {
		totalMs = float64Ptr(mf.Data[0].ValueMs)
		p.PushFrame(*mf, Downstream)
	}
	p.PushFrame(NewLLMResponseEndFrame(), Downstream)
	p.emitLLMCallResult(result.Model, ttfbMs, totalMs, "completed")
	p.fireLLMCallCompleted(responseText.String(), false)
	if len(result.ToolCalls) > 0 {
		p.Go(func() { p.executeToolCalls(ctx, result.ToolCalls) })
	}
}

type toolExecutionResult struct {
	frame  FunctionCallResultFrame
	runLLM bool
}

func (p *LLMProcessor) executeToolCalls(turnCtx context.Context, toolCalls []ToolCall) {
	results := make(chan toolExecutionResult, len(toolCalls))
	runNextLLM := false
	started := 0
	for _, call := range toolCalls {
		name := call.Function.Name
		tool, ok := p.toolMap[name]
		if !ok || tool.handler == nil {
			args, parseErr := parseToolArguments(call.Function.Arguments)
			if parseErr != nil {
				p.PushError(fmt.Sprintf("parse tool arguments for %q: %v", name, parseErr), false)
			}
			p.PushError(fmt.Sprintf("unregistered tool call %q", name), false)
			p.PushFrame(NewFunctionCallInProgressFrame(name, call.ID, args, call.Function.Arguments, true), Upstream)
			results <- toolExecutionResult{
				frame:  NewFunctionCallResultFrame(name, call.ID, args, call.Function.Arguments, toolErrorResultString(fmt.Sprintf("unregistered tool call %q", name)), false),
				runLLM: true,
			}
			started++
			continue
		}
		args, parseErr := parseToolArguments(call.Function.Arguments)
		if parseErr != nil {
			p.PushError(fmt.Sprintf("parse tool arguments for %q: %v", name, parseErr), false)
		}
		p.PushFrame(NewFunctionCallInProgressFrame(name, call.ID, args, call.Function.Arguments, tool.options.CancelOnInterruption), Upstream)
		started++
		go func(call ToolCall, tool registeredTool, args map[string]any) {
			results <- p.executeOneToolCall(turnCtx, call, tool, args)
		}(call, tool, args)
	}

	for remaining := started; remaining > 0; remaining-- {
		result := <-results
		if result.runLLM {
			runNextLLM = true
		}
		if remaining == 1 && runNextLLM {
			result.frame.RunLLM = true
		}
		p.PushFrame(result.frame, Upstream)
	}
}

func (p *LLMProcessor) executeOneToolCall(turnCtx context.Context, call ToolCall, tool registeredTool, args map[string]any) toolExecutionResult {
	name := call.Function.Name
	parent := turnCtx
	if !tool.options.CancelOnInterruption && p.taskCtx != nil && p.taskCtx.Ctx != nil {
		parent = p.taskCtx.Ctx
	}
	toolCtx := parent
	cancel := func() {}
	if tool.options.Timeout > 0 {
		toolCtx, cancel = context.WithTimeout(parent, tool.options.Timeout)
	}
	resp, err := tool.handler(toolCtx, ToolCallRequest{
		FunctionName: name,
		ToolCallID:   call.ID,
		Arguments:    args,
		RawArguments: call.Function.Arguments,
	})
	cancel()
	if err != nil {
		p.PushError(fmt.Sprintf("execute tool %q: %v", name, err), false)
		return toolExecutionResult{
			frame: NewFunctionCallResultFrame(name, call.ID, args, call.Function.Arguments, toolErrorResultString(err.Error()), false),
		}
	}
	result, resultErr := toolResultString(resp.Result)
	if resultErr != nil {
		p.reportToolResultError(name, call.ID, resultErr)
		result = toolErrorResultString(resultErr.Error())
		resp.RunLLM = false
	}
	return toolExecutionResult{
		frame:  NewFunctionCallResultFrame(name, call.ID, args, call.Function.Arguments, result, false),
		runLLM: resp.RunLLM,
	}
}

func parseToolArguments(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, nil
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return map[string]any{}, err
	}
	if args == nil {
		args = map[string]any{}
	}
	return args, nil
}

func toolResultString(result any) (string, error) {
	switch v := result.(type) {
	case nil:
		return "", errors.New("empty tool result")
	case string:
		if strings.TrimSpace(v) == "" {
			return "", errors.New("empty tool result")
		}
		return v, nil
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v), nil
		}
		if len(raw) == 0 || string(raw) == "null" {
			return "", errors.New("empty tool result")
		}
		return string(raw), nil
	}
}

func toolErrorResultString(message string) string {
	raw, err := json.Marshal(map[string]any{"error": message})
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, message)
	}
	return string(raw)
}

func (p *LLMProcessor) reportToolResultError(functionName, toolCallID string, err error) {
	if err == nil {
		return
	}
	p.PushError(fmt.Sprintf("tool %q returned empty result", functionName), false)
	sentryutil.Capture(sentryutil.Event{
		Hub: p.taskCtx.SentryHub(),
		Err: err,
		Tags: map[string]string{
			"component": "llm",
			"operation": "tool_call",
		},
		Details: map[string]any{
			"function_name": functionName,
			"tool_call_id":  toolCallID,
		},
	})
}

// fireLLMCallCompleted forwards the finished call's generated text to
// the OnLLMCallCompleted call event (Python's on_llm_call_complete with
// is_interrupted = not completed).
func (p *LLMProcessor) fireLLMCallCompleted(text string, interrupted bool) {
	if p.taskCtx == nil || p.taskCtx.callEvents == nil {
		return
	}
	p.taskCtx.callEvents.fireLLMCallCompleted(text, interrupted)
}

// emitLLMCallResult publishes the Python-compatible RTVI server-message
// reporting which model served the turn and its latency. Mirrors
// CustomOpenAILLMService.on_llm_call_complete -> rtvi llm_call_result.
func (p *LLMProcessor) emitLLMCallResult(model string, ttfbMs, totalMs *float64, status string) {
	if p.taskCtx == nil || p.taskCtx.UIEvents == nil {
		return
	}
	data := map[string]any{
		"type":     "llm_call_result",
		"status":   status,
		"model":    model,
		"ttfb_ms":  nil,
		"total_ms": nil,
	}
	if ttfbMs != nil {
		data["ttfb_ms"] = *ttfbMs
	}
	if totalMs != nil {
		data["total_ms"] = *totalMs
	}
	p.taskCtx.UIEvents.ServerMessage(data, time.Now())
}

func millisecondsSince(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func float64Ptr(v float64) *float64 {
	return &v
}
