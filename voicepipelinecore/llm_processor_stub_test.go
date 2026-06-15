package voicepipelinecore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// stubLLMClient is a deterministic LLMClient for verifying the processor
// delegates streaming to its client and reports the client's model.
type stubLLMClient struct {
	tokens    []string
	model     string
	err       error // non-nil simulates a live endpoint error after the tokens
	toolCalls []ToolCall
	responses []stubLLMResponse

	mu       sync.Mutex
	requests []LLMRequest
	calls    int
}

type stubLLMResponse struct {
	tokens                []string
	model                 string
	err                   error
	toolCalls             []ToolCall
	blockBeforeTokenIndex int
	blockUntil            <-chan struct{}
}

func (s *stubLLMClient) Stream(ctx context.Context, req LLMRequest, onToken func(string)) (LLMResult, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	callIndex := s.calls
	s.calls++
	resp := stubLLMResponse{
		tokens:    s.tokens,
		model:     s.model,
		err:       s.err,
		toolCalls: s.toolCalls,
	}
	if callIndex < len(s.responses) {
		resp = s.responses[callIndex]
		if resp.model == "" {
			resp.model = s.model
		}
	}
	s.mu.Unlock()

	for i, tok := range resp.tokens {
		if ctx.Err() != nil {
			return LLMResult{Model: resp.model, Interrupted: true}, ctx.Err()
		}
		if resp.blockUntil != nil && i == resp.blockBeforeTokenIndex {
			select {
			case <-resp.blockUntil:
			case <-ctx.Done():
				return LLMResult{Model: resp.model, Interrupted: true}, ctx.Err()
			}
		}
		onToken(tok)
	}
	if resp.err != nil {
		return LLMResult{Model: resp.model}, resp.err
	}
	return LLMResult{Model: resp.model, TTFB: 10 * time.Millisecond, Total: 20 * time.Millisecond, ToolCalls: resp.toolCalls}, nil
}

func (s *stubLLMClient) Requests() []LLMRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LLMRequest, len(s.requests))
	copy(out, s.requests)
	return out
}

// TestLLM_DelegatesToClientAndReportsModel verifies NewLLMProcessorWithClient
// streams the client's tokens downstream and emits an llm_call_result
// RTVI event carrying the real model the client reported.
func TestLLM_DelegatesToClientAndReportsModel(t *testing.T) {
	fix := newTestFixture(t)
	client := &stubLLMClient{tokens: []string{"foo ", "bar"}, model: "grok-4-1-fast-non-reasoning"}
	p := NewLLMProcessorWithClient(fix.TaskCtx, client)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			LLMMessagesFrame{Messages: []Message{{Role: "user", Content: "hi"}}},
		},
		settleDelay:  200 * time.Millisecond,
		sendEndFrame: true,
	})

	if _, ok := findFrame[LLMResponseStartFrame](down); !ok {
		t.Errorf("expected LLMResponseStartFrame, got %s", describeFrameTypes(down))
	}
	if c := countFrames[TextFrame](down); c != 2 {
		t.Errorf("expected 2 TextFrames, got %d in %s", c, describeFrameTypes(down))
	}
	if _, ok := findFrame[LLMResponseEndFrame](down); !ok {
		t.Errorf("expected LLMResponseEndFrame, got %s", describeFrameTypes(down))
	}

	var combined string
	for _, f := range down {
		if tf, ok := f.(TextFrame); ok {
			combined += tf.Text
		}
	}
	if combined != "foo bar" {
		t.Errorf("combined text: got %q, want %q", combined, "foo bar")
	}

	found := false
	for _, e := range fix.TaskCtx.UIEvents.Snapshot() {
		if e.Type != "server-message" {
			continue
		}
		data, ok := e.Data.(map[string]any)
		if ok && data["type"] == "llm_call_result" && data["model"] == "grok-4-1-fast-non-reasoning" && data["status"] == "completed" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected llm_call_result server-message with the client's model")
	}
}

// TestLLM_LiveErrorClosesTurn verifies that a live (non-cancellation)
// client error still emits LLMResponseEndFrame, so the turn returns to a
// terminal state (TTS flushes, UserIdle can arm) instead of leaving the
// pipeline half-open. No TextFrames are produced.
func TestLLM_LiveErrorClosesTurn(t *testing.T) {
	fix := newTestFixture(t)
	client := &stubLLMClient{model: "grok-4-1-fast-non-reasoning", err: errors.New("endpoint 500")}
	p := NewLLMProcessorWithClient(fix.TaskCtx, client)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			LLMMessagesFrame{Messages: []Message{{Role: "user", Content: "hi"}}},
		},
		settleDelay:  200 * time.Millisecond,
		sendEndFrame: true,
	})

	if _, ok := findFrame[LLMResponseStartFrame](down); !ok {
		t.Errorf("expected LLMResponseStartFrame, got %s", describeFrameTypes(down))
	}
	if _, ok := findFrame[LLMResponseEndFrame](down); !ok {
		t.Errorf("a live error must still close the turn with LLMResponseEndFrame, got %s", describeFrameTypes(down))
	}
	if c := countFrames[TextFrame](down); c != 0 {
		t.Errorf("expected no TextFrames on a live error, got %d", c)
	}
}

func TestLLM_NativeToolCallLoopUsesRegisteredToolAndContext(t *testing.T) {
	fix := newTestFixture(t)
	client := &stubLLMClient{
		model: "grok-4-1-fast-non-reasoning",
		responses: []stubLLMResponse{
			{
				toolCalls: []ToolCall{{
					ID:   "call_1",
					Type: "function",
					Function: ToolCallFunction{
						Name:      "get_guidance",
						Arguments: `{"situation":"pain"}`,
					},
				}},
			},
			{tokens: []string{"guidance ", "received"}},
		},
	}
	aggregator := NewUserContextAggregator(fix.TaskCtx, []Message{
		{Role: "system", Content: "sales prompt"},
		{Role: "user", Content: "hello?"},
	}, "")
	llm := NewLLMProcessorWithClient(fix.TaskCtx, client)

	handlerCalls := make(chan ToolCallRequest, 1)
	llm.RegisterTool(ToolDefinition{
		Type: "function",
		Function: ToolFunction{
			Name:        "get_guidance",
			Description: "Fetch guidance for the next turn.",
			Parameters:  map[string]any{"type": "object"},
		},
	}, func(ctx context.Context, req ToolCallRequest) (ToolCallResponse, error) {
		handlerCalls <- req
		return ToolCallResponse{Result: "guidance text", RunLLM: true}, nil
	}, ToolOptions{})

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(aggregator)
	aggregator.Link(llm)
	llm.Link(sink)
	source.Start(fix.RootCtx)
	aggregator.Start(fix.RootCtx)
	llm.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(LLMMessagesAppendFrame{RunLLM: true}, Downstream)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countFrames[TextFrame](sink.Captured()) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	source.QueueFrame(EndFrame{}, Downstream)
	time.Sleep(50 * time.Millisecond)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, aggregator, llm, sink)

	select {
	case req := <-handlerCalls:
		if req.FunctionName != "get_guidance" || req.ToolCallID != "call_1" {
			t.Fatalf("handler request metadata = %+v", req)
		}
		if req.Arguments["situation"] != "pain" {
			t.Fatalf("handler arguments = %+v, want situation=pain", req.Arguments)
		}
	default:
		t.Fatal("tool handler was not called")
	}

	requests := client.Requests()
	if len(requests) != 2 {
		t.Fatalf("client requests = %+v, want two LLM calls", requests)
	}
	if len(requests[0].Tools) != 1 || requests[0].Tools[0].Function.Name != "get_guidance" {
		t.Fatalf("first request tools = %+v, want get_guidance", requests[0].Tools)
	}
	secondMessages := requests[1].Messages
	if len(secondMessages) != 4 {
		t.Fatalf("second request messages = %+v, want prompt + user + assistant tool call + tool result", secondMessages)
	}
	assistant := secondMessages[2]
	if assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].Function.Name != "get_guidance" {
		t.Fatalf("assistant tool message = %+v", assistant)
	}
	tool := secondMessages[3]
	if tool.Role != "tool" || tool.ToolCallID != "call_1" || tool.Content != "guidance text" {
		t.Fatalf("tool result message = %+v", tool)
	}
	if c := countFrames[FunctionCallInProgressFrame](source.Captured()); c != 1 {
		t.Fatalf("FunctionCallInProgressFrame count = %d, want 1", c)
	}
	if c := countFrames[FunctionCallResultFrame](source.Captured()); c != 1 {
		t.Fatalf("FunctionCallResultFrame count = %d, want 1", c)
	}

	var combined string
	for _, f := range sink.Captured() {
		if tf, ok := f.(TextFrame); ok {
			combined += tf.Text
		}
	}
	if combined != "guidance received" {
		t.Fatalf("text output = %q, want guidance received", combined)
	}
}

func TestLLM_ToolCallsStartInParallel(t *testing.T) {
	fix := newTestFixture(t)
	client := &stubLLMClient{
		model: "grok-4-1-fast-non-reasoning",
		responses: []stubLLMResponse{
			{
				toolCalls: []ToolCall{
					{
						ID:   "call_1",
						Type: "function",
						Function: ToolCallFunction{
							Name:      "first_tool",
							Arguments: `{"value":"a"}`,
						},
					},
					{
						ID:   "call_2",
						Type: "function",
						Function: ToolCallFunction{
							Name:      "second_tool",
							Arguments: `{"value":"b"}`,
						},
					},
				},
			},
			{tokens: []string{"done"}},
		},
	}
	aggregator := NewUserContextAggregator(fix.TaskCtx, []Message{
		{Role: "system", Content: "sales prompt"},
		{Role: "user", Content: "hello?"},
	}, "")
	llm := NewLLMProcessorWithClient(fix.TaskCtx, client)

	started := make(chan string, 2)
	release := make(chan struct{})
	handler := func(ctx context.Context, req ToolCallRequest) (ToolCallResponse, error) {
		started <- req.FunctionName
		select {
		case <-release:
		case <-ctx.Done():
			return ToolCallResponse{}, ctx.Err()
		}
		return ToolCallResponse{Result: req.FunctionName + " result", RunLLM: true}, nil
	}
	for _, name := range []string{"first_tool", "second_tool"} {
		llm.RegisterTool(ToolDefinition{
			Type: "function",
			Function: ToolFunction{
				Name:       name,
				Parameters: map[string]any{"type": "object"},
			},
		}, handler, ToolOptions{})
	}

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(aggregator)
	aggregator.Link(llm)
	llm.Link(sink)
	source.Start(fix.RootCtx)
	aggregator.Start(fix.RootCtx)
	llm.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(LLMMessagesAppendFrame{RunLLM: true}, Downstream)

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case name := <-started:
			seen[name] = true
		case <-time.After(time.Second):
			close(release)
			t.Fatalf("tool calls did not both start concurrently; seen=%v", seen)
		}
	}
	close(release)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countFrames[TextFrame](sink.Captured()) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	source.QueueFrame(EndFrame{}, Downstream)
	time.Sleep(50 * time.Millisecond)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, aggregator, llm, sink)

	requests := client.Requests()
	if len(requests) != 2 {
		t.Fatalf("client requests = %+v, want two LLM calls", requests)
	}
	secondMessages := requests[1].Messages
	if len(secondMessages) != 6 {
		t.Fatalf("second request messages = %+v, want prompt + user + two assistant/tool pairs", secondMessages)
	}
	if len(secondMessages[2].ToolCalls) != 1 || len(secondMessages[4].ToolCalls) != 1 {
		t.Fatalf("assistant tool calls = %+v / %+v, want one call per assistant message", secondMessages[2].ToolCalls, secondMessages[4].ToolCalls)
	}
}
