package voicepipelinecore

import (
	"strings"
	"testing"
	"time"
)

func TestAssistantContextAggregator_FunctionCallFramesUpdateContextAndRunLLM(t *testing.T) {
	fix := newTestFixture(t)
	a := NewContextAggregatorPair(fix.TaskCtx, []Message{
		{Role: "system", Content: "sales prompt"},
	}, "").Assistant()

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewFunctionCallInProgressFrame("get_guidance", "call_1", map[string]any{"situation": "pain"}, `{"situation":"pain"}`, false), Downstream)
	time.Sleep(20 * time.Millisecond)
	source.QueueFrame(NewFunctionCallResultFrame("get_guidance", "call_1", map[string]any{"situation": "pain"}, `{"situation":"pain"}`, "guidance text", true), Downstream)
	time.Sleep(30 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)

	messages := a.messagesForTest()
	if len(messages) != 3 {
		t.Fatalf("context messages = %+v, want prompt + assistant tool call + tool result", messages)
	}
	assistant := messages[1]
	if assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 {
		t.Fatalf("assistant tool message = %+v, want one tool call", assistant)
	}
	if assistant.ToolCalls[0].ID != "call_1" || assistant.ToolCalls[0].Function.Name != "get_guidance" {
		t.Fatalf("tool call = %+v, want call_1 get_guidance", assistant.ToolCalls[0])
	}
	if assistant.ToolCalls[0].Function.Arguments != `{"situation":"pain"}` {
		t.Fatalf("tool arguments = %q, want raw JSON", assistant.ToolCalls[0].Function.Arguments)
	}
	tool := messages[2]
	if tool.Role != "tool" || tool.ToolCallID != "call_1" || tool.Content != "guidance text" {
		t.Fatalf("tool result message = %+v, want call_1 guidance text", tool)
	}

	llmMsg, ok := findFrame[LLMContextFrame](source.Captured())
	if !ok {
		t.Fatalf("expected LLMContextFrame after tool result, got %s", describeFrameTypes(sink.Captured()))
	}
	if len(llmMsg.Context.GetMessages()) != 3 || llmMsg.Context.GetMessages()[2].Role != "tool" || llmMsg.Context.GetMessages()[2].Content != "guidance text" {
		t.Fatalf("LLM context after tool result = %+v", llmMsg.Context.GetMessages())
	}
}

func TestAssistantContextAggregator_FunctionCallsUsePipecatAssistantToolPairs(t *testing.T) {
	fix := newTestFixture(t)
	a := NewContextAggregatorPair(fix.TaskCtx, []Message{
		{Role: "system", Content: "sales prompt"},
	}, "").Assistant()

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewFunctionCallInProgressFrame("get_guidance", "call_1", nil, `{"situation":"pain"}`, false), Downstream)
	source.QueueFrame(NewFunctionCallInProgressFrame("lookup_plan", "call_2", nil, `{"plan":"starter"}`, false), Downstream)
	time.Sleep(20 * time.Millisecond)
	source.QueueFrame(NewFunctionCallResultFrame("get_guidance", "call_1", nil, `{"situation":"pain"}`, "guidance text", false), Downstream)
	source.QueueFrame(NewFunctionCallResultFrame("lookup_plan", "call_2", nil, `{"plan":"starter"}`, "plan text", true), Downstream)
	time.Sleep(30 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)

	messages := a.messagesForTest()
	if len(messages) != 5 {
		t.Fatalf("context messages = %+v, want prompt + two assistant/tool pairs", messages)
	}
	assistant := messages[1]
	if assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 {
		t.Fatalf("first assistant tool message = %+v, want one tool call", assistant)
	}
	if assistant.ToolCalls[0].Function.Name != "get_guidance" {
		t.Fatalf("first tool call = %+v, want get_guidance", assistant.ToolCalls)
	}
	if messages[2].Role != "tool" || messages[2].ToolCallID != "call_1" || messages[2].Content != "guidance text" {
		t.Fatalf("first tool result = %+v", messages[2])
	}
	assistant = messages[3]
	if assistant.Role != "assistant" || len(assistant.ToolCalls) != 1 {
		t.Fatalf("second assistant tool message = %+v, want one tool call", assistant)
	}
	if assistant.ToolCalls[0].Function.Name != "lookup_plan" {
		t.Fatalf("second tool call = %+v, want lookup_plan", assistant.ToolCalls)
	}
	if messages[4].Role != "tool" || messages[4].ToolCallID != "call_2" || messages[4].Content != "plan text" {
		t.Fatalf("second tool result = %+v", messages[4])
	}
	llmMsg, ok := findFrame[LLMContextFrame](source.Captured())
	if !ok {
		t.Fatalf("expected LLMContextFrame after final tool result, got %s", describeFrameTypes(sink.Captured()))
	}
	if len(llmMsg.Context.GetMessages()) != 5 || len(llmMsg.Context.GetMessages()[1].ToolCalls) != 1 || len(llmMsg.Context.GetMessages()[3].ToolCalls) != 1 {
		t.Fatalf("LLM context after tool results = %+v", llmMsg.Context.GetMessages())
	}
}

func TestAssistantContextAggregator_EmptyFunctionResultPushesError(t *testing.T) {
	fix := newTestFixture(t)
	a := NewContextAggregatorPair(fix.TaskCtx, []Message{
		{Role: "system", Content: "sales prompt"},
	}, "").Assistant()

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewFunctionCallInProgressFrame("get_guidance", "call_1", nil, `{"situation":"pain"}`, false), Downstream)
	time.Sleep(20 * time.Millisecond)
	source.QueueFrame(NewFunctionCallResultFrame("get_guidance", "call_1", nil, `{"situation":"pain"}`, "", true), Downstream)
	time.Sleep(30 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)

	if c := countFrames[ErrorFrame](source.Captured()); c != 1 {
		t.Fatalf("expected one ErrorFrame for empty tool result, got %d in %s", c, describeFrameTypes(source.Captured()))
	}
	messages := a.messagesForTest()
	if len(messages) != 3 {
		t.Fatalf("context messages = %+v", messages)
	}
	tool := messages[2]
	if tool.Role != "tool" || tool.ToolCallID != "call_1" || !strings.Contains(tool.Content, "empty tool result") {
		t.Fatalf("tool result message = %+v, want explicit empty-result error", tool)
	}
}

func TestAssistantContextAggregator_EmitsToolResultCallEvent(t *testing.T) {
	fix := newTestFixture(t)
	var assistantToolCalls []Message
	var toolResults []Message
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnToolResultCommitted: func(assistantToolCall Message, toolResult Message, at time.Time) {
			assistantToolCalls = append(assistantToolCalls, assistantToolCall)
			toolResults = append(toolResults, toolResult)
		},
	})
	a := NewContextAggregatorPair(fix.TaskCtx, []Message{{Role: "system", Content: "prompt"}}, "").Assistant()

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewFunctionCallInProgressFrame("get_guidance", "call_1", nil, `{"situation":"pain"}`, false), Downstream)
	time.Sleep(20 * time.Millisecond)
	source.QueueFrame(NewFunctionCallResultFrame("get_guidance", "call_1", nil, `{"situation":"pain"}`, "guidance text", false), Downstream)
	time.Sleep(30 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)
	fix.TaskCtx.callEvents.stopAndDrain()

	if len(assistantToolCalls) != 1 || len(toolResults) != 1 {
		t.Fatalf("tool events = assistant:%+v tool:%+v", assistantToolCalls, toolResults)
	}
	if assistantToolCalls[0].Role != "assistant" ||
		len(assistantToolCalls[0].ToolCalls) != 1 ||
		assistantToolCalls[0].ToolCalls[0].ID != "call_1" ||
		assistantToolCalls[0].ToolCalls[0].Function.Name != "get_guidance" ||
		assistantToolCalls[0].ToolCalls[0].Function.Arguments != `{"situation":"pain"}` {
		t.Fatalf("assistant tool event = %+v", assistantToolCalls[0])
	}
	if toolResults[0].Role != "tool" || toolResults[0].ToolCallID != "call_1" || toolResults[0].Content != "guidance text" {
		t.Fatalf("tool result event = %+v", toolResults[0])
	}
}
