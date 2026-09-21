package voicepipelinecore

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestQueuedLLMContextReadsMessagesAtProcessingTime(t *testing.T) {
	fix := newTestFixture(t)
	release := make(chan struct{})
	client := &stubLLMClient{responses: []stubLLMResponse{{tokens: []string{"A"}, blockUntil: release}, {tokens: []string{"B"}}}}
	llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
	llm.SetMessagesEnricher(func(_ context.Context, messages []Message) []Message {
		messages[0].Content += " enriched"
		return messages
	})
	sink := newQueueProcessor(fix.TaskCtx, "output", Downstream)
	llm.Link(sink)
	sink.Start(fix.RootCtx)
	llm.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, llm, sink)
	shared := NewLLMContext([]Message{{Role: "system", Content: "old stage"}, {Role: "user", Content: "hello"}})
	llm.QueueFrame(NewLLMContextFrame(shared), Downstream)
	awaitSpeechCondition(t, "first generation started", func() bool { return len(client.Requests()) == 1 })
	llm.QueueFrame(NewLLMContextFrame(shared), Downstream)
	shared.replaceSystemMessage("new stage")
	shared.AddMessages([]Message{{Role: "user", Content: "continue"}})
	close(release)
	awaitSpeechCondition(t, "queued generation started", func() bool { return len(client.Requests()) == 2 })
	first, second := client.Requests()[0].Messages, client.Requests()[1].Messages
	if first[0].Content != "old stage enriched" || len(first) != 2 || second[0].Content != "new stage enriched" || len(second) != 3 {
		t.Fatalf("provider snapshots: first=%+v second=%+v", first, second)
	}
}

func TestMessageAppendIsImmediateAndDoesNotDeduplicateRuns(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	sink := newQueueProcessor(fix.TaskCtx, "requests", Downstream)
	pair.User().Link(sink)
	sink.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, sink)
	frame := NewLLMMessagesAppendFrame([]Message{{Role: "user", Content: "continue"}}, true)
	pair.User().ProcessFrame(fix.RootCtx, frame, Downstream)
	pair.User().ProcessFrame(fix.RootCtx, frame, Downstream)
	awaitSpeechCondition(t, "two context requests", func() bool { return countFrames[LLMContextFrame](sink.Captured()) == 2 })
	messages := pair.MessagesSnapshot()
	if messages[len(messages)-1].Content != "continue" || messages[len(messages)-2].Content != "continue" {
		t.Fatalf("appended messages=%+v", messages)
	}
	for _, f := range sink.Captured() {
		if request, ok := f.(LLMContextFrame); ok && request.Context != pair.user.state {
			t.Fatal("request copied the shared context")
		}
	}
}

func TestContinuationInstructionUsesNormalUserMessageMerging(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	instruction := "<system_message>continue</system_message>"
	pair.User().ProcessFrame(fix.RootCtx, NewLLMMessagesAppendFrame([]Message{{Role: "user", Content: instruction}}, true), Downstream)
	pair.User().ProcessFrame(fix.RootCtx, NewTranscriptFrame("my actual answer", true, 1, false), Downstream)
	pair.User().ProcessFrame(fix.RootCtx, NewTranscriptFrame("<end>", true, 1, false), Downstream)
	messages := pair.MessagesSnapshot()
	if messages[len(messages)-1].Content != instruction+" my actual answer" {
		t.Fatalf("messages=%+v", messages)
	}
}

func TestAssistantHistoryContainsOnlyDownstreamText(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	before := len(pair.MessagesSnapshot())
	pair.User().ProcessFrame(fix.RootCtx, NewLLMMessagesAppendFrame(nil, true), Downstream)
	pair.User().ProcessFrame(fix.RootCtx, NewLLMResponseEndFrame(), Upstream)
	if len(pair.MessagesSnapshot()) != before {
		t.Fatal("generation request/end added speculative history")
	}
	pair.Assistant().ProcessFrame(fix.RootCtx, NewWordTimestampFrame([]string{"actually", "heard"}), Downstream)
	pair.Assistant().ProcessFrame(fix.RootCtx, NewInterruptFrame(), Downstream)
	messages := pair.MessagesSnapshot()
	if len(messages) != before+1 || messages[len(messages)-1].Content != "actually heard" {
		t.Fatalf("messages=%+v", messages)
	}
}

func TestNativeToolCancellationAndLateResult(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	assistant := pair.Assistant()
	assistant.ProcessFrame(fix.RootCtx, NewFunctionCallInProgressFrame("lookup", "call-1", nil, "{}", true), Downstream)
	assistant.ProcessFrame(fix.RootCtx, NewFunctionCallCancelFrame("lookup", "call-1"), Downstream)
	assistant.ProcessFrame(fix.RootCtx, NewFunctionCallResultFrame("lookup", "call-1", nil, "{}", "late result", true), Downstream)
	messages := pair.MessagesSnapshot()
	if messages[len(messages)-1].Content != "CANCELLED" || len(assistant.functionCallsInProgress) != 0 {
		t.Fatalf("tool state=%+v", messages)
	}
}

func TestLLMEnrichmentPreservesConcurrentHistoryUpdates(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, []Message{
		{Role: "system", Content: "prompt"},
		{Role: "user", Content: "hello"},
	}, "")
	assistant := pair.Assistant()
	assistant.ProcessFrame(fix.RootCtx, NewFunctionCallInProgressFrame("lookup", "call-1", nil, "{}", false), Downstream)
	before := pair.MessagesSnapshot()
	started, release := make(chan struct{}), make(chan struct{})
	finished := make(chan time.Time, 1)
	client := &stubLLMClient{tokens: []string{"response"}}
	llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
	llm.SetMessagesEnricher(func(ctx context.Context, messages []Message) []Message {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil
		}
		messages[0].Content = "request-only prompt"
		messages[2].ToolCalls[0].Function.Arguments = `{"request_only":true}`
		finished <- time.Now()
		return append(messages, Message{Role: "user", Content: "retrieved protocol"})
	})
	sink := newQueueProcessor(fix.TaskCtx, "capture", Downstream)
	llm.Link(sink)
	sink.Start(fix.RootCtx)
	llm.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, llm, sink)
	llm.QueueFrame(NewLLMContextFrame(pair.user.state), Downstream)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("enrichment did not start")
	}
	// These writes happen after the request snapshot was taken but before
	// enrichment returns. None may be replaced by that earlier snapshot.
	assistant.ProcessFrame(fix.RootCtx, NewWordTimestampFrame([]string{"already", "heard"}), Downstream)
	assistant.ProcessFrame(fix.RootCtx, NewLLMResponseEndFrame(), Downstream)
	pair.User().ProcessFrame(fix.RootCtx, NewLLMMessagesAppendFrame([]Message{{Role: "user", Content: "new patient input"}}, false), Downstream)
	assistant.ProcessFrame(fix.RootCtx, NewFunctionCallResultFrame("lookup", "call-1", nil, "{}", "completed lookup", false), Downstream)
	after := pair.MessagesSnapshot()
	if len(after) != len(before)+2 || after[3].Content != "completed lookup" {
		t.Fatalf("concurrent history writes did not land: %+v", after)
	}
	if len(client.Requests()) != 0 || countFrames[LLMResponseStartFrame](sink.Captured()) != 0 {
		t.Fatal("generation started before enrichment completed")
	}
	close(release)
	awaitSpeechCondition(t, "enriched generation completed", func() bool { return countFrames[LLMResponseEndFrame](sink.Captured()) == 1 })
	if got := pair.MessagesSnapshot(); !reflect.DeepEqual(got, after) {
		t.Fatalf("enrichment overwrote concurrent history: got %+v, want %+v", got, after)
	}
	outgoing := client.Requests()[0].Messages
	if len(outgoing) != len(before)+1 || outgoing[0].Content != "request-only prompt" || outgoing[len(outgoing)-1].Content != "retrieved protocol" {
		t.Fatalf("outgoing snapshot was not enriched: %+v", outgoing)
	}
	start, _ := findFrame[LLMResponseStartFrame](sink.Captured())
	if start.StartedAt.Before(<-finished) {
		t.Fatal("LLM timing boundary started before enrichment completed")
	}
}

func TestAssistantWordsAndResponseBoundariesStayOrdered(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	assistant := pair.Assistant()
	sink := newQueueProcessor(fix.TaskCtx, "commits", Downstream)
	assistant.Link(sink)
	// Backlog the real ingress to catch word frames overtaking response-end.
	for _, f := range []Frame{
		NewWordTimestampFrame([]string{"first"}), NewLLMResponseEndFrame(),
		NewWordTimestampFrame([]string{"second"}), NewLLMResponseEndFrame(),
	} {
		assistant.QueueFrame(f, Downstream)
	}
	sink.Start(fix.RootCtx)
	assistant.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, assistant, sink)
	awaitSpeechCondition(t, "two commits", func() bool { return countFrames[LLMContextFrame](sink.Captured()) == 2 })
	var texts []string
	for _, m := range pair.MessagesSnapshot() {
		if m.Role == "assistant" {
			texts = append(texts, m.Content)
		}
	}
	if len(texts) != 2 || texts[0] != "first" || texts[1] != "second" {
		t.Fatalf("committed utterances=%v", texts)
	}
}

func TestLLMToolInterruptionUsesNativeCancellationPolicy(t *testing.T) {
	for _, cancellable := range []bool{true, false} {
		name := "cancel on interruption"
		if !cancellable {
			name = "finish despite interruption"
		}
		t.Run(name, func(t *testing.T) {
			fix := newTestFixture(t)
			pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
			client := &stubLLMClient{responses: []stubLLMResponse{
				{toolCalls: []ToolCall{{ID: "lookup-1", Type: "function", Function: ToolCallFunction{Name: "lookup", Arguments: "{}"}}}},
				{tokens: []string{"result received"}},
			}}
			llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
			release := make(chan struct{})
			started := make(chan struct{})
			llm.RegisterTool(ToolDefinition{Function: ToolFunction{Name: "lookup"}}, func(ctx context.Context, _ ToolCallRequest) (ToolCallResponse, error) {
				close(started)
				select {
				case <-ctx.Done():
					return ToolCallResponse{}, ctx.Err()
				case <-release:
				}
				return ToolCallResponse{Result: "found", RunLLM: true}, nil
			}, ToolOptions{CancelOnInterruption: cancellable})
			source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
			sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
			processors := []Processor{source, pair.User(), llm, pair.Assistant(), sink}
			NewPipeline(processors).Start(fix.RootCtx)
			defer stopProcessorsAndWait(t, fix, time.Second, processors...)
			pair.User().QueueFrame(NewLLMMessagesAppendFrame(nil, true), Downstream)
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("tool did not start")
			}
			awaitSpeechCondition(t, "tool entered assistant context", func() bool {
				msgs := pair.MessagesSnapshot()
				return msgs[len(msgs)-1].Content == "IN_PROGRESS"
			})
			llm.QueueFrame(NewInterruptFrame(), Downstream)
			awaitSpeechCondition(t, "interruption forwarded", func() bool { return countFrames[InterruptFrame](sink.Captured()) == 1 })
			if cancellable {
				awaitSpeechCondition(t, "native cancellation in context", func() bool {
					msgs := pair.MessagesSnapshot()
					return msgs[len(msgs)-1].Content == "CANCELLED"
				})
				if len(client.Requests()) != 1 {
					t.Fatal("cancelled tool reran LLM")
				}
				awaitSpeechCondition(t, "cancellation broadcast upstream", func() bool { return countFrames[FunctionCallCancelFrame](source.Captured()) == 1 })
			} else {
				close(release)
				awaitSpeechCondition(t, "tool result requested inference", func() bool { return len(client.Requests()) == 2 })
				if countFrames[FunctionCallCancelFrame](source.Captured()) != 0 {
					t.Fatal("non-cancellable tool was cancelled")
				}
			}
		})
	}
}
