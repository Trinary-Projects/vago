package voicepipelinecore

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestLLMEnrichmentOnlyChangesOutgoingCopy(t *testing.T) {
	for _, sharedFrame := range []bool{true, false} {
		name := "shared context"
		if !sharedFrame {
			name = "legacy messages"
		}
		t.Run(name, func(t *testing.T) {
			for _, result := range []string{"enriched", "nil", "empty"} {
				t.Run(result, func(t *testing.T) {
					fix := newTestFixture(t)
					original := []Message{{Role: "system", Content: "prompt"}, {Role: "user", Content: "hello"}}
					messages := cloneMessages(original)
					shared := NewLLMContext(original)
					client := &stubLLMClient{tokens: []string{"response"}}
					llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
					llm.SetMessagesEnricher(func(_ context.Context, copy []Message) []Message {
						copy[0].Content = "enriched prompt"
						switch result {
						case "nil":
							return nil
						case "empty":
							return []Message{}
						default:
							return append(copy, Message{Role: "user", Content: "retrieved protocol"})
						}
					})
					var frame Frame = NewLLMMessagesFrame(messages)
					if sharedFrame {
						frame = NewLLMContextFrame(shared)
					}
					llm.ProcessFrame(fix.RootCtx, frame, Downstream)
					if !reflect.DeepEqual(shared.GetMessages(), original) || !reflect.DeepEqual(messages, original) {
						t.Fatal("enrichment modified the shared context or the incoming frame")
					}
					requests := client.Requests()
					if len(requests) != 1 {
						t.Fatalf("requests=%+v", requests)
					}
					want := cloneMessages(original)
					if result == "enriched" {
						want[0].Content = "enriched prompt"
						want = append(want, Message{Role: "user", Content: "retrieved protocol"})
					}
					if !reflect.DeepEqual(requests[0].Messages, want) {
						t.Fatalf("outgoing messages=%+v, want %+v", requests[0].Messages, want)
					}
					metrics := fix.Metrics()
					if len(metrics) == 0 || metrics[0].Data[0].Processor != "context_enricher" || metrics[0].Data[0].Label != MetricContextEnrich {
						t.Fatalf("enrichment must retain its separate metric before LLM metrics: %+v", metrics)
					}
				})
			}
		})
	}
}

func TestLLMEnrichmentCancelledBeforeProviderRequest(t *testing.T) {
	fix := newTestFixture(t)
	client := &stubLLMClient{tokens: []string{"response"}}
	llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
	started, cancelled := make(chan struct{}), make(chan struct{})
	llm.SetMessagesEnricher(func(ctx context.Context, messages []Message) []Message {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return append(messages, Message{Role: "user", Content: "abandoned enrichment"})
	})
	sink := newQueueProcessor(fix.TaskCtx, "capture", Downstream)
	llm.Link(sink)
	sink.Start(fix.RootCtx)
	llm.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, llm, sink)
	shared := NewLLMContext(testInitialMessages())
	llm.QueueFrame(NewLLMContextFrame(shared), Downstream)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("enrichment did not start")
	}
	llm.QueueFrame(NewInterruptFrame(), Downstream)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("interruption did not cancel enrichment")
	}
	awaitSpeechCondition(t, "interruption forwarded", func() bool { return countFrames[InterruptFrame](sink.Captured()) == 1 })
	if len(client.Requests()) != 0 || countFrames[LLMResponseStartFrame](sink.Captured()) != 0 {
		t.Fatal("cancelled enrichment started an LLM request")
	}
	if !reflect.DeepEqual(shared.GetMessages(), testInitialMessages()) {
		t.Fatal("cancelled enrichment changed shared history")
	}
}

func TestLLMEnrichmentSkippedForAlreadyCancelledRequest(t *testing.T) {
	fix := newTestFixture(t)
	client := &stubLLMClient{}
	llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
	llm.SetMessagesEnricher(func(_ context.Context, messages []Message) []Message {
		t.Fatal("enrichment ran after cancellation")
		return messages
	})
	ctx, cancel := context.WithCancel(fix.RootCtx)
	cancel()
	llm.ProcessFrame(ctx, NewLLMContextFrame(NewLLMContext(testInitialMessages())), Downstream)
	if len(client.Requests()) != 0 {
		t.Fatal("cancelled request reached the provider")
	}
}
