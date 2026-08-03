package voicepipelinecore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestContextEnricherRewritesMessages(t *testing.T) {
	fix := newTestFixture(t)

	enrich := func(ctx context.Context, messages []Message) []Message {
		return append(messages, Message{Role: "user", Content: "injected"})
	}
	p := NewContextEnricherProcessor(fix.TaskCtx, enrich)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMMessagesFrame([]Message{{Role: "system", Content: "sys"}, {Role: "user", Content: "hi"}}),
		},
		sendEndFrame: true,
	})

	frame, ok := findFrame[LLMMessagesFrame](down)
	if !ok {
		t.Fatalf("no LLMMessagesFrame forwarded, got %v", describeFrameTypes(down))
	}
	if len(frame.Messages) != 3 || frame.Messages[2].Content != "injected" {
		t.Fatalf("enriched messages not forwarded: %+v", frame.Messages)
	}

	var sawMetric bool
	for _, mf := range fix.Metrics() {
		for _, d := range mf.Data {
			if d.Label == MetricContextEnrich && d.Processor == "context_enricher" {
				sawMetric = true
			}
		}
	}
	if !sawMetric {
		t.Fatal("expected a MetricContextEnrich measurement")
	}
}

func TestContextEnricherEmptyResultForwardsOriginal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result []Message
	}{
		{"nil", nil},
		{"empty", []Message{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fix := newTestFixture(t)
			original := []Message{{Role: "system", Content: "sys"}, {Role: "user", Content: "hi"}}
			p := NewContextEnricherProcessor(fix.TaskCtx, func(context.Context, []Message) []Message {
				return tc.result
			})

			down, _ := runProcessorTest(t, fix, runConfig{
				processor:    p,
				framesToSend: []Frame{NewLLMMessagesFrame(original)},
				sendEndFrame: true,
			})

			frame, ok := findFrame[LLMMessagesFrame](down)
			if !ok {
				t.Fatalf("no LLMMessagesFrame forwarded, got %v", describeFrameTypes(down))
			}
			if len(frame.Messages) != len(original) {
				t.Fatalf("expected the original %d messages, got %+v", len(original), frame.Messages)
			}
		})
	}
}

// The enricher must not see frames it has no business rewriting.
func TestContextEnricherIgnoresOtherFrames(t *testing.T) {
	fix := newTestFixture(t)
	var calls atomic.Int32
	p := NewContextEnricherProcessor(fix.TaskCtx, func(_ context.Context, m []Message) []Message {
		calls.Add(1)
		return m
	})

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewTextFrame("text"),
			NewInterruptFrame(),
			NewTranscriptFrame("hello", true, 1, false),
		},
		sendEndFrame: true,
	})

	if got := calls.Load(); got != 0 {
		t.Fatalf("enricher called %d times for non-LLMMessagesFrame frames", got)
	}
	if len(down) < 3 {
		t.Fatalf("expected pass-through of all frames, got %v", describeFrameTypes(down))
	}
}

// A barge-in that lands while the enricher is running must cancel it, and the
// processor must not sit past the base's procLoopExitTimeout.
func TestContextEnricherCancelledEnricherForwardsOriginal(t *testing.T) {
	fix := newTestFixture(t)
	released := make(chan struct{})
	var observedCancel atomic.Bool

	p := NewContextEnricherProcessor(fix.TaskCtx, func(ctx context.Context, m []Message) []Message {
		select {
		case <-ctx.Done():
			observedCancel.Store(true)
			return nil // fail open, exactly like the real enricher
		case <-released:
			return append(m, Message{Role: "user", Content: "late"})
		case <-time.After(3 * time.Second):
			return m
		}
	})

	go func() {
		time.Sleep(150 * time.Millisecond)
		close(released)
	}()

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMMessagesFrame([]Message{{Role: "system", Content: "sys"}, {Role: "user", Content: "hi"}}),
			SleepFrame{Duration: 20 * time.Millisecond},
			NewInterruptFrame(),
		},
		settleDelay:  300 * time.Millisecond,
		sendEndFrame: true,
	})

	if !observedCancel.Load() {
		t.Fatal("enricher did not observe ctx cancellation on InterruptFrame")
	}
	// The interrupt must still reach downstream processors.
	if _, ok := findFrame[InterruptFrame](down); !ok {
		t.Fatalf("InterruptFrame not forwarded, got %v", describeFrameTypes(down))
	}
}

// An enricher that is already too late (turn abandoned before the frame was
// processed) must not be invoked at all.
func TestContextEnricherSkipsWhenContextAlreadyCancelled(t *testing.T) {
	fix := newTestFixture(t)
	var calls atomic.Int32
	p := NewContextEnricherProcessor(fix.TaskCtx, func(_ context.Context, m []Message) []Message {
		calls.Add(1)
		return append(m, Message{Role: "user", Content: "should not happen"})
	})
	p.Start(fix.RootCtx)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	frame := NewLLMMessagesFrame([]Message{{Role: "user", Content: "hi"}})
	out := p.enrichFrame(cancelled, frame)

	p.Stop()
	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("processor did not stop: %v", err)
	}

	if got := calls.Load(); got != 0 {
		t.Fatalf("enricher invoked %d times on an already-cancelled turn", got)
	}
	forwarded, ok := out.(LLMMessagesFrame)
	if !ok || len(forwarded.Messages) != 1 {
		t.Fatalf("expected the original frame back, got %#v", out)
	}
}

func TestContextEnricherRequiresEnricher(t *testing.T) {
	fix := newTestFixture(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for a nil MessagesEnricher")
		}
	}()
	NewContextEnricherProcessor(fix.TaskCtx, nil)
}
