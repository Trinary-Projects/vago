package voicepipelinecore

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestLLM_StreamsTextFramesDownstream verifies a complete LLM run
// produces LLMResponseStart → TextFrames → LLMResponseEnd downstream.
func TestLLM_StreamsTextFramesDownstream(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMProcessorWithClient(fix.TaskCtx, &stubLLMClient{
		tokens: []string{"hello ", "world"},
		model:  "test-model",
	})

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			LLMMessagesFrame{Messages: []Message{
				{Role: "user", Content: "hi"},
			}},
		},
		settleDelay:  200 * time.Millisecond,
		sendEndFrame: true,
		timeout:      3 * time.Second,
	})

	if _, ok := findFrame[LLMResponseStartFrame](down); !ok {
		t.Errorf("expected LLMResponseStartFrame, got %s", describeFrameTypes(down))
	}
	if c := countFrames[TextFrame](down); c != 2 {
		t.Errorf("expected 2 TextFrames (one per chunk), got %d in %s", c, describeFrameTypes(down))
	}
	if _, ok := findFrame[LLMResponseEndFrame](down); !ok {
		t.Errorf("expected LLMResponseEndFrame, got %s", describeFrameTypes(down))
	}

	// Verify TextFrame contents.
	var combined string
	for _, f := range down {
		if tf, ok := f.(TextFrame); ok {
			combined += tf.Text
		}
	}
	if combined != "hello world" {
		t.Errorf("combined text: got %q, want %q", combined, "hello world")
	}
}

// TestLLM_InterruptCancelsClientStream verifies an InterruptFrame cancels the
// in-flight LLM client stream and stops further TextFrames from being
// emitted.
func TestLLM_InterruptCancelsClientStream(t *testing.T) {
	// Client emits one chunk, then waits before emitting the second. The
	// test interrupts in between.
	blockUntil := make(chan struct{})
	var serverClosed sync.Once
	defer serverClosed.Do(func() { close(blockUntil) })

	fix := newTestFixture(t)
	p := NewLLMProcessorWithClient(fix.TaskCtx, &stubLLMClient{
		model: "test-model",
		responses: []stubLLMResponse{{
			tokens:                []string{"first ", "second"},
			blockBeforeTokenIndex: 1,
			blockUntil:            blockUntil,
		}},
	})

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(LLMMessagesFrame{Messages: []Message{{Role: "user", Content: "hi"}}}, Downstream)

	// Wait for the first chunk to arrive.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hasText := false
		for _, f := range sink.Captured() {
			if _, ok := f.(TextFrame); ok {
				hasText = true
				break
			}
		}
		if hasText {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Send InterruptFrame; it should cancel the in-flight HTTP request.
	source.QueueFrame(InterruptFrame{}, Downstream)
	time.Sleep(100 * time.Millisecond)

	// Now unblock the client. Any second chunk it emits should NOT reach
	// the sink because the stream context was cancelled.
	serverClosed.Do(func() { close(blockUntil) })
	time.Sleep(100 * time.Millisecond)

	// Clean shutdown.
	source.QueueFrame(EndFrame{}, Downstream)
	time.Sleep(50 * time.Millisecond)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink)

	got := sink.Captured()
	textCount := countFrames[TextFrame](got)
	if textCount > 1 {
		t.Errorf("expected at most 1 TextFrame (the first chunk before interrupt), got %d in %s", textCount, describeFrameTypes(got))
	}
	if c := countFrames[InterruptFrame](got); c != 1 {
		t.Errorf("expected InterruptFrame forwarded, got %d", c)
	}
}

// TestLLM_EndFrameCancelsInFlight verifies EndFrame cancels the
// in-flight LLM request via the stored cancel func.
func TestLLM_EndFrameCancelsInFlight(t *testing.T) {
	blockUntil := make(chan struct{})
	defer close(blockUntil)

	fix := newTestFixture(t)
	p := NewLLMProcessorWithClient(fix.TaskCtx, &stubLLMClient{
		model: "test-model",
		responses: []stubLLMResponse{{
			tokens:                []string{"first ", "second"},
			blockBeforeTokenIndex: 1,
			blockUntil:            blockUntil,
		}},
	})

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(LLMMessagesFrame{Messages: []Message{{Role: "user", Content: "hi"}}}, Downstream)

	// Wait for the first chunk.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		for _, f := range sink.Captured() {
			if _, ok := f.(TextFrame); ok {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// EndFrame should cancel the in-flight request.
	source.QueueFrame(EndFrame{Reason: "test"}, Downstream)
	time.Sleep(50 * time.Millisecond)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink)

	// Verify EndFrame reached the sink.
	got := sink.Captured()
	if c := countFrames[EndFrame](got); c == 0 {
		t.Errorf("expected EndFrame forwarded, got %s", describeFrameTypes(got))
	}
}

// TestLLM_PassesThroughOtherFrames verifies the default-forward
// behaviour for frames LLM doesn't specifically handle (e.g., upstream
// frames going back to UserContextAggregator).
func TestLLM_PassesThroughOtherFrames(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMProcessorWithClient(fix.TaskCtx, &stubLLMClient{model: "test-model"})

	// Push an upstream frame; should reach the source.
	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	// Simulate downstream-of-LLM pushing TTSDoneFrame upstream.
	sink.QueueFrame(TTSDoneFrame{}, Upstream)
	time.Sleep(50 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink)

	if c := countFrames[TTSDoneFrame](source.Captured()); c != 1 {
		t.Errorf("expected TTSDoneFrame to pass through upstream, got %d", c)
	}
}

// stubLogger satisfies the io.Writer interface to silence LLM logs
// during the error-path test.
type stubLogger struct{}

func (stubLogger) Write(p []byte) (int, error) { return len(p), nil }

// TestLLM_HandlesClientError verifies LLM handles a client stream error
// gracefully (no panic, just no downstream tokens).
func TestLLM_HandlesClientError(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMProcessorWithClient(fix.TaskCtx, &stubLLMClient{
		model: "test-model",
		err:   errors.New("boom"),
	})

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			LLMMessagesFrame{Messages: []Message{{Role: "user", Content: "hi"}}},
		},
		settleDelay:  200 * time.Millisecond,
		sendEndFrame: true,
	})

	// LLMResponseStart still goes out before the request fails.
	if _, ok := findFrame[LLMResponseStartFrame](down); !ok {
		t.Errorf("expected LLMResponseStartFrame even on error, got %s", describeFrameTypes(down))
	}
	if c := countFrames[TextFrame](down); c != 0 {
		t.Errorf("expected no TextFrames on error response, got %d", c)
	}

	// Verify TextFrame logic doesn't leak the error message.
	for _, f := range down {
		if tf, ok := f.(TextFrame); ok {
			if strings.Contains(tf.Text, "boom") {
				t.Errorf("error body leaked into TextFrame: %q", tf.Text)
			}
		}
	}
}

// Suppress unused-warning for stubLogger when not invoked.
var _ = stubLogger{}
