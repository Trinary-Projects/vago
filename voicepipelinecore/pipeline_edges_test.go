package voicepipelinecore

import (
	"testing"
	"time"
)

// TestPipelineSource_QueueForwardsDownstream verifies an external frame
// queued via Queue() is forwarded downstream.
func TestPipelineSource_QueueForwardsDownstream(t *testing.T) {
	fix := newTestFixture(t)
	p := NewPipelineSourceProcessor(fix.TaskCtx)

	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	p.Link(sink)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	p.Queue(EndFrame{Reason: "test"})
	time.Sleep(50 * time.Millisecond)
	stopProcessorsAndWait(t, fix, 3*time.Second, p, sink)

	got := sink.Captured()
	endFrame, ok := findFrame[EndFrame](got)
	if !ok {
		t.Fatalf("expected EndFrame downstream, got %s", describeFrameTypes(got))
	}
	if endFrame.Reason != "test" {
		t.Errorf("EndFrame.Reason: got %q, want 'test'", endFrame.Reason)
	}
}

// TestPipelineSink_CallsOnEndCallback verifies the onEnd callback fires
// when an EndFrame arrives.
func TestPipelineSink_CallsOnEndCallback(t *testing.T) {
	fix := newTestFixture(t)

	called := make(chan EndFrame, 1)
	p := NewPipelineSinkProcessor(fix.TaskCtx, func(f EndFrame) {
		called <- f
	})

	p.Start(fix.RootCtx)
	p.QueueFrame(EndFrame{Reason: "test-reason"}, Downstream)

	select {
	case f := <-called:
		if f.Reason != "test-reason" {
			t.Errorf("EndFrame.Reason: got %q, want 'test-reason'", f.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("onEnd was not called within 2s")
	}

	stopProcessorsAndWait(t, fix, 2*time.Second, p)
}

// TestPipelineSource_FatalErrorTriggersEndTask verifies that when a
// fatal ErrorFrame bubbles up to PipelineSourceProcessor, taskCtx.EndTask
// fires (which in production injects an EndFrame at the source). Here
// we wire EndTask to a captured channel to assert it was called with
// the expected reason.
func TestPipelineSource_FatalErrorTriggersEndTask(t *testing.T) {
	fix := newTestFixture(t)

	endCalls := make(chan EndReason, 1)
	fix.TaskCtx.EndTask = func(reason EndReason) { endCalls <- reason }

	p := NewPipelineSourceProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)

	// Simulate a fatal error arriving upstream from a downstream
	// processor.
	p.QueueFrame(NewErrorFrame("STT", "websocket dead", true), Upstream)

	select {
	case reason := <-endCalls:
		if reason != EndReasonError {
			t.Errorf("EndTask reason = %q, want %q", reason, EndReasonError)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("EndTask was not called within 2s")
	}

	p.Stop()
	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}

// TestPipelineSource_NonFatalErrorIsLoggedNotEnded verifies that
// non-fatal ErrorFrames are logged but do NOT trigger EndTask.
func TestPipelineSource_NonFatalErrorIsLoggedNotEnded(t *testing.T) {
	fix := newTestFixture(t)

	endCalls := make(chan EndReason, 1)
	fix.TaskCtx.EndTask = func(reason EndReason) { endCalls <- reason }

	p := NewPipelineSourceProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)

	p.QueueFrame(NewErrorFrame("STT", "transient hiccup", false), Upstream)
	time.Sleep(50 * time.Millisecond)

	select {
	case reason := <-endCalls:
		t.Errorf("EndTask should not fire for non-fatal error, got reason=%q", reason)
	default:
	}

	p.Stop()
	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}

// TestPushError_RoutesUpstream verifies that BaseProcessor.PushError
// emits an ErrorFrame in the upstream direction (so it reaches the
// source).
func TestPushError_RoutesUpstream(t *testing.T) {
	fix := newTestFixture(t)
	mid := newQueueProcessor(fix.TaskCtx, "mid", Downstream)
	sourceCapture := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sourceCapture.Link(mid)
	sourceCapture.Start(fix.RootCtx)
	mid.Start(fix.RootCtx)

	// Mid pushes an error — it should flow upstream to sourceCapture.
	mid.PushError("synthetic failure", true)
	time.Sleep(50 * time.Millisecond)

	sourceCapture.Stop()
	mid.Stop()
	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}

	captured := sourceCapture.Captured()
	ef, ok := findFrame[ErrorFrame](captured)
	if !ok {
		t.Fatalf("expected ErrorFrame upstream, got %s", describeFrameTypes(captured))
	}
	if ef.Processor != "mid" {
		t.Errorf("ErrorFrame.Processor: got %q want %q", ef.Processor, "mid")
	}
	if ef.Err != "synthetic failure" {
		t.Errorf("ErrorFrame.Err: got %q", ef.Err)
	}
	if !ef.Fatal {
		t.Error("expected Fatal=true")
	}
}

// TestPipelineSink_IgnoresNonEndFrames verifies non-EndFrame frames are
// silently terminated (do not fire onEnd).
func TestPipelineSink_IgnoresNonEndFrames(t *testing.T) {
	fix := newTestFixture(t)

	called := make(chan EndFrame, 1)
	p := NewPipelineSinkProcessor(fix.TaskCtx, func(f EndFrame) {
		called <- f
	})

	p.Start(fix.RootCtx)
	p.QueueFrame(TextFrame{Text: "ignored"}, Downstream)
	p.QueueFrame(TranscriptFrame{Text: "also ignored"}, Downstream)
	time.Sleep(50 * time.Millisecond)

	select {
	case <-called:
		t.Error("onEnd should not have fired for non-EndFrame frames")
	default:
	}

	// Now actually shut down with an EndFrame.
	p.QueueFrame(EndFrame{Reason: "shutdown"}, Downstream)
	time.Sleep(50 * time.Millisecond)
	stopProcessorsAndWait(t, fix, 2*time.Second, p)
}
