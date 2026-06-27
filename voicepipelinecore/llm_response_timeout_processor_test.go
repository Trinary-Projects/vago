package voicepipelinecore

import (
	"testing"
	"time"
)

func TestLLMResponseTimeout_TimeoutInterruptsAndRetries(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMResponseTimeoutProcessorWithTimeout(fix.TaskCtx, 20*time.Millisecond)

	down, up := runProcessorTest(t, fix, runConfig{
		processor:    p,
		framesToSend: []Frame{NewLLMResponseStartFrame(time.Now())},
		settleDelay:  40 * time.Millisecond,
		sendEndFrame: true,
	})

	if c := countFrames[InterruptFrame](down); c != 1 {
		t.Fatalf("downstream InterruptFrame count = %d, want 1 in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[InterruptFrame](up); c != 1 {
		t.Fatalf("upstream InterruptFrame count = %d, want 1 in %s", c, describeFrameTypes(up))
	}
	if c := countFrames[LLMMessagesAppendFrame](up); c != 1 {
		t.Fatalf("upstream LLMMessagesAppendFrame count = %d, want 1 in %s", c, describeFrameTypes(up))
	}
}

func TestLLMResponseTimeout_ResponseEndCancelsTimer(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMResponseTimeoutProcessorWithTimeout(fix.TaskCtx, 20*time.Millisecond)

	down, up := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			SleepFrame{Duration: 5 * time.Millisecond},
			NewLLMResponseEndFrame(),
		},
		settleDelay:  40 * time.Millisecond,
		sendEndFrame: true,
	})

	if c := countFrames[InterruptFrame](down); c != 0 {
		t.Fatalf("downstream InterruptFrame count = %d, want 0 in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[InterruptFrame](up); c != 0 {
		t.Fatalf("upstream InterruptFrame count = %d, want 0 in %s", c, describeFrameTypes(up))
	}
	if c := countFrames[LLMMessagesAppendFrame](up); c != 0 {
		t.Fatalf("upstream LLMMessagesAppendFrame count = %d, want 0 in %s", c, describeFrameTypes(up))
	}
}

func TestLLMResponseTimeout_InterruptCancelsTimer(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMResponseTimeoutProcessorWithTimeout(fix.TaskCtx, 20*time.Millisecond)

	down, up := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			SleepFrame{Duration: 5 * time.Millisecond},
			NewInterruptFrame(),
		},
		settleDelay:  40 * time.Millisecond,
		sendEndFrame: true,
	})

	if c := countFrames[InterruptFrame](down); c != 1 {
		t.Fatalf("downstream InterruptFrame count = %d, want input interrupt only in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[InterruptFrame](up); c != 0 {
		t.Fatalf("upstream InterruptFrame count = %d, want 0 in %s", c, describeFrameTypes(up))
	}
	if c := countFrames[LLMMessagesAppendFrame](up); c != 0 {
		t.Fatalf("upstream LLMMessagesAppendFrame count = %d, want 0 in %s", c, describeFrameTypes(up))
	}
}

func TestLLMResponseTimeout_ResponseStartResetsTimer(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMResponseTimeoutProcessorWithTimeout(fix.TaskCtx, 40*time.Millisecond)

	down, up := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			SleepFrame{Duration: 20 * time.Millisecond},
			NewLLMResponseStartFrame(time.Now()),
		},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	if c := countFrames[InterruptFrame](down); c != 1 {
		t.Fatalf("downstream InterruptFrame count = %d, want 1 in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[InterruptFrame](up); c != 1 {
		t.Fatalf("upstream InterruptFrame count = %d, want 1 in %s", c, describeFrameTypes(up))
	}
	if c := countFrames[LLMMessagesAppendFrame](up); c != 1 {
		t.Fatalf("upstream LLMMessagesAppendFrame count = %d, want 1 in %s", c, describeFrameTypes(up))
	}
}
