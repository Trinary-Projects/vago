package voicepipelinecore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TextFrames must be forwarded before the check even runs, and must never be
// rewritten by the verdict. Using a guard that always fires proves the
// forwarding path is unconditional, not merely "usually" untouched.
func TestResponseGuardTextFramesForwardUnchanged(t *testing.T) {
	fix := newTestFixture(t)
	guard := func(context.Context, string) bool { return true }
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	texts := []string{"Hello, ", "world."}
	frames := []Frame{NewLLMResponseStartFrame(time.Now())}
	for _, tx := range texts {
		frames = append(frames, NewTextFrame(tx))
	}

	down, _ := runProcessorTest(t, fix, runConfig{
		processor:    p,
		framesToSend: frames,
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	var gotTexts []string
	for _, f := range down {
		if tf, ok := f.(TextFrame); ok {
			gotTexts = append(gotTexts, tf.Text)
		}
	}
	if len(gotTexts) != len(texts) {
		t.Fatalf("expected %d TextFrames forwarded, got %d: %v", len(texts), len(gotTexts), gotTexts)
	}
	for i, want := range texts {
		if gotTexts[i] != want {
			t.Fatalf("TextFrame[%d]: got %q, want %q", i, gotTexts[i], want)
		}
	}
}

// The boundary fires once per punctuation flush and resets the buffer.
// Fragments with no alphanumeric content (a bare em-dash here) must not fire.
func TestResponseGuardFiresOncePerFragmentBoundary(t *testing.T) {
	fix := newTestFixture(t)

	var mu sync.Mutex
	var seen []string
	guard := func(_ context.Context, fragment string) bool {
		mu.Lock()
		seen = append(seen, fragment)
		mu.Unlock()
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	_, _ = runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Hello, "),
			NewTextFrame("world."),
			NewTextFrame(" —"), // punctuation-only: must flush the buffer, must not fire
			NewTextFrame("More text!"),
		},
		settleDelay:  100 * time.Millisecond,
		sendEndFrame: true,
	})

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("expected exactly 2 checks, got %d: %v", len(seen), seen)
	}
	// Each fragment is checked on its own goroutine, so completion order
	// across fragments is not guaranteed — only membership is.
	want := map[string]bool{"Hello, world.": false, "More text!": false}
	for _, f := range seen {
		if _, ok := want[f]; !ok {
			t.Fatalf("unexpected fragment checked: %q (all: %v)", f, seen)
		}
		want[f] = true
	}
	for f, ok := range want {
		if !ok {
			t.Fatalf("expected fragment %q to be checked, got: %v", f, seen)
		}
	}
}

// A true verdict must emit exactly one InterruptFrame (broadcast both
// directions) and one upstream LLMMessagesAppendFrame{RunLLM: true}.
func TestResponseGuardTrueVerdictFiresInterruptAndAppend(t *testing.T) {
	fix := newTestFixture(t)
	guard := func(context.Context, string) bool { return true }
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	down, up := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("This is unacceptable."),
		},
		settleDelay:  100 * time.Millisecond,
		sendEndFrame: true,
	})

	if got := countFrames[InterruptFrame](down); got != 1 {
		t.Fatalf("downstream InterruptFrame count = %d, want 1 (%s)", got, describeFrameTypes(down))
	}
	if got := countFrames[InterruptFrame](up); got != 1 {
		t.Fatalf("upstream InterruptFrame count = %d, want 1 (%s)", got, describeFrameTypes(up))
	}
	appendFrame, ok := findFrame[LLMMessagesAppendFrame](up)
	if !ok {
		t.Fatalf("no upstream LLMMessagesAppendFrame, got %s", describeFrameTypes(up))
	}
	if !appendFrame.RunLLM {
		t.Fatal("expected LLMMessagesAppendFrame.RunLLM = true")
	}
	if len(appendFrame.Messages) != 0 {
		t.Fatalf("expected no appended messages, got %+v", appendFrame.Messages)
	}
	if got := countFrames[LLMMessagesAppendFrame](up); got != 1 {
		t.Fatalf("upstream LLMMessagesAppendFrame count = %d, want 1", got)
	}
}

// Two checks in the same turn both returning true must still yield exactly
// one interrupt: the fired atomic.Bool CAS must let only one winner through.
// A WaitGroup barrier inside the guard forces both checks to be genuinely
// in flight together before either resolves, so the race is real rather than
// accidental.
func TestResponseGuardConcurrentTrueVerdictsFireOnce(t *testing.T) {
	fix := newTestFixture(t)

	var ready sync.WaitGroup
	ready.Add(2)
	guard := func(context.Context, string) bool {
		ready.Done()
		ready.Wait()
		return true
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	down, up := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Bad first sentence."),
			NewTextFrame("Bad second sentence."),
		},
		settleDelay:  150 * time.Millisecond,
		sendEndFrame: true,
	})

	if got := countFrames[InterruptFrame](down); got != 1 {
		t.Fatalf("downstream InterruptFrame count = %d, want exactly 1 (%s)", got, describeFrameTypes(down))
	}
	if got := countFrames[LLMMessagesAppendFrame](up); got != 1 {
		t.Fatalf("upstream LLMMessagesAppendFrame count = %d, want exactly 1", got)
	}
}

// skipTurn is a one-retry latch: the generation immediately following a
// self-triggered interrupt runs no checks at all, and the one after that is
// guarded normally again.
func TestResponseGuardSkipTurnLatchIsOneRetryOnly(t *testing.T) {
	fix := newTestFixture(t)

	var calls atomic.Int32
	guard := func(context.Context, string) bool {
		n := calls.Add(1)
		// Only the first call (turn 1) fires; later calls (turn 3) must not
		// re-trigger for this test's purposes.
		return n == 1
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	down, up := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			// Turn 1: fires and self-interrupts.
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Turn one is bad."),
			SleepFrame{Duration: 80 * time.Millisecond}, // let the check resolve and set skipTurn
			// Turn 2: the unguarded regeneration retry — must run no checks.
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Turn two should not be checked."),
			SleepFrame{Duration: 50 * time.Millisecond},
			// Turn 3: guarded normally again.
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Turn three is checked."),
		},
		settleDelay:  80 * time.Millisecond,
		sendEndFrame: true,
	})

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected exactly 2 guard invocations (turn 1 and turn 3), got %d", got)
	}
	if got := countFrames[InterruptFrame](down); got != 1 {
		t.Fatalf("expected exactly 1 InterruptFrame (from turn 1 only), got %d (%s)", got, describeFrameTypes(down))
	}
	if got := countFrames[LLMMessagesAppendFrame](up); got != 1 {
		t.Fatalf("expected exactly 1 LLMMessagesAppendFrame, got %d", got)
	}
}

// A foreign InterruptFrame (a genuine barge-in, as opposed to the one this
// processor itself just broadcast) must clear skipTurn so the next real turn
// is not left unguarded. Per the documented contract the flag is consumed by
// the first InterruptFrame this processor observes after firing — so this
// test sends two: the first is indistinguishable from "my own" echo and is
// absorbed, and the second is unambiguously foreign and clears the latch.
func TestResponseGuardForeignInterruptClearsSkipTurn(t *testing.T) {
	fix := newTestFixture(t)

	// Fires on the first call only (turn 1's self-trigger); a second firing
	// would mean turn 3 was never guarded, which is exactly what this test
	// checks against.
	var fireCalls atomic.Int32
	fireGuard := func(context.Context, string) bool {
		return fireCalls.Add(1) == 1
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, fireGuard)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Turn one is bad."),
			SleepFrame{Duration: 80 * time.Millisecond}, // let the self-trigger resolve (skipTurn=true)
			// A single barge-in is enough: Broadcast never routes this
			// processor's own interrupt back to it, so every InterruptFrame
			// observed here is foreign and must clear skipTurn. If it did
			// not, the turn below would go unguarded -- i.e. the user's
			// genuine reply would silently consume the one-retry latch.
			NewInterruptFrame(),
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Should be checked again."),
		},
		settleDelay:  80 * time.Millisecond,
		sendEndFrame: true,
	})

	if got := fireCalls.Load(); got != 2 {
		t.Fatalf("expected 2 guard invocations (turn 1, and the guarded turn after the foreign interrupt), got %d", got)
	}
	if got := countFrames[InterruptFrame](down); got != 2 {
		// 1 self-triggered + 1 explicitly sent by the test.
		t.Fatalf("expected 2 InterruptFrames downstream, got %d (%s)", got, describeFrameTypes(down))
	}
}

// InterruptFrame must cancel every in-flight check for the turn.
func TestResponseGuardInterruptCancelsInFlightChecks(t *testing.T) {
	fix := newTestFixture(t)

	cancelled := make(chan struct{})
	guard := func(ctx context.Context, _ string) bool {
		<-ctx.Done()
		close(cancelled)
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("A sentence that ends here."),
			SleepFrame{Duration: 50 * time.Millisecond}, // let the check start and block on ctx.Done()
			NewInterruptFrame(),
		},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	select {
	case <-cancelled:
	default:
		t.Fatal("guard did not observe ctx cancellation after InterruptFrame")
	}
}

// EndFrame must also cancel every in-flight check for the turn.
func TestResponseGuardEndFrameCancelsInFlightChecks(t *testing.T) {
	fix := newTestFixture(t)

	cancelled := make(chan struct{})
	guard := func(ctx context.Context, _ string) bool {
		<-ctx.Done()
		close(cancelled)
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("A sentence that ends here."),
			SleepFrame{Duration: 50 * time.Millisecond}, // let the check start and block on ctx.Done()
			NewEndFrame(""),
		},
		settleDelay: 50 * time.Millisecond,
	})

	select {
	case <-cancelled:
	default:
		t.Fatal("guard did not observe ctx cancellation after EndFrame")
	}
}

// A slow guard must never delay TextFrame propagation: the check runs on its
// own goroutine, so TTS keeps receiving text immediately regardless of how
// long the guard takes. The guard here blocks until ctx is cancelled by the
// trailing EndFrame, proving the pipeline doesn't wait on it.
func TestResponseGuardSlowGuardDoesNotDelayTextForwarding(t *testing.T) {
	fix := newTestFixture(t)

	entered := make(chan struct{})
	guard := func(ctx context.Context, _ string) bool {
		close(entered)
		<-ctx.Done()
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Hello there."),
			SleepFrame{Duration: 50 * time.Millisecond},
		},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	select {
	case <-entered:
	default:
		t.Fatal("guard was never invoked")
	}
	if _, ok := findFrame[TextFrame](down); !ok {
		t.Fatalf("TextFrame was not forwarded while the guard was still blocked, got %s", describeFrameTypes(down))
	}
}

func TestResponseGuardRequiresGuard(t *testing.T) {
	fix := newTestFixture(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for a nil ResponseGuard")
		}
	}()
	NewResponseGuardProcessor(fix.TaskCtx, nil)
}

// A turn fans out to one check per fragment, and every check must contribute
// its own measurement. A shared ProcessorMetrics timer cannot express that --
// it holds one start time per label, so concurrent Starts overwrite each other
// and the first Stop deletes the entry, leaving all later checks unmeasured.
// This asserts one metric per fragment, each with a real duration.
func TestResponseGuardEmitsOneMetricPerCheck(t *testing.T) {
	fix := newTestFixture(t)

	const guardDelay = 25 * time.Millisecond
	slowGuard := func(ctx context.Context, _ string) bool {
		select {
		case <-time.After(guardDelay):
		case <-ctx.Done():
		}
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, slowGuard)

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("First fragment."),
			NewTextFrame("Second fragment."),
			NewTextFrame("Third fragment."),
		},
		settleDelay:  200 * time.Millisecond,
		sendEndFrame: true,
	})

	var samples []float64
	for _, mf := range fix.Metrics() {
		for _, d := range mf.Data {
			if d.Label == MetricResponseGuard {
				if d.Processor != responseGuardProcessorName {
					t.Fatalf("metric processor = %q, want %q", d.Processor, responseGuardProcessorName)
				}
				samples = append(samples, d.ValueMs)
			}
		}
	}

	if len(samples) != 3 {
		t.Fatalf("expected one MetricResponseGuard sample per fragment (3), got %d: %v", len(samples), samples)
	}
	// Each sample must reflect its own guard call, not a duration truncated by
	// a sibling's Start/Stop stomping a shared timer.
	floor := float64(guardDelay.Microseconds()) / 1000.0 * 0.5
	for i, ms := range samples {
		if ms < floor {
			t.Fatalf("sample %d = %.2fms, implausibly short for a %v guard (shared-timer regression?): %v",
				i, ms, guardDelay, samples)
		}
	}
}
