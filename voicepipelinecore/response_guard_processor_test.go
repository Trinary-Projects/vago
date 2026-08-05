package voicepipelinecore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 1. TextFrames are forwarded unchanged regardless of the guard's eventual
// verdict. The guard is held open until after the TextFrame is observed
// downstream, which is the only race-free way to prove this: once a check
// actually resolves to a violation, the InterruptFrame it broadcasts is free
// to race ahead of (and legitimately purge) any not-yet-processed TextFrame
// queued at the next hop — that purge is correct pipeline interrupt
// semantics, not something this processor controls or this test should
// depend on.
func TestResponseGuardForwardsTextFramesRegardlessOfVerdict(t *testing.T) {
	for _, verdict := range []bool{true, false} {
		t.Run(fmt.Sprintf("verdict_%v", verdict), func(t *testing.T) {
			fix := newTestFixture(t)
			release := make(chan struct{})
			guardStarted := make(chan struct{}, 1)
			guard := func(context.Context, string) bool {
				guardStarted <- struct{}{}
				<-release
				return verdict
			}
			p := NewResponseGuardProcessor(fix.TaskCtx, guard)

			source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
			sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
			fix.TaskCtx.EndTask = func(reason EndReason) {
				source.QueueFrame(NewEndFrame(string(reason)), Downstream)
			}
			source.Link(p)
			p.Link(sink)
			source.Start(fix.RootCtx)
			p.Start(fix.RootCtx)
			sink.Start(fix.RootCtx)

			text := NewTextFrame("Hello there.")
			source.QueueFrame(NewLLMResponseStartFrame(time.Now()), Downstream)
			source.QueueFrame(text, Downstream)

			select {
			case <-guardStarted:
			case <-time.After(1 * time.Second):
				t.Fatal("guard was never invoked")
			}

			deadline := time.Now().Add(200 * time.Millisecond)
			var found bool
			for time.Now().Before(deadline) {
				if frame, ok := findFrame[TextFrame](sink.Captured()); ok && frame.Text == text.Text {
					found = true
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			if !found {
				t.Fatalf("TextFrame not forwarded before the (still in-flight) check resolved, got %s", describeFrameTypes(sink.Captured()))
			}

			close(release)
			stopProcessorsAndWait(t, fix, 2*time.Second, source, p, sink)
		})
	}
}

// 2. One check fires per complete sentence.
func TestResponseGuardOneCheckPerSentence(t *testing.T) {
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

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Sentence one. Sentence two."),
		},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	mu.Lock()
	defer mu.Unlock()
	assertSameSentences(t, seen, []string{"Sentence one.", "Sentence two."})
}

// 3. A delta straddling a sentence boundary still yields one check per
// sentence, with the correct (reassembled) sentence text.
func TestResponseGuardDeltaStraddlingSentenceBoundary(t *testing.T) {
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

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("One. Sta"),
			SleepFrame{Duration: 10 * time.Millisecond},
			NewTextFrame("rt two."),
		},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	mu.Lock()
	defer mu.Unlock()
	assertSameSentences(t, seen, []string{"One.", "Start two."})
}

// 4. Punctuation-only fragments (no letter/digit) never fire a check.
func TestResponseGuardSkipsPunctuationOnlyFragments(t *testing.T) {
	fix := newTestFixture(t)
	var calls atomic.Int32
	guard := func(context.Context, string) bool {
		calls.Add(1)
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("... !? "),
		},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	if got := calls.Load(); got != 0 {
		t.Fatalf("expected no checks for punctuation-only fragments, got %d", got)
	}
}

// 5. Exactly one InterruptFrame (+ one upstream regeneration request) is
// produced even when several sentences resolve to a violation concurrently.
func TestResponseGuardConcurrentViolationsProduceOneInterrupt(t *testing.T) {
	fix := newTestFixture(t)
	const n = 3
	var waiting atomic.Int32
	release := make(chan struct{})
	guard := func(context.Context, string) bool {
		waiting.Add(1)
		<-release
		return true
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for waiting.Load() < n && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		close(release)
	}()

	down, up := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("One. Two. Three."),
		},
		settleDelay: 150 * time.Millisecond,
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

// 6. skipTurn is consumed by exactly one generation: the regeneration that
// follows a violation runs unguarded, and the turn after that is guarded
// again.
func TestResponseGuardSkipTurnConsumedByOneGeneration(t *testing.T) {
	fix := newTestFixture(t)
	var calls atomic.Int32
	guard := func(context.Context, string) bool {
		calls.Add(1)
		return true
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()), // turn 1: guarded, violates
			NewTextFrame("Bad."),
			SleepFrame{Duration: 40 * time.Millisecond},
			NewLLMResponseStartFrame(time.Now()), // turn 2: the regeneration, unguarded
			NewTextFrame("Bad."),
			SleepFrame{Duration: 40 * time.Millisecond},
			NewLLMResponseStartFrame(time.Now()), // turn 3: guarded again
			NewTextFrame("Bad."),
			SleepFrame{Duration: 40 * time.Millisecond},
		},
	})

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 guard calls (turn 1 + turn 3; turn 2 skipped), got %d", got)
	}
	if c := countFrames[InterruptFrame](down); c != 2 {
		t.Fatalf("expected 2 interrupts (turn 1 + turn 3 violations), got %d in %s", c, describeFrameTypes(down))
	}
}

// 7. A foreign InterruptFrame clears skipTurn, so a barge-in landing between
// a violation and its regeneration leaves the next turn guarded.
func TestResponseGuardForeignInterruptClearsSkipTurn(t *testing.T) {
	fix := newTestFixture(t)
	var calls atomic.Int32
	guard := func(context.Context, string) bool {
		calls.Add(1)
		return true
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()), // turn 1: guarded, violates
			NewTextFrame("Bad."),
			SleepFrame{Duration: 40 * time.Millisecond},
			NewInterruptFrame(), // foreign barge-in before the regeneration starts
			SleepFrame{Duration: 10 * time.Millisecond},
			NewLLMResponseStartFrame(time.Now()), // turn 2: must still be guarded
			NewTextFrame("Bad."),
			SleepFrame{Duration: 40 * time.Millisecond},
		},
	})

	if got := calls.Load(); got != 2 {
		t.Fatalf("expected 2 guard calls (turn 1 + turn 2, both guarded), got %d", got)
	}
	// turn 1's violation, the foreign interrupt, and turn 2's violation.
	if c := countFrames[InterruptFrame](down); c != 3 {
		t.Fatalf("expected 3 downstream interrupts, got %d in %s", c, describeFrameTypes(down))
	}
}

// 8. InterruptFrame cancels in-flight checks.
func TestResponseGuardInterruptCancelsInFlightChecks(t *testing.T) {
	fix := newTestFixture(t)
	observedCancel := make(chan struct{})
	guard := func(ctx context.Context, fragment string) bool {
		select {
		case <-ctx.Done():
			close(observedCancel)
		case <-time.After(3 * time.Second):
		}
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Blocked sentence."),
			SleepFrame{Duration: 20 * time.Millisecond},
			NewInterruptFrame(),
		},
		settleDelay:  100 * time.Millisecond,
		sendEndFrame: true,
	})

	select {
	case <-observedCancel:
	default:
		t.Fatal("in-flight check did not observe ctx cancellation on InterruptFrame")
	}
}

// 9. EndFrame cancels in-flight checks.
func TestResponseGuardEndFrameCancelsInFlightChecks(t *testing.T) {
	fix := newTestFixture(t)
	observedCancel := make(chan struct{})
	guard := func(ctx context.Context, fragment string) bool {
		select {
		case <-ctx.Done():
			close(observedCancel)
		case <-time.After(3 * time.Second):
		}
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Blocked sentence."),
		},
		settleDelay:  20 * time.Millisecond,
		sendEndFrame: true,
	})

	select {
	case <-observedCancel:
	default:
		t.Fatal("in-flight check did not observe ctx cancellation on EndFrame")
	}
}

// 10. A slow guard never delays TextFrames: the frame reaches the sink while
// the check for it is still blocked.
func TestResponseGuardSlowGuardNeverDelaysTextFrames(t *testing.T) {
	fix := newTestFixture(t)
	release := make(chan struct{})
	guardStarted := make(chan struct{}, 1)
	guard := func(context.Context, string) bool {
		guardStarted <- struct{}{}
		<-release
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	fix.TaskCtx.EndTask = func(reason EndReason) {
		source.QueueFrame(NewEndFrame(string(reason)), Downstream)
	}
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewLLMResponseStartFrame(time.Now()), Downstream)
	source.QueueFrame(NewTextFrame("Blocked sentence one."), Downstream)

	select {
	case <-guardStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("guard was never invoked")
	}

	// The check is now blocked inside guard(). The TextFrame must already
	// be downstream regardless.
	deadline := time.Now().Add(200 * time.Millisecond)
	found := false
	for time.Now().Before(deadline) {
		if _, ok := findFrame[TextFrame](sink.Captured()); ok {
			found = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !found {
		t.Fatal("TextFrame was delayed behind a slow guard check")
	}

	close(release)
	stopProcessorsAndWait(t, fix, 2*time.Second, source, p, sink)
}

// 11. A violation cancels the turn's remaining in-flight checks.
func TestResponseGuardViolationCancelsRemainingChecks(t *testing.T) {
	fix := newTestFixture(t)
	observedCancel := make(chan struct{})
	guard := func(ctx context.Context, fragment string) bool {
		if strings.Contains(fragment, "Trigger") {
			return true
		}
		select {
		case <-ctx.Done():
			close(observedCancel)
		case <-time.After(3 * time.Second):
		}
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Trigger one. Slow two."),
		},
		settleDelay:  100 * time.Millisecond,
		sendEndFrame: true,
	})

	select {
	case <-observedCancel:
	default:
		t.Fatal("sibling check did not observe cancellation after a violation on the same turn")
	}
}

// 12. A check that finishes after its turn is already done is dropped, even
// when its verdict is a violation.
func TestResponseGuardStaleCheckWithTrueVerdictDropped(t *testing.T) {
	fix := newTestFixture(t)
	release := make(chan struct{})
	guardStarted := make(chan struct{}, 1)
	guard := func(context.Context, string) bool {
		guardStarted <- struct{}{}
		<-release
		return true // must still be dropped: the turn is cancelled before this returns.
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	fix.TaskCtx.EndTask = func(reason EndReason) {
		source.QueueFrame(NewEndFrame(string(reason)), Downstream)
	}
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewLLMResponseStartFrame(time.Now()), Downstream)
	source.QueueFrame(NewTextFrame("Pending sentence."), Downstream)

	select {
	case <-guardStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("guard was never invoked")
	}

	// Cancel the turn out from under the still-running check with a foreign
	// InterruptFrame, then let the check finish with a "true" verdict.
	source.QueueFrame(NewInterruptFrame(), Downstream)
	time.Sleep(20 * time.Millisecond)
	close(release)
	time.Sleep(50 * time.Millisecond)

	if c := countFrames[InterruptFrame](sink.Captured()); c != 1 {
		t.Fatalf("expected exactly 1 InterruptFrame (the foreign one only), got %d in %s", c, describeFrameTypes(sink.Captured()))
	}
	if c := countFrames[LLMMessagesAppendFrame](source.Captured()); c != 0 {
		t.Fatalf("expected no regeneration request from a dropped stale check, got %d", c)
	}

	stopProcessorsAndWait(t, fix, 2*time.Second, source, p, sink)
}

// 13. A straggler from a previous turn never attaches to the next turn.
func TestResponseGuardStragglerFromPreviousTurnNeverAttachesToNext(t *testing.T) {
	fix := newTestFixture(t)
	release := make(chan struct{})
	guardStarted := make(chan struct{}, 1)
	guard := func(_ context.Context, fragment string) bool {
		if fragment == "Stale sentence." {
			guardStarted <- struct{}{}
			<-release
			return true
		}
		return false
	}
	p := NewResponseGuardProcessor(fix.TaskCtx, guard)

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	fix.TaskCtx.EndTask = func(reason EndReason) {
		source.QueueFrame(NewEndFrame(string(reason)), Downstream)
	}
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewLLMResponseStartFrame(time.Now()), Downstream) // turn 1
	source.QueueFrame(NewTextFrame("Stale sentence."), Downstream)

	select {
	case <-guardStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("guard was never invoked for turn 1")
	}

	// Start a new turn while turn 1's check is still blocked.
	source.QueueFrame(NewLLMResponseStartFrame(time.Now()), Downstream) // turn 2
	source.QueueFrame(NewTextFrame("Fresh sentence."), Downstream)
	time.Sleep(30 * time.Millisecond)

	// Now let the stale check return true.
	close(release)
	time.Sleep(50 * time.Millisecond)

	if c := countFrames[InterruptFrame](sink.Captured()); c != 0 {
		t.Fatalf("straggler from turn 1 triggered an interrupt affecting turn 2, got %d InterruptFrames", c)
	}

	stopProcessorsAndWait(t, fix, 2*time.Second, source, p, sink)
}

// 14. A nil ResponseGuard is a wiring bug and must panic.
func TestResponseGuardNilGuardPanics(t *testing.T) {
	fix := newTestFixture(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for a nil ResponseGuard")
		}
	}()
	NewResponseGuardProcessor(fix.TaskCtx, nil)
}

// assertSameSentences checks that got contains exactly the sentences in
// want, ignoring order (concurrently-fired checks may complete in any
// order).
func assertSameSentences(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d checks, got %d: %v", len(want), len(got), got)
	}
	remaining := make(map[string]int, len(want))
	for _, w := range want {
		remaining[w]++
	}
	for _, g := range got {
		if remaining[g] <= 0 {
			t.Fatalf("unexpected or duplicate sentence checked: %q (all got: %v, want: %v)", g, got, want)
		}
		remaining[g]--
	}
}
