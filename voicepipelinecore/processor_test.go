package voicepipelinecore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// passThroughProcessor is a minimal Processor for testing the base
// itself. It just forwards every frame in its arrival direction and
// records the per-frame ctx so tests can verify ctx propagation.
type passThroughProcessor struct {
	*BaseProcessor
	mu           sync.Mutex
	received     []recordedFrame
	onInterrupt  func()
	blockOnFrame chan struct{} // if non-nil, ProcessFrame blocks on this until ctx is cancelled
}

type recordedFrame struct {
	frame Frame
	dir   Direction
	// ctxAlive is true if the per-call ctx had not been cancelled when
	// ProcessFrame began.
	ctxAlive bool
}

func newPassThroughProcessor(taskCtx *TaskContext, name string) *passThroughProcessor {
	p := &passThroughProcessor{}
	p.BaseProcessor = NewBaseProcessor(name, p, taskCtx)
	return p
}

func (p *passThroughProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	alive := ctx.Err() == nil
	p.mu.Lock()
	p.received = append(p.received, recordedFrame{frame: frame, dir: dir, ctxAlive: alive})
	p.mu.Unlock()
	if _, isInt := frame.(InterruptFrame); isInt && p.onInterrupt != nil {
		p.onInterrupt()
	}
	if p.blockOnFrame != nil {
		select {
		case <-p.blockOnFrame:
		case <-ctx.Done():
		}
	}
	p.PushFrame(frame, dir)
}

func (p *passThroughProcessor) Received() []recordedFrame {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]recordedFrame, len(p.received))
	copy(out, p.received)
	return out
}

// TestBaseProcessor_BasicForward verifies frames flow source → pt → sink.
func TestBaseProcessor_BasicForward(t *testing.T) {
	fix := newTestFixture(t)
	pt := newPassThroughProcessor(fix.TaskCtx, "pt")

	down, up := runProcessorTest(t, fix, runConfig{
		processor: pt,
		framesToSend: []Frame{
			TextFrame{Text: "hello"},
			TranscriptFrame{Text: "world", IsFinal: true},
		},
		sendEndFrame: true,
	})
	if len(up) != 0 {
		t.Errorf("expected no upstream frames, got %d: %s", len(up), describeFrameTypes(up))
	}
	assertFrameTypes(t, down, []Frame{TextFrame{}, TranscriptFrame{}})
}

// TestBaseProcessor_UpstreamForward verifies a processor's upstream
// PushFrame reaches the source.
//
// We use a settleDelay between the trigger frame and shutdown so the
// upstream push has time to reach the source before the test chain stops.
func TestBaseProcessor_UpstreamForward(t *testing.T) {
	fix := newTestFixture(t)
	pe := &upstreamEmitter{emitted: &atomic.Bool{}}
	pe.BaseProcessor = NewBaseProcessor("emit", pe, fix.TaskCtx)

	down, up := runProcessorTest(t, fix, runConfig{
		processor:    pe,
		framesToSend: []Frame{TextFrame{Text: "hi"}},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	assertFrameTypes(t, down, []Frame{TextFrame{}})
	assertFrameTypes(t, up, []Frame{TTSDoneFrame{}})
}

type upstreamEmitter struct {
	*BaseProcessor
	emitted *atomic.Bool
}

func (e *upstreamEmitter) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	if _, ok := frame.(TextFrame); ok {
		if e.emitted.CompareAndSwap(false, true) {
			e.PushFrame(TTSDoneFrame{}, Upstream)
		}
	}
	e.PushFrame(frame, dir)
}

// TestBaseProcessor_EndFrameDoesNotAutoCancel verifies the base keeps a
// processor alive after EndFrame passes through. PipelineTask cleanup is
// responsible for stopping processors once EndFrame reaches the sink, which
// leaves post-playback processors able to flush state during graceful shutdown.
func TestBaseProcessor_EndFrameDoesNotAutoCancel(t *testing.T) {
	fix := newTestFixture(t)
	pt := newTrackedProcessor(fix.TaskCtx)

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(pt)
	pt.Link(sink)
	source.Start(fix.RootCtx)
	pt.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(EndFrame{}, Downstream)
	time.Sleep(50 * time.Millisecond)

	select {
	case <-pt.exited:
		t.Fatal("background goroutine exited after EndFrame before explicit Stop")
	default:
	}

	source.Stop()
	pt.Stop()
	sink.Stop()
	if err := waitForWG(fix.WG, 3*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}

	select {
	case <-pt.exited:
	case <-time.After(time.Second):
		t.Fatal("background goroutine did not exit after explicit Stop")
	}
}

// trackedProcessor spawns a Go-tracked goroutine in Start that closes
// `exited` when b.ctx is cancelled. Used to verify EndFrame propagates
// the cancel down to user-spawned goroutines.
type trackedProcessor struct {
	*BaseProcessor
	exited chan struct{}
}

func newTrackedProcessor(taskCtx *TaskContext) *trackedProcessor {
	p := &trackedProcessor{exited: make(chan struct{})}
	p.BaseProcessor = NewBaseProcessor("tracked", p, taskCtx)
	return p
}

func (p *trackedProcessor) Start(parent context.Context) {
	p.BaseProcessor.Start(parent)
	p.Go(func() {
		<-p.ctx.Done()
		close(p.exited)
	})
}

func (p *trackedProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	p.PushFrame(frame, dir)
}

// TestBaseProcessor_MetricsIntercept verifies MetricsFrames pushed by a
// processor are routed to taskCtx.Metrics and never propagate
// downstream.
func TestBaseProcessor_MetricsIntercept(t *testing.T) {
	fix := newTestFixture(t)
	em := &metricEmitter{}
	em.BaseProcessor = NewBaseProcessor("metric-emitter", em, fix.TaskCtx)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor:    em,
		framesToSend: []Frame{TextFrame{Text: "trigger"}},
		sendEndFrame: true,
	})

	if c := countFrames[MetricsFrame](down); c != 0 {
		t.Errorf("MetricsFrame should have been intercepted, but %d reached the sink", c)
	}
	if got := len(fix.Metrics()); got != 1 {
		t.Errorf("expected 1 captured MetricsFrame, got %d", got)
	}
}

type metricEmitter struct {
	*BaseProcessor
}

func (m *metricEmitter) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	if _, ok := frame.(TextFrame); ok {
		m.PushFrame(MetricsFrame{Data: []MetricsData{{Processor: "test", Label: MetricTTFB, ValueMs: 42}}}, Downstream)
	}
	m.PushFrame(frame, dir)
}

// TestBaseProcessor_SystemPriority verifies that a system frame queued
// behind a backlog of data frames is processed before them.
func TestBaseProcessor_SystemPriority(t *testing.T) {
	fix := newTestFixture(t)

	// blockingProcessor blocks ProcessFrame for data frames until we
	// release them, while letting system frames flow.
	bp := &blockingProcessor{
		release: make(chan struct{}),
		seen:    make(chan string, 16),
	}
	bp.BaseProcessor = NewBaseProcessor("blocking", bp, fix.TaskCtx)

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(bp)
	bp.Link(sink)

	source.Start(fix.RootCtx)
	bp.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	// Queue 3 data frames; the first will be in flight (blocked), the
	// other two queued.
	source.QueueFrame(TextFrame{Text: "1"}, Downstream)
	source.QueueFrame(TextFrame{Text: "2"}, Downstream)
	source.QueueFrame(TextFrame{Text: "3"}, Downstream)

	// Give time for the first to enter ProcessFrame and block.
	time.Sleep(50 * time.Millisecond)

	// Queue an InterruptFrame. It should be observed via inputSysCh and
	// trigger handleSystem (which cancels procCtx → unblocks blocking
	// ProcessFrame).
	source.QueueFrame(InterruptFrame{}, Downstream)
	if !waitForSeen(bp.seen, "interrupt", 2*time.Second) {
		t.Fatal("InterruptFrame was not processed within 2s")
	}

	// Now release any blocked ProcessFrame calls so they can return.
	close(bp.release)

	stopProcessorsAndWait(t, fix, 3*time.Second, source, bp, sink)

	// waitForSeen above proves the base dispatched the interrupt before
	// more data frames queued in procCh.
}

type blockingProcessor struct {
	*BaseProcessor
	release chan struct{}
	seen    chan string
}

func (b *blockingProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case TextFrame:
		select {
		case b.seen <- "text:" + f.Text:
		default:
		}
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	case InterruptFrame:
		select {
		case b.seen <- "interrupt":
		default:
		}
	}
	b.PushFrame(frame, dir)
}

func drain(ch chan string) []string {
	var out []string
loop:
	for {
		select {
		case s := <-ch:
			out = append(out, s)
		default:
			break loop
		}
	}
	return out
}

// waitForSeen blocks until `want` is sent on ch or timeout elapses.
// Items not matching `want` are discarded.
func waitForSeen(ch chan string, want string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case got := <-ch:
			if got == want {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// TestBaseProcessor_InterruptPurgesProcCh verifies that data frames
// queued in procCh during an interrupt are purged (except !IsInterruptible
// frames like EndFrame).
//
// This test bypasses the test source/sink helpers and wires bp + sink
// directly so EndFrame propagation through the source doesn't shut the
// source down before we can queue the InterruptFrame.
func TestBaseProcessor_InterruptPurgesProcCh(t *testing.T) {
	fix := newTestFixture(t)

	bp := &blockingProcessor{
		release: make(chan struct{}),
		seen:    make(chan string, 16),
	}
	bp.BaseProcessor = NewBaseProcessor("blocking", bp, fix.TaskCtx)

	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	bp.Link(sink)

	bp.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	// Block ProcessFrame on the first frame
	bp.QueueFrame(TextFrame{Text: "blocked"}, Downstream)
	time.Sleep(150 * time.Millisecond)

	// Pile up interruptible frames; they sit in bp.procCh
	for i := 0; i < 5; i++ {
		bp.QueueFrame(TextFrame{Text: "queued"}, Downstream)
	}
	// EndFrame is !IsInterruptible and must survive the purge
	bp.QueueFrame(EndFrame{}, Downstream)
	// Generous settle to accommodate -race slowdown of inputLoop moving
	// frames from inputDataCh into procCh. Without this, some queued
	// frames remain in inputDataCh when InterruptFrame arrives and are
	// moved into procCh by the new processLoop after the purge — slipping
	// through.
	time.Sleep(500 * time.Millisecond)

	// Trigger interrupt → cancel procCtx → blocked ProcessFrame returns.
	// We then wait for "interrupt" to appear in the seen channel before
	// closing release. Otherwise, close(release) can win the race
	// against the inputLoop dispatching the InterruptFrame: the blocked
	// ProcessFrame unblocks via release instead of ctx.Done, processLoop
	// continues reading from procCh, and the queued TextFrames slip
	// through before the interrupt is ever processed.
	bp.QueueFrame(InterruptFrame{}, Downstream)
	if !waitForSeen(bp.seen, "interrupt", 2*time.Second) {
		t.Fatal("InterruptFrame was not processed within 2s")
	}
	close(bp.release)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got := sink.Captured()
		if countFrames[EndFrame](got) == 1 && countFrames[InterruptFrame](got) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopProcessorsAndWait(t, fix, 3*time.Second, bp, sink)

	got := sink.Captured()
	// "blocked" TextFrame survives — already in flight. The 5 "queued"
	// TextFrames should be purged; EndFrame should be kept.
	textCount := countFrames[TextFrame](got)
	if textCount > 1 {
		t.Errorf("expected at most 1 TextFrame to survive purge (the in-flight one), got %d: %s", textCount, describeFrameTypes(got))
	}
	if c := countFrames[EndFrame](got); c != 1 {
		t.Errorf("expected EndFrame to survive purge, got %d EndFrames: %s", c, describeFrameTypes(got))
	}
	if c := countFrames[InterruptFrame](got); c != 1 {
		t.Errorf("expected InterruptFrame to be forwarded, got %d: %s", c, describeFrameTypes(got))
	}
}

// TestBaseProcessor_QueueFrameDirectionRouting verifies that QueueFrame
// routes system frames to inputSysCh (priority) and data frames to
// inputDataCh (regular).
func TestBaseProcessor_QueueFrameDirectionRouting(t *testing.T) {
	fix := newTestFixture(t)
	pt := newPassThroughProcessor(fix.TaskCtx, "pt")

	pt.Start(fix.RootCtx)

	// Send a data frame and a system frame and verify both are received.
	pt.QueueFrame(TextFrame{Text: "data"}, Downstream)
	pt.QueueFrame(InterruptFrame{}, Downstream)

	// Wait for them to be processed
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(pt.Received()) >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	pt.Stop()
	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}

	rec := pt.Received()
	if len(rec) < 2 {
		t.Fatalf("expected at least 2 frames processed, got %d", len(rec))
	}

	// Both frames should be in the received list (order may vary because
	// system priority is independent of data ordering).
	var sawData, sawSystem bool
	for _, r := range rec {
		switch r.frame.(type) {
		case TextFrame:
			sawData = true
		case InterruptFrame:
			sawSystem = true
		}
	}
	if !sawData {
		t.Error("data frame not received")
	}
	if !sawSystem {
		t.Error("system frame not received")
	}
}

// TestBaseProcessor_LinkSetsNeighbors verifies Link wires prev/next
// pointers bidirectionally.
func TestBaseProcessor_LinkSetsNeighbors(t *testing.T) {
	fix := newTestFixture(t)
	a := newPassThroughProcessor(fix.TaskCtx, "a")
	b := newPassThroughProcessor(fix.TaskCtx, "b")
	c := newPassThroughProcessor(fix.TaskCtx, "c")

	a.Link(b)
	b.Link(c)

	if a.Next() != b {
		t.Error("a.Next should be b")
	}
	if b.Prev() != a {
		t.Error("b.Prev should be a")
	}
	if b.Next() != c {
		t.Error("b.Next should be c")
	}
	if c.Prev() != b {
		t.Error("c.Prev should be b")
	}
	if a.Prev() != nil {
		t.Error("a.Prev should be nil (head of chain)")
	}
	if c.Next() != nil {
		t.Error("c.Next should be nil (tail of chain)")
	}
}

// TestBaseProcessor_BroadcastSendsBothDirections verifies Broadcast
// emits frames upstream and downstream.
func TestBaseProcessor_BroadcastSendsBothDirections(t *testing.T) {
	fix := newTestFixture(t)
	emitter := &broadcastEmitter{}
	emitter.BaseProcessor = NewBaseProcessor("broadcaster", emitter, fix.TaskCtx)

	down, up := runProcessorTest(t, fix, runConfig{
		processor:    emitter,
		framesToSend: []Frame{TextFrame{Text: "trigger"}},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	// Expect: downstream got TextFrame + BotStartedSpeakingFrame (clone),
	//          upstream got BotStartedSpeakingFrame (other clone).
	if c := countFrames[BotStartedSpeakingFrame](down); c != 1 {
		t.Errorf("expected 1 BotStartedSpeakingFrame downstream, got %d: %s", c, describeFrameTypes(down))
	}
	if c := countFrames[BotStartedSpeakingFrame](up); c != 1 {
		t.Errorf("expected 1 BotStartedSpeakingFrame upstream, got %d: %s", c, describeFrameTypes(up))
	}
}

type broadcastEmitter struct {
	*BaseProcessor
}

func (b *broadcastEmitter) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	if _, ok := frame.(TextFrame); ok {
		b.Broadcast(BotStartedSpeakingFrame{})
	}
	b.PushFrame(frame, dir)
}
