package voicepipelinecore

import (
	"context"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Testing infrastructure ported from Pipecat's tests/utils.py.
//
// Pattern: wrap the processor under test between a `QueueProcessor` that
// captures upstream-going frames (call it the source) and another
// `QueueProcessor` that captures downstream-going frames (the sink).
// Inject frames at the source going downstream; they flow through the
// processor under test; any upstream pushes from the processor are
// captured by the source, downstream pushes by the sink.
//
// At end-of-test we queue an EndFrame through the processor under test,
// then explicitly Stop the isolated test chain and wait on the shared
// WaitGroup before inspecting captured slices.

// SleepFrame is a synthetic frame interpreted by runProcessorTest: when
// the send loop encounters one, it sleeps for the given duration before
// continuing. It never actually flows through the pipeline.
type SleepFrame struct {
	FrameBase
	Duration time.Duration
}

func (f SleepFrame) FrameType() FrameType  { return -1 }
func (f SleepFrame) IsSystem() bool        { return false }
func (f SleepFrame) IsInterruptible() bool { return true }

// QueueProcessor captures frames travelling in a specific direction and
// passes everything through. Used as a source (captures upstream) or
// sink (captures downstream) in tests.
type QueueProcessor struct {
	*BaseProcessor
	captureDir Direction
	mu         sync.Mutex
	captured   []Frame
}

func newQueueProcessor(taskCtx *TaskContext, name string, captureDir Direction) *QueueProcessor {
	q := &QueueProcessor{captureDir: captureDir}
	q.BaseProcessor = NewBaseProcessor(name, q, taskCtx)
	return q
}

func (q *QueueProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	if dir == q.captureDir {
		q.mu.Lock()
		q.captured = append(q.captured, frame)
		q.mu.Unlock()
	}
	q.PushFrame(frame, dir)
}

func (q *QueueProcessor) Captured() []Frame {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Frame, len(q.captured))
	copy(out, q.captured)
	return out
}

// testFixture bundles together a TaskContext, root ctx, and the shared
// WaitGroup so individual tests don't need to wire them up manually.
type testFixture struct {
	TaskCtx    *TaskContext
	Logger     *log.Logger
	RootCtx    context.Context
	RootCancel context.CancelFunc
	WG         *sync.WaitGroup

	// metricsMu protects metrics from concurrent test reads/writes.
	metricsMu sync.Mutex
	metrics   []MetricsFrame
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := log.New(io.Discard, "", 0)
	if testing.Verbose() {
		// In verbose mode, surface logs to the test output.
		logger = log.New(&testLogWriter{t: t}, "[test] ", 0)
	}

	var wg sync.WaitGroup
	fix := &testFixture{
		Logger:     logger,
		RootCtx:    ctx,
		RootCancel: cancel,
		WG:         &wg,
	}
	fix.TaskCtx = &TaskContext{
		Ctx:      ctx,
		Logger:   logger,
		UIEvents: NewUIEventSender(logger),
		Metrics:  fix.captureMetrics,
		metrics:  &perTurnMetrics{},
		wg:       &wg,
	}
	return fix
}

func (f *testFixture) captureMetrics(mf MetricsFrame) {
	f.metricsMu.Lock()
	defer f.metricsMu.Unlock()
	f.metrics = append(f.metrics, mf)
}

func (f *testFixture) Metrics() []MetricsFrame {
	f.metricsMu.Lock()
	defer f.metricsMu.Unlock()
	out := make([]MetricsFrame, len(f.metrics))
	copy(out, f.metrics)
	return out
}

// testLogWriter forwards log output to t.Log for verbose test runs.
type testLogWriter struct{ t *testing.T }

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// runConfig configures a single processor test run.
type runConfig struct {
	processor    Processor
	framesToSend []Frame
	sendEndFrame bool
	// timeout is the overall test timeout (default 5s).
	timeout time.Duration
	// settleDelay is an optional sleep after sending all frames and before
	// the (optional) EndFrame, giving async work time to surface. Default 0.
	settleDelay time.Duration
}

// runProcessorTest wires the processor under test between two
// QueueProcessors, sends the given frames into the source, optionally
// queues an EndFrame to gracefully shut everything down, and returns
// the captured downstream and upstream frames. EndFrame is stripped
// from the returned downstream slice if sendEndFrame was true.
//
// After the send loop finishes we always call Stop() on all three
// processors. This mirrors PipelineTask.completeEnd calling
// pipeline.Stop() in production after EndFrame reaches the sink.
func runProcessorTest(t *testing.T, fix *testFixture, cfg runConfig) (downstream, upstream []Frame) {
	t.Helper()
	if cfg.timeout == 0 {
		cfg.timeout = 5 * time.Second
	}

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)

	// Wire taskCtx.EndTask so processors that call it (e.g.
	// TalkTimeMonitor) inject EndFrame at the source — same behavior as
	// PipelineSource in production.
	fix.TaskCtx.EndTask = func(reason EndReason) {
		source.QueueFrame(NewEndFrame(string(reason)), Downstream)
	}

	source.Link(cfg.processor)
	cfg.processor.Link(sink)

	source.Start(fix.RootCtx)
	cfg.processor.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	// Send frames. SleepFrame is consumed inline; other frames are queued
	// downstream into the source so they flow through the processor.
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		time.Sleep(5 * time.Millisecond)
		for _, f := range cfg.framesToSend {
			if sf, ok := f.(SleepFrame); ok {
				time.Sleep(sf.Duration)
				continue
			}
			source.QueueFrame(f, Downstream)
		}
		if cfg.settleDelay > 0 {
			time.Sleep(cfg.settleDelay)
		}
		if cfg.sendEndFrame {
			source.QueueFrame(EndFrame{}, Downstream)
		}
	}()

	// Wait for the send goroutine to finish.
	select {
	case <-sendDone:
	case <-time.After(cfg.timeout):
		t.Fatalf("runProcessorTest: send loop did not complete within %s", cfg.timeout)
	}

	// Give in-flight frames a chance to propagate before forcing cleanup.
	time.Sleep(50 * time.Millisecond)

	// Force shutdown after the test chain had a chance to process
	// EndFrame and any immediately-following frames.
	source.Stop()
	cfg.processor.Stop()
	sink.Stop()

	if err := waitForWG(fix.WG, cfg.timeout); err != nil {
		t.Fatalf("runProcessorTest: %v (captured %d down / %d up frames so far)", err, len(sink.Captured()), len(source.Captured()))
	}

	down := sink.Captured()
	up := source.Captured()
	if cfg.sendEndFrame {
		down = stripEndFrames(down)
	}
	return down, up
}

// stripEndFrames removes EndFrames from the slice (used to filter out
// the test harness's own shutdown frame).
func stripEndFrames(frames []Frame) []Frame {
	out := frames[:0]
	for _, f := range frames {
		if _, ok := f.(EndFrame); ok {
			continue
		}
		out = append(out, f)
	}
	return out
}

// waitForWG waits for wg to reach zero, bounded by timeout.
func waitForWG(wg *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout after %s waiting for goroutines", timeout)
	}
}

func stopProcessorsAndWait(t *testing.T, fix *testFixture, timeout time.Duration, processors ...Processor) {
	t.Helper()
	for _, p := range processors {
		if p != nil {
			p.Stop()
		}
	}
	if err := waitForWG(fix.WG, timeout); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}

// assertFrameTypes asserts that frames match the expected concrete types
// in order. Each expected entry should be a zero-value frame of the
// desired type, e.g. `TextFrame{}` (or pointer for system frames).
// Mismatches produce a readable error including the actual sequence.
func assertFrameTypes(t *testing.T, got []Frame, expected []Frame) {
	t.Helper()
	if len(got) != len(expected) {
		t.Errorf("frame count mismatch: got %d, want %d\n  got:  %s\n  want: %s",
			len(got), len(expected), describeFrameTypes(got), describeFrameTypes(expected))
		return
	}
	for i := range got {
		gotT := reflect.TypeOf(got[i])
		expT := reflect.TypeOf(expected[i])
		if gotT != expT {
			t.Errorf("frame[%d] type mismatch: got %v, want %v\n  full got:  %s\n  full want: %s",
				i, gotT, expT, describeFrameTypes(got), describeFrameTypes(expected))
			return
		}
	}
}

func describeFrameTypes(frames []Frame) string {
	parts := make([]string, 0, len(frames))
	for _, f := range frames {
		parts = append(parts, reflect.TypeOf(f).Name())
	}
	return "[" + joinComma(parts) + "]"
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

// findFrame returns the first frame of type T from the slice, or
// nil/false if not found.
func findFrame[T Frame](frames []Frame) (T, bool) {
	var zero T
	for _, f := range frames {
		if t, ok := f.(T); ok {
			return t, true
		}
	}
	return zero, false
}

// countFrames returns the number of frames of type T in the slice.
func countFrames[T Frame](frames []Frame) int {
	count := 0
	for _, f := range frames {
		if _, ok := f.(T); ok {
			count++
		}
	}
	return count
}
