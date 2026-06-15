package voicepipelinecore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Direction indicates which way a frame is travelling through the
// pipeline. It is carried alongside each frame in an Envelope so
// processors don't have to infer direction from frame type.
type Direction int

const (
	Downstream Direction = iota
	Upstream
)

// Envelope wraps a frame with its travel direction. The pipeline uses
// envelopes inside channels so processors do not need to infer direction
// from frame type; instead they receive (frame, direction) as arguments
// to ProcessFrame.
type Envelope struct {
	Frame     Frame
	Direction Direction
}

// Processor is the interface every pipeline node implements. Concrete
// processors embed *BaseProcessor (which provides every method except
// ProcessFrame) and only implement ProcessFrame for their own routing
// logic.
type Processor interface {
	Name() string

	// Linkage
	Prev() Processor
	Next() Processor
	Link(next Processor)
	SetPrev(prev Processor)

	// Frame plumbing
	QueueFrame(frame Frame, dir Direction)
	ProcessFrame(ctx context.Context, frame Frame, dir Direction)
	PushFrame(frame Frame, dir Direction)
	Broadcast(frame BroadcastableFrame)

	// Lifecycle
	Start(ctx context.Context)
	Stop()
}

const (
	inputDataChCapacity = 100
	inputSysChCapacity  = 16
	procChCapacity      = 100

	// procLoopExitTimeout caps how long the base will wait for processLoop
	// to exit after procCtx is cancelled, in case a user's ProcessFrame
	// implementation does not respect ctx. After this timeout we proceed
	// with the purge anyway; the in-flight ProcessFrame may complete
	// concurrently with the new processLoop, which is a documented
	// constraint of the user contract.
	procLoopExitTimeout = 3 * time.Second
)

// BaseProcessor is embedded into every concrete processor. It owns the
// per-processor goroutine layout, queues, and lifecycle. The design
// mirrors Pipecat's FrameProcessor:
//
//   - inputLoop drains inputSysCh + inputDataCh with system-frame
//     priority, dispatching system frames inline and forwarding data
//     frames to procCh.
//   - processLoop drains procCh, invoking ProcessFrame one frame at a
//     time. On InterruptFrame the base cancels procCtx (stopping any
//     in-flight ProcessFrame work that respects ctx), waits for the
//     processLoop goroutine to exit, purges procCh keeping frames where
//     IsInterruptible() is false, and restarts processLoop with a fresh
//     procCtx.
//
// EndFrame is not a per-processor cancellation signal in the base. It
// travels downstream in normal frame order and final task cleanup stops
// the whole pipeline after EndFrame reaches the sink. This lets post-
// playback processors, such as the assistant context aggregator, flush
// played audio state before cleanup runs.
//
// Concrete processors interact with BaseProcessor through five
// affordances:
//
//   - PushFrame(frame, dir): emit to a neighbor; intercepts MetricsFrame.
//   - QueueFrame(frame, dir): called by neighbors; routes to inputSysCh
//     or inputDataCh based on IsSystem().
//   - Broadcast(frame): emit clones of frame in both directions.
//   - Go(fn): register a background goroutine in the task-wide
//     WaitGroup so PipelineTask.cleanup can wait for it.
//   - taskCtx: shared dependencies (Logger, Room, UIEvents, Metrics,
//     wg, root ctx).
type BaseProcessor struct {
	name    string
	self    Processor // back-reference for ProcessFrame dispatch
	taskCtx *TaskContext

	prev Processor
	next Processor

	// Channels are created at construction so QueueFrame is safe to call
	// before Start (e.g., during Pipeline wiring).
	inputSysCh  chan Envelope
	inputDataCh chan Envelope
	procCh      chan Envelope

	// Per-processor cancellation tree:
	//   taskCtx.Ctx  ->  ctx (per-processor)  ->  procCtx (per-interrupt)
	ctx        context.Context
	cancel     context.CancelFunc
	procCtx    context.Context
	procCancel context.CancelFunc
	procMu     sync.Mutex // guards procCtx/procCancel swap on interrupt

	procWG sync.WaitGroup // tracks processLoop only, for cancel-and-recreate

	cancelling atomic.Bool
	started    atomic.Bool
}

// NewBaseProcessor constructs a BaseProcessor. The self parameter must
// be the outer struct that embeds *BaseProcessor; it is used to dispatch
// ProcessFrame and to set bidirectional prev/next links.
//
// The per-processor ctx is derived from taskCtx.Ctx at construction
// time (not in Start) so that QueueFrame, PushFrame, and Stop can read
// b.ctx safely from other goroutines without a data race against Start.
// b.cancel is also wired now, so Stop is safe to call before Start.
func NewBaseProcessor(name string, self Processor, taskCtx *TaskContext) *BaseProcessor {
	parent := context.Background()
	if taskCtx != nil && taskCtx.Ctx != nil {
		parent = taskCtx.Ctx
	}
	ctx, cancel := context.WithCancel(parent)
	return &BaseProcessor{
		name:        name,
		self:        self,
		taskCtx:     taskCtx,
		ctx:         ctx,
		cancel:      cancel,
		inputSysCh:  make(chan Envelope, inputSysChCapacity),
		inputDataCh: make(chan Envelope, inputDataChCapacity),
		procCh:      make(chan Envelope, procChCapacity),
	}
}

// Name returns the processor identifier used in logs.
func (b *BaseProcessor) Name() string { return b.name }

// Prev returns the previous processor in the chain, or nil at the source.
func (b *BaseProcessor) Prev() Processor { return b.prev }

// Next returns the next processor in the chain, or nil at the sink.
func (b *BaseProcessor) Next() Processor { return b.next }

// SetPrev is called by Link on the next processor to wire up the
// bidirectional link. It is exposed on the interface so Link can call
// it through Processor without needing access to BaseProcessor.
func (b *BaseProcessor) SetPrev(p Processor) { b.prev = p }

// Link establishes a bidirectional link between this processor and next.
func (b *BaseProcessor) Link(next Processor) {
	b.next = next
	next.SetPrev(b.self)
}

// Go registers a background goroutine in the task-wide WaitGroup so
// that PipelineTask.cleanup can wait for all per-task goroutines to
// exit before tearing down session resources.
func (b *BaseProcessor) Go(fn func()) {
	if b.taskCtx == nil || b.taskCtx.wg == nil {
		go fn()
		return
	}
	b.taskCtx.wg.Add(1)
	go func() {
		defer b.taskCtx.wg.Done()
		fn()
	}()
}

// QueueFrame is called by neighboring processors to deliver a frame to
// this one. System frames go to inputSysCh (priority), data frames to
// inputDataCh.
func (b *BaseProcessor) QueueFrame(frame Frame, dir Direction) {
	if b.cancelling.Load() {
		return
	}
	target := b.inputDataCh
	if frame.IsSystem() {
		target = b.inputSysCh
	}
	select {
	case target <- Envelope{Frame: frame, Direction: dir}:
	case <-b.ctx.Done():
	}
}

// PushFrame routes a frame to a neighbor. MetricsFrame is intercepted
// here and delivered to the task's metrics handler instead of
// propagating to the next processor.
func (b *BaseProcessor) PushFrame(frame Frame, dir Direction) {
	if mf, ok := frame.(MetricsFrame); ok {
		if b.taskCtx != nil && b.taskCtx.Metrics != nil {
			b.taskCtx.Metrics(mf)
		}
		return
	}
	target := b.next
	if dir == Upstream {
		target = b.prev
	}
	if target == nil {
		return
	}
	target.QueueFrame(frame, dir)
}

// Broadcast emits two clones of frame: one downstream and one upstream.
// Each receiver sees a distinct frame instance so downstream and
// upstream consumers can hold independent state. Sibling IDs (Pipecat's
// broadcast_sibling_id) are intentionally omitted; they exist in
// Pipecat only to let frame observers deduplicate. Our integration
// callbacks attach at committed conversation-turn boundaries instead.
func (b *BaseProcessor) Broadcast(frame BroadcastableFrame) {
	down := frame.Clone()
	up := frame.Clone()
	b.PushFrame(down, Downstream)
	b.PushFrame(up, Upstream)
}

// PushError emits an ErrorFrame upstream so PipelineSourceProcessor can
// log it and (if fatal) terminate the task via taskCtx.EndTask. Mirrors
// Pipecat's FrameProcessor.push_error: the processor that detects an
// error reports it, the source decides what to do.
//
// ErrorFrame is a system frame so it bubbles past any queued data
// frames upstream; non-fatal errors are informational, fatal errors
// trigger graceful task shutdown at the source.
func (b *BaseProcessor) PushError(errMsg string, fatal bool) {
	frame := NewErrorFrame(b.name, errMsg, fatal)
	if b.taskCtx != nil && b.taskCtx.Logger != nil {
		fatalTag := ""
		if fatal {
			fatalTag = " (fatal)"
		}
		b.taskCtx.Logger.Printf("ErrorFrame pushed from %s%s: %s\n", b.name, fatalTag, errMsg)
	}
	b.PushFrame(frame, Upstream)
}

// Start begins the input and process loops. Concrete processors that
// need their own background goroutines should call BaseProcessor.Start
// first, then b.Go(...) to register them.
//
// b.ctx and b.cancel were initialised in NewBaseProcessor from
// taskCtx.Ctx, so the parent passed here is currently unused. Keeping
// the parameter preserves API symmetry with PipelineTask.Start, where
// a future per-task overriding context might be useful.
func (b *BaseProcessor) Start(_ context.Context) {
	if !b.started.CompareAndSwap(false, true) {
		return
	}
	b.startProcessLoop()
	b.Go(b.inputLoop)
}

// Stop signals cancellation to every goroutine in this processor's
// tree. The actual wait for goroutines to exit happens at the
// PipelineTask level via the shared WaitGroup; Stop itself returns
// immediately. Stop is idempotent and safe to call before Start.
func (b *BaseProcessor) Stop() {
	if !b.cancelling.CompareAndSwap(false, true) {
		return
	}
	b.cancel()
}

// inputLoop drains inputSysCh + inputDataCh with system-frame priority.
// System frames are dispatched inline so they cannot wait behind queued
// data frames; data frames are forwarded to procCh for the processLoop
// to handle one at a time.
func (b *BaseProcessor) inputLoop() {
	for {
		// Priority pass: try the system channel first.
		select {
		case <-b.ctx.Done():
			return
		case env := <-b.inputSysCh:
			b.handleSystem(env)
			continue
		default:
		}
		// Fair pass: either channel.
		select {
		case <-b.ctx.Done():
			return
		case env := <-b.inputSysCh:
			b.handleSystem(env)
		case env := <-b.inputDataCh:
			select {
			case b.procCh <- env:
			case <-b.ctx.Done():
				return
			}
		}
	}
}

// startProcessLoop creates a fresh procCtx and spawns a new processLoop
// goroutine. Called from Start and from interruptProcessLoop after an
// interrupt.
func (b *BaseProcessor) startProcessLoop() {
	b.procMu.Lock()
	b.procCtx, b.procCancel = context.WithCancel(b.ctx)
	procCtx := b.procCtx
	b.procMu.Unlock()

	b.procWG.Add(1)
	b.taskCtx.wg.Add(1)
	go func() {
		defer b.taskCtx.wg.Done()
		defer b.procWG.Done()
		b.processLoop(procCtx)
	}()
}

// processLoop drains procCh and invokes ProcessFrame for each data
// frame. Exits when ctx is cancelled (interrupt or task shutdown).
//
// The non-blocking ctx.Done check at the top of each iteration matters
// for correctness during cancel-and-recreate: after an in-flight
// ProcessFrame returns due to ctx cancellation, Go's `select` would
// pick randomly between ctx.Done and a non-empty procCh. The priority
// check guarantees we exit instead of consuming more frames that
// interruptProcessLoop is about to purge.
func (b *BaseProcessor) processLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		select {
		case <-ctx.Done():
			return
		case env := <-b.procCh:
			b.self.ProcessFrame(ctx, env.Frame, env.Direction)
		}
	}
}

// handleSystem dispatches a system frame. For InterruptFrame, it
// performs the cancel-and-recreate dance so that any in-flight
// ProcessFrame work that respects ctx is cancelled, procCh is purged of
// interruptible frames, and a fresh procCtx is wired up before the
// user's InterruptFrame handler runs.
func (b *BaseProcessor) handleSystem(env Envelope) {
	if _, isInterrupt := env.Frame.(InterruptFrame); isInterrupt {
		b.interruptProcessLoop()
	}
	b.self.ProcessFrame(b.ctx, env.Frame, env.Direction)
}

// interruptProcessLoop cancels the current procCtx, waits for the
// processLoop goroutine to exit (bounded), purges procCh of
// interruptible frames, then starts a fresh processLoop. After this
// returns, no in-flight ProcessFrame call is running and procCh holds
// only frames marked !IsInterruptible.
func (b *BaseProcessor) interruptProcessLoop() {
	b.procMu.Lock()
	cancel := b.procCancel
	b.procMu.Unlock()
	if cancel != nil {
		cancel()
	}

	// Bounded wait for the cancelled processLoop to exit so the next
	// startProcessLoop sees a clean WaitGroup.
	done := make(chan struct{})
	go func() {
		b.procWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(procLoopExitTimeout):
		if b.taskCtx != nil && b.taskCtx.Logger != nil {
			b.taskCtx.Logger.Printf("%s: processLoop did not exit within %s after interrupt; continuing anyway", b.name, procLoopExitTimeout)
		}
	}

	// Drain procCh, keeping frames that must survive an interrupt.
	var keep []Envelope
drain:
	for {
		select {
		case env := <-b.procCh:
			if !env.Frame.IsInterruptible() {
				keep = append(keep, env)
			}
		default:
			break drain
		}
	}

	// Start a fresh processLoop and push the kept frames back.
	b.startProcessLoop()
	for _, env := range keep {
		select {
		case b.procCh <- env:
		case <-b.ctx.Done():
			return
		}
	}
}
