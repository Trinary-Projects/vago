package voicepipelinecore

import (
	"context"
	"sync"
	"time"
)

const defaultLLMResponseTimeout = 7 * time.Second

// LLMResponseTimeoutProcessor mirrors Disha backend's
// LLMResponseTimeoutProcessor: when an LLM response remains open too long, it
// interrupts the current turn and asks the upstream context aggregator to run
// the LLM again from the current conversation context.
type LLMResponseTimeoutProcessor struct {
	*BaseProcessor

	timeout time.Duration

	mu          sync.Mutex
	timerCancel context.CancelFunc
	timerSeq    int
}

func NewLLMResponseTimeoutProcessor(taskCtx *TaskContext) *LLMResponseTimeoutProcessor {
	return NewLLMResponseTimeoutProcessorWithTimeout(taskCtx, defaultLLMResponseTimeout)
}

func NewLLMResponseTimeoutProcessorWithTimeout(taskCtx *TaskContext, timeout time.Duration) *LLMResponseTimeoutProcessor {
	if timeout <= 0 {
		timeout = defaultLLMResponseTimeout
	}
	p := &LLMResponseTimeoutProcessor{timeout: timeout}
	p.BaseProcessor = NewBaseProcessor("LLMResponseTimeout", p, taskCtx)
	return p
}

func (p *LLMResponseTimeoutProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case LLMResponseStartFrame:
		p.startTimer()
		p.PushFrame(f, dir)
	case LLMResponseEndFrame:
		p.cancelTimer()
		p.PushFrame(f, dir)
	case InterruptFrame:
		p.cancelTimer()
		p.PushFrame(f, dir)
	default:
		p.PushFrame(frame, dir)
	}
}

func (p *LLMResponseTimeoutProcessor) startTimer() {
	p.mu.Lock()
	p.cancelTimerLocked()

	timerCtx, cancel := context.WithCancel(p.ctx)
	p.timerSeq++
	seq := p.timerSeq
	timeout := p.timeout
	p.timerCancel = cancel
	p.mu.Unlock()

	if p.taskCtx != nil && p.taskCtx.Logger != nil {
		p.taskCtx.Logger.Printf("[LLM_TIMEOUT] Timer started (%s)\n", timeout)
	}

	p.Go(func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			p.timerExpired(seq, timeout)
		case <-timerCtx.Done():
			return
		}
	})
}

func (p *LLMResponseTimeoutProcessor) cancelTimer() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelTimerLocked()
}

func (p *LLMResponseTimeoutProcessor) cancelTimerLocked() {
	if p.timerCancel == nil {
		return
	}
	if p.taskCtx != nil && p.taskCtx.Logger != nil {
		p.taskCtx.Logger.Println("[LLM_TIMEOUT] Timer cancelled")
	}
	p.timerCancel()
	p.timerCancel = nil
}

func (p *LLMResponseTimeoutProcessor) timerExpired(seq int, timeout time.Duration) {
	p.mu.Lock()
	if seq != p.timerSeq || p.timerCancel == nil {
		p.mu.Unlock()
		return
	}
	p.timerCancel = nil
	p.mu.Unlock()

	if p.taskCtx != nil && p.taskCtx.Logger != nil {
		p.taskCtx.Logger.Printf("LLM response timeout (%s) - interrupting and retrying\n", timeout)
	}
	p.Broadcast(NewInterruptFrame())
	p.PushFrame(NewLLMMessagesAppendFrame(nil, true), Upstream)
}
