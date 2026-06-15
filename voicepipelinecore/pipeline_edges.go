package voicepipelinecore

import (
	"context"
	"log"
)

// PipelineSourceProcessor is the entry point for externally-queued
// frames (today only EndFrame, queued by PipelineTask.End). It has no
// upstream neighbor; frames sent upstream from the chain reach it via
// ProcessFrame and are dropped.
type PipelineSourceProcessor struct {
	*BaseProcessor
	frames chan Frame
	logger *log.Logger
}

func NewPipelineSourceProcessor(taskCtx *TaskContext) *PipelineSourceProcessor {
	p := &PipelineSourceProcessor{
		frames: make(chan Frame, 100),
		logger: taskCtx.Logger,
	}
	p.BaseProcessor = NewBaseProcessor("PipelineSource", p, taskCtx)
	return p
}

// Queue is called by external code (e.g., PipelineTask.End) to inject
// a frame at the head of the pipeline. The frame is delivered
// asynchronously by the source's background goroutine.
func (p *PipelineSourceProcessor) Queue(frame Frame) {
	if f, ok := frame.(EndFrame); ok && p.logger != nil {
		p.logger.Printf("EndFrame queued at pipeline source: reason=%q\n", f.Reason)
	}
	p.frames <- frame
}

func (p *PipelineSourceProcessor) Start(ctx context.Context) {
	p.BaseProcessor.Start(ctx)
	p.Go(p.drainExternalFrames)
}

func (p *PipelineSourceProcessor) drainExternalFrames() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case f := <-p.frames:
			if ef, ok := f.(EndFrame); ok && p.logger != nil {
				p.logger.Printf("EndFrame entering pipeline from source: reason=%q\n", ef.Reason)
			}
			p.PushFrame(f, Downstream)
			if _, isEnd := f.(EndFrame); isEnd {
				// EndFrame is the last frame this processor will emit.
				// Cancel b.ctx so the base's inputLoop and processLoop
				// exit too (they have no other shutdown signal because
				// EndFrame doesn't reach them via neighbors here).
				if p.cancel != nil {
					p.cancel()
				}
				return
			}
		}
	}
}

func (p *PipelineSourceProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	// ErrorFrames bubble up to the source so they can be logged
	// centrally and (if fatal) trigger task shutdown via EndTask.
	// PushError on every processor routes here.
	if ef, ok := frame.(ErrorFrame); ok {
		if p.logger != nil {
			fatalTag := ""
			if ef.Fatal {
				fatalTag = " (fatal)"
			}
			p.logger.Printf("ErrorFrame at pipeline source from %s%s: %s\n", ef.Processor, fatalTag, ef.Err)
		}
		if ef.Fatal && p.taskCtx != nil && p.taskCtx.EndTask != nil {
			if p.taskCtx.Logger != nil {
				p.taskCtx.Logger.Printf("Fatal error details: fatal error from %s: %s\n", ef.Processor, ef.Err)
			}
			p.taskCtx.EndTask(EndReasonError)
		}
		return
	}
	// Other upstream-direction frames from neighbors have no further
	// upstream target (Prev is nil at the source). Downstream-direction
	// frames arrive only via Queue, not via ProcessFrame. Drop silently.
}

// PipelineSinkProcessor terminates the chain. It invokes onEnd when an
// EndFrame arrives. All other frames terminate here silently.
type PipelineSinkProcessor struct {
	*BaseProcessor
	logger *log.Logger
	onEnd  func(EndFrame)
}

func NewPipelineSinkProcessor(taskCtx *TaskContext, onEnd func(EndFrame)) *PipelineSinkProcessor {
	p := &PipelineSinkProcessor{
		logger: taskCtx.Logger,
		onEnd:  onEnd,
	}
	p.BaseProcessor = NewBaseProcessor("PipelineSink", p, taskCtx)
	return p
}

func (p *PipelineSinkProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	if f, ok := frame.(EndFrame); ok {
		if p.logger != nil {
			p.logger.Printf("EndFrame reached pipeline sink: reason=%q\n", f.Reason)
		}
		if p.onEnd != nil {
			p.onEnd(f)
		}
		// PipelineTask cleanup stops the pipeline after this returns.
		return
	}
	// Other frames (upstream from the chain) terminate here.
}
