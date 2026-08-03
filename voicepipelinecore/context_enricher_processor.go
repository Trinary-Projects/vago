package voicepipelinecore

import (
	"context"
)

// MessagesEnricher rewrites the conversation snapshot immediately before it
// reaches the LLM.
//
// It runs synchronously on the processor's frame loop, so it BLOCKS the turn:
// implementations own their own timeout and must return usable messages rather
// than fail the turn — returning the input unchanged is the correct response to
// any internal failure. It must honour ctx, which is cancelled on barge-in and
// at call end; returning nil or an empty slice leaves the turn's messages
// untouched.
//
// The core knows nothing about what enrichment means. Business packages supply
// the implementation (Disha's follow-up calls use it for blocking protocol
// retrieval), keeping vector databases and prompts out of the pipeline package.
type MessagesEnricher func(ctx context.Context, messages []Message) []Message

// ContextEnricherProcessor sits between the user-side context aggregator and
// LLMProcessor and gives an injected MessagesEnricher the chance to rewrite
// each turn's messages before generation starts.
//
// It is placed upstream of LLMProcessor on purpose. LLMProcessor starts its
// TTFB timer before calling the LLM client, so doing this work inside the
// client (or anywhere downstream) would fold enrichment latency into
// llm_ttfb_ms on every persisted turn. Here it finishes first and reports its
// own MetricContextEnrich instead.
//
// No new frame type: it consumes LLMMessagesFrame and emits a fresh one.
type ContextEnricherProcessor struct {
	*BaseProcessor

	taskCtx *TaskContext
	enrich  MessagesEnricher
	metrics *ProcessorMetrics
}

// NewContextEnricherProcessor builds the processor. A nil enricher is a wiring
// bug — a pass-through processor in the pipeline would silently do nothing —
// so it panics rather than degrading quietly, matching how the pipeline treats
// a nil LLMClient.
func NewContextEnricherProcessor(taskCtx *TaskContext, enrich MessagesEnricher) *ContextEnricherProcessor {
	if enrich == nil {
		panic("voicepipelinecore: MessagesEnricher is required")
	}
	p := &ContextEnricherProcessor{
		taskCtx: taskCtx,
		enrich:  enrich,
		metrics: NewProcessorMetrics("context_enricher"),
	}
	p.BaseProcessor = NewBaseProcessor("ContextEnricher", p, taskCtx)
	return p
}

func (p *ContextEnricherProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case LLMMessagesFrame:
		p.PushFrame(p.enrichFrame(ctx, f), dir)
	case InterruptFrame:
		p.metrics.Reset()
		p.PushFrame(f, dir)
	case EndFrame:
		p.metrics.Reset()
		p.PushFrame(f, dir)
	default:
		p.PushFrame(frame, dir)
	}
}

// enrichFrame runs the enricher and returns the frame to forward. Any path that
// can't produce enriched messages returns the original frame, so a failing
// enricher costs the turn nothing.
func (p *ContextEnricherProcessor) enrichFrame(ctx context.Context, frame LLMMessagesFrame) Frame {
	// Already-cancelled turn (barge-in landed between queueing and processing):
	// don't spend the budget, and don't rewrite a turn that is being abandoned.
	if ctx.Err() != nil {
		return frame
	}

	p.metrics.Start(MetricContextEnrich)
	enriched := p.enrich(ctx, cloneMessages(frame.Messages))
	mf := p.metrics.Stop(MetricContextEnrich)
	if mf != nil {
		p.PushFrame(*mf, Downstream)
	}

	if len(enriched) == 0 {
		return frame
	}
	return NewLLMMessagesFrame(enriched)
}
