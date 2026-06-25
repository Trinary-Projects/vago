package voicepipelinecore

import (
	"context"
	"fmt"
	"strings"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

var (
	defaultLLMOutputKillPrefixes      = []string{"<", "_", "{", "startcall:"}
	defaultLLMOutputKillAfterPrefixes = []string{"?"}
)

type llmOutputFilterState struct {
	suppressing       bool
	suppressionReason string
	suppressionPrefix string
	suppressedText    string
	anyTextPushed     bool
}

// LLMOutputFilterProcessor filters known LLM hallucination/control-token
// leaks before they reach TTS. It mirrors Disha backend's
// bots.llm_output_filter.LLMOutputFilter behavior.
type LLMOutputFilterProcessor struct {
	*BaseProcessor
	taskCtx           *TaskContext
	killPrefixes      []string
	killAfterPrefixes []string
	state             llmOutputFilterState
	responseID        int
}

func NewLLMOutputFilterProcessor(taskCtx *TaskContext) *LLMOutputFilterProcessor {
	return NewLLMOutputFilterProcessorWithPrefixes(
		taskCtx,
		defaultLLMOutputKillPrefixes,
		defaultLLMOutputKillAfterPrefixes,
	)
}

func NewLLMOutputFilterProcessorWithPrefixes(taskCtx *TaskContext, killPrefixes, killAfterPrefixes []string) *LLMOutputFilterProcessor {
	p := &LLMOutputFilterProcessor{
		taskCtx:           taskCtx,
		killPrefixes:      append([]string(nil), killPrefixes...),
		killAfterPrefixes: append([]string(nil), killAfterPrefixes...),
	}
	p.BaseProcessor = NewBaseProcessor("LLMOutputFilter", p, taskCtx)
	return p
}

func (p *LLMOutputFilterProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case LLMResponseStartFrame:
		p.onResponseStart()
		p.PushFrame(f, dir)
	case TextFrame:
		p.handleTextFrame(f, dir)
	case LLMResponseEndFrame:
		p.onResponseEnd()
		p.PushFrame(f, dir)
	default:
		p.PushFrame(frame, dir)
	}
}

func (p *LLMOutputFilterProcessor) onResponseStart() {
	p.responseID++
	p.state = llmOutputFilterState{}
	p.logf("LLMOutputFilter: response %d start", p.responseID)
}

func (p *LLMOutputFilterProcessor) onResponseEnd() {
	if !p.state.suppressing {
		return
	}
	p.logf(
		"LLMOutputFilter: response %d ended while suppressing (reason=%s, suppressed_chars=%d)",
		p.responseID,
		p.state.suppressionReason,
		len(p.state.suppressedText),
	)
}

func (p *LLMOutputFilterProcessor) handleTextFrame(frame TextFrame, dir Direction) {
	if p.state.suppressing {
		p.state.suppressedText += frame.Text
		return
	}

	prefix, pos, includePrefix, ok := p.findEarliestKillPrefix(frame.Text)
	if ok {
		p.startSuppressionFromPrefix(frame, dir, prefix, pos, includePrefix)
		return
	}

	p.state.anyTextPushed = true
	p.PushFrame(frame, dir)
}

func (p *LLMOutputFilterProcessor) startSuppressionFromPrefix(frame TextFrame, dir Direction, prefix string, pos int, includePrefix bool) {
	p.state.suppressing = true
	p.state.suppressionReason = "kill-prefix"
	p.state.suppressionPrefix = prefix

	cut := pos
	if includePrefix {
		cut += len(prefix)
	}
	before := frame.Text[:cut]
	p.state.suppressedText = frame.Text[cut:]

	p.logf(
		"LLMOutputFilter: kill-prefix %q in response %d (kept=%d chars, suppressed=%d chars)",
		prefix,
		p.responseID,
		len(before),
		len(p.state.suppressedText),
	)

	if !p.state.anyTextPushed && strings.TrimSpace(before) == "" {
		sentryutil.Capture(sentryutil.Event{
			Message: fmt.Sprintf(
				"LLMOutputFilter: entire response suppressed by kill-prefix %q (response_id=%d)",
				prefix,
				p.responseID,
			),
			Level: sentry.LevelWarning,
			Tags: map[string]string{
				"component": "llm_output_filter",
				"operation": "filter_text",
			},
			Details: map[string]any{
				"response_id": p.responseID,
				"prefix":      prefix,
			},
		})
	}

	if strings.TrimSpace(before) == "" {
		return
	}
	p.PushFrame(NewTextFrame(before), dir)
	p.PushFrame(NewTextFrame("."), dir)
	p.state.anyTextPushed = true
}

func (p *LLMOutputFilterProcessor) findEarliestKillPrefix(text string) (prefix string, pos int, includePrefix bool, ok bool) {
	earliest := len(text)
	for _, candidate := range p.killPrefixes {
		if idx := strings.Index(text, candidate); idx != -1 && idx < earliest {
			earliest = idx
			prefix = candidate
			pos = idx
			includePrefix = false
			ok = true
		}
	}
	for _, candidate := range p.killAfterPrefixes {
		idx := strings.Index(text, candidate)
		if idx != -1 && idx+len(candidate) < len(text) && idx < earliest {
			earliest = idx
			prefix = candidate
			pos = idx
			includePrefix = true
			ok = true
		}
	}
	return prefix, pos, includePrefix, ok
}

func (p *LLMOutputFilterProcessor) logf(format string, args ...any) {
	if p.taskCtx == nil || p.taskCtx.Logger == nil {
		return
	}
	p.taskCtx.Logger.Printf(format+"\n", args...)
}
