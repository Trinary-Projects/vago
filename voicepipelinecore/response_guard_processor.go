package voicepipelinecore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

// ResponseGuard checks one completed sentence of an in-flight LLM response.
// It returns true when the sentence violates a policy and the turn must be
// interrupted and regenerated. Implementations own all policy, recording and
// error handling; a guard that cannot decide must return false (fail open).
//
// The core knows nothing about what a violation means. Business packages
// supply the implementation (Disha's follow-up calls check against a vector
// store of response policies), keeping vector databases and judge prompts out
// of the pipeline package.
type ResponseGuard func(ctx context.Context, fragment string) bool

// ResponseGuardProcessor sits between LLMOutputFilterProcessor and
// TTSProcessor. It observes the downstream TextFrame stream of the LLM's
// response, splits it into complete sentences, and fires one ResponseGuard
// check per sentence in the background. If a check reports a violation, the
// processor interrupts the current turn and asks the upstream context
// aggregator to regenerate — mirroring LLMResponseTimeoutProcessor's
// interrupt-and-retry shape, but triggered by content instead of a timer.
//
// TextFrames are always forwarded before any processing, so this processor
// never delays audio/text flow: checks run entirely in the background.
//
// Per-turn state (buffer, sentence index, the turn's own cancellable
// context) is reset on every LLMResponseStartFrame. Exactly one violated
// check wins the interrupt via an atomic CAS; the regeneration that follows
// runs with checks disabled (skipTurn/unguarded) so the system can never
// loop on its own correction. A foreign InterruptFrame (a real user
// barge-in) always clears that allowance, because Broadcast never delivers
// back to the processor that sent it — every InterruptFrame this processor
// observes originated elsewhere.
type ResponseGuardProcessor struct {
	*BaseProcessor

	taskCtx *TaskContext
	guard   ResponseGuard

	mu     sync.Mutex
	buffer string

	// turnCtx/turnCancel are the current turn's own cancellable context,
	// derived from the processor's ctx. Checks capture turnCtx by value at
	// spawn time, so cancelling and replacing these fields never affects a
	// check already in flight for the turn it was spawned under.
	turnCtx    context.Context
	turnCancel context.CancelFunc

	// cancelReason records why turnCtx was last cancelled, purely for
	// logging a dropped check: "violation" | "barge_in" | "turn_advanced" |
	// "call_ended".
	cancelReason string

	// skipTurn is set by a winning violation check and consumed by the very
	// next LLMResponseStartFrame, which runs that one turn "unguarded" (no
	// checks fired) since it is the allowed correction retry. A foreign
	// InterruptFrame always clears it first, so a real barge-in can never
	// leave the user's next turn unguarded.
	skipTurn  bool
	unguarded bool

	sentenceIndex int

	// violated is a per-turn CAS gate so exactly one check can win the
	// interrupt for a given turn. Reset at the start of every turn.
	violated atomic.Bool
}

// NewResponseGuardProcessor builds the processor. A nil guard is a wiring
// bug — a pass-through processor in the pipeline would silently never check
// anything — so it panics rather than degrading quietly, matching
// NewContextEnricherProcessor's nil-enricher panic.
func NewResponseGuardProcessor(taskCtx *TaskContext, guard ResponseGuard) *ResponseGuardProcessor {
	if guard == nil {
		panic("voicepipelinecore: ResponseGuard is required")
	}
	p := &ResponseGuardProcessor{
		taskCtx: taskCtx,
		guard:   guard,
	}
	p.BaseProcessor = NewBaseProcessor("ResponseGuard", p, taskCtx)

	// No turn is active yet. Start with an already-cancelled context so
	// that any TextFrame arriving before the first LLMResponseStartFrame
	// (which should not happen in practice) accumulates no state and fires
	// no checks.
	turnCtx, turnCancel := context.WithCancel(p.ctx)
	turnCancel()
	p.turnCtx = turnCtx
	p.turnCancel = turnCancel
	p.cancelReason = "turn_advanced"

	return p
}

func (p *ResponseGuardProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case TextFrame:
		// Forward first: this processor must never delay audio/text flow.
		p.PushFrame(f, dir)
		p.handleText(f.Text)
	case LLMResponseStartFrame:
		p.startTurn()
		p.PushFrame(f, dir)
	case LLMResponseEndFrame:
		// A trailing un-punctuated remainder is not a completed sentence
		// and is never checked.
		p.PushFrame(f, dir)
	case InterruptFrame:
		// Every InterruptFrame observed here is foreign (Broadcast never
		// delivers to the processor that sent it), so a barge-in must
		// never leave the next real turn unguarded.
		p.cancelTurn("barge_in")
		p.mu.Lock()
		p.buffer = ""
		p.sentenceIndex = 0
		p.skipTurn = false
		p.unguarded = false
		p.mu.Unlock()
		p.violated.Store(false)
		p.PushFrame(f, dir)
	case EndFrame:
		p.cancelTurn("call_ended")
		p.PushFrame(f, dir)
	default:
		p.PushFrame(frame, dir)
	}
}

// startTurn cancels the previous turn's context (so a straggling check
// drops itself instead of attaching to the new turn), resets per-turn
// state, and creates a fresh turn context. If the previous turn ended in a
// violation (skipTurn), this turn runs unguarded — it is the one allowed
// correction retry.
func (p *ResponseGuardProcessor) startTurn() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.cancelReason = "turn_advanced"
	if p.turnCancel != nil {
		p.turnCancel()
	}

	turnCtx, turnCancel := context.WithCancel(p.ctx)
	p.turnCtx = turnCtx
	p.turnCancel = turnCancel

	p.buffer = ""
	p.sentenceIndex = 0
	p.violated.Store(false)

	if p.skipTurn {
		p.skipTurn = false
		p.unguarded = true
	} else {
		p.unguarded = false
	}
}

// cancelTurn is the single cancellation path besides startTurn's own
// turn-advance cancel. It is called from the check that wins a violation
// ("violation") and from ProcessFrame's InterruptFrame ("barge_in") and
// EndFrame ("call_ended") handling. It records why, then cancels the turn
// context so in-flight checks observe cancellation and drop themselves.
func (p *ResponseGuardProcessor) cancelTurn(reason string) {
	p.mu.Lock()
	p.cancelReason = reason
	cancel := p.turnCancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// handleText accumulates newly streamed text into the turn buffer, splits
// out complete sentences, and fires one background check per sentence that
// contains at least one alphanumeric rune. It does nothing for an unguarded
// turn or a turn whose context is already done.
func (p *ResponseGuardProcessor) handleText(text string) {
	p.mu.Lock()
	if p.unguarded {
		p.mu.Unlock()
		return
	}
	turnCtx := p.turnCtx
	if turnCtx.Err() != nil {
		p.mu.Unlock()
		return
	}

	p.buffer += text
	sentences, remainder := splitSentences(p.buffer)
	p.buffer = remainder

	type pending struct {
		index    int
		sentence string
	}
	var toCheck []pending
	for _, s := range sentences {
		if !hasAlnum(s) {
			continue
		}
		p.sentenceIndex++
		toCheck = append(toCheck, pending{index: p.sentenceIndex, sentence: s})
	}
	p.mu.Unlock()

	for _, item := range toCheck {
		p.fireCheck(turnCtx, item.index, item.sentence)
	}
}

// fireCheck runs one ResponseGuard check in the background. Exactly one
// violated check per turn can win the interrupt (via the violated CAS); a
// check that finishes after its turn's context is already done is logged
// and dropped rather than acted on, per the "when a check finishes, if its
// turn is already done, log it and drop it" rule.
func (p *ResponseGuardProcessor) fireCheck(turnCtx context.Context, index int, sentence string) {
	p.Go(func() {
		start := time.Now()
		violated := p.guard(turnCtx, sentence)
		elapsedMs := float64(time.Since(start).Microseconds()) / 1000.0

		if turnCtx.Err() != nil {
			p.logCancelledCheck(index, sentence)
			return
		}

		p.PushFrame(NewMetricsFrame([]MetricsData{{
			Processor: "response_guard",
			Label:     MetricResponseGuard,
			ValueMs:   elapsedMs,
		}}), Downstream)

		if !violated {
			return
		}
		if !p.violated.CompareAndSwap(false, true) {
			// Another sentence already won the interrupt for this turn.
			return
		}

		p.mu.Lock()
		p.skipTurn = true
		p.mu.Unlock()
		// The winner must cancel the turn itself: it will never observe
		// the InterruptFrame it is about to broadcast (Broadcast only
		// pushes to prev/next).
		p.cancelTurn("violation")

		if p.taskCtx != nil && p.taskCtx.Logger != nil {
			p.taskCtx.Logger.Printf("[RESPONSE_GUARD] sentence %d violated, interrupting: %q\n", index, sentence)
		}
		p.Broadcast(NewInterruptFrame())
		p.PushFrame(NewLLMMessagesAppendFrame(nil, true), Upstream)
	})
}

// logCancelledCheck logs a check that finished after its turn was already
// cancelled. Silent loss of checks has been this feature's recurring
// failure mode, so every cancelled check is logged individually with its
// sentence index, text, and the turn's cancellation reason. This is
// greppable diagnostic logging, not a Sentry-worthy event: a barge-in
// cancelling checks is ordinary traffic.
func (p *ResponseGuardProcessor) logCancelledCheck(index int, sentence string) {
	p.mu.Lock()
	reason := p.cancelReason
	p.mu.Unlock()
	if p.taskCtx != nil && p.taskCtx.Logger != nil {
		p.taskCtx.Logger.Printf("[RESPONSE_GUARD] check %d cancelled (%s), dropped: %q\n", index, reason, sentence)
	}
}

// hasAlnum reports whether s contains at least one letter or digit rune.
// Punctuation-only fragments (e.g. "...", "!?") are never worth checking.
func hasAlnum(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}
