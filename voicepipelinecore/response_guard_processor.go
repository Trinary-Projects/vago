package voicepipelinecore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

// endsWithSentenceTerminator reports whether s ends a sentence.
//
// Deliberately NOT tts_processor.go's endsWithPunctuation, and the divergence
// is the point. That helper is unicode.IsPunct on the final rune, so it also
// flushes on commas, semicolons, colons, dashes and quotes — which is right
// for TTS, where flushing a clause early gets audio started sooner, but wrong
// for a guard. A clause is usually too little context for a similarity match
// to mean anything, and clause-level splitting produced roughly 8-12 checks on
// an ordinary three-sentence turn instead of 3.
//
// Trailing closing delimiters and whitespace are skipped before the test, so
// `He said "stop."` and `(that's final!)` both terminate.
//
// The Devanagari danda is included: Disha speaks Hindi and Hinglish, and a
// Hindi sentence ends in U+0964, not a full stop.
//
// Known and accepted: an abbreviation ("Dr.") or a decimal mid-number
// ("take 2.5 mg", momentarily buffered as "take 2.") terminates early and
// costs one extra check. That check fails open and is harmless — cheaper than
// the sentence-segmentation machinery avoiding it would need.
func endsWithSentenceTerminator(s string) bool {
	for len(s) > 0 {
		r, size := utf8.DecodeLastRuneInString(s)
		if r == utf8.RuneError && size <= 1 {
			return false
		}
		if unicode.IsSpace(r) || isClosingDelimiter(r) {
			s = s[:len(s)-size]
			continue
		}
		return isSentenceTerminator(r)
	}
	return false
}

func isSentenceTerminator(r rune) bool {
	switch r {
	case '.', '!', '?',
		'…', // … horizontal ellipsis
		'।', // । Devanagari danda
		'॥': // ॥ Devanagari double danda
		return true
	}
	return false
}

func isClosingDelimiter(r rune) bool {
	switch r {
	case '"', '\'', ')', ']', '}',
		'”', // ” right double quote
		'’', // ’ right single quote
		'»': // » right guillemet
		return true
	}
	return false
}

// responseGuardProcessorName labels this processor's metrics. Unlike every
// other processor here, the guard does NOT use a ProcessorMetrics timer map:
// a turn fans out to one check per fragment, all sharing a single label, and
// that map holds one start time per label — so concurrent Start calls
// overwrite each other and the first Stop deletes the entry, leaving later
// checks unmeasured. Each check therefore times itself and emits its own
// MetricsFrame directly.
const responseGuardProcessorName = "response_guard"

// ResponseGuard inspects one completed fragment of the assistant's in-flight
// response. It runs on its own goroutine (see ResponseGuardProcessor.Go) and
// must not block the pipeline — it has no deadline imposed by the core, so
// implementations own their own timeout and must fail open (return false) on
// any internal failure rather than stall forever. It must honour ctx, which
// is cancelled the moment another check in the same turn wins the race, and
// at InterruptFrame/EndFrame.
//
// Returning true means the response is unacceptable and the turn should be
// discarded and regenerated. The core knows nothing about what makes a
// fragment unacceptable; business packages supply the implementation (Disha's
// follow-up calls use it for a vector-similarity + judge-LLM check), keeping
// vector databases and prompts out of the pipeline package.
type ResponseGuard func(ctx context.Context, fragment string) bool

// ResponseGuardProcessor sits between LLMOutputFilterProcessor and TTS and
// gives an injected ResponseGuard the chance to flag a completed fragment of
// the assistant's response as unacceptable, interrupting and regenerating the
// turn when it does.
//
// Unlike ContextEnricherProcessor this step is never on the critical path:
// every TextFrame is forwarded immediately and unconditionally, and the check
// itself runs on a background goroutine so TTS never waits on it. The cost of
// that non-blocking guarantee is that the assistant may already be speaking a
// fragment by the time its check resolves — that trade-off belongs to the
// caller's guard implementation and configuration, not to this processor.
//
// No new frame type: it reuses InterruptFrame and LLMMessagesAppendFrame
// exactly as LLMResponseTimeoutProcessor does to interrupt-and-regenerate.
type ResponseGuardProcessor struct {
	*BaseProcessor

	taskCtx *TaskContext
	guard   ResponseGuard

	// Per-turn state. buffer/checks are only ever touched from ProcessFrame
	// (TextFrame/LLMResponseStartFrame run on the processLoop goroutine,
	// InterruptFrame runs on the inputLoop goroutine as a system frame), and
	// turnCtx/turnCancel/skipTurn are additionally written by background
	// check goroutines on a successful verdict, so all of it is guarded by mu
	// rather than confined to a single goroutine.
	mu         sync.Mutex
	buffer     string
	checks     int
	skipTurn   bool // one-retry latch: the regeneration right after a self-triggered interrupt runs no checks
	turnCtx    context.Context
	turnCancel context.CancelFunc

	// fired is a per-turn "have I already interrupted" latch. Exactly one
	// check per turn may win the CompareAndSwap and fire the interrupt.
	fired atomic.Bool
}

// NewResponseGuardProcessor builds the processor. A nil guard is a wiring
// bug — a pass-through guard would silently never interrupt anything — so it
// panics rather than degrading quietly, matching NewContextEnricherProcessor.
func NewResponseGuardProcessor(taskCtx *TaskContext, guard ResponseGuard) *ResponseGuardProcessor {
	if guard == nil {
		panic("voicepipelinecore: ResponseGuard is required")
	}
	p := &ResponseGuardProcessor{
		taskCtx: taskCtx,
		guard:   guard,
	}
	p.BaseProcessor = NewBaseProcessor("ResponseGuard", p, taskCtx)
	return p
}

func (p *ResponseGuardProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case TextFrame:
		// Forward FIRST: the check must never delay audio.
		p.PushFrame(f, dir)
		p.handleText(f.Text)
	case LLMResponseStartFrame:
		p.handleTurnStart()
		p.PushFrame(f, dir)
	case LLMResponseEndFrame:
		// Deliberately no check here: any trailing un-punctuated remainder
		// in the buffer is not a completed fragment.
		p.PushFrame(f, dir)
	case InterruptFrame:
		p.handleInterrupt()
		p.PushFrame(f, dir)
	case EndFrame:
		p.cancelTurn()
		p.PushFrame(f, dir)
	default:
		p.PushFrame(frame, dir)
	}
}

// handleText accumulates text into the per-turn buffer and fires a check
// whenever the buffer ends a sentence. The aggregate-then-flush shape mirrors
// TTS's loop (tts_processor.go), but the boundary test does NOT: TTS flushes
// on any punctuation, while a guard wants whole sentences — see
// endsWithSentenceTerminator. Fragments with no alphanumeric content (a bare
// "..." or " —") are skipped without counting as a check. When the current
// turn has no turnCtx (the skipTurn latch consumed it — see
// handleTurnStart), nothing is accumulated or fired: no checks run this turn.
func (p *ResponseGuardProcessor) handleText(text string) {
	p.mu.Lock()
	if p.turnCtx == nil {
		p.mu.Unlock()
		return
	}
	p.buffer += text

	var (
		fire     bool
		fragment string
		turnCtx  context.Context
	)
	if endsWithSentenceTerminator(p.buffer) {
		fragment = p.buffer
		p.buffer = ""
		if containsAlnum(fragment) {
			p.checks++
			fire = true
			turnCtx = p.turnCtx
		}
	}
	p.mu.Unlock()

	if fire {
		p.spawnCheck(turnCtx, fragment)
	}
}

// handleTurnStart resets per-turn state for a new generation. If skipTurn is
// set (the previous turn's check fired and this is its one allowed retry),
// it is consumed here and no turnCtx is created, so handleText's guard above
// keeps this turn entirely unchecked. Otherwise a fresh turn context is
// created, derived from the processor's own lifetime (b.ctx), so
// InterruptFrame/EndFrame can cancel every in-flight check for this turn at
// once.
func (p *ResponseGuardProcessor) handleTurnStart() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = ""
	p.checks = 0
	p.fired.Store(false)

	if p.skipTurn {
		p.skipTurn = false
		p.turnCtx = nil
		p.turnCancel = nil
		return
	}

	turnCtx, cancel := context.WithCancel(p.ctx)
	p.turnCtx = turnCtx
	p.turnCancel = cancel
}

// handleInterrupt cancels every in-flight check for the current turn, resets
// per-turn state, and always clears skipTurn.
//
// Always clearing is correct because Broadcast pushes only to prev and next,
// never back into this processor's own input queues, so the interrupt this
// processor fires is never one it observes. Every InterruptFrame reaching
// ProcessFrame is therefore foreign by construction — a barge-in travelling
// downstream from UserContextAggregator, or a talk-time/timeout interrupt —
// and each must clear the latch. Leaving skipTurn set would silently drop the
// guard on the user's *next real* turn, which is the exact failure the latch
// is meant to avoid: a barge-in landing between our interrupt and the
// regeneration's first token (a full LLM round trip, and the user is likely
// reacting to what was just said) would otherwise consume the latch on their
// genuine reply instead of on our own retry.
func (p *ResponseGuardProcessor) handleInterrupt() {
	p.mu.Lock()
	if p.turnCancel != nil {
		p.turnCancel()
	}
	p.turnCtx = nil
	p.turnCancel = nil
	p.buffer = ""
	p.checks = 0
	p.skipTurn = false
	p.mu.Unlock()
}

// cancelTurn cancels any in-flight checks on EndFrame. Unlike
// handleInterrupt it does not touch skipTurn/selfOriginated: the call is
// ending, so there is no next turn left to protect or unguard.
func (p *ResponseGuardProcessor) cancelTurn() {
	p.mu.Lock()
	if p.turnCancel != nil {
		p.turnCancel()
	}
	p.turnCtx = nil
	p.turnCancel = nil
	p.mu.Unlock()
}

// spawnCheck runs one guard call on a tracked background goroutine. Exactly
// one check per turn may win the race and fire the interrupt+regenerate
// sequence; every other check (in flight or completing later) is a no-op.
func (p *ResponseGuardProcessor) spawnCheck(turnCtx context.Context, fragment string) {
	if turnCtx == nil {
		return
	}
	p.Go(func() {
		select {
		case <-turnCtx.Done():
			return
		default:
		}

		// Each check times itself rather than sharing a ProcessorMetrics
		// timer — see responseGuardProcessorName for why the shared map
		// cannot represent a fan-out.
		started := time.Now()
		violated := p.guard(turnCtx, fragment)
		elapsed := time.Since(started)

		// A cancelled check was cut short by a sibling winning or by
		// interrupt/shutdown, so its duration measures nothing; emitting it
		// would pollute the metric with truncated samples.
		if turnCtx.Err() == nil {
			p.PushFrame(NewMetricsFrame([]MetricsData{{
				Processor: responseGuardProcessorName,
				Label:     MetricResponseGuard,
				ValueMs:   float64(elapsed.Microseconds()) / 1000.0,
			}}), Downstream)
		}

		if !violated {
			return
		}
		if !p.fired.CompareAndSwap(false, true) {
			// Another check already won the race for this turn.
			return
		}

		p.mu.Lock()
		p.skipTurn = true
		cancel := p.turnCancel
		p.turnCancel = nil
		p.turnCtx = nil
		p.mu.Unlock()

		if cancel != nil {
			cancel() // kill every sibling check for this turn
		}

		// Copied verbatim from llm_response_timeout_processor.go: broadcast
		// the interrupt, then ask the upstream aggregator to run the LLM
		// again from the current context.
		p.Broadcast(NewInterruptFrame())
		p.PushFrame(NewLLMMessagesAppendFrame(nil, true), Upstream)
	})
}

// containsAlnum reports whether s has at least one letter or digit. Used to
// skip firing a check for a fragment that is punctuation-only.
func containsAlnum(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}
	return false
}
