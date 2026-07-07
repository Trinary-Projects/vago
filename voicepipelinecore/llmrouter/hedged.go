package llmrouter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

// Hedged one-shot LLM execution, the Go port of Disha's
// services/llm_failover_service.py generate_with_hedged_request. It is a
// thin racing layer over two ordinary fixed-endpoint Routers — all
// request shaping, streaming, blacklist, and logging behavior comes from
// Router.Stream. Built for background calls that must respond fast so
// the conversation can move forward (deep thinking first; get_guidance
// adopts it later, after it soaks).

// defaultHedgeThreshold matches Python's hedge_threshold_ms=1000.
const defaultHedgeThreshold = 1000 * time.Millisecond

// HedgedConfig configures a one-shot client racing a fixed primary/hedge
// endpoint pair.
type HedgedConfig struct {
	// Pair is the hedged pair key, e.g. GroupGPTOSS120FastHedged.
	Pair   string
	Redis  RedisStore
	Logger *log.Logger
	// HTTPClient is optional; used by both attempts (tests inject it).
	HTTPClient *http.Client
	// LogSink receives a CallLog per COMPLETED attempt — both when both
	// complete; an attempt cancelled mid-flight never logs, matching
	// Python's cancelled asyncio task never reaching logging.
	LogSink        func(CallLog)
	PromptMetadata map[string]any

	// HedgeThreshold is the whole-call wait before the hedge fires
	// (0 → 1000ms, Python's default).
	HedgeThreshold time.Duration
	// Temperature/MaxTokens apply to both attempts (Python passes them
	// per call; deep thinking: 0.7 / 4000).
	Temperature *float64
	MaxTokens   *int
}

// Hedged satisfies voicepipelinecore.LLMClient structurally (the Stream
// method), so adopting it is a constructor swap from New.
type Hedged struct {
	primary   *Router
	hedge     *Router
	threshold time.Duration
	logger    *log.Logger
}

func NewHedged(cfg HedgedConfig) (*Hedged, error) {
	pair, ok := hedgedPairs[cfg.Pair]
	if !ok {
		return nil, fmt.Errorf("llmrouter: unknown hedged pair %q", cfg.Pair)
	}
	threshold := cfg.HedgeThreshold
	if threshold <= 0 {
		threshold = defaultHedgeThreshold
	}
	newAttemptRouter := func(endpoint string) (*Router, error) {
		return New(Config{
			FixedEndpoint:  endpoint,
			Redis:          cfg.Redis,
			Logger:         cfg.Logger,
			HTTPClient:     cfg.HTTPClient,
			LogSink:        dropInterruptedLogs(cfg.LogSink),
			PromptMetadata: cfg.PromptMetadata,
			Temperature:    cfg.Temperature,
			MaxTokens:      cfg.MaxTokens,
		})
	}
	primary, err := newAttemptRouter(pair.Primary)
	if err != nil {
		return nil, err
	}
	hedge, err := newAttemptRouter(pair.Hedge)
	if err != nil {
		return nil, err
	}
	return &Hedged{primary: primary, hedge: hedge, threshold: threshold, logger: cfg.Logger}, nil
}

// dropInterruptedLogs filters the per-attempt LogSink: an interrupted
// entry on a background one-shot can only mean the attempt was
// ctx-cancelled mid-flight (hedge loser or parent cancellation), which
// per the closed D4 semantics never logs. Completed attempts — success
// or error — log normally.
func dropInterruptedLogs(sink func(CallLog)) func(CallLog) {
	if sink == nil {
		return nil
	}
	return func(entry CallLog) {
		if entry.Interrupted {
			return
		}
		sink(entry)
	}
}

func (h *Hedged) logf(format string, args ...any) {
	if h.logger != nil {
		h.logger.Printf("llmrouter: "+format, args...)
	}
}

// Stream runs the hedged race. Tokens cannot be forwarded live from an
// attempt that might lose, so each attempt buffers privately and the
// winner's full text is replayed to onToken once decided (background
// consumers only concatenate).
func (h *Hedged) Stream(ctx context.Context, req vpc.LLMRequest, onToken func(string)) (vpc.LLMResult, error) {
	out, err := runHedged(ctx, h.threshold, h.attempt(h.primary, req), h.attempt(h.hedge, req))
	if err != nil {
		return out.res, err
	}
	if onToken != nil && out.text != "" {
		onToken(out.text)
	}
	return out.res, nil
}

func (h *Hedged) attempt(r *Router, req vpc.LLMRequest) func(context.Context) (attemptOutcome, error) {
	return func(ctx context.Context) (attemptOutcome, error) {
		var text strings.Builder
		res, err := r.Stream(ctx, req, func(token string) { text.WriteString(token) })
		return attemptOutcome{res: res, text: text.String()}, err
	}
}

type attemptOutcome struct {
	res  vpc.LLMResult
	text string
}

type attemptResult struct {
	out attemptOutcome
	err error
}

// runHedged is the generic hedge helper (Python-exact semantics, closed
// 2026-07-06):
//
//  1. Fire primary; wait up to threshold (whole-call elapsed time, no
//     first-byte special-casing).
//  2. Primary errors before the threshold → fire the hedge immediately
//     and return its result (sequential fallback, no race).
//  3. Threshold hit → fire the hedge in parallel; the primary is NOT
//     cancelled at the threshold. The first successful finisher wins and
//     the slower attempt is then ctx-cancelled (via the deferred
//     cancels); if the first finisher failed, await the other.
//  4. Parent-ctx cancellation cancels both attempts and returns ctx.Err()
//     — never an endpoint error.
func runHedged(
	ctx context.Context,
	threshold time.Duration,
	primary, hedge func(context.Context) (attemptOutcome, error),
) (attemptOutcome, error) {
	primaryCtx, cancelPrimary := context.WithCancel(ctx)
	defer cancelPrimary()
	primaryCh := make(chan attemptResult, 1)
	go func() {
		out, err := primary(primaryCtx)
		primaryCh <- attemptResult{out, err}
	}()

	timer := time.NewTimer(threshold)
	defer timer.Stop()

	var primaryDone *attemptResult
	select {
	case r := <-primaryCh:
		primaryDone = &r
	case <-timer.C:
		// Threshold hit: race the hedge below.
	case <-ctx.Done():
		return attemptOutcome{}, ctx.Err()
	}

	if primaryDone != nil {
		if primaryDone.err == nil {
			return primaryDone.out, nil
		}
		if ctx.Err() != nil {
			return attemptOutcome{}, ctx.Err()
		}
		// Primary failed before the threshold → sequential hedge.
		hedgeCtx, cancelHedge := context.WithCancel(ctx)
		defer cancelHedge()
		out, err := hedge(hedgeCtx)
		return out, err
	}

	hedgeCtx, cancelHedge := context.WithCancel(ctx)
	defer cancelHedge()
	hedgeCh := make(chan attemptResult, 1)
	go func() {
		out, err := hedge(hedgeCtx)
		hedgeCh <- attemptResult{out, err}
	}()

	var first attemptResult
	var firstFromPrimary bool
	select {
	case first = <-primaryCh:
		firstFromPrimary = true
	case first = <-hedgeCh:
	case <-ctx.Done():
		return attemptOutcome{}, ctx.Err()
	}
	if first.err == nil {
		// Winner; the deferred cancels stop the slower attempt.
		return first.out, nil
	}

	// First finisher failed → await the other.
	otherCh := hedgeCh
	if !firstFromPrimary {
		otherCh = primaryCh
	}
	var other attemptResult
	select {
	case other = <-otherCh:
	case <-ctx.Done():
		return attemptOutcome{}, ctx.Err()
	}
	if other.err == nil {
		return other.out, nil
	}
	return attemptOutcome{}, errors.Join(first.err, other.err)
}
