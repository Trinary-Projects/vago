# D4 design note — hedged one-shot LLM execution in `llmrouter` (Go interface shape)

Companion to `reports/onboarding-call-migration-plan.html` §5 D4. The semantics are
**closed** (2026-07-06, Python-exact, verified against disha-backend
`services/llm_failover_service.py` `generate_with_hedged_request`); this note only fixes
the Go types, constructor, and how the fixed primary/hedge pair is declared. Implementation
is phase 2.

## Python reference facts (verified 2026-07-07)

- `generate_with_hedged_request(config_key=gpt_oss120_fast, ..., hedge_threshold_ms=1000)`
  picks `FAILOVER_CONFIGS[key][0]` as primary and `[1]` as hedge — fixed order, not
  health-ranked.
- Deep thinking calls it with `config_key=gpt_oss120_fast_hedged`, whose pair is
  **Cerebras `gpt-oss-120b`** (primary) and **OpenRouter `gpt-oss-120b` with
  `provider_sort="throughput"`** (hedge). The throughput sort is an OpenRouter
  `provider: {"sort": "throughput"}` request field — the hedge endpoint differs from the
  plain `openrouter_gpt_oss_120b` config `get_guidance` uses today.
- Temperature / max_tokens are **per-call** in Python (deep thinking: 0.7 / 4000; the
  existing Go `cerebras_gpt_oss_120b` / `openrouter_gpt_oss_120b` endpoint configs pin
  `MaxTokens: 500` for get_guidance). The Go shape must allow per-client overrides.
- Fewer than 2 configs in the pair → Python degrades to regular sequential failover; the
  Go registry makes a 1-endpoint "pair" unrepresentable instead.

## Layer 1 — generic hedge helper (unexported)

Plain goroutines + `select`; no frames, no pipeline types, no HTTP knowledge. Reusable by
any future "background call must respond fast" case inside `llmrouter`.

```go
// runHedged runs primary, and — if it hasn't completed within threshold,
// or completed with an error before it — also runs hedge. First success
// wins and the loser's ctx is cancelled; if the first finisher failed,
// the other is awaited. Parent-ctx cancellation cancels both.
func runHedged(
    ctx context.Context,
    threshold time.Duration,
    primary, hedge func(context.Context) (attemptOutcome, error),
) (attemptOutcome, error)
```

Semantics encoded here (all Python-exact, per the closed D4 decision):

1. Whole-call threshold (default 1000ms), no first-byte special-casing.
2. Primary error **before** threshold → hedge fired immediately, sequentially.
3. Threshold hit → hedge fired in parallel; **primary is not cancelled at the threshold**.
4. First *successful* finisher wins; the slower attempt is then ctx-cancelled.
   First finisher failed → await the other. Both fail → return an error.
5. Parent-ctx cancellation cancels both attempts and is returned as `ctx.Err()` — never an
   endpoint error.

## Layer 2 — reuse `Router` per attempt (no second streaming client)

`Router.Stream` already implements everything a hedged *attempt* needs: provider request
shaping, SSE streaming, error classification, blacklist-on-completed-error, cancellation
treated as interruption **without** blacklist, LogSink emission with usage/finish-reason,
and truncation diagnostics. The only mismatch is endpoint selection (health-ranked vs
fixed). So instead of a new client, `Config` gains one option:

```go
type Config struct {
    // ... existing fields unchanged ...

    // FixedEndpoint pins the router to a single endpoint config key,
    // bypassing health-ranked selection entirely (Python's failover
    // service picks configs by fixed list position, not health). In
    // this mode the router never triggers a poll (nothing to re-rank);
    // blacklist write-back on completed transient errors is kept.
    // Group is ignored when set.
    FixedEndpoint string

    // Temperature/MaxTokens override the endpoint config's values for
    // this client (Python passes them per call; deep thinking: 0.7/4000).
    Temperature *float64
    MaxTokens   *int
}
```

The hedged client is then pure orchestration over two ordinary `Router`s — zero HTTP or
streaming code of its own:

```go
// HedgedConfig configures a one-shot client that races a fixed
// primary/hedge endpoint pair (Python generate_with_hedged_request).
type HedgedConfig struct {
    Pair           string        // key into hedgedPairs, e.g. "gpt-oss120-fast-hedged"
    Redis          RedisStore
    Logger         *log.Logger
    LogSink        LogSink
    PromptMetadata map[string]any
    HedgeThreshold time.Duration // 0 → 1000ms default
    Temperature    *float64      // per-call override (deep thinking: 0.7)
    MaxTokens      *int          // per-call override (deep thinking: 4000)
}

// NewHedged builds two fixed-endpoint Routers (pair primary/hedge) and
// returns the thin racing wrapper. Stream satisfies
// voicepipelinecore.LLMClient structurally, so get_guidance's later
// adoption is a constructor swap.
func NewHedged(cfg HedgedConfig) (*Hedged, error)

func (h *Hedged) Stream(ctx context.Context, req vpc.LLMRequest, onToken func(string)) (vpc.LLMResult, error)
```

`Hedged.Stream` is layer 1 applied to `primary.Stream` / `hedge.Stream` with buffered
tokens (each attempt accumulates privately; the winner's full text is replayed to
`onToken` once decided — background consumers only concatenate).

Notes:

- **Blacklisting** comes for free from `Router.Stream`: a completed transient error runs
  `handleError` (classification + health-key write-back), and a cancelled attempt returns
  via the `ctx.Err() != nil` branch that skips `handleError` — exactly the "slow losers
  are never blacklisted" rule. Fixed-endpoint mode only turns `triggerPoll` into a no-op.
- **Logging**: `Router.Stream`'s deferred LogSink fires on every exit, including
  cancellation (as `Interrupted: true`). The hedged wrapper passes each attempt's Router a
  wrapped sink that drops `Interrupted` entries — for a background one-shot, interrupted
  can only mean "cancelled mid-flight", which per the closed semantics never logs. Both
  attempts log when both complete. The live conversation path is untouched.
- `LLMResult` carries the winner's real model/usage/finish-reason as usual.

## Fixed pair declaration (registry, `groups.go`)

```go
type hedgedPair struct {
    Primary string // endpoint config key
    Hedge   string
}

var hedgedPairs = map[string]hedgedPair{
    // Python LLMFailoverConfigName.gpt_oss120_fast_hedged:
    // Cerebras first, OpenRouter throughput-sorted second.
    "gpt-oss120-fast-hedged": {
        Primary: "cerebras_gpt_oss_120b",
        Hedge:   "openrouter_gpt_oss_120b_throughput",
    },
}
```

Plus one new endpoint config, `openrouter_gpt_oss_120b_throughput`: identical to
`openrouter_gpt_oss_120b` but with `ExtraBody: {"provider": {"sort": "throughput"}}`.
It gets its own key so the existing get_guidance endpoint's health/blacklist entries are
untouched, and blacklist write-back for the hedge attempt lands on the key that actually
misbehaved. (Blacklist keys are per-config; Python's hedged path blacklists the shared
`OPENROUTER_GPT_OSS_120B` target — a deliberate, tiny delta: Go keys the throughput variant
separately. Flag if you want Python-exact key sharing instead.)

No new env vars: both endpoints reuse `CEREBRAS_ENTERPRISE_API_KEY` / `OPENROUTER_API_KEY`.

## Test matrix (phase 2, httptest fake endpoints)

- Fast primary → no hedge fired.
- Slow-but-successful primary racing a fired hedge → first success wins, loser cancelled,
  primary NOT cancelled at threshold.
- Primary error before threshold → immediate sequential hedge.
- First finisher fails → other awaited and returned.
- Both fail → error returned.
- Parent-ctx cancellation → both cancelled, no blacklist, `ctx.Err()` returned.
- Transient-error completion → blacklists that attempt's config key.
- Slow loser (cancelled) → never blacklisted.
- LogSink: logs every completed attempt (two entries when both complete), never a
  cancelled one.
- Temperature/MaxTokens overrides land in both attempts' request bodies; hedge request
  carries `provider.sort=throughput`.
- Fixed-endpoint mode: selection is bypassed (no health reads), no poll trigger on error
  or slow completion, blacklist write-back still happens; existing group-mode Router tests
  stay green (no behavior change when FixedEndpoint is empty).
