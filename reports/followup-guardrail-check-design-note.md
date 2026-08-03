# Follow-up calls: non-blocking guardrail check after every generated sentence

Status: **design agreed 2026-08-03, not yet implemented.** Sibling to
`reports/followup-protocol-retrieval-design-note.md`, which this note assumes
and reuses heavily — read its §5.2b (the shared Weaviate client) and §16 (the
naming pre-split that reserved every seam this feature needs) first.

Companion decisions live in `AGENTS.md` under the guardrail-check bullet.

---

## 1. What we're adding

A **non-blocking** guardrail check that runs while Disha is generating and
speaking. Each completed sentence of the assistant's response is used as a
vector query against a guardrail corpus; a sufficiently similar guardrail means
the response is violating, and the pipeline is interrupted and regenerated with
the guardrail injected as a correction.

Two similarity bands:

| Cosine similarity | Action |
|---|---|
| **> 0.90** | Interrupt and regenerate immediately, on similarity alone. Also run the judge **audit-only**, off the critical path, to detect false positives. |
| **0.75 – 0.90** | Call an ultra-fast judge LLM. Violated → interrupt and regenerate. Not violated → continue. |
| **< 0.75** | Nothing. |

Unlike protocol retrieval this step is **not** on the critical path and has **no
hard latency budget** — it never delays a turn. Its cost is that Disha may
already be speaking when it fires (§14.1).

Scope: **follow-up calls only**, both the dynamic check-in and agenda-based
paths (same scope as protocol retrieval). Sales and onboarding never construct
the processor and their pipelines stay byte-identical.

---

## 2. What already exists (verified, not assumed)

### 2.1 In vago

| Surface | Where | Reused how |
|---|---|---|
| `internal/weaviate` `Client`, `NearText`, `Hit`, `Ref`, filter builders | `internal/weaviate/client.go`, `filters.go` | **unchanged.** Collection-agnostic by design. No write path is needed (§7.3) and none exists. |
| `weaviateEnvFlagField()` | `disha/protocol_retrieval.go:150` | **unchanged.** Deliberately not protocol-named; maps `ENVIRONMENT` → `isProduction`/`isStaging`. |
| `ChunkRetrievalMetrics` umbrella + merge discipline | `disha/types.go:91`, `disha/retrieval_chunk_decorator.go:73-79` | The `Guardrail` sibling field is already commented in place. The decorator **merges**, so both steps can fill their own field. |
| `newRetrievalChunkDecorator` | `disha/retrieval_chunk_decorator.go:40` | Extended to take a second record box. `SetChunkDecorator` is a **single-occupancy slot** (`disha/call_event_callbacks.go:46`), so extending is the only option — a second decorator cannot be registered. |
| Interrupt-and-regenerate | `voicepipelinecore/llm_response_timeout_processor.go:111-112` | **Copied verbatim.** `Broadcast(NewInterruptFrame())` + `PushFrame(NewLLMMessagesAppendFrame(nil, true), Upstream)`, consumed at `user_context_aggregator.go:244-261`. Ordering is safe: the interrupt lands in `inputSysCh` and is dispatched inline before the append is forwarded from `inputDataCh`, so the append is not purged by the interrupt preceding it. |
| `ContextEnricherProcessor` / `MessagesEnricher` | `voicepipelinecore/context_enricher_processor.go` | Reused to inject the correction message into the regeneration snapshot only. Composed with the protocol enricher (§6.2). |
| `endsWithPunctuation` | `voicepipelinecore/tts_processor.go:21-27` | **Reused directly.** Unexported, but the new processor lives in the same package. See §5.1 for the fan-out consequence. |
| Hedged one-shot LLM client | `voicepipelinecore/llmrouter` `NewHedged(HedgedConfig{...})` | The judge client. Its `LogSink` wrapper already drops `Interrupted` entries, so cancelled judge calls do not pollute `llmlog`. |
| `NewUSBucketJSONUploaderFromEnv`, `JSONUploader` | `disha/s3_uploader.go:69`, `:31` | The S3 record uploader. |
| `taskSentryHub` / `SetSentryHub` late-binding | `disha/protocol_retrieval.go` (`protocolEnricher`) | Same pattern for the checker. |

### 2.2 In disha-backend

All three Weaviate classes **already shipped** in commit `cf565d68`
("add guardrail weaviate models (#1614)"). Models at
`weaviate/models.py:465-562`; class definitions (the authoritative vectorizer
config) at `weaviate/migrations/0014_GuardrailInstruction.json`,
`0015_GuardrailAnchor.json`, `0016_GuardrailLiveQueryAnchor.json`.

There is **no** guardrail read path, write path, ingest task, or cron yet —
models and migrations only.

Chunk-sync consumer seam already anticipates this step:
`services/conversation_chunk_manager.py:427-439` reads only the namespaced
child of the umbrella, with the comment *"The Go worker writes an umbrella keyed
by step (`protocol`, later `guardrail`)"*.

---

## 3. The guardrail collections

```
GuardrailAnchor.answeredBy ──► GuardrailInstruction
   (US Weaviate)                  (US Weaviate)

GuardrailLiveQueryAnchor           (PRIMARY / India Weaviate)
```

| | `GuardrailInstruction` | `GuardrailAnchor` | `GuardrailLiveQueryAnchor` |
|---|---|---|---|
| Instance | **US** (`WEAVIATE_US_URL`) | **US** | **PRIMARY** (`WEAVIATE_URL`) |
| Vectorizer | `text2vec-huggingface` (in-cluster TEI), `vectorizeClassName: false`, `distance: cosine` | same | `none` — caller supplies the vector |
| Properties | `instructionText` (vectorized), `title`, `documentVersionPath`, `isProduction`, `isStaging` | `anchorText` (vectorized), `answeredBy` → cross-ref | `anchorText`, `conversationId` |

**`GuardrailInstruction` has no `turnsThresholdCount`.** This is the only
property difference from `ProtocolInstruction`, and it is the schema confirming
that guardrails have **no resident set, no TTL, no capacity, and no eviction**.
There is no `ProtocolStore` equivalent. Every check is independent and keeps
only its top-1 hit.

**Collection contract (not optional), inherited from protocols:** the anchor
class must keep `text2vec-huggingface` + the in-cluster TEI `endpointURL` +
`vectorizeClassName: false` + per-property `vectorizePropertyName: false` +
`distance: cosine`. A class created `vectorizer: none` does not expose
`nearText` at all, and a vectorizer cannot be changed in place. **TEI applies
the `Document: ` prefix server-side, so every client sends raw unprefixed
text** — double-prefixing degrades silently (cosine 0.9937 vs ≥0.99995).

**Live-query anchors are written by disha-backend, not by vago** (§7.3).

---

## 4. Where the check hooks in

### 4.1 A new core processor

`reports/followup-protocol-retrieval-design-note.md` §16 already ruled out
reusing `ContextEnricherProcessor` here: *"the guardrail step is **not** this
processor: it runs after generation and must interrupt, so it needs its own core
hook."* That is still true, but the hook is narrower than anticipated because
the interrupt-and-regenerate mechanism already exists in core (§2.1).

New, business-free, in `voicepipelinecore/response_guard_processor.go`:

```go
// ResponseGuard inspects one completed fragment of the assistant's in-flight
// response. It runs on its own goroutine and must not block. Returning true
// means the response is unacceptable and the turn should be discarded and
// regenerated.
type ResponseGuard func(ctx context.Context, sentence string) bool

type ResponseGuardProcessor struct {
    *BaseProcessor
    taskCtx *TaskContext
    guard   ResponseGuard
    metrics *ProcessorMetrics
    // per-turn state, owned by the processLoop goroutine
    aggregation   string
    checks        int
    fired         atomic.Bool // this turn already triggered
    skipTurn      bool        // one-retry latch: the regeneration is unguarded
    turnCancel    context.CancelFunc
}

func NewResponseGuardProcessor(taskCtx *TaskContext, guard ResponseGuard) *ResponseGuardProcessor
```

Panics on a nil guard, mirroring `NewContextEnricherProcessor` — a pass-through
guard processor would silently do nothing.

New metric label in `voicepipelinecore/metrics.go`:

```go
MetricResponseGuard MetricLabel = "response_guard"
```

Deliberately separate from `MetricTTFB` for the same reason
`MetricContextEnrich` is: `llm_ttfb_ms` must keep measuring model time only.

### 4.2 Pipeline placement

```
llm → llmResponseTimeout → llmOutputFilter → [ResponseGuard] → tts → playback → assistantAgg → sink
```

**After `LLMOutputFilterProcessor`, before TTS.** After the filter because the
guardrail must judge the text the user will actually hear, not raw model output
with leaked artifacts. Before TTS because the fragments must be observable
before they are spoken.

Every `TextFrame` is forwarded **immediately and unconditionally** — the check
is non-blocking and TTS never waits on it.

### 4.3 Frame handling

| Frame | Behaviour |
|---|---|
| `TextFrame` | Forward first, then accumulate; on `endsWithPunctuation(aggregation)` fire a check and reset the buffer. Skip fragments with no alphanumeric content. |
| `LLMResponseStartFrame` | Reset per-turn state. If `skipTurn` is set, clear it and guard nothing this turn (the one-retry latch). Forward. |
| `LLMResponseEndFrame` | Nothing special: any trailing un-punctuated remainder is deliberately **not** checked (it is not a completed sentence). Forward. |
| `InterruptFrame` | Cancel all in-flight checks for the turn, reset state, `metrics.Reset()`, forward. If the interrupt was **not** self-originated, also clear `skipTurn` so a genuine barge-in mid-regeneration does not leave the next real turn unguarded. |
| `EndFrame` | Cancel in-flight checks, forward. |
| default | Forward untouched. |

Note an interrupted generation pushes **no** `LLMResponseEndFrame` at all
(`llm_processor.go:196-203`), so a user barge-in cannot leave the turn
half-open.

---

## 5. The check round

### 5.1 Fragment boundaries — reuse `endsWithPunctuation`

**Decided: reuse `voicepipelinecore/tts_processor.go:21-27` as-is**, mirroring
TTS's own aggregate-then-flush loop (`tts_processor.go:425-447`): append the
delta, test the buffer, reset on match.

Consequence to be explicit about: `endsWithPunctuation` is
`unicode.IsPunct` on the last rune, so it flushes on **commas, semicolons,
colons and quotes**, not only sentence terminators. Fan-out is therefore
clause-level, not sentence-level. A three-sentence turn with ordinary comma use
produces roughly 8–12 fragments, each its own vector query and potentially its
own judge call.

This was chosen over a stricter terminal-`.!?`-plus-minimum-word-count
predicate for consistency with what TTS already treats as a flushable unit, and
because more frequent checks fire earlier, which is the whole point (§14.1).
The cost is query and judge volume.

**No cap on parallel checks.** If a single turn exceeds
`guardrailFanoutSentryThreshold` (10) checks, capture a Sentry event
(`operation: guardrail_check_fanout`) once for that turn and keep going.
Expect this to be noisy at 10 given the clause-level fan-out; the threshold is
a named constant and the first tuning knob to reach for.

Fragments with no alphanumeric content (a bare `"."`, `" —"`) are skipped
without counting as a check.

### 5.2 Query text is the fragment, never the accumulation

**Decided: each check queries exactly its own fragment.** Not the text
accumulated so far. Cumulative text would make check N+1 subsume check N,
making the parallelism largely wasted work.

Known detection gap, accepted: a violation that only exists across a boundary
— *"You should stop. The medication is unnecessary."* — is not detected,
because neither half crosses threshold alone.

### 5.3 The vector query

One `NearText` per fragment, top-1 after dedupe:

```go
hits, err := client.NearText(ctx, weaviate.NearTextQuery{
    Class:    guardrailAnchorClass,     // "GuardrailAnchor"
    Concepts: []string{fragment},       // RAW — TEI prefixes server-side
    Fields:   guardrailAnchorFields,
    Where: weaviate.EqualBool(
        []string{"answeredBy", guardrailInstructionClass, weaviateEnvFlagField()}, true),
    Limit: guardrailQueryLimit,          // 10
})
```

```go
const guardrailAnchorFields = `anchorText
answeredBy { ... on GuardrailInstruction { instructionText title documentVersionPath _additional { id } } }`
```

No `turnsThresholdCount` in the selection set — the property does not exist on
`GuardrailInstruction`.

Per-hit skips, mirroring `queryProtocols` (`disha/protocol_retrieval.go:491`):
`!hit.DistancePresent`, missing `answeredBy` cross-ref, empty
`instructionText`, empty instruction ID. Similarity is `1 - distance`.

**Dedupe by instruction ID, best-similarity wins** — many anchors map to one
instruction, so without dedupe the top-N is dominated by one guardrail. Then
keep **only the top-1**.

**No Jinja rendering.** Decided: guardrail `instructionText` is plain text.
`DocumentStore.RenderTemplate` is not used and `templateNeedsRender` has no
counterpart here.

### 5.4 Bands, judge, and cancellation

Given the top-1 hit for a fragment:

- **similarity > 0.90** → return violated **immediately**. Separately spawn the
  **audit judge** (§5.6), fire-and-forget.
- **0.75 ≤ similarity ≤ 0.90** → call the judge and wait for it *within this
  check's goroutine* (never on the pipeline goroutine). Violated → return
  violated. Not violated, judge error, judge timeout, or malformed output →
  return not-violated (**fail open**, §8).
- **similarity < 0.75** → return not-violated.

All checks for a turn share one `context.WithCancel` derived from the turn
context. **The first check to return violated cancels that context**, so every
other in-flight check (vector query or judge call) is abandoned. Cancelled
checks log and are **not** sent to Sentry (`context.Canceled` exemption,
matching `reportFailure` in `disha/protocol_retrieval.go:1078`). Cancelled
judge LLM calls are dropped by the hedged client's existing `LogSink` wrapper
and never reach `llmlog`.

`fired` is an `atomic.Bool` compare-and-swap so exactly one check wins the race
and exactly one interrupt is emitted.

### 5.5 The judge client

`llmrouter.NewHedged`, one client constructed per call and shared by every
check on that call.

New endpoint configs in `voicepipelinecore/llmrouter/groups.go`:

| Config key | Model | Notes |
|---|---|---|
| `openrouter_llama_3_1_8b_instruct_nitro` | `meta-llama/llama-3.1-8b-instruct:nitro` | primary |
| `openrouter_gpt_oss_20b_nitro` | `openai/gpt-oss-20b:nitro` | hedge; `reasoning: {effort: "low"}` via a new `extraBodyFor` case |

New pair registry key `llmrouter.GroupGuardrailJudgeHedged`
(`"guardrail-judge-hedged"`), primary then hedge — which is exactly the stated
priority order, since a primary error before the threshold triggers an
immediate sequential hedge.

`HedgedConfig`: `HedgeThreshold` **400ms** (well below the 1s default, since
this is latency-sensitive), `Temperature` 0, and — learning directly from
**VAGO-15** — an **explicit `MaxTokens` of 512**. A nil `MaxTokens` inherits the
endpoint config's value, which is how careplan detection silently truncated
mid-JSON in prod. 512 rather than something tiny because `gpt-oss-20b` at low
reasoning effort spends output tokens on reasoning before the verdict.

Both configs are **fixed-endpoint only**, so they bypass health-ranked
selection and **need no disha-backend polling change** — a real deploy-order
simplification versus the gemma work. Auth is the existing `OPENROUTER_API_KEY`.

**`:nitro` verified live 2026-08-03.** It is **not** a catalog id suffix: both
`GET /models/meta-llama/llama-3.1-8b-instruct:nitro/endpoints` and the gpt-oss-20b
equivalent return HTTP 200 but with the suffix **silently stripped** (`data.id`
comes back as the bare model id). It is an OpenRouter completion-time routing
shorthand, equivalent to a request-time `provider.sort="throughput"` preference.
It does work where it matters: live non-streaming completions with
`model: "…:nitro"` returned 200 and routed to CoreWeave (llama) and Amazon
Bedrock (gpt-oss-20b). So the ids are correct as written and no substitution is
needed — but note that `/endpoints` is **not** a valid way to validate a
`:nitro` id, because it answers about the base model.

This mattered because these are fixed-endpoint configs with no health polling:
unlike the gemma case there is no poll to catch a bad id, so a wrong model would
fail 100% of judge calls and silently degrade the 0.75–0.90 band to fail-open.

**`MaxTokens` 512 is empirically justified, not a guess.** A live call at
`max_tokens: 16` with `reasoning: {"effort":"low"}` returned
`finish_reason: length` with `completion_tokens_details.reasoning_tokens: 7` —
i.e. 7 of 16 tokens went to reasoning before any verdict text. Note also that
neither endpoint config carries a `MaxTokens`, so a nil caller-side value sends
**no `max_tokens` field at all** and falls through to the provider ceiling
(32k–131k). That is the opposite failure mode from VAGO-15 — no silent
truncation — but it means the caller must pass 512 explicitly, and a regression
test asserts the caller override reaches the request body.

### 5.6 The judge prompt and the audit judge

Prompt comes from `DocumentStore` (`document:{name}:{env}`), **not** hardcoded.

> **TODO (launch blocker):** the real Langfuse prompt name is not yet decided
> and the prompt does not exist. This note assumes
> `follow_up_call/guardrail_judge`. Until that key is pre-rendered into Redis by
> Disha's Langfuse sync, every judge call fails and the 0.75–0.90 band
> fails open to "not violated" — i.e. only the >0.90 band works. Replace the
> constant and confirm the variable names when the prompt lands.

Assumed variables: the guardrail `instructionText` and the fragment under
review. Assumed output contract: a JSON object with a boolean verdict;
**malformed, empty, or unparseable output is treated as not-violated** (§8).

`prompt_metadata` carries `system_prompt_name`, the **resolved** version, and
`system_prompt_variables` — the exact variables used to render it — per the
standing strict prompt-metadata rule. `usecase_type` is
`follow_up_guardrail_judge`.

**The audit judge** exists only to close the loop on the >0.90 band, where the
interrupt fires on similarity alone. It runs the *same* prompt against the same
fragment, concurrently with the regeneration, purely to record whether the
interrupt was justified:

- verdict **violated** → true positive.
- verdict **not violated** → **false positive**: we interrupted and regenerated
  for nothing, which points at an anchor pulling unrelated sentences over 0.90.

It is fire-and-forget on a context **derived from the call context, not the
cancelled check context** (it must survive the very cancellation its own
violation triggered). If it is cancelled — call ended first — that is **logged**,
not Sentry'd. Its verdict is written into the pending record if the record has
not yet been taken; if it arrives too late, log and drop (§7.1).

### 5.7 Record selection — one record per turn

Multiple checks run per turn but exactly **one** record reaches the chunk:

1. **If any check violated** → that check's record. It is the one that fired the
   interrupt, and the guardrail it matched is the one that was broken.
2. **Otherwise** → the record of the check with the **highest top-similarity**.

Implemented as a **best-so-far box with a lock flag**, extending
`protocolRecordBox`'s single-slot pattern:

```go
type guardrailRecordBox struct {
    mu      sync.Mutex
    pending *guardrailCheckRecord
    locked  bool // a violating record has been stored; later completions cannot overwrite
}

func (b *guardrailRecordBox) offer(record guardrailCheckRecord) // keeps the better one
func (b *guardrailRecordBox) offerViolation(record guardrailCheckRecord) // stores and locks
func (b *guardrailRecordBox) setAuditVerdict(verdict string) bool // false if already taken
func (b *guardrailRecordBox) take() *guardrailCheckRecord
```

Each completing check `offer`s its record; higher similarity wins. A violation
`offerViolation`s and locks. No coordination and no waiting: `take()` always
gets the best of whatever has completed.

---

## 6. Regeneration

### 6.1 The interrupt

Copied verbatim from `llm_response_timeout_processor.go:111-112`:

```go
p.Broadcast(NewInterruptFrame())
p.PushFrame(NewLLMMessagesAppendFrame(nil, true), Upstream)
```

`skipTurn` is set so the regeneration itself is **completely unguarded** — one
retry only, so the system can never loop.

**Nothing is removed from conversation history.** Decided 2026-08-03: the
existing interruption flow already discards the ungenerated remainder, which is
all that needs discarding. `AssistantContextAggregator` commits whatever was
actually *played* (`assistant_context_aggregator.go:92-93` → `:59`) and that
commit stands — it is an honest record of what the user heard. There is no new
shared-state mutator, no text matching, and therefore no race between the
downstream commit and the upstream regeneration snapshot.

Consequences, accepted:

- Conversation history ends up with **two consecutive assistant messages** on a
  triggering turn: the truncated violating text, then the regenerated text.
  **Verified safe 2026-08-03** against all three `gemini-flash-3.1-lite`
  endpoints (`vertex_gemini_flash_3_1_lite`,
  `google_ai_studio_gemini_flash_3_1_lite`, `openrouter_gemini_flash_3_1_lite`),
  in production request shape — streaming, `stream_options.include_usage`, an
  `end_call` tool, and each provider's real `extraBodyFor` extras. All returned
  200 with `finish_reason: stop` and no message-ordering error.
  `messagesForModel` (`llmrouter/providers.go:25-75`) does no consecutive-role
  handling at all, so the pair reaches the provider exactly as constructed.
  A `prompt_tokens` probe shows the Gemini OpenAI-compat shims **silently
  coalesce the two into a single model turn, preserving both texts** — Vertex
  joins with a space, AI Studio with a newline. Two consequences: the model sees
  one assistant turn rather than two, so any semantic distinction between
  "interrupted partial" and "retry" is invisible to it; and the join separator is
  provider-dependent, so the concatenated text differs slightly by endpoint.
  Both are harmless here. Note the verification covers **plain-content**
  assistant messages, which is the only shape this feature produces — the
  partial commit at `assistant_context_aggregator.go:59` appends played words as
  plain content and never carries `tool_calls`, so the
  `messagesForModel` thought-signature rewrite branch is never entered by a
  guardrail regeneration.
- The violating text **persists in context for the rest of the call** while the
  correction does not (§6.2), so it can re-anchor the behaviour that was
  corrected. Accepted for v1; making the block persistent is the lever if this
  shows up in transcripts.
- The already-written partial Redis chunk is not retracted, so resume replays
  it.

### 6.2 The correction message — ephemeral

**Decided: ephemeral, not persisted.** The regenerated `LLMMessagesFrame`
travels through `ContextEnricherProcessor` on its way back to the LLM, so the
correction is injected into **that turn's snapshot only** and never enters
`aggregatorSharedState`. Exactly the mechanism `injectProtocolBlock` uses
(`disha/protocol_retrieval.go:714-726`): copy and insert, never mutate.

Placement: **appended as the last message in the array**, because it refers to
"your response" and must follow the violating turn. This differs deliberately
from the protocol block's 3-assistant-turns-above-tail placement.

Role `user`, wrapped in `<system_message>`, byte-exact:

```
<system_message>
Your response violates the following guardrail -
{{guardrail}}
please regenerate with correction
</system_message>
```

`{{guardrail}}` is the matched `instructionText`. Hardcoded in Go as a
`guardrailBlockTemplate` constant, mirroring `protocolBlockHeader` — it is not a
`DocumentStore` prompt.

**Composition with the protocol enricher.** `ContextEnricherProcessor` holds one
`MessagesEnricher`, so follow-up composes the two in `disha/` with a small
`composeEnrichers(...)` helper — one processor, one `MetricContextEnrich`, no
core change and no second metric label. The guardrail enricher is a no-op on
every turn with no pending correction.

Interaction check: on the regeneration turn the protocol enricher sees messages
ending in an assistant message, so `buildProtocolQueryText` returns `""`, it
skips retrieval and merely re-injects its resident set. `box.put` is not called
(it fires only when retrieval ran), so protocol telemetry is unaffected by the
extra generation.

---

## 7. Telemetry

### 7.1 Which chunk, and why the regenerated one

A triggering turn produces **two** assistant chunks: the partial (committed on
the interrupt) and the regenerated one. **Decided: the guardrail metrics go on
the regenerated chunk.**

The mechanism is that the record is boxed only once the check fully resolves,
including a bounded wait for the audit verdict
(`guardrailAuditVerdictWait`, 3s). The partial chunk's `take()` therefore
returns `nil` and gets no guardrail metrics; the regenerated chunk, committed
seconds later, gets the complete record with the verdict present.

This is the difference between having and not having the false-positive signal:
boxing immediately would attach the record to the partial chunk with
`judge_verdict` null, which is precisely the data the >0.90 audit exists to
collect.

Non-triggering turns are unaffected — one chunk, record boxed as soon as the
last check completes.

**Documented mismatch, not a bug:** on a triggering turn the chunk's own `text`
is the *regenerated* utterance while its `guardrail.query_text` is the
*violating* one. That is intended — the live-query corpus should collect what
went wrong, not the correction.

### 7.2 Chunk metrics

Filling the sibling field reserved at `disha/types.go:87-89`:

```go
type ChunkRetrievalMetrics struct {
    Protocol  *ProtocolRetrievalMetrics `json:"protocol,omitempty"`
    Guardrail *GuardrailCheckMetrics    `json:"guardrail,omitempty"`
}

// GuardrailCheckMetrics summarizes the selected check for one Disha turn. The
// full per-fragment candidate list lives in the S3 object at RawDataS3Key.
type GuardrailCheckMetrics struct {
    E2EMs           float64  `json:"e2e_ms"`
    SimilarityScore *float64 `json:"similarity_score"`
    JudgeVerdict    string   `json:"judge_verdict,omitempty"` // "yes" | "no" | "" (not run / too late)
    Interrupted     bool     `json:"interrupted"`
    CheckCount      int      `json:"check_count"`
    // QueryText is the ENTIRE Disha turn, carried inline so disha-backend can
    // seed GuardrailLiveQueryAnchor from the chunk alone — reading it from S3
    // would mean one GET per Disha turn inside the chunk-sync job.
    QueryText  string `json:"query_text,omitempty"`
    RawDataS3Key string `json:"raw_data_s3_key"`
    Status     string `json:"status"` // ok | skipped | error
    Error      string `json:"error,omitempty"`
}
```

The `guardrail_check_` prefix is dropped inside the sub-object (it would read
`guardrail.guardrail_check_e2e_ms`), matching how `protocol` nests. The **DB
columns keep the prefix** to match `protocol_retrieval_*`.

`E2EMs` is the **selected check's own** latency — fragment boundary to verdict —
not the whole turn's guardrail work. On the violating path that is the number
that matters: how long from the sentence completing to the interrupt firing.

`newRetrievalChunkDecorator` gains a second box parameter and merges rather than
assigns, per `disha/retrieval_chunk_decorator.go:73-79`. Guard clause is
unchanged and load-bearing: `role == "assistant" && !IsDebugLog &&
AdditionalData == nil` — the tool-pair chunk is also assistant-role and must not
consume a record.

### 7.3 The S3 record and the live-query corpus

**S3 blob** — `guardrail_check/{conversation_id}/{chunk_id}.json`, uploaded
synchronously in the decorator **before** the Redis write so the persisted key
never points at a missing object (onboarding's ordering rule). Upload failure →
Sentry (`operation: guardrail_check_upload`) + chunk still written with an empty
key.

**The blob contains every check for the turn**, not just the selected one. The
chunk/DB carries the selected check; S3 is the calibration dataset (§9), and
discarding the other fragments' candidate distributions would throw away
exactly the data needed to tune the bands later.

```json
{ "chunk_id": "...", "conversation_id": "...", "user_id": "...",
  "bot_type": "follow_up", "checked_at": "...",
  "turn_text": "the entire Disha turn as generated",
  "thresholds": {"metric": "cosine_similarity", "interrupt": 0.9, "judge": 0.75},
  "check_count": 4, "selected_index": 2, "interrupted": true,
  "checks": [
    { "index": 0, "fragment": "...", "latency_ms": {"vector_query": 0, "judge": 0, "total": 0},
      "band": "below|judge|interrupt", "cancelled": false,
      "top": {"instruction_id":"...","anchor_id":"...","anchor_text":"...","title":"...",
              "document_version_path":"...","similarity":0.83},
      "candidates": [{"instruction_id":"...","anchor_id":"...","similarity":0.66}],
      "judge": {"ran": true, "audit_only": false, "verdict": "yes",
                "model": "...", "latency_ms": 0, "error": ""},
      "violated": true, "status": "ok" }
  ],
  "status": "ok" }
```

**Live-query corpus — written by disha-backend, from the chunk.** vago has no
Weaviate write path and deliberately gains none: object writes are REST
(`POST /v1/batch/objects`), not GraphQL, and this is a realtime path.

**Decided: one anchor per Disha turn, carrying the entire turn text** — not one
anchor per fragment. `query_text` therefore stays singular, exactly matching
the protocol shape. Backend work mirrors
`weaviate/tasks.py:61` `insert_protocol_live_query_anchors`:

- `insert_guardrail_live_query_anchors(conversation_id, anchor_texts)` — same
  `uuid5(NAMESPACE_URL, f"{conversation_id}:{text}")` idempotency, same
  `batch_generate_embedding(..., azure_text_small_embedding)`, same
  `batch_insert`. Target class `GuardrailLiveQueryAnchor` on **PRIMARY**.
- a `_guardrail_metrics(chunk_data)` accessor beside `_protocol_metrics`
  (`services/conversation_chunk_manager.py:427`), and a guardrail branch in
  `_sync_retrieval_logs` (`:470-537`). Written for **every** Disha turn with a
  guardrail sub-object, no similarity filter.

⚠️ **The live-query corpus is not vector-comparable to the anchors it feeds.**
`GuardrailLiveQueryAnchor` is on PRIMARY embedded with
`azure_text_small_embedding` (1536 dims); `GuardrailAnchor` is on US and
server-vectorized by jina-v5. You can cluster live queries among themselves and
read them, but you cannot `nearVector` a live query against `GuardrailAnchor`.
Anchor promotion is a read-and-curate workflow, not a similarity join.
Inherited from protocols.

### 7.4 RTVI trace

One `server-message` per turn with `data["type"] = "guardrail_check"`, mirroring
`protocolEnricher.publish` (`disha/protocol_retrieval.go:1102`): `status`,
`check_count`, `selected_similarity`, `band`, `judge_verdict`, `interrupted`,
`total_ms`, and `error` when non-empty. Plus one `app.log` line per turn.

### 7.5 Backend DB columns

New nullable columns on `ConversationChunkRetrievalLog`
(`calling/models.py:302-347`), which already prefixes per-step:

| Column | Type |
|---|---|
| `guardrail_check_e2e_ms` | `sa.Float()` |
| `guardrail_check_raw_data_s3_key` | `sa.String(length=1024)` |
| `guardrail_check_similarity_score` | `sa.Float()` |
| `guardrail_judge_verdict` | `sqlmodel.sql.sqltypes.AutoString()` |

Alembic migration with `down_revision = 'c7a1f3d9b2e4'` (current head,
`add_conversation_chunk_retrieval_log`), symmetric `downgrade()`, and the
`info={'__db__': 'main'}` routing convention. Retention stays **30 days** via
the existing `chunk_id` cascade.

### 7.6 Retention for the live-query corpus

**24 hours, and it must be registered explicitly.** Add
`delete_old_guardrail_live_query_anchors()` to `weaviate/tasks.py` — a
copy-paste of its three siblings (24h cutoff, `batch_delete` on
`_creationTimeUnix`, loop until `matches == 0`) — and then create the schedule:

```
POST /common/create_periodic_task_event
{"module_name": "weaviate.tasks",
 "function_name": "delete_old_guardrail_live_query_anchors",
 "cron": "15 */3 * * ? *", "kwargs": {}, "type": "api", "sqs_queue": "p2-fast-l2"}
```

`15 */3` staggers off the two existing `weaviate.tasks` rows at `:00` and `:30`.

⚠️ **Defining the function is not enough — the schedule row is a separate,
easily-forgotten step.** Retention in disha-backend is not expressed in code:
`periodictaskevent` rows (verified 2026-08-03 as the live mechanism, 32 rows,
including `common.tasks.poll_openai_models`) generate the EventBridge rules, and
no alembic migration ever seeds one. A `weaviate/tasks.py` deleter with no row
is simply never invoked, and nothing in the repo will reveal that. Register the
row explicitly and confirm it exists before treating the 24h retention as real.

---

## 8. Failure policy — fail open, always

Any Weaviate non-2xx, GraphQL `errors` payload, transport error, judge error,
judge timeout, or malformed judge output: **do not interrupt.** Disha keeps
speaking.

Rationale: a broken guardrail must never cost the user a turn, and a
false interrupt is worse than a missed one — it truncates a good response and
burns a regeneration. This mirrors protocol retrieval's rule and is a
deliberate choice against the "fail closed for safety" argument.

Recorded as `status: error` plus the message in the chunk metrics. Sentry
(`component: disha_followup`) with `operation`:

| Operation | When |
|---|---|
| `guardrail_check` | vector query failure |
| `guardrail_judge` | judge call failure or unparseable output |
| `guardrail_check_fanout` | more than 10 checks in one turn |
| `guardrail_check_upload` | S3 record upload failure |
| `guardrail_check_config` | `NewClientFromEnv` / judge-client construction failure at setup |

`context.Canceled` is exempt everywhere — cancelled checks and a cancelled
audit judge are logged only.

---

## 9. Configuration

Named constants in `disha/guardrail_check.go` — behavioural decisions, not
deployment config:

```go
guardrailAnchorClass            = "GuardrailAnchor"
guardrailInstructionClass       = "GuardrailInstruction"
guardrailInterruptThreshold     = 0.90
guardrailJudgeThreshold         = 0.75
guardrailQueryLimit             = 10
guardrailFanoutSentryThreshold  = 10
guardrailCheckTimeout           = 3 * time.Second  // per check, generous: not on the critical path
guardrailAuditVerdictWait       = 3 * time.Second
guardrailJudgePromptName        = "follow_up_call/guardrail_judge" // TODO §5.6
guardrailJudgeUsecaseType       = "follow_up_guardrail_judge"
guardrailS3KeyPrefix            = "guardrail_check"
guardrailCheckEnabledEnv        = "FOLLOWUP_GUARDRAIL_CHECK_ENABLED"
```

One env var, no fallback chain: `FOLLOWUP_GUARDRAIL_CHECK_ENABLED=1`.
Weaviate connection reuses `WEAVIATE_URL` / `WEAVIATE_API_KEY`; the judge
reuses `OPENROUTER_API_KEY`.

**Thresholds are uncalibrated and knowingly assumed.** Decided: ship the stated
values and calibrate later from the accumulated S3 records. Expect them to be
wrong in a specific direction — the TEI model showed a high similarity floor on
protocols (irrelevant items still scoring 0.54–0.67, a narrow discriminative
band), and guardrails compare *Disha utterance to Disha utterance*: same
speaker, same register, same domain. The floor is likely higher still, so 0.75
may sit inside the noise. The S3 record carries every candidate from day one
precisely so this can be measured without a rerun.

**Shared Weaviate client.** Both retrieval steps use one `*weaviate.Client` so
the guardrail path inherits the protocol path's warm-up
(`protocolEnricher.warmUp`, `disha/protocol_retrieval.go:1195`) and its pooled
keep-alive connection. If only the guardrail flag is set, the setup builds the
client and runs an equivalent warm-up itself.

---

## 10. Files

**New — core (business-free):**

- `voicepipelinecore/response_guard_processor.go` — `ResponseGuard`,
  `ResponseGuardProcessor`, `NewResponseGuardProcessor`.
- `voicepipelinecore/metrics.go` — add `MetricResponseGuard`.

**New — disha:** split across two files so each stacked-PR layer compiles on its
own — the chunk decorator needs the record types, and the checker that produces
them can land afterwards.

- `disha/guardrail_record.go` — `guardrailCheckRecord`, `guardrailCheck`,
  `guardrailRecordBox` (`offer` / `offerViolation` / `setAuditVerdict` /
  `take`), and the S3 payload builder.
- `disha/guardrail_check.go` — constants, `guardrailChecker` (the
  `ResponseGuard` implementation), the vector query, band logic, judge call,
  cancellation, `renderGuardrailBlock`, `appendGuardrailBlock`, the enricher,
  Sentry/RTVI/publish helpers, `setupGuardrailCheck`.

**Band comparison operators are load-bearing:** `similarity > 0.90` interrupts
and `similarity >= 0.75` judges, so exactly 0.90 falls in the judge band. Go and
`scripts/seed_guardrail_collections.py` must agree exactly, or the calibration
tool misreports what production does.

**Modified — vago:**

- `disha/types.go` — `GuardrailCheckMetrics` + the umbrella sibling field.
- `disha/retrieval_chunk_decorator.go` — second box, guardrail payload builder.
- `disha/followup_call.go` — `GuardrailChecker` on `followUpPlan`,
  `setupGuardrailCheck` in `plan()`, processor insertion and enricher
  composition in `BuildTask`, shared Weaviate client.
- `voicepipelinecore/llmrouter/groups.go` — two endpoint configs + the pair key.
- `voicepipelinecore/llmrouter/providers.go` — `extraBodyFor` case for
  gpt-oss low reasoning effort.

**New — scripts:**

- `scripts/weaviate/guardrail_{instruction,anchor}.class.json` — convenience
  copies of the backend migrations (which remain the source of truth).
- `scripts/seed_guardrail_collections.py` — inspect / recreate / seed / probe,
  a twin of `seed_protocol_collections.py`. The `--probe` mode is what makes
  §9's calibration possible.

**Modified — disha-backend:**

- `weaviate/tasks.py` — `insert_guardrail_live_query_anchors`,
  `delete_old_guardrail_live_query_anchors`.
- `services/conversation_chunk_manager.py` — `_guardrail_metrics`, guardrail
  branch in `_sync_retrieval_logs`.
- `calling/models.py` — four columns on `ConversationChunkRetrievalLog`.
- `alembic/versions/<new>.py` — the migration.
- Langfuse — the judge prompt (§5.6).

---

## 11. Rollout order

1. **disha-backend first**: the four DB columns + migration, the
   `_guardrail_metrics` accessor and sync branch, the two `weaviate/tasks.py`
   functions. Additive and inert until vago writes the key — safe to deploy
   early. (`redis_dict_to_model` reads named keys only, so an unknown chunk key
   is dropped harmlessly regardless.)
2. **The judge prompt** into Langfuse, and confirm `document:{name}:{env}` is
   pre-rendered into Redis. Until this exists the 0.75–0.90 band is dead
   (fails open).
3. **Seed `GuardrailInstruction` / `GuardrailAnchor`** with the real corpus and
   run `--probe` to sanity-check the score distribution against §9's warning.
4. **vago** with `FOLLOWUP_GUARDRAIL_CHECK_ENABLED` unset — dead code, zero
   pipeline change, verifies nothing regressed.
5. **Enable on staging**, QA the interrupt path end to end, inspect S3 records
   and `guardrail_check` RTVI lines.
6. **Register the 24h cron** for `GuardrailLiveQueryAnchor` (§7.6).
7. **Prod is blocked** on §12.

---

## 12. Prod blockers outside vago

Both inherited from protocol retrieval, unchanged:

1. The TEI model `jinaai/jina-embeddings-v5-text-small-text-matching` is
   **CC BY-NC 4.0 (non-commercial)**.
2. Neither Weaviate nor TEI is deployed in the **us-east4 prod cluster**.

Guardrails cannot reach prod before protocol retrieval does.

---

## 13. Tests

Core (`voicepipelinecore/response_guard_processor_test.go`):

- `TextFrame`s forwarded immediately and unchanged, regardless of guard verdict.
- Fragment boundary fires the guard once per punctuation flush; buffer resets.
- Fragments with no alphanumeric content do not fire the guard.
- A guard returning true emits exactly one `InterruptFrame` (broadcast both
  directions) and one upstream `LLMMessagesAppendFrame{RunLLM: true}`.
- Two concurrent guards returning true produce exactly **one** interrupt
  (`atomic.Bool` CAS).
- `skipTurn`: the generation immediately following a self-triggered interrupt
  fires no guard; the one after that does.
- A foreign `InterruptFrame` clears `skipTurn`.
- `InterruptFrame` / `EndFrame` cancel in-flight check contexts.
- A slow guard does not delay `TextFrame` propagation (the non-blocking
  guarantee).
- Nil guard panics.

disha (`disha/guardrail_check_test.go`):

- Band routing at 0.94 / 0.83 / 0.60 → interrupt-without-judge /
  judge-then-decide / nothing.
- Dedupe by instruction ID keeps best similarity; top-1 selection.
- Record selection: violating record wins over a higher-similarity
  non-violating one; otherwise highest similarity wins; box lock prevents
  overwrite after a violation.
- Judge error / timeout / malformed output → not violated, status `error`,
  Sentry once.
- `context.Canceled` → no Sentry.
- Fan-out above 10 fires the Sentry event exactly once per turn.
- Audit verdict lands in the record when it arrives before `take()`; logged and
  dropped when after.
- `appendGuardrailBlock` places the block last, `user` role, exact wording, and
  does not mutate the caller's slice.
- `composeEnrichers` runs both enrichers in order and is a no-op with no
  pending correction.
- Chunk decorator: guardrail record merges beside an existing protocol record;
  partial chunk with an unresolved record gets no guardrail metrics;
  tool-pair assistant chunk (`AdditionalData != nil`) never consumes a record.

`llmrouter`: the two new configs build correct requests; `gpt-oss-20b` carries
`reasoning.effort = "low"`; explicit `MaxTokens` is sent (regression guard for
VAGO-15).

---

## 14. Known deltas and accepted limitations

### 14.1 Disha may already be speaking

Accepted. The check cannot begin until a fragment is complete, and TTS is fed
that same fragment at the same moment.

Per-fragment triggering is what makes this tolerable rather than fatal. Firing
on the **first** fragment gives the check a real chance to complete before
Cartesia has produced any audio at all: the measured vector-query path is p50
~17ms / p95 ~21ms, against Cartesia TTFB plus playback pacing. The >0.90 band is
therefore often a genuine **pre-speech** guardrail; the judge band, which adds a
few hundred milliseconds, is usually a **mid-speech correction**.

`AssistantContextAggregator` commits nothing when no words have played, so the
fast path leaves history clean automatically — history damage is exactly
proportional to how much of the race was lost.

The only mechanism that would *close* the gap rather than narrow it is holding
back the first fragment's audio (~200ms) in `PlaybackSink`. Not proposed: it
taxes every turn's `v2v_latency_ms`, a Python-parity metric, to fix a minority
of turns. Revisit only with data.

### 14.2 `end_call` can be cancelled by a false positive

Accepted (Jaideep, 2026-08-03). Tool calls execute **after**
`LLMResponseEndFrame` on `LLMProcessor`'s goroutine, so an interrupt cancels
`procCtx` and with it any pending tool execution. A farewell turn like
*"Okay, take care, bye"* + `end_call` would have its hangup cancelled by a
guardrail trigger. Rare, and a farewell is unlikely to be a true positive — but
a >0.90 false positive leaves the call running after Disha has said goodbye.
Detecting "this generation had pending tool calls" is not possible at this
pipeline position without a new signal, which was judged not worth it for v1.

### 14.3 Other accepted deltas

- **Cross-boundary violations are not detected** (§5.2).
- **Clause-level fan-out** from reusing `endsWithPunctuation` (§5.1), with a
  predictably noisy Sentry threshold at 10.
- **Two consecutive assistant messages persist** after a regeneration, and the
  violating text outlives the correction (§6.1).
- **The trailing un-punctuated remainder of a response is never checked** — it
  is not a completed fragment.
- **Thresholds are unvalidated** (§9).
- **The greeting turn is checked** like any other; no special case.
- **Regenerated turns carry their own latency metrics**, so `v2v_latency_ms` on
  a regenerated chunk is measured from the regeneration's own
  `LLMResponseStartFrame` and understates what the user actually waited. No
  Python counterpart exists, so this is not a parity gap.

---

## 15. Naming

Per protocol §16, everything shared stays unprefixed and everything
guardrail-specific carries a `guardrail` prefix.

**Shared, reused as-is:** all of `internal/weaviate`; `weaviateEnvFlagField`;
`ChunkRetrievalMetrics`; `newRetrievalChunkDecorator`;
`ContextEnricherProcessor` / `MessagesEnricher` / `MetricContextEnrich`;
`conversationTurnStarts` (unused by this step, but still generic);
`composeEnrichers` (new, generic).

**New and generic in core:** `ResponseGuard`, `ResponseGuardProcessor`,
`MetricResponseGuard` — no business knowledge, no new frame types, no
`TaskContext` fields, no new `CallEvents` members.

**Guardrail-scoped:** `guardrailChecker`, `guardrailCheckRecord`,
`guardrailCheck`, `guardrailRecordBox`, `GuardrailCheckMetrics`,
`guardrailEnricher`, `guardrailAnchorClass`, `guardrailInstructionClass`,
`guardrailAnchorFields`, `guardrailInterruptThreshold`,
`guardrailJudgeThreshold`, `guardrailQueryLimit`,
`guardrailFanoutSentryThreshold`, `guardrailCheckTimeout`,
`guardrailAuditVerdictWait`, `guardrailBlockTemplate`, `guardrailS3KeyPrefix`,
`guardrailCheckEnabledEnv`, `guardrailJudgePromptName`,
`guardrailJudgeUsecaseType`, `buildGuardrailQuery`, `queryGuardrails`,
`renderGuardrailBlock`, `appendGuardrailBlock`,
`uploadGuardrailCheckRecord`, `setupGuardrailCheck`,
`followUpPlan.GuardrailChecker`, `llmrouter.GroupGuardrailJudgeHedged`.

The RTVI event type (`"guardrail_check"`), the chunk sub-object key
(`"guardrail"`), and the S3 prefix (`guardrail_check/`) are all namespaced, so
a third retrieval-shaped step would slot in the same way.
