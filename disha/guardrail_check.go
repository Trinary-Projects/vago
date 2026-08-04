package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/internal/weaviate"
	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

// Non-blocking guardrail check for follow-up calls (both the dynamic
// check-in path and the agenda-based path).
//
// Every completed fragment of the assistant's in-flight response is checked
// against a guardrail corpus while TTS is already speaking it (this step is
// never on the critical path — see voicepipelinecore.ResponseGuard). A
// sufficiently similar guardrail interrupts and regenerates the turn. This
// file is the checker itself: the vector query, the similarity bands, the
// judge call, cancellation, the correction-message enricher, and telemetry.
// The record types it produces (guardrailCheck / guardrailCheckRecord /
// guardrailRecordBox) and every named constant live in guardrail_record.go,
// a layer beneath this one; wiring this checker into FollowUpBot.plan/
// BuildTask (setupGuardrailCheck, composeEnrichers) is a later layer.
//
// Design note: reports/followup-guardrail-check-design-note.md

// guardrailAnchorFields is the GraphQL selection set for the GuardrailAnchor
// query. Deliberately no turnsThresholdCount: unlike ProtocolInstruction,
// GuardrailInstruction has no such property — guardrails have no resident
// set, TTL, or eviction (design note §3).
const guardrailAnchorFields = `anchorText
answeredBy { ... on GuardrailInstruction { instructionText title documentVersionPath _additional { id } } }`

// guardrailBlockTemplate is byte-exact per the design note §6.2. "%s" is
// where the matched instructionText goes.
const guardrailBlockTemplate = `<system_message>
Your response violates the following guardrail -
%s
please regenerate with correction
</system_message>`

// ------------------------------------------------------- weaviate decoding

// guardrailQueryResult is one fragment's vector-query outcome: the top
// deduped-by-instruction-id hit (nil if every candidate was skipped) plus
// every deduped candidate, kept for the calibration dataset regardless of
// whether it qualified into a band. TopInstructionText travels separately
// from guardrailTopHit — the record types deliberately keep only enough to
// join back to the instruction/anchor, not the full instruction text, since
// that text is only needed transiently here (for the correction block and
// the judge prompt) and never persisted to the chunk/DB.
type guardrailQueryResult struct {
	Top                *guardrailTopHit
	TopInstructionText string
	Candidates         []guardrailCandidateHit
}

// queryGuardrails runs one nearText round against GuardrailAnchor and
// returns candidates deduped by instruction id (best similarity wins), plus
// the overall top hit. Mirrors queryProtocols's decoding rules exactly:
// skip hits with no distance, no answeredBy cross-reference, or an empty
// instructionText/id.
func queryGuardrails(ctx context.Context, client *weaviate.Client, fragment string) (guardrailQueryResult, error) {
	hits, err := client.NearText(ctx, weaviate.NearTextQuery{
		Class:    guardrailAnchorClass,
		Concepts: []string{fragment}, // RAW: TEI applies the "Document: " prefix server-side
		Fields:   guardrailAnchorFields,
		Where: weaviate.EqualBool(
			[]string{"answeredBy", guardrailInstructionClass, weaviateEnvFlagField()}, true),
		Limit: guardrailQueryLimit,
	})
	if err != nil {
		return guardrailQueryResult{}, err
	}

	type decodedHit struct {
		hit             guardrailTopHit
		instructionText string
	}
	best := make(map[string]decodedHit, len(hits))
	order := make([]string, 0, len(hits))
	for _, hit := range hits {
		if !hit.DistancePresent {
			continue
		}
		instruction, ok := hit.CrossRef("answeredBy")
		if !ok {
			continue
		}
		text := strings.TrimSpace(instruction.String("instructionText"))
		instructionID := instruction.ID()
		if text == "" || instructionID == "" {
			continue
		}
		candidate := guardrailTopHit{
			InstructionID: instructionID,
			AnchorID:      hit.ID,
			AnchorText:    hit.String("anchorText"),
			Title:         instruction.String("title"),
			DocumentPath:  instruction.String("documentVersionPath"),
			Similarity:    hit.Similarity(),
		}

		// Dedupe by instruction: many anchors point at one guardrail, and
		// without this a single guardrail would dominate the candidate list.
		if existing, seen := best[instructionID]; seen {
			if candidate.Similarity > existing.hit.Similarity {
				best[instructionID] = decodedHit{hit: candidate, instructionText: text}
			}
			continue
		}
		best[instructionID] = decodedHit{hit: candidate, instructionText: text}
		order = append(order, instructionID)
	}

	candidates := make([]guardrailCandidateHit, 0, len(order))
	for _, id := range order {
		d := best[id]
		candidates = append(candidates, guardrailCandidateHit{
			InstructionID: d.hit.InstructionID,
			AnchorID:      d.hit.AnchorID,
			Similarity:    d.hit.Similarity,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Similarity > candidates[j].Similarity })

	result := guardrailQueryResult{Candidates: candidates}
	topID, topSim := "", -1.0
	for _, id := range order {
		if best[id].hit.Similarity > topSim {
			topSim = best[id].hit.Similarity
			topID = id
		}
	}
	if topID != "" {
		top := best[topID].hit
		result.Top = &top
		result.TopInstructionText = best[topID].instructionText
	}
	return result, nil
}

// ------------------------------------------------------------ block render

// renderGuardrailBlock fills the byte-exact correction template with the
// matched guardrail's instruction text.
func renderGuardrailBlock(instructionText string) string {
	return fmt.Sprintf(guardrailBlockTemplate, instructionText)
}

// appendGuardrailBlock returns messages with the correction block appended
// as the LAST message (unlike the protocol block, which sits above the
// tail — this one refers to "your response" and must follow the violating
// turn). It never mutates the caller's slice, mirroring injectProtocolBlock.
func appendGuardrailBlock(messages []voicepipelinecore.Message, instructionText string) []voicepipelinecore.Message {
	out := make([]voicepipelinecore.Message, len(messages), len(messages)+1)
	copy(out, messages)
	return append(out, voicepipelinecore.Message{Role: "user", Content: renderGuardrailBlock(instructionText)})
}

// -------------------------------------------------------------- the judge

// guardrailJudgeClientFactory builds a one-shot LLMClient for a single judge
// call, given that call's exact prompt metadata (system_prompt_name/version/
// variables, per AGENTS.md's strict prompt-metadata rule).
//
// Deliberate deviation from the design note §5.5 ("one client per call
// reused by every check — construct once in the constructor, not per
// check"): llmrouter.Hedged exposes no way to change its bound
// PromptMetadata after construction (unlike llmrouter.Router's own
// SetPromptMetadata), so a single Hedged instance shared by every
// concurrent fragment check could never carry the correct per-fragment
// instructionText/fragment variables past the first call — every later
// judge call would log stale (or, if llmrouter grew a setter later, racily
// interleaved) system_prompt_variables. Either failure mode violates the
// strict prompt-metadata rule. NewHedged performs no I/O itself (it only
// builds two Router structs), and a zero-value *http.Client falls back to
// Go's shared http.DefaultTransport connection pool, so building a fresh
// Hedged client per judge call carries no real per-call dial/TLS cost. The
// factory closure itself — the thing actually worth building once — is
// still constructed exactly once, in newGuardrailChecker's caller
// (newGuardrailJudgeClientFactory below).
type guardrailJudgeClientFactory func(promptMetadata map[string]any) (voicepipelinecore.LLMClient, error)

// guardrailJudgeHedgeThreshold is deliberately far below the hedged
// primitive's 1000ms default (llmrouter.defaultHedgeThreshold): this is an
// "ultra-fast judge" per the design note's intro, not a background call.
const guardrailJudgeHedgeThreshold = 400 * time.Millisecond

// guardrailJudgeMaxTokens is explicit and non-nil, learning directly from
// VAGO-15 (careplan detection's silent mid-JSON truncation from an inherited
// endpoint-config token cap): a nil MaxTokens here would send NO max_tokens
// field at all, since neither judge endpoint config carries one, which is
// the opposite failure mode but still wrong. 512 rather than something
// tiny because gpt-oss-20b at low reasoning effort spends output tokens on
// reasoning before any verdict text (design note §5.5: a live max_tokens:16
// probe returned finish_reason=length with 7 of 16 tokens spent on
// reasoning).
const guardrailJudgeMaxTokens = 512

// newGuardrailJudgeClientFactory builds the PRODUCTION guardrailJudgeClientFactory:
// a hedged primary/hedge race over llmrouter.GroupGuardrailJudgeHedged,
// temperature 0 (deterministic verdicts). Constructed once per call by the
// wiring layer and passed into newGuardrailChecker.
func newGuardrailJudgeClientFactory(deps Deps, logger *log.Logger, userID, conversationID string) guardrailJudgeClientFactory {
	temperature := 0.0
	maxTokens := guardrailJudgeMaxTokens
	return func(promptMetadata map[string]any) (voicepipelinecore.LLMClient, error) {
		client, err := llmrouter.NewHedged(llmrouter.HedgedConfig{
			Pair:           llmrouter.GroupGuardrailJudgeHedged,
			Redis:          deps.Redis,
			Logger:         logger,
			LogSink:        newLLMLogSink(deps.API, logger, guardrailJudgeUsecaseType, userID, conversationID),
			PromptMetadata: promptMetadata,
			HedgeThreshold: guardrailJudgeHedgeThreshold,
			Temperature:    &temperature,
			MaxTokens:      &maxTokens,
		})
		if err != nil {
			return nil, err
		}
		return client, nil
	}
}

// guardrailJudgeOutput is the judge output contract: a JSON object carrying a
// "violated" verdict.
type guardrailJudgeOutput struct {
	Violated guardrailJudgeVerdict `json:"violated"`
}

// guardrailJudgeVerdict decodes "violated" from either a JSON boolean or a
// quoted string.
//
// Both forms are real. The staging prompt
// (followup_call/guardrails/test_prompt) specifies `"violated": "true" or
// "false"` — quoted STRINGS — while a bare bool is the shape a reader would
// assume. A strict bool decode rejected the string form outright, and because
// an unparseable verdict fails OPEN (§8) the whole judge band went quiet
// without erroring anywhere visible. Accepting both costs nothing and removes
// a silent failure mode that depends on how a human happened to word a prompt.
type guardrailJudgeVerdict bool

func (v *guardrailJudgeVerdict) UnmarshalJSON(b []byte) error {
	var asBool bool
	if err := json.Unmarshal(b, &asBool); err == nil {
		*v = guardrailJudgeVerdict(asBool)
		return nil
	}
	var asString string
	if err := json.Unmarshal(b, &asString); err == nil {
		switch strings.ToLower(strings.TrimSpace(asString)) {
		case "true", "yes", "violated", "1":
			*v = true
			return nil
		case "false", "no", "not_violated", "0":
			*v = false
			return nil
		}
		return fmt.Errorf("unrecognised violated value %q", asString)
	}
	return fmt.Errorf("violated is neither boolean nor string: %s", string(b))
}

// guardrailJSONObject extracts the outermost {...} span from raw model output.
//
// Models wrap JSON in markdown fences — the staging prompt literally asks for
// a ```-fenced block — and some add a sentence of preamble. Either defeats a
// bare Unmarshal of the whole response. Taking the first "{" through the last
// "}" handles fences, prose and both together without needing to recognise
// each wrapper individually. Returns "" when there is no object at all.
func guardrailJSONObject(s string) string {
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start < 0 || end < start {
		return ""
	}
	return s[start : end+1]
}

// parseGuardrailJudgeOutput strips <think>...</think> (thinkBlockRe, shared
// with deep thinking's output handling — same package) and parses the
// remaining text as a guardrailJudgeOutput. ok=false for empty or
// unparseable output, which the caller treats as fail-open.
func parseGuardrailJudgeOutput(raw string) (violated, ok bool) {
	cleaned := strings.TrimSpace(thinkBlockRe.ReplaceAllString(raw, ""))
	object := guardrailJSONObject(cleaned)
	if object == "" {
		return false, false
	}
	var out guardrailJudgeOutput
	if err := json.Unmarshal([]byte(object), &out); err != nil {
		return false, false
	}
	return bool(out.Violated), true
}

func guardrailVerdictString(violated bool) string {
	if violated {
		return "yes"
	}
	return "no"
}

// -------------------------------------------------------------- the checker

// guardrailChecker implements voicepipelinecore.ResponseGuard (via Check)
// and voicepipelinecore.MessagesEnricher (via Enrich) for follow-up calls.
//
// callCtx is the call's own long-lived context (TaskContext.Ctx), injected
// separately from the per-check ctx Check() receives from
// ResponseGuardProcessor. It exists for exactly one purpose: the >0.85
// band's fire-and-forget audit judge (spawnAuditJudge) must survive the very
// interrupt its own violation triggers, and that interrupt is what cancels
// the turn ctx Check() was given. Deriving the audit's bound context from
// callCtx instead of the turn ctx means it keeps running until the CALL
// ends, not the turn.
type guardrailChecker struct {
	client             *weaviate.Client
	docs               *DocumentStore
	box                *guardrailRecordBox
	logger             *log.Logger
	judgeClientFactory guardrailJudgeClientFactory

	callCtx context.Context

	// ui is the only late-bound infrastructure this checker needs, hence
	// SetUI rather than protocolEnricher's SetInfrastructure(router, ui).
	// Deliberately NO promptMetadataSetter: protocol retrieval holds one to
	// republish the conversation prompt's metadata with retrieved_protocols,
	// but the guardrail correction is ephemeral (§6.2) and no equivalent
	// prompt variable is specified, so a router handle here would be a
	// long-lived dependency with no reader.
	ui serverMessageEmitter
	taskSentryHub

	userID         string
	conversationID string

	// mu guards every field below AND ui: writers are rare (one
	// SetUI/SetSentryHub call at wiring time, one Enrich per turn) and
	// readers are per-fragment-check frequency, so one mutex is simplest and
	// never meaningfully contended (mirrors protocolEnricher, which also
	// shares one mutex between its infra fields and its per-round state).
	mu                sync.Mutex
	turnText          string
	checkCount        int
	checks            []guardrailCheck
	pendingCorrection string
	fanoutSentryFired bool
}

func newGuardrailChecker(
	callCtx context.Context,
	client *weaviate.Client,
	docs *DocumentStore,
	box *guardrailRecordBox,
	logger *log.Logger,
	judgeClientFactory guardrailJudgeClientFactory,
	userID, conversationID string,
) *guardrailChecker {
	return &guardrailChecker{
		callCtx:            callCtx,
		client:             client,
		docs:               docs,
		box:                box,
		logger:             logger,
		judgeClientFactory: judgeClientFactory,
		userID:             userID,
		conversationID:     conversationID,
	}
}

// SetUI injects the RTVI emitter, which only exists once the task is built.
// Nil-safe on both the receiver and the argument, like the onboarding
// managers' SetUI.
func (c *guardrailChecker) SetUI(ui serverMessageEmitter) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ui = ui
}

func (c *guardrailChecker) emitter() serverMessageEmitter {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ui
}

// Check implements voicepipelinecore.ResponseGuard. ResponseGuardProcessor
// invokes it on its own background goroutine per completed fragment
// (spawnCheck) — never on the pipeline goroutine — so the 0.70-0.85 band's
// blocking judge call below is safe to run inline: it blocks this
// goroutine, never TTS/playback.
func (c *guardrailChecker) Check(ctx context.Context, fragment string) bool {
	checkCtx, cancel := context.WithTimeout(ctx, guardrailCheckTimeout)
	defer cancel()

	index, turnText := c.accumulate(fragment)
	c.box.setTurnText(turnText)
	c.maybeReportFanout(index)

	started := time.Now()
	check := guardrailCheck{Index: index, Fragment: fragment, Status: "ok"}

	queryStarted := time.Now()
	result, err := queryGuardrails(checkCtx, c.client, fragment)
	check.VectorQueryLatencyMs = msSince(queryStarted)
	if err != nil {
		check.Cancelled = errors.Is(err, context.Canceled) || ctx.Err() != nil
		check.Status = "error"
		check.Err = err.Error()
		// NOT "below": we never obtained a similarity, so recording this as a
		// below-threshold sample would pollute the S3 calibration dataset with
		// a non-observation that reads as "scored under 0.70".
		check.Band = "error"
		check.TotalLatencyMs = msSince(started)
		c.reportFailure(ctx, err, "guardrail_check", map[string]any{"fragment_chars": len(fragment)})
		c.finish(check, false)
		c.publish(check)
		return false
	}
	check.Top = result.Top
	check.Candidates = result.Candidates

	violated := false
	switch {
	case result.Top != nil && result.Top.Similarity > guardrailInterruptThreshold:
		check.Band = "interrupt"
		violated = true
		c.spawnAuditJudge(result.Top, result.TopInstructionText, fragment)
	case result.Top != nil && result.Top.Similarity >= guardrailJudgeThreshold:
		check.Band = "judge"
		judgeViolated, detail := c.runJudge(checkCtx, ctx, result.TopInstructionText, fragment, false)
		check.Judge = detail
		// A judge failure is a failure of this check even though it fails open
		// to not-violated (§8). Without this the chunk would report status
		// "ok" for a turn whose judge never answered, hiding exactly the
		// condition the status field exists to surface.
		if detail.Error != "" {
			check.Status = "error"
			check.Err = detail.Error
		}
		violated = judgeViolated
	default:
		check.Band = "below"
	}
	check.Violated = violated
	check.TotalLatencyMs = msSince(started)

	if violated {
		c.mu.Lock()
		c.pendingCorrection = result.TopInstructionText
		c.mu.Unlock()
	}

	c.finish(check, violated)
	c.publish(check)
	return violated
}

// accumulate appends fragment to the running per-turn text and returns this
// fragment's 1-based index plus the full accumulated text so far.
func (c *guardrailChecker) accumulate(fragment string) (index int, turnText string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-insert the separator splitSentences consumed when it cut this
	// fragment off. Fragments are deliberately whitespace-free so the vector
	// query and the calibration dataset stay tidy, but turnText is a different
	// consumer: it becomes GuardrailCheckMetrics.QueryText and, backend-side,
	// GuardrailLiveQueryAnchor's own text. Without this the corpus accumulates
	// run-together sentences — observed on staging call 891aaa9f, where Disha
	// said "…रह गई थी। जैसा कि…" and the record persisted "…थी।जैसा कि…" —
	// which reads wrong and embeds slightly worse than what was actually said.
	if c.turnText != "" && !strings.HasSuffix(c.turnText, " ") && !strings.HasPrefix(fragment, " ") {
		c.turnText += " "
	}
	c.turnText += fragment
	c.checkCount++
	return c.checkCount, c.turnText
}

// maybeReportFanout captures ONE Sentry event per turn once check volume
// exceeds guardrailFanoutSentryThreshold, then keeps going (there is no cap).
//
// Fragments are whole sentences (voicepipelinecore.endsWithSentenceTerminator),
// so a typical turn is ~3 checks and crossing 10 means a genuinely unusual
// turn worth looking at — not routine traffic. That was not true of the
// earlier clause-level boundary, under which this alert would have been noise.
func (c *guardrailChecker) maybeReportFanout(index int) {
	if index <= guardrailFanoutSentryThreshold {
		return
	}
	c.mu.Lock()
	already := c.fanoutSentryFired
	c.fanoutSentryFired = true
	c.mu.Unlock()
	if already {
		return
	}
	sentryutil.Capture(sentryutil.Event{
		Hub:     c.sentryHub(),
		Message: "guardrail check fan-out exceeded threshold for one turn",
		Tags: map[string]string{
			"component": "disha_followup",
			"operation": "guardrail_check_fanout",
		},
		Details: map[string]any{
			"conversation_id": c.conversationID,
			"user_id":         c.userID,
			"check_count":     index,
			"threshold":       guardrailFanoutSentryThreshold,
		},
	})
}

// finish appends check to the turn's accumulated check list and offers a
// record built from that full list to the box. Selection follows §5.7:
// a violating check always wins (offerViolation, which locks the box), and
// selecting by "this check" as SelectedIndex is correct there because the
// violating check is always the newest element appended; otherwise
// SelectedIndex is recomputed as the highest-similarity check across the
// WHOLE accumulated list, so each successive offer's own comparator value
// (record.selectedSimilarity) is monotonically non-decreasing as more
// checks complete — which is what lets the box's "keep the higher one" rule
// converge on the fullest, best snapshot in practice.
//
// Accepted limitation: once the box is locked by a violation, checks that
// complete afterward (already in-flight when the winning check fired,
// mostly cancelled) still append to this checker's own list and still call
// box.offer, but the locked box no-ops — so the final S3 record can miss a
// small tail of checks that finished after the violation. In practice
// almost every other in-flight check is cancelled by the same interrupt
// that fired the violation, so this tail is typically empty.
func (c *guardrailChecker) finish(check guardrailCheck, violated bool) {
	c.mu.Lock()
	c.checks = append(c.checks, check)
	checksCopy := append([]guardrailCheck(nil), c.checks...)
	turnText := c.turnText
	c.mu.Unlock()

	selectedIndex := len(checksCopy) - 1
	if !violated {
		selectedIndex = guardrailBestSimilarityIndex(checksCopy)
	}

	// The chunk-level metrics describe the SELECTED check, so the record's
	// status/error must come from that check rather than being hardcoded.
	// Hardcoding "ok" meant a turn whose check failed (Weaviate unreachable,
	// judge erroring) still reported status "ok" with an empty error all the
	// way into the DB column — so an outage would look like a call where every
	// guardrail check ran cleanly, which is the one thing this field must
	// never do.
	record := guardrailCheckRecord{
		TurnText:      turnText,
		Checks:        checksCopy,
		SelectedIndex: selectedIndex,
		Interrupted:   violated,
		CheckCount:    len(checksCopy),
		Status:        checksCopy[selectedIndex].Status,
		Err:           checksCopy[selectedIndex].Err,
	}
	if violated {
		// awaitAudit only for the >0.85 band: its interrupt fires on similarity
		// alone and the audit judge answers later, so the box must hold the
		// record until that verdict lands (see guardrailRecordBox.take). A
		// judge-band violation already knows its verdict and is released at
		// once, landing on the partial chunk -- the violating turn itself.
		c.box.offerViolation(record, check.Band == "interrupt")
		return
	}
	c.box.offer(record)
}

func guardrailBestSimilarityIndex(checks []guardrailCheck) int {
	best, bestSim := 0, -1.0
	for i, check := range checks {
		sim := -1.0
		if check.Top != nil {
			sim = check.Top.Similarity
		}
		if sim > bestSim {
			bestSim = sim
			best = i
		}
	}
	return best
}

// runJudge renders the judge prompt and runs the hedged one-shot judge
// call, blocking the CALLING goroutine — a Check() background goroutine for
// the 0.70-0.85 band, or the audit fire-and-forget goroutine for the >0.85
// band. judgeCtx bounds the LLM call itself; reportCtx is the ctx checked
// to decide the Sentry-exemption for failures — the check's own ctx for the
// blocking judge-band path, or the call ctx for the audit path — mirroring
// protocolEnricher.retrieve's outer/inner ctx split (the outer ctx is
// checked for real cancellation while the inner one may simply have timed
// out on its own budget).
//
// Fails open on every path: missing document store/factory, a missing
// judge prompt document, an LLM error, empty output, or unparseable output
// all return violated=false. Each failure is reported via reportFailure
// under the "guardrail_judge" operation, which Sentries every case except
// reportCtx already being cancelled (call/check genuinely ended, not just a
// timeout of judgeCtx's own budget).
//
// TODO (launch blocker, design note §5.6): guardrailJudgePromptName does not
// exist in DocumentStore yet, so every real call currently fails at
// GetDocument and this band always fails open — only the >0.85 band
// functions until the real Langfuse prompt lands.
func (c *guardrailChecker) runJudge(judgeCtx, reportCtx context.Context, instructionText, fragment string, auditOnly bool) (bool, guardrailJudgeDetail) {
	detail := guardrailJudgeDetail{Ran: true, AuditOnly: auditOnly}
	started := time.Now()

	fail := func(err error) (bool, guardrailJudgeDetail) {
		detail.LatencyMs = msSince(started)
		detail.Error = err.Error()
		c.reportFailure(reportCtx, err, "guardrail_judge", map[string]any{"audit_only": auditOnly})
		return false, detail
	}

	if c.docs == nil {
		return fail(errors.New("disha: guardrail judge document store is not configured"))
	}
	if c.judgeClientFactory == nil {
		return fail(errors.New("disha: guardrail judge client factory is not configured"))
	}

	// Key names must match the deployed prompt exactly. It reads {{guardrail}}
	// and {{fragment}}; sending "guardrail_instruction" rendered the guardrail
	// line EMPTY, so the judge was asked to rule on a fragment against no rule
	// at all — and still answered, which is why this surfaced as a parse
	// problem rather than an obvious blank-prompt error.
	variables := DocumentVariables{
		"guardrail": instructionText,
		"fragment":  fragment,
	}
	sysText, version, err := c.docs.GetDocument(judgeCtx, guardrailJudgePromptName, 0, variables)
	if err != nil {
		return fail(fmt.Errorf("disha: render guardrail judge prompt %q: %w", guardrailJudgePromptName, err))
	}

	metadata := buildPromptTraceMetadata("system", guardrailJudgePromptName, version, variables)
	client, err := c.judgeClientFactory(metadata)
	if err != nil {
		return fail(fmt.Errorf("disha: build guardrail judge client: %w", err))
	}
	if client == nil {
		return fail(errors.New("disha: guardrail judge client unavailable"))
	}

	req := voicepipelinecore.LLMRequest{Messages: []voicepipelinecore.Message{
		{Role: "system", Content: sysText},
	}}
	var out strings.Builder
	result, err := client.Stream(judgeCtx, req, func(token string) { out.WriteString(token) })
	detail.LatencyMs = msSince(started)
	detail.Model = result.Model
	if err != nil {
		detail.Error = err.Error()
		c.reportFailure(reportCtx, err, "guardrail_judge", map[string]any{"audit_only": auditOnly})
		return false, detail
	}

	violated, ok := parseGuardrailJudgeOutput(out.String())
	if !ok {
		parseErr := errors.New("disha: guardrail judge output empty or unparseable")
		detail.Error = parseErr.Error()
		c.reportFailure(reportCtx, parseErr, "guardrail_judge", map[string]any{
			"audit_only": auditOnly,
			"raw_output": runePrefix(out.String(), 200),
		})
		return false, detail
	}

	detail.Verdict = guardrailVerdictString(violated)
	return violated, detail
}

// spawnAuditJudge runs the >0.85 band's audit-only judge, fire-and-forget,
// purely to record whether the interrupt it accompanies was justified
// (design note §5.6): verdict violated -> true positive; verdict not
// violated -> false positive, meaning the anchor that fired the interrupt
// is pulling in unrelated sentences above 0.85.
//
// Derived from c.callCtx, NOT the check ctx Check() was given, because that
// ctx is what the interrupt this very check just fired will cancel — the
// audit must outlive it. Bounded by guardrailAuditVerdictWait; runJudge's
// reportCtx is also callCtx, so a timeout while the call is still alive is
// Sentried like any other judge failure, while callCtx itself ending first
// (the call ended) is logged only, per the design note.
func (c *guardrailChecker) spawnAuditJudge(top *guardrailTopHit, instructionText, fragment string) {
	if top == nil {
		return
	}
	go func() {
		auditCtx, cancel := context.WithTimeout(c.callCtx, guardrailAuditVerdictWait)
		defer cancel()

		_, detail := c.runJudge(auditCtx, c.callCtx, instructionText, fragment, true)
		if detail.Verdict == "" {
			// runJudge already reported the failure (or, if callCtx itself
			// was already done, logged the cancellation) — nothing to
			// attach to the record.
			return
		}
		if ok := c.box.setAuditVerdict(detail.Verdict); !ok {
			c.logf("guardrail audit verdict arrived after the record was taken instruction=%s verdict=%s",
				top.InstructionID, detail.Verdict)
		}
	}()
}

// reportFailure logs every failure and Sentries every failure except one
// where ctx is already done — a genuine cancellation (call ended, turn
// interrupted) rather than an internal timeout — mirroring
// protocolEnricher.reportFailure's exemption exactly. Callers pass the
// OUTER ctx (the check's own ctx, or callCtx for the audit path), not an
// inner budget-limited one, so a query/judge call timing out on its own
// budget while the outer call is still alive is still reported.
func (c *guardrailChecker) reportFailure(ctx context.Context, err error, operation string, details map[string]any) {
	cancelled := errors.Is(err, context.Canceled) || ctx.Err() != nil
	c.logf("guardrail %s failed cancelled=%v: %v", operation, cancelled, err)
	if cancelled {
		return
	}
	merged := map[string]any{"conversation_id": c.conversationID, "user_id": c.userID}
	for k, v := range details {
		merged[k] = v
	}
	sentryutil.Capture(sentryutil.Event{
		Hub: c.sentryHub(),
		Err: err,
		Tags: map[string]string{
			"component": "disha_followup",
			"operation": operation,
		},
		Details: merged,
	})
}

// publish always logs one app.log line per check. §7.4 correction: the
// checker has no turn-end signal (unlike protocolEnricher, which runs once
// per turn before generation), so publishing one RTVI server-message per
// TURN is not available to it — only per CHECK, and a turn can fan out to a
// dozen of those (§5.1). Emitting one per check would flood the debug-log
// stream at exactly the volume the fan-out Sentry guard exists to flag.
// Instead this publishes an RTVI "guardrail_check" server-message only for
// checks that are actually interesting — judge-band or violated — and stays
// silent for below-threshold checks, which bounds volume while keeping
// every signal that matters (every judge call and every interrupt still
// gets its own trace line).
func (c *guardrailChecker) publish(check guardrailCheck) {
	c.logf("guardrail check index=%d band=%s violated=%v status=%s similarity=%s judge_verdict=%q query_ms=%.1f total_ms=%.1f",
		check.Index, check.Band, check.Violated, check.Status, guardrailSimilarityString(check.Top),
		check.Judge.Verdict, check.VectorQueryLatencyMs, check.TotalLatencyMs)

	if check.Band == "below" {
		return
	}
	ui := c.emitter()
	if ui == nil {
		return
	}
	data := map[string]any{
		"type":        "guardrail_check",
		"status":      check.Status,
		"band":        check.Band,
		"violated":    check.Violated,
		"check_index": check.Index,
		"total_ms":    check.TotalLatencyMs,
	}
	if check.Top != nil {
		data["similarity"] = check.Top.Similarity
	}
	if check.Judge.Ran {
		data["judge_verdict"] = check.Judge.Verdict
	}
	if check.Err != "" {
		data["error"] = check.Err
	}
	ui.ServerMessage(data, time.Now())
}

func guardrailSimilarityString(top *guardrailTopHit) string {
	if top == nil {
		return "none"
	}
	return fmt.Sprintf("%.4f", top.Similarity)
}

func (c *guardrailChecker) logf(format string, args ...any) {
	if c.logger != nil {
		c.logger.Printf("disha: "+format+"\n", args...)
	}
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t).Microseconds()) / 1000.0
}

// ------------------------------------------------------------- the enricher

// Enrich implements voicepipelinecore.MessagesEnricher. It is called once
// per turn, before generation, by the same ContextEnricherProcessor
// protocol retrieval uses (composed together by a later wiring layer's
// composeEnrichers).
//
// This is the only turn-start signal available to the checker: Check()
// only ever sees fragments, with no explicit "turn began" callback, so
// per-turn state (the running turn text, the check counter, and the
// fan-out Sentry latch) is reset HERE instead. Enrich necessarily runs
// before the turn's first fragment reaches Check (it produces the
// LLMMessagesFrame that starts generation), so there is no ordering hazard.
func (c *guardrailChecker) Enrich(_ context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
	c.mu.Lock()
	c.turnText = ""
	c.checkCount = 0
	c.checks = nil
	c.fanoutSentryFired = false
	correction := c.pendingCorrection
	c.pendingCorrection = ""
	c.mu.Unlock()

	if correction == "" {
		return messages
	}
	return appendGuardrailBlock(messages, correction)
}
