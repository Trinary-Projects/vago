package disha

import (
	"sync"
	"time"
)

// Non-blocking guardrail check for follow-up calls (both the dynamic
// check-in path and the agenda-based path).
//
// Every completed fragment of the assistant's in-flight response is checked
// against a guardrail corpus while TTS is already speaking it. A sufficiently
// similar guardrail interrupts and regenerates the turn. This file holds the
// record types the checker (disha/guardrail_check.go, a later stacked layer)
// produces and the chunk decorator consumes, plus every named constant this
// feature needs — kept together so this layer compiles on its own before the
// checker lands.
//
// Design note: reports/followup-guardrail-check-design-note.md

const (
	guardrailAnchorClass      = "GuardrailAnchor"
	guardrailInstructionClass = "GuardrailInstruction"

	// guardrailInterruptThreshold and guardrailJudgeThreshold are cosine
	// similarity (1 - distance) bands on the top-1 deduped hit for one
	// fragment:
	//   > 0.85            -> interrupt immediately; also run the judge
	//                        audit-only, off the critical path.
	//   0.70 <= x <= 0.85 -> call the judge and wait on it; violated ->
	//                        interrupt.
	//   < 0.70            -> nothing.
	//
	// The comparison operators are load-bearing: > interrupts, >= judges, so
	// exactly 0.85 falls in the judge band. Go and
	// scripts/seed_guardrail_collections.py must agree exactly, or the
	// calibration tool reports bands production does not use.
	//
	// Calibrated 2026-08-04 against the seeded fixture corpus (7 probe queries,
	// 4 guardrails, 16 anchors) rather than assumed:
	//
	//   true positives   0.8892  stop your BP tablets
	//                    0.8277  take one tablet of paracetamol
	//                    0.7768  you will definitely lose weight
	//                    0.7491  sounds like it could be your thyroid
	//   true negatives   0.6525  ask your doctor before changing medication
	//                    0.5692  let's talk about your diet plan instead
	//                    0.4654  that's a great question
	//
	// The first values tried, 0.75/0.90, were wrong in both directions. Nothing
	// reached 0.90, so the interrupt band never fired and every violation took
	// the slow judge path — the pre-speech fast path was dead. And the hedged
	// diagnosis at 0.7491 missed the judge band by 0.0009 despite being a
	// genuine violation.
	//
	// Nothing lands between 0.6525 and 0.7491, so 0.70 separates with margin on
	// both sides. Note the floor really is high, as the design note predicted:
	// wholly unrelated small talk still scores 0.4654, so absolute distance from
	// zero means nothing here — only distance from the noise band does.
	//
	// This is fixture data, not live traffic. The S3 record carries every
	// candidate from day one so these can be re-derived from real calls without
	// a rerun.
	// TEMPORARILY LOWERED to 0.55 (Jaideep, 2026-08-04) to exercise the judge
	// band on staging: real benign traffic tops out at 0.6282 across calls
	// 287f66ae and 891aaa9f, so at 0.70 no live turn ever reached the judge
	// and the prompt stayed untested. 0.55 puts ~5 of 6 fragments per call
	// into the band. NOT a shippable value — it gates almost everything, so
	// the judge verdict becomes the only thing preventing false interrupts.
	// Restore to 0.70 (the calibrated value above) before rollout.
	guardrailInterruptThreshold = 0.85
	guardrailJudgeThreshold     = 0.55

	// guardrailQueryLimit is how many anchors to fetch before dedupe-by-
	// instruction-id and top-1 selection.
	guardrailQueryLimit = 10

	// guardrailFanoutSentryThreshold: above this many checks in one turn,
	// capture one Sentry event and keep going (there is no cap, and it is one
	// event per turn rather than one per check).
	//
	// Fragments are whole sentences
	// (voicepipelinecore.endsWithSentenceTerminator), so a typical turn is ~3
	// checks and 10 means a genuinely unusual turn. Under the earlier
	// clause-level boundary an ordinary turn ran 8-12 checks, which would have
	// made this alert pure noise.
	guardrailFanoutSentryThreshold = 10

	// guardrailCheckTimeout bounds one fragment's whole check (vector query +
	// judge). Generous because this step is not on the critical path.
	guardrailCheckTimeout = 3 * time.Second

	// guardrailAuditVerdictWait bounds how long the chunk decorator's box wait
	// for the >0.85 band's fire-and-forget audit judge before the record is
	// taken with judge_verdict empty. See guardrailRecordBox.setAuditVerdict.
	guardrailAuditVerdictWait = 3 * time.Second

	// guardrailJudgePromptName is the DocumentStore prompt for the judge LLM,
	// read from Redis as document:{name}:{ENVIRONMENT}.
	//
	// This is a TEST prompt (Jaideep, 2026-08-04), enough to exercise the
	// 0.70-0.85 band end to end on staging. Swap it for the production
	// Langfuse prompt before rollout.
	//
	// The prompt must satisfy three things, all asserted by runJudge:
	//   1. It renders with exactly two variables: guardrail and fragment. These
	//      must match the prompt's placeholders exactly -- sending
	//      "guardrail_instruction" to a {{guardrail}} prompt rendered the
	//      guardrail line EMPTY on staging call d822753b.
	//   2. It is sent as the ONLY message, with role "system" — there is no
	//      separate user message carrying the fragment, so the prompt itself
	//      has to place {{ fragment }}.
	//   3. It returns a bare JSON object {"violated": true|false}. Anything
	//      unparseable fails open to not-violated (§8), so a prompt that
	//      chats around the JSON silently disables the band rather than
	//      erroring loudly. A <think>...</think> preamble is stripped first.
	guardrailJudgePromptName  = "followup_call/guardrails/test_prompt"
	guardrailJudgeUsecaseType = "follow_up_guardrail_judge"

	guardrailS3KeyPrefix = "guardrail_check"

	// guardrailCheckEnabledEnv is the one gate, no fallback chain.
	guardrailCheckEnabledEnv = "FOLLOWUP_GUARDRAIL_CHECK_ENABLED"
)

// guardrailTopHit is the best deduped-by-instruction-id candidate for one
// fragment's vector query. nil on a guardrailCheck means the query returned no
// usable hit (every candidate skipped: missing distance, missing answeredBy
// cross-ref, empty instructionText/id).
type guardrailTopHit struct {
	InstructionID string
	AnchorID      string
	AnchorText    string
	Title         string
	DocumentPath  string
	Similarity    float64
}

// guardrailCandidateHit is one deduped candidate kept for the calibration
// dataset, whether or not it qualified into a band. Deliberately narrower than
// guardrailTopHit — the S3 record's "candidates" list only needs enough to
// join back to the instruction/anchor, not the full text.
type guardrailCandidateHit struct {
	InstructionID string
	AnchorID      string
	Similarity    float64
}

// guardrailJudgeDetail records what the judge LLM did for one check — either
// the 0.70-0.85 band's blocking-within-the-check-goroutine call, or the >0.85
// band's fire-and-forget audit of an already-fired interrupt.
type guardrailJudgeDetail struct {
	Ran       bool
	AuditOnly bool
	Verdict   string // "yes" | "no" | "" (did not run, or arrived too late)
	Model     string
	LatencyMs float64
	Error     string
}

// guardrailCheck is the result of checking one completed fragment of the
// assistant's in-flight response against the guardrail corpus.
type guardrailCheck struct {
	Index    int
	Fragment string

	Top        *guardrailTopHit
	Candidates []guardrailCandidateHit

	Band string // "below" | "judge" | "interrupt"

	VectorQueryLatencyMs float64
	JudgeLatencyMs       float64
	// TotalLatencyMs is this check's own latency, fragment boundary to
	// verdict — what GuardrailCheckMetrics.E2EMs carries forward when this
	// check is the one selected for the chunk.
	TotalLatencyMs float64

	Judge guardrailJudgeDetail

	Violated  bool
	Cancelled bool
	Status    string // ok | skipped | error
	Err       string
}

// guardrailCheckRecord is one turn's guardrail telemetry, handed to the chunk
// decorator. Unlike protocolRetrievalRecord (one record per retrieval round),
// a turn can run many concurrent fragment checks; this record carries all of
// them plus which one was selected for the chunk (design note §5.7 and §7.1 —
// on a triggering turn the record is boxed for the REGENERATED chunk, not the
// interrupted partial one).
type guardrailCheckRecord struct {
	// TurnText is the entire Disha turn as generated, not a single fragment —
	// it becomes GuardrailCheckMetrics.QueryText and seeds
	// GuardrailLiveQueryAnchor backend-side.
	TurnText      string
	Checks        []guardrailCheck
	SelectedIndex int
	Interrupted   bool
	CheckCount    int
	Status        string // ok | skipped | error
	Err           string
}

// selectedSimilarity is the top similarity of the record's selected check, or
// -1 when there is no selected check or it had no usable hit — so a record
// with nothing to compare never wins a box selection against a real one.
func (r *guardrailCheckRecord) selectedSimilarity() float64 {
	if r == nil || r.SelectedIndex < 0 || r.SelectedIndex >= len(r.Checks) {
		return -1
	}
	top := r.Checks[r.SelectedIndex].Top
	if top == nil {
		return -1
	}
	return top.Similarity
}

// --------------------------------------------------------------- the box

// guardrailRecordBox hands one turn's guardrail record from the checker's
// concurrent per-fragment goroutines to the chunk decorator on the
// call-events dispatcher goroutine. Extends protocolRecordBox's single-slot
// pattern with the §5.7 selection rule: many checks complete concurrently per
// turn, but exactly one record reaches the chunk.
//
//  1. If any check violated, that check's record wins — it is the one that
//     fired the interrupt, and the guardrail it matched is the one that was
//     broken.
//  2. Otherwise, the record with the highest selected-check similarity wins.
//
// No coordination and no waiting on the offer side: take() always returns the
// best of whatever has completed so far.
type guardrailRecordBox struct {
	mu      sync.Mutex
	pending *guardrailCheckRecord
	locked  bool // a violating record has been stored; later completions cannot overwrite

	// auditPending holds the record back until the >0.85 band's audit judge
	// answers, or auditDeadline passes. See take() for why that matters.
	auditPending  bool
	auditDeadline time.Time
}

// offer keeps the better of the pending and the given record: ignored once the
// box is locked by a violation; otherwise the MORE COMPLETE record wins, with
// similarity only as a tie-break.
//
// Completeness has to come first. Every offer from a turn carries that turn's
// whole accumulated check list, so a later offer is a strict superset of an
// earlier one, while selectedSimilarity is the max across the list and is
// therefore merely non-decreasing — equal whenever the newly finished check
// was not the best one.
//
// Comparing on similarity alone with a strict > silently dropped those
// records. Observed on staging call 287f66ae: turn two ran two checks,
// 0.6176 then 0.6172. The second offer held BOTH checks but still scored
// 0.6176, so `0.6176 > 0.6176` was false and the one-check record survived —
// the chunk persisted check_count 1 while its turn_text covered the sentence
// whose check had been discarded.
func (b *guardrailRecordBox) offer(record guardrailCheckRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.locked {
		// A violation already claimed this turn and must keep its selection,
		// but still adopt the longer check list. Offers carry the whole
		// accumulated list in completion order, so a later one is a
		// prefix-superset and SelectedIndex keeps pointing at the same check.
		//
		// Without this, checks finishing after the violation vanished from the
		// record: on staging call 3a7d60a2 turn one ran four checks and
		// persisted three, losing a judge-band sample at 0.5702. The S3 record
		// is the calibration dataset, so dropping its tail biases exactly the
		// data the thresholds get re-derived from -- and biases it toward
		// whichever check happened to win the race.
		if b.pending != nil && len(record.Checks) > len(b.pending.Checks) {
			b.pending.Checks = record.Checks
			b.pending.CheckCount = len(record.Checks)
		}
		return
	}
	if b.pending == nil ||
		len(record.Checks) > len(b.pending.Checks) ||
		(len(record.Checks) == len(b.pending.Checks) &&
			record.selectedSimilarity() > b.pending.selectedSimilarity()) {
		b.pending = &record
	}
}

// offerViolation stores the violating record and locks the box so later
// completions — including a losing race to offerViolation itself, or the
// eventual audit judge's own record if it were mistakenly re-offered — cannot
// overwrite the record that fired the interrupt.
// awaitAudit must be true when the violation came from the >0.85 band, whose
// audit judge is still running: the record is then held back until that
// verdict lands (see take).
func (b *guardrailRecordBox) offerViolation(record guardrailCheckRecord, awaitAudit bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.locked {
		return
	}
	b.pending = &record
	b.locked = true
	b.auditPending = awaitAudit
	if awaitAudit {
		b.auditDeadline = time.Now().Add(guardrailAuditVerdictWait)
	}
}

// setAuditVerdict fills the >0.85 band's fire-and-forget audit judge verdict
// onto the pending record's selected check. Returns false when there is no
// pending record — it was already taken — so the caller can log that the
// verdict arrived too late instead of silently dropping it.
func (b *guardrailRecordBox) setAuditVerdict(verdict string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pending == nil {
		return false
	}
	idx := b.pending.SelectedIndex
	if idx < 0 || idx >= len(b.pending.Checks) {
		return false
	}
	check := &b.pending.Checks[idx]
	check.Judge.Ran = true
	check.Judge.AuditOnly = true
	check.Judge.Verdict = verdict
	// Fully resolved now, so the record may be released.
	b.auditPending = false
	return true
}

// take removes and returns the pending record, so one turn maps to exactly
// one chunk. Resets the lock so the next turn starts unlocked.
func (b *guardrailRecordBox) take() *guardrailCheckRecord {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Hold the record while the >0.85 band's audit verdict is outstanding.
	//
	// That band interrupts on similarity alone, and its audit judge is the ONLY
	// false-positive detector for it, answering a few hundred ms later. The
	// interrupt commits whatever was already spoken as a chunk almost
	// immediately, so releasing the record now would hand it to that partial
	// chunk and the verdict would then arrive to an empty box and be dropped --
	// the detector would produce nothing, ever.
	//
	// Returning nil lets the partial chunk through without metrics; the
	// regenerated chunk, committed seconds later, collects the resolved record
	// instead. Deliberately does NOT block: take() runs on the call-events
	// dispatcher during the Redis chunk write, and stalling that to wait on a
	// judge would be far worse than losing telemetry.
	//
	// Judge-band violations already know their verdict, are not held, and their
	// record lands on the partial chunk -- which IS the violating turn, and the
	// honest place for it.
	if b.pending != nil && b.auditPending && time.Now().Before(b.auditDeadline) {
		return nil
	}

	record := b.pending
	b.pending = nil
	b.locked = false
	b.auditPending = false
	return record
}

// ------------------------------------------------------------ the S3 payload

// guardrailCheckRecordPayload is deliberately self-describing, mirroring
// protocolRetrievalRecordPayload. It carries EVERY check for the turn, not
// just the one selected for the chunk: the chunk/DB row is the summary, this
// blob is the calibration dataset that will validate or move the bands in §9.
func guardrailCheckRecordPayload(
	chunkID, userID, conversationID, botType string,
	record guardrailCheckRecord,
) map[string]any {
	checks := make([]map[string]any, 0, len(record.Checks))
	for _, check := range record.Checks {
		candidates := make([]map[string]any, 0, len(check.Candidates))
		for _, candidate := range check.Candidates {
			candidates = append(candidates, map[string]any{
				"instruction_id": candidate.InstructionID,
				"anchor_id":      candidate.AnchorID,
				"similarity":     candidate.Similarity,
			})
		}

		entry := map[string]any{
			"index":    check.Index,
			"fragment": check.Fragment,
			"latency_ms": map[string]any{
				"vector_query": check.VectorQueryLatencyMs,
				"judge":        check.JudgeLatencyMs,
				"total":        check.TotalLatencyMs,
			},
			"band":       check.Band,
			"cancelled":  check.Cancelled,
			"candidates": candidates,
			"judge": map[string]any{
				"ran":        check.Judge.Ran,
				"audit_only": check.Judge.AuditOnly,
				"verdict":    check.Judge.Verdict,
				"model":      check.Judge.Model,
				"latency_ms": check.Judge.LatencyMs,
				"error":      check.Judge.Error,
			},
			"violated": check.Violated,
			"status":   check.Status,
		}
		if check.Top != nil {
			entry["top"] = map[string]any{
				"instruction_id":        check.Top.InstructionID,
				"anchor_id":             check.Top.AnchorID,
				"anchor_text":           check.Top.AnchorText,
				"title":                 check.Top.Title,
				"document_version_path": check.Top.DocumentPath,
				"similarity":            check.Top.Similarity,
			}
		}
		if check.Err != "" {
			entry["error"] = check.Err
		}
		checks = append(checks, entry)
	}

	payload := map[string]any{
		"chunk_id":        chunkID,
		"conversation_id": conversationID,
		"user_id":         userID,
		"bot_type":        botType,
		"checked_at":      time.Now().UTC().Format(time.RFC3339Nano),
		"turn_text":       record.TurnText,
		"thresholds": map[string]any{
			"metric":    "cosine_similarity",
			"interrupt": guardrailInterruptThreshold,
			"judge":     guardrailJudgeThreshold,
		},
		"check_count":    record.CheckCount,
		"selected_index": record.SelectedIndex,
		"interrupted":    record.Interrupted,
		"checks":         checks,
		"status":         record.Status,
	}
	if record.Err != "" {
		payload["error"] = record.Err
	}
	return payload
}
