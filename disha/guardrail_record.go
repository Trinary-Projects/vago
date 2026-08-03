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
	//   > 0.90            -> interrupt immediately; also run the judge
	//                        audit-only, off the critical path.
	//   0.75 <= x <= 0.90 -> call the judge and wait on it; violated ->
	//                        interrupt.
	//   < 0.75            -> nothing.
	//
	// The comparison operators are load-bearing: > interrupts, >= judges, so
	// exactly 0.90 falls in the judge band. Go and
	// scripts/seed_guardrail_collections.py must agree exactly.
	//
	// Uncalibrated and knowingly assumed (design note §9): guardrails compare
	// Disha utterance to Disha utterance — same speaker, same register, same
	// domain — so the high similarity floor observed on protocol retrieval is
	// likely higher still here. The S3 record carries every candidate from day
	// one so the bands can be recalibrated without a rerun.
	guardrailInterruptThreshold = 0.90
	guardrailJudgeThreshold     = 0.75

	// guardrailQueryLimit is how many anchors to fetch before dedupe-by-
	// instruction-id and top-1 selection.
	guardrailQueryLimit = 10

	// guardrailFanoutSentryThreshold: reusing endsWithPunctuation for fragment
	// boundaries produces clause-level (not sentence-level) fan-out, so a
	// single turn can trigger many checks. Above this many checks in one turn,
	// capture one Sentry event and keep going rather than one per check.
	guardrailFanoutSentryThreshold = 10

	// guardrailCheckTimeout bounds one fragment's whole check (vector query +
	// judge). Generous because this step is not on the critical path.
	guardrailCheckTimeout = 3 * time.Second

	// guardrailAuditVerdictWait bounds how long the chunk decorator's box wait
	// for the >0.90 band's fire-and-forget audit judge before the record is
	// taken with judge_verdict empty. See guardrailRecordBox.setAuditVerdict.
	guardrailAuditVerdictWait = 3 * time.Second

	// guardrailJudgePromptName is the DocumentStore prompt for the judge LLM.
	//
	// TODO (launch blocker, design note §5.6): the real Langfuse prompt name
	// is not yet decided and this prompt does not exist yet. Until
	// document:{name}:{env} is pre-rendered into Redis, every judge call fails
	// and the 0.75-0.90 band fails open to "not violated" — only the >0.90
	// band works. Replace this constant and confirm the variable names
	// (instructionText, fragment) when the prompt lands.
	guardrailJudgePromptName  = "follow_up_call/guardrail_judge"
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
// the 0.75-0.90 band's blocking-within-the-check-goroutine call, or the >0.90
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
}

// offer keeps the better of the pending and the given record: ignored once the
// box is locked by a violation; otherwise the record with the higher
// selected-check similarity wins.
func (b *guardrailRecordBox) offer(record guardrailCheckRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.locked {
		return
	}
	if b.pending == nil || record.selectedSimilarity() > b.pending.selectedSimilarity() {
		b.pending = &record
	}
}

// offerViolation stores the violating record and locks the box so later
// completions — including a losing race to offerViolation itself, or the
// eventual audit judge's own record if it were mistakenly re-offered — cannot
// overwrite the record that fired the interrupt.
func (b *guardrailRecordBox) offerViolation(record guardrailCheckRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.locked {
		return
	}
	b.pending = &record
	b.locked = true
}

// setAuditVerdict fills the >0.90 band's fire-and-forget audit judge verdict
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
	return true
}

// take removes and returns the pending record, so one turn maps to exactly
// one chunk. Resets the lock so the next turn starts unlocked.
func (b *guardrailRecordBox) take() *guardrailCheckRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	record := b.pending
	b.pending = nil
	b.locked = false
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
