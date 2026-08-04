package disha

import (
	"testing"
	"time"
)

// testGuardrailCheck builds one check with a top hit at the given similarity.
// Violated/band are set independently so tests can construct the exact
// scenario they need.
func testGuardrailCheck(index int, similarity float64, violated bool) guardrailCheck {
	band := "below"
	switch {
	case similarity > guardrailInterruptThreshold:
		band = "interrupt"
	case similarity >= guardrailJudgeThreshold:
		band = "judge"
	}
	return guardrailCheck{
		Index:    index,
		Fragment: "fragment text",
		Top: &guardrailTopHit{
			InstructionID: "instr-G",
			AnchorID:      "anchor-G",
			AnchorText:    "anchor text",
			Title:         "title",
			DocumentPath:  "p/v/1",
			Similarity:    similarity,
		},
		Candidates: []guardrailCandidateHit{
			{InstructionID: "instr-G", AnchorID: "anchor-G", Similarity: similarity},
		},
		Band:                 band,
		VectorQueryLatencyMs: 15,
		TotalLatencyMs:       40,
		Violated:             violated,
		Status:               "ok",
	}
}

func testGuardrailCheckRecord(similarity float64, violated bool) guardrailCheckRecord {
	check := testGuardrailCheck(0, similarity, violated)
	return guardrailCheckRecord{
		TurnText:      "the entire disha turn",
		Checks:        []guardrailCheck{check},
		SelectedIndex: 0,
		Interrupted:   violated,
		CheckCount:    1,
		Status:        "ok",
	}
}

func TestGuardrailRecordBoxOfferKeepsHighestSimilarity(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offer(testGuardrailCheckRecord(0.60, false))
	box.offer(testGuardrailCheckRecord(0.83, false))
	box.offer(testGuardrailCheckRecord(0.71, false))

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	if got := record.Checks[record.SelectedIndex].Top.Similarity; got != 0.83 {
		t.Fatalf("selected similarity = %v, want 0.83 (the highest offered)", got)
	}
}

func TestGuardrailRecordBoxViolationBeatsHigherSimilarity(t *testing.T) {
	box := &guardrailRecordBox{}
	// A non-violating check with higher similarity arrives first...
	box.offer(testGuardrailCheckRecord(0.95, false))
	// ...but a violating check, even at lower similarity, must win.
	box.offerViolation(testGuardrailCheckRecord(0.78, true), false)

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	if !record.Interrupted {
		t.Fatalf("expected the violating record to win, got Interrupted=%v similarity=%v",
			record.Interrupted, record.Checks[record.SelectedIndex].Top.Similarity)
	}
	if got := record.Checks[record.SelectedIndex].Top.Similarity; got != 0.78 {
		t.Fatalf("similarity = %v, want the violating check's 0.78", got)
	}
}

func TestGuardrailRecordBoxAbsentViolationHighestSimilarityWins(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offer(testGuardrailCheckRecord(0.71, false))
	box.offer(testGuardrailCheckRecord(0.66, false))
	box.offer(testGuardrailCheckRecord(0.80, false))

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	if got := record.Checks[record.SelectedIndex].Top.Similarity; got != 0.80 {
		t.Fatalf("similarity = %v, want the highest offered 0.80", got)
	}
}

func TestGuardrailRecordBoxOfferCannotOverwriteAfterViolation(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offerViolation(testGuardrailCheckRecord(0.78, true), false)
	// A later-completing check with much higher similarity must not
	// overwrite the locked violation.
	box.offer(testGuardrailCheckRecord(0.99, false))
	// Nor can a second violation.
	box.offerViolation(testGuardrailCheckRecord(0.91, true), false)

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	if got := record.Checks[record.SelectedIndex].Top.Similarity; got != 0.78 {
		t.Fatalf("similarity = %v, want the original violation's 0.78 (box must stay locked)", got)
	}
}

func TestGuardrailRecordBoxSetAuditVerdictBeforeTake(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offerViolation(testGuardrailCheckRecord(0.94, true), false)

	if ok := box.setAuditVerdict("no"); !ok {
		t.Fatal("setAuditVerdict should succeed on a pending record")
	}

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	got := record.Checks[record.SelectedIndex].Judge
	if !got.Ran || !got.AuditOnly || got.Verdict != "no" {
		t.Fatalf("judge detail = %+v, want Ran=true AuditOnly=true Verdict=no", got)
	}
}

func TestGuardrailRecordBoxSetAuditVerdictAfterTakeReturnsFalse(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offerViolation(testGuardrailCheckRecord(0.94, true), false)
	box.take()

	if ok := box.setAuditVerdict("yes"); ok {
		t.Fatal("setAuditVerdict must return false once the record has been taken")
	}
}

func TestGuardrailRecordBoxSetAuditVerdictNoPendingRecord(t *testing.T) {
	box := &guardrailRecordBox{}
	if ok := box.setAuditVerdict("yes"); ok {
		t.Fatal("setAuditVerdict must return false with nothing ever offered")
	}
}

func TestGuardrailRecordBoxTakeEmptiesBox(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offer(testGuardrailCheckRecord(0.80, false))

	if record := box.take(); record == nil {
		t.Fatal("expected a record on the first take")
	}
	if record := box.take(); record != nil {
		t.Fatalf("second take should be empty, got %+v", record)
	}
}

func TestGuardrailRecordBoxTakeResetsLockForNextTurn(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offerViolation(testGuardrailCheckRecord(0.94, true), false)
	box.take()

	// A fresh turn: a non-violating offer must be accepted, proving the lock
	// did not leak across turns.
	box.offer(testGuardrailCheckRecord(0.80, false))
	record := box.take()
	if record == nil {
		t.Fatal("expected the next turn's record; lock leaked across turns")
	}
	if record.Interrupted {
		t.Fatal("next turn's record should not carry the previous turn's violation")
	}
}

// The S3 payload is the calibration dataset: every check the turn ran must
// appear, not just the one selected for the chunk.
func TestGuardrailCheckRecordPayloadIncludesEveryCheck(t *testing.T) {
	record := guardrailCheckRecord{
		TurnText: "the entire disha turn",
		Checks: []guardrailCheck{
			testGuardrailCheck(0, 0.40, false),
			testGuardrailCheck(1, 0.78, false),
			testGuardrailCheck(2, 0.94, true),
		},
		SelectedIndex: 2,
		Interrupted:   true,
		CheckCount:    3,
		Status:        "ok",
	}

	payload := guardrailCheckRecordPayload("chunk-1", "user-1", "conv-1", FollowUpBotType, record)

	checks, ok := payload["checks"].([]map[string]any)
	if !ok {
		t.Fatalf("checks type = %T", payload["checks"])
	}
	if len(checks) != 3 {
		t.Fatalf("checks = %d, want 3 (every check, not just the selected one)", len(checks))
	}
	if payload["selected_index"] != 2 {
		t.Errorf("selected_index = %v, want 2", payload["selected_index"])
	}
	if payload["check_count"] != 3 {
		t.Errorf("check_count = %v, want 3", payload["check_count"])
	}
	if payload["turn_text"] != "the entire disha turn" {
		t.Errorf("turn_text = %v", payload["turn_text"])
	}
	thresholds, ok := payload["thresholds"].(map[string]any)
	if !ok {
		t.Fatalf("thresholds type = %T", payload["thresholds"])
	}
	if thresholds["interrupt"] != guardrailInterruptThreshold || thresholds["judge"] != guardrailJudgeThreshold {
		t.Errorf("thresholds = %+v", thresholds)
	}

	// Spot-check one check's shape.
	first := checks[0]
	if first["index"] != 0 || first["fragment"] != "fragment text" {
		t.Errorf("first check = %+v", first)
	}
	top, ok := first["top"].(map[string]any)
	if !ok {
		t.Fatalf("top type = %T", first["top"])
	}
	if top["similarity"] != 0.40 {
		t.Errorf("top similarity = %v, want 0.40", top["similarity"])
	}
}

// Regression for staging call 287f66ae. Each offer from a turn carries the
// whole accumulated check list, so a later offer is a strict superset; but
// selectedSimilarity is the max across that list and so only non-decreasing.
// Comparing on similarity alone with a strict > therefore discarded the more
// complete record whenever the newest check was not the best one — the live
// call persisted check_count 1 for a turn that had run two checks.
func TestGuardrailRecordBoxPrefersTheMoreCompleteRecord(t *testing.T) {
	box := &guardrailRecordBox{}

	first := guardrailCheckRecord{
		Checks:        []guardrailCheck{{Index: 1, Top: &guardrailTopHit{Similarity: 0.6176}}},
		SelectedIndex: 0,
		CheckCount:    1,
	}
	// The second check scored LOWER, so the cumulative record's selected
	// similarity is unchanged — but it now holds both checks.
	second := guardrailCheckRecord{
		Checks: []guardrailCheck{
			{Index: 1, Top: &guardrailTopHit{Similarity: 0.6176}},
			{Index: 2, Top: &guardrailTopHit{Similarity: 0.6172}},
		},
		SelectedIndex: 0,
		CheckCount:    2,
	}

	box.offer(first)
	box.offer(second)

	got := box.take()
	if got == nil {
		t.Fatal("expected a pending record")
	}
	if len(got.Checks) != 2 {
		t.Fatalf("kept %d checks, want 2 — the more complete record must win even when "+
			"its selected similarity ties", len(got.Checks))
	}
	if got.SelectedIndex != 0 {
		t.Fatalf("SelectedIndex = %d, want 0 (the higher-scoring check)", got.SelectedIndex)
	}
}

// A violation still beats a later, more complete non-violating record: the box
// locks, so completeness must not override the record that actually fired.
func TestGuardrailRecordBoxViolationBeatsMoreCompleteRecord(t *testing.T) {
	box := &guardrailRecordBox{}

	box.offerViolation(guardrailCheckRecord{
		Checks:        []guardrailCheck{{Index: 1, Violated: true, Top: &guardrailTopHit{Similarity: 0.91}}},
		SelectedIndex: 0,
		CheckCount:    1,
		Interrupted:   true,
	}, false)
	box.offer(guardrailCheckRecord{
		Checks: []guardrailCheck{
			{Index: 1, Top: &guardrailTopHit{Similarity: 0.91}},
			{Index: 2, Top: &guardrailTopHit{Similarity: 0.40}},
		},
		SelectedIndex: 0,
		CheckCount:    2,
	})

	got := box.take()
	if got == nil || !got.Interrupted {
		t.Fatalf("expected the violating record to survive, got %+v", got)
	}
	// Selection is what the lock protects, not the check list. A later offer
	// must not steal Interrupted/SelectedIndex...
	if got.SelectedIndex != 0 {
		t.Fatalf("SelectedIndex = %d, want 0 — the lock must preserve the violating selection", got.SelectedIndex)
	}
	// ...but its checks ARE adopted, so the calibration dataset keeps every
	// check that ran rather than only those that beat the violation.
	if len(got.Checks) != 2 {
		t.Fatalf("kept %d checks, want 2 — a locked box must still adopt later checks", len(got.Checks))
	}
	if got.CheckCount != 2 {
		t.Fatalf("CheckCount = %d, want 2", got.CheckCount)
	}
}

// The >0.85 band interrupts on similarity alone and its audit judge is the only
// false-positive detector for it. The interrupt commits whatever was already
// spoken as a chunk almost immediately, so if the box released the record then,
// the verdict would arrive to an empty box and be dropped — the detector would
// produce nothing, ever. Verified on staging call 3a7d60a2, where judge-band
// records did land on the partial chunk.
func TestGuardrailRecordBoxHoldsRecordUntilAuditVerdict(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offerViolation(guardrailCheckRecord{
		Checks:        []guardrailCheck{{Index: 1, Violated: true, Top: &guardrailTopHit{Similarity: 0.91}}},
		SelectedIndex: 0,
		CheckCount:    1,
		Interrupted:   true,
	}, true) // awaitAudit: the >0.85 band

	// The partial chunk, committed microseconds after the interrupt, must get
	// nothing rather than consuming a record whose verdict has not landed.
	if got := box.take(); got != nil {
		t.Fatalf("take() returned a record while the audit verdict was outstanding: %+v", got)
	}

	if ok := box.setAuditVerdict("no"); !ok {
		t.Fatal("setAuditVerdict returned false — the record must still be held, not taken")
	}

	// The regenerated chunk, committed seconds later, collects the resolved record.
	got := box.take()
	if got == nil {
		t.Fatal("take() returned nil after the audit verdict landed")
	}
	if v := got.Checks[got.SelectedIndex].Judge.Verdict; v != "no" {
		t.Fatalf("audit verdict = %q, want %q — this is the false-positive signal", v, "no")
	}
	if !got.Checks[got.SelectedIndex].Judge.AuditOnly {
		t.Fatal("expected AuditOnly to be marked on the audited check")
	}
}

// The hold must be bounded: if the audit judge never answers, the record is
// released with an empty verdict rather than being stranded forever.
func TestGuardrailRecordBoxReleasesRecordAfterAuditDeadline(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offerViolation(guardrailCheckRecord{
		Checks:        []guardrailCheck{{Index: 1, Violated: true, Top: &guardrailTopHit{Similarity: 0.91}}},
		SelectedIndex: 0,
		CheckCount:    1,
		Interrupted:   true,
	}, true)

	if got := box.take(); got != nil {
		t.Fatalf("expected the record to be held before the deadline, got %+v", got)
	}

	// Expire the hold rather than sleeping for guardrailAuditVerdictWait.
	box.mu.Lock()
	box.auditDeadline = time.Now().Add(-time.Millisecond)
	box.mu.Unlock()

	got := box.take()
	if got == nil {
		t.Fatal("record was still held after the audit deadline passed")
	}
	if v := got.Checks[got.SelectedIndex].Judge.Verdict; v != "" {
		t.Fatalf("verdict = %q, want empty — no audit answer arrived", v)
	}
}

// A judge-band violation already knows its verdict, so it must NOT be held —
// its record belongs on the partial chunk, which is the violating turn.
func TestGuardrailRecordBoxDoesNotHoldJudgeBandViolation(t *testing.T) {
	box := &guardrailRecordBox{}
	box.offerViolation(guardrailCheckRecord{
		Checks: []guardrailCheck{{
			Index: 1, Violated: true,
			Top:   &guardrailTopHit{Similarity: 0.60},
			Judge: guardrailJudgeDetail{Ran: true, Verdict: "yes"},
		}},
		SelectedIndex: 0,
		CheckCount:    1,
		Interrupted:   true,
	}, false) // awaitAudit false: verdict already known

	got := box.take()
	if got == nil {
		t.Fatal("judge-band violation must be available immediately, not held")
	}
	if v := got.Checks[0].Judge.Verdict; v != "yes" {
		t.Fatalf("verdict = %q, want yes", v)
	}
}
