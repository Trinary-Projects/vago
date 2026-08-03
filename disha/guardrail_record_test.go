package disha

import "testing"

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
	box.offerViolation(testGuardrailCheckRecord(0.78, true))

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
	box.offerViolation(testGuardrailCheckRecord(0.78, true))
	// A later-completing check with much higher similarity must not
	// overwrite the locked violation.
	box.offer(testGuardrailCheckRecord(0.99, false))
	// Nor can a second violation.
	box.offerViolation(testGuardrailCheckRecord(0.91, true))

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
	box.offerViolation(testGuardrailCheckRecord(0.94, true))

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
	box.offerViolation(testGuardrailCheckRecord(0.94, true))
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
	box.offerViolation(testGuardrailCheckRecord(0.94, true))
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
