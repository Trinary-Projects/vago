package disha

import (
	"encoding/json"
	"io"
	"log"
	"reflect"
	"sync"
	"testing"
)

func discardLogger() *log.Logger { return log.New(io.Discard, "", 0) }

func TestNewConversationStateInitial(t *testing.T) {
	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")

	if state.Variant() != "student_test" {
		t.Fatalf("variant = %q", state.Variant())
	}
	if state.CurrentStage().Name != "introduction" {
		t.Fatalf("current stage = %q, want introduction", state.CurrentStage().Name)
	}
	if state.SelectedCarePlan() != nil {
		t.Fatal("selected care plan should start nil")
	}
	if state.StageTurnCount() != 0 {
		t.Fatalf("stage_turn_count = %d", state.StageTurnCount())
	}
}

func TestConversationStatePersistDictShape(t *testing.T) {
	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	state.MergeVariables(map[string]any{"diet_intensity_level": "high"})
	state.IncrementStageTurnCount()
	state.SetSelectedCarePlan(cfg.FindCarePlan("general"))

	got := state.ToPersistDict()
	want := map[string]any{
		"variant":                  "student_test",
		"agenda":                   "introduction",
		"stage_turn_count":         1,
		"variable_store":           map[string]any{"diet_intensity_level": "high"},
		"care_plan_name":           "general",
		"stage_threshold_reminded": false,
		"stage_threshold_alerted":  false,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persist dict = %#v, want %#v", got, want)
	}

	// No care plan selected → explicit null, matching Python.
	fresh := NewConversationState(cfg, "student_test").ToPersistDict()
	if v, ok := fresh["care_plan_name"]; !ok || v != nil {
		t.Fatalf("care_plan_name = %#v, want present nil", v)
	}

	// The dict must be JSON-serializable as-is (it becomes the S3 state
	// blob after the persistence hook appends user/conversation ids).
	if _, err := json.Marshal(got); err != nil {
		t.Fatalf("marshal persist dict: %v", err)
	}
}

func TestConversationStateAdvanceStageResets(t *testing.T) {
	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	state.IncrementStageTurnCount()
	state.IncrementStageTurnCount()

	next := cfg.ResolveStage("problem_discovery_and_exploration", nil)
	state.AdvanceStage(next)

	if state.CurrentStage().Name != "problem_discovery_and_exploration" {
		t.Fatalf("current stage = %q", state.CurrentStage().Name)
	}
	if state.StageTurnCount() != 0 {
		t.Fatalf("stage_turn_count = %d, want reset to 0", state.StageTurnCount())
	}
}

func TestConversationStateMergeVariables(t *testing.T) {
	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")

	if state.MergeVariables(nil) {
		t.Fatal("merging nil should report unchanged")
	}
	if !state.MergeVariables(map[string]any{"a": "1"}) {
		t.Fatal("new key should report changed")
	}
	if state.MergeVariables(map[string]any{"a": "1"}) {
		t.Fatal("same value should report unchanged")
	}
	if !state.MergeVariables(map[string]any{"a": "2"}) {
		t.Fatal("changed value should report changed")
	}
	// Python .get() semantics: nil into a missing key is not a change.
	if state.MergeVariables(map[string]any{"b": nil}) {
		t.Fatal("nil into missing key should report unchanged")
	}
	if got := state.GetConversationVariables(); got["a"] != "2" {
		t.Fatalf("variables = %#v", got)
	}

	// Snapshot is a copy — mutating it must not affect the store.
	snap := state.VariableStoreSnapshot()
	snap["a"] = "mutated"
	if got := state.GetConversationVariables(); got["a"] != "2" {
		t.Fatalf("snapshot mutation leaked into store: %#v", got)
	}
}

func TestConversationStateIntensityLevels(t *testing.T) {
	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")

	if got := state.GetIntensityLevels(); len(got) != 0 {
		t.Fatalf("empty store intensity = %#v", got)
	}

	state.MergeVariables(map[string]any{
		"diet_intensity_level":    "medium",
		"fitness_intensity_level": "high",
	})
	got := state.GetIntensityLevels()
	if got["diet_plan_intensity_level"] != "medium" || got["fitness_plan_intensity_level"] != "high" {
		t.Fatalf("intensity = %#v", got)
	}

	// Empty string and nil are skipped like Python's `not in (None, "")`.
	state.MergeVariables(map[string]any{"diet_intensity_level": "", "fitness_intensity_level": nil})
	if got := state.GetIntensityLevels(); len(got) != 0 {
		t.Fatalf("intensity after clearing = %#v", got)
	}
}

// TestConversationStateIntensityLevelsNonStringSkipped verifies
// GetIntensityLevels only recognizes string values (onboarding assumes the
// variable store holds strings or nil for these keys): a non-string value
// (a JSON-decoded number or bool) is skipped exactly like a missing key,
// with no numeric/bool coercion.
func TestConversationStateIntensityLevelsNonStringSkipped(t *testing.T) {
	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")

	state.MergeVariables(map[string]any{
		"diet_intensity_level":    float64(2),
		"fitness_intensity_level": "advanced",
	})
	got := state.GetIntensityLevels()
	if _, ok := got["diet_plan_intensity_level"]; ok {
		t.Fatalf("diet_plan_intensity_level present for non-string value: %#v", got)
	}
	if got["fitness_plan_intensity_level"] != "advanced" {
		t.Fatalf("fitness_plan_intensity_level = %q, want %q", got["fitness_plan_intensity_level"], "advanced")
	}

	// A bool value is skipped the same way.
	state2 := NewConversationState(cfg, "student_test")
	state2.MergeVariables(map[string]any{"diet_intensity_level": true})
	got2 := state2.GetIntensityLevels()
	if _, ok := got2["diet_plan_intensity_level"]; ok {
		t.Fatalf("diet_plan_intensity_level present for bool value: %#v", got2)
	}

	// Missing key stays absent (not just skipped-when-empty).
	state3 := NewConversationState(cfg, "student_test")
	state3.MergeVariables(map[string]any{"diet_intensity_level": "2"})
	got3 := state3.GetIntensityLevels()
	if _, ok := got3["fitness_plan_intensity_level"]; ok {
		t.Fatalf("fitness_plan_intensity_level present without a value: %#v", got3)
	}
}

func TestConversationStateFromResume(t *testing.T) {
	cfg := parseStudentTestConfig(t)

	resume := map[string]any{
		"variant":          "student_test",
		"agenda":           "root_cause_diagnosis",
		"stage_turn_count": float64(4),
		"variable_store":   map[string]any{"diet_intensity_level": "low"},
		"care_plan_name":   "general",
	}
	state, err := ConversationStateFromResume(cfg, "student_test", resume, discardLogger())
	if err != nil {
		t.Fatalf("from_resume: %v", err)
	}
	if state.CurrentStage().Name != "root_cause_diagnosis" {
		t.Fatalf("stage = %q", state.CurrentStage().Name)
	}
	if plan := state.SelectedCarePlan(); plan == nil || plan.Name != "general" {
		t.Fatalf("care plan = %+v", plan)
	}
	if state.StageTurnCount() != 4 {
		t.Fatalf("stage_turn_count = %d", state.StageTurnCount())
	}
	if got := state.GetIntensityLevels()["diet_plan_intensity_level"]; got != "low" {
		t.Fatalf("variable store not restored: %q", got)
	}
}

func TestConversationStateFromResumeDiscoversCarePlan(t *testing.T) {
	cfg := parseStudentTestConfig(t)

	// No care_plan_name persisted, but the agenda only exists inside
	// porn_addiction → the plan is discovered from the stage.
	state, err := ConversationStateFromResume(cfg, "student_test", map[string]any{
		"agenda": "understanding_patterns",
	}, discardLogger())
	if err != nil {
		t.Fatalf("from_resume: %v", err)
	}
	if plan := state.SelectedCarePlan(); plan == nil || plan.Name != "porn_addiction" {
		t.Fatalf("discovered plan = %+v", plan)
	}
}

func TestConversationStateFromResumeFallbacks(t *testing.T) {
	cfg := parseStudentTestConfig(t)

	// Unknown care plan logs and continues; unknown agenda falls back to
	// the start stage.
	state, err := ConversationStateFromResume(cfg, "student_test", map[string]any{
		"agenda":         "renamed_stage",
		"care_plan_name": "no_such_plan",
	}, discardLogger())
	if err != nil {
		t.Fatalf("from_resume: %v", err)
	}
	if state.CurrentStage().Name != cfg.StartStage.Name {
		t.Fatalf("stage = %q, want start stage fallback", state.CurrentStage().Name)
	}
	if state.SelectedCarePlan() != nil {
		t.Fatalf("care plan = %+v, want nil", state.SelectedCarePlan())
	}

	// Missing agenda is a malformed state (Python raises KeyError).
	if _, err := ConversationStateFromResume(cfg, "student_test", map[string]any{}, discardLogger()); err == nil {
		t.Fatal("missing agenda: want error")
	}
}

func TestConversationStateConcurrentAccess(t *testing.T) {
	cfg := parseStudentTestConfig(t)
	state := NewConversationState(cfg, "student_test")
	next := cfg.ResolveStage("problem_discovery_and_exploration", nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				switch i % 4 {
				case 0:
					state.MergeVariables(map[string]any{"k": j})
				case 1:
					state.AdvanceStage(next)
					state.IncrementStageTurnCount()
				case 2:
					_ = state.ToPersistDict()
				case 3:
					_ = state.GetIntensityLevels()
					_ = state.CurrentStage()
				}
			}
		}(i)
	}
	wg.Wait()
}
