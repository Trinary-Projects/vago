package disha

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// loadStudentTestConfigJSON returns the captured staging
// OB_Call_Configs/student_test_config document (testdata fixture).
func loadStudentTestConfigJSON(t *testing.T) (DocumentVersion, map[string]any) {
	t.Helper()
	raw, err := os.ReadFile("testdata/onboarding/student_test_config.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var doc DocumentVersion
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return doc, doc.ConfigJSON
}

func parseStudentTestConfig(t *testing.T) *OnboardingConfig {
	t.Helper()
	_, configJSON := loadStudentTestConfigJSON(t)
	cfg, err := ParseOnboardingConfig(configJSON)
	if err != nil {
		t.Fatalf("ParseOnboardingConfig: %v", err)
	}
	return cfg
}

func TestParseOnboardingConfigStudentTestFixture(t *testing.T) {
	cfg := parseStudentTestConfig(t)

	if cfg.Model != "grok-4.1-fast" {
		t.Fatalf("model = %q, want grok-4.1-fast", cfg.Model)
	}
	if cfg.MainSystemPrompt.Name != "onboarding_callV3_4/00_main_call_agent_sys" || cfg.MainSystemPrompt.Version != 1 {
		t.Fatalf("main_system_prompt = %+v", cfg.MainSystemPrompt)
	}
	if cfg.CareplanSwitcherStageName != "problem_rca_discussion" {
		t.Fatalf("careplan_switcher_stage_name = %q", cfg.CareplanSwitcherStageName)
	}
	if cfg.StartStage.Name != "introduction" {
		t.Fatalf("start_stage = %q, want introduction", cfg.StartStage.Name)
	}
	if len(cfg.StartStage.NextStages) != 1 || cfg.StartStage.NextStages[0] != "problem_discovery_and_exploration" {
		t.Fatalf("start_stage next_stages = %v", cfg.StartStage.NextStages)
	}
	if len(cfg.CommonStages) != 1 || cfg.CommonStages[0].Name != "problem_discovery_and_exploration" {
		t.Fatalf("common_stages = %+v", cfg.CommonStages)
	}
	if len(cfg.CarePlans) != 4 {
		t.Fatalf("care_plans len = %d, want 4", len(cfg.CarePlans))
	}
	wantPlans := []string{"general", "hair_loss", "porn_addiction", "ed_pe"}
	for i, want := range wantPlans {
		if cfg.CarePlans[i].Name != want {
			t.Fatalf("care_plans[%d] = %q, want %q", i, cfg.CarePlans[i].Name, want)
		}
	}
	if cfg.RecordingEnabledPercentage != nil {
		t.Fatalf("recording_enabled_percentage = %v, want nil", *cfg.RecordingEnabledPercentage)
	}

	// Spot-check nested stage parsing against the live config: general's
	// root_cause_diagnosis has a blocking deep-thinking prompt and a
	// turn threshold of 15.
	general := cfg.FindCarePlan("general")
	if general == nil {
		t.Fatal("general care plan not found")
	}
	rca := cfg.ResolveStage("root_cause_diagnosis", general)
	if rca == nil {
		t.Fatal("root_cause_diagnosis not resolved")
	}
	if rca.TurnThreshold == nil || *rca.TurnThreshold != 15 {
		t.Fatalf("turn_threshold = %v, want 15", rca.TurnThreshold)
	}
	if len(rca.DeepThinking) != 1 || !rca.DeepThinking[0].Blocking ||
		rca.DeepThinking[0].Prompt.Name != "onboarding_callV3_4/general/22_root_cause_insight_generator" {
		t.Fatalf("deep_thinking = %+v", rca.DeepThinking)
	}
	closing := cfg.ResolveStage("closing_and_assurance", general)
	if closing == nil || !closing.IsEndStage {
		t.Fatalf("closing_and_assurance = %+v, want is_end_stage", closing)
	}
}

func TestParseOnboardingConfigRequiredFields(t *testing.T) {
	for _, missing := range []string{"model", "main_system_prompt", "careplan_switcher_prompt", "careplan_switcher_stage_name", "start_stage"} {
		_, configJSON := loadStudentTestConfigJSON(t)
		delete(configJSON, missing)
		if _, err := ParseOnboardingConfig(configJSON); err == nil {
			t.Fatalf("ParseOnboardingConfig with %q removed: want error", missing)
		}
	}
	if _, err := ParseOnboardingConfig(nil); err == nil {
		t.Fatal("ParseOnboardingConfig(nil): want error")
	}
}

// Every care plan shares the stage name problem_rca_discussion, so the
// resolution order (start → common → selected plan → all plans) is
// observable: with a selected plan the plan's own stage must win over
// the first plan's.
func TestResolveStageSearchOrder(t *testing.T) {
	cfg := parseStudentTestConfig(t)

	if got := cfg.ResolveStage("introduction", nil); got == nil || got != &cfg.StartStage {
		t.Fatalf("introduction did not resolve to start stage: %+v", got)
	}
	if got := cfg.ResolveStage("problem_discovery_and_exploration", nil); got == nil || got != &cfg.CommonStages[0] {
		t.Fatalf("common stage not resolved first: %+v", got)
	}

	porn := cfg.FindCarePlan("porn_addiction")
	got := cfg.ResolveStage("problem_rca_discussion", porn)
	if got == nil || got.NextStages[0] != "understanding_patterns" {
		t.Fatalf("selected-plan stage did not win: %+v", got)
	}

	// Without a selected plan, the first declaring plan (general) wins.
	got = cfg.ResolveStage("problem_rca_discussion", nil)
	if got == nil || got.NextStages[0] != "diet_information" {
		t.Fatalf("all-plans search did not return general's stage: %+v", got)
	}

	if got := cfg.ResolveStage("no_such_stage", nil); got != nil {
		t.Fatalf("unknown stage resolved: %+v", got)
	}
}

func TestResolveStageWithCarePlanDiscoversPlan(t *testing.T) {
	cfg := parseStudentTestConfig(t)

	stage, plan := cfg.ResolveStageWithCarePlan("understanding_patterns", nil)
	if stage == nil || plan == nil || plan.Name != "porn_addiction" {
		t.Fatalf("stage=%+v plan=%+v, want porn_addiction discovery", stage, plan)
	}

	// A stage on the selected plan keeps the selection (no re-discovery).
	hair := cfg.FindCarePlan("hair_loss")
	stage, plan = cfg.ResolveStageWithCarePlan("problem_rca_discussion", hair)
	if stage == nil || plan != hair {
		t.Fatalf("selected plan not kept: stage=%+v plan=%+v", stage, plan)
	}

	// Common stages keep whatever plan was already selected (nil here).
	stage, plan = cfg.ResolveStageWithCarePlan("problem_discovery_and_exploration", nil)
	if stage == nil || plan != nil {
		t.Fatalf("common stage changed plan: stage=%+v plan=%+v", stage, plan)
	}

	stage, plan = cfg.ResolveStageWithCarePlan("no_such_stage", hair)
	if stage != nil || plan != hair {
		t.Fatalf("unknown stage: stage=%+v plan=%+v, want nil + kept plan", stage, plan)
	}
}

func TestCollectPromptConfigs(t *testing.T) {
	cfg := parseStudentTestConfig(t)

	// Python order: main system prompt, careplan switcher, start stage
	// prompt, then common stages (each stage prompt followed by its
	// deep-thinking prompts). Care plans excluded by default.
	common := cfg.CollectPromptConfigs(false)
	wantCommon := []string{
		"onboarding_callV3_4/00_main_call_agent_sys",
		"onboarding_callV3_4/care_plan_switcher",
		"onboarding_callV3_4/01_introduction_and_call_overview",
		"onboarding_callV3_4/02_problem_discovery_and_exploration",
	}
	if len(common) != len(wantCommon) {
		t.Fatalf("common prompts = %d, want %d: %+v", len(common), len(wantCommon), common)
	}
	for i, want := range wantCommon {
		if common[i].Name != want {
			t.Fatalf("common[%d] = %q, want %q", i, common[i].Name, want)
		}
	}

	all := cfg.CollectPromptConfigs(true)
	var stageCount, dtCount int
	for _, cp := range cfg.CarePlans {
		for _, s := range cp.Stages {
			stageCount++
			dtCount += len(s.DeepThinking)
		}
	}
	if want := len(wantCommon) + stageCount + dtCount; len(all) != want {
		t.Fatalf("all prompts = %d, want %d", len(all), want)
	}

	general := cfg.FindCarePlan("general")
	planPrompts := cfg.CollectCarePlanPromptConfigs(general)
	var generalDT int
	for _, s := range general.Stages {
		generalDT += len(s.DeepThinking)
	}
	if len(planPrompts) != len(general.Stages)+generalDT {
		t.Fatalf("general plan prompts = %d, want %d", len(planPrompts), len(general.Stages)+generalDT)
	}
}

func TestLoadOnboardingConfigFromDocumentStore(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer := miniredis.RunT(t)
	redisClient := NewRedisClient(redisServer.Addr(), "", 0, log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = redisClient.Close() })

	raw, err := os.ReadFile("testdata/onboarding/student_test_config.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	redisServer.Set("document:OB_Call_Configs/student_test_config:latest", string(raw))

	store := newDocumentStore(redisClient, log.New(io.Discard, "", 0), simpleTemplateRenderer{})
	cfg, err := LoadOnboardingConfig(context.Background(), store, "student_test")
	if err != nil {
		t.Fatalf("LoadOnboardingConfig: %v", err)
	}
	if cfg.Model != "grok-4.1-fast" || cfg.StartStage.Name != "introduction" {
		t.Fatalf("unexpected config: model=%q start=%q", cfg.Model, cfg.StartStage.Name)
	}

	if _, err := LoadOnboardingConfig(context.Background(), store, "missing_variant"); err == nil {
		t.Fatal("missing variant config: want error")
	}
	if _, err := LoadOnboardingConfig(context.Background(), store, ""); err == nil {
		t.Fatal("empty variant: want error")
	}
	if _, err := LoadOnboardingConfig(context.Background(), nil, "student_test"); err == nil {
		t.Fatal("nil document store: want error")
	}
}

func TestPromptPathToVarName(t *testing.T) {
	cases := map[string]string{
		"onboarding_callV3_4/general/22_root_cause_insight_generator": "onboarding_callV3_4__general__22_root_cause_insight_generator",
		"a/b-c.d": "a__b_c_d",
		"plain":   "plain",
	}
	for in, want := range cases {
		if got := PromptPathToVarName(in); got != want {
			t.Fatalf("PromptPathToVarName(%q) = %q, want %q", in, got, want)
		}
	}
}
