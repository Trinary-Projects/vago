package disha

import (
	"errors"
	"math"
	"testing"

	"github.com/getsentry/sentry-go"
)

// Go port of Disha's Python parity corpus
// bots/onboarding_call/test_stage_transition_fuzzy_matcher.py. Every test
// case below mirrors one Python test 1:1 (same inputs, same expected
// decisions/outputs/scores). If changing fuzzy decision logic in
// onboarding_stage_fuzzy_matcher.go, keep this file in sync, per the
// convention at the top of the Python source.

const scoreEpsilon = 0.01

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= scoreEpsilon
}

func stagePromptConfigFixture(stageName string, triggerStatements ...string) map[string]any {
	triggers := make([]any, len(triggerStatements))
	for i, s := range triggerStatements {
		triggers[i] = s
	}
	return map[string]any{
		"next_stages": []any{
			map[string]any{
				"stage_name":         stageName,
				"trigger_statements": triggers,
			},
		},
	}
}

type evaluateOpts struct {
	patientInfo       string
	allowedNextStages []string
}

func evaluateTransition(t *testing.T, stageName string, triggerStatements []string, latestAssistantResponse string, opts *evaluateOpts) *StageTransitionFuzzyResult {
	t.Helper()

	allowed := []string{stageName}
	patientInfo := ""
	if opts != nil {
		if opts.allowedNextStages != nil {
			allowed = opts.allowedNextStages
		}
		patientInfo = opts.patientInfo
	}

	result, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig:       stagePromptConfigFixture(stageName, triggerStatements...),
		AllowedNextStages:       allowed,
		LatestAssistantResponse: latestAssistantResponse,
		PatientInfo:             patientInfo,
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}
	return result
}

// test_changed_preface_diet_transition_is_recoverable_with_core_trigger
func TestChangedPrefaceDietTransitionIsRecoverableWithCoreTrigger(t *testing.T) {
	result := evaluateTransition(t, "diet_information",
		[]string{"अब बताइए, आप vegetarian हैं, non-vegetarian हैं, या eggetarian?"},
		"अच्छा, तीन-चार महीने पहले tests हुए हैं, तो next step ये होगा कि आप "+
			"latest blood tests करवाइए - Vitamin D, B12, thyroid TSH, CBC, और HbA1c. "+
			"Report share कीजिए. अब बताइए, आप vegetarian हैं, non-vegetarian हैं, "+
			"या egg-vegetarian?",
		nil,
	)

	if result.Decision == StageTransitionDecisionNo {
		t.Fatalf("decision = %q, want not \"no\"", result.Decision)
	}
	if result.CandidateStage == nil || *result.CandidateStage != "diet_information" {
		t.Fatalf("candidate_stage = %v, want diet_information", result.CandidateStage)
	}
	if len(result.LLMNextStageNames) != 1 || result.LLMNextStageNames[0] != "diet_information" {
		t.Fatalf("llm_next_stage_names = %v, want [diet_information]", result.LLMNextStageNames)
	}
}

// test_blood_test_preface_diet_question_with_short_trigger_is_yes
func TestBloodTestPrefaceDietQuestionWithShortTriggerIsYes(t *testing.T) {
	result := evaluateTransition(t, "diet_information",
		[]string{"आप vegetarian हैं, non-vegetarian हैं, या eggetarian?"},
		"अच्छा. चूंकि past six months में test नहीं हुआ, मैं recommend करूंगी "+
			"basic blood test जैसे Vitamin D, B12, और Iron levels check करवाएँ. "+
			"Good. तो बताइए, आप vegetarian हैं, non-vegetarian हैं, या eggetarian?",
		nil,
	)

	if result.Decision != StageTransitionDecisionYes {
		t.Fatalf("decision = %q, want yes", result.Decision)
	}
	if result.Output != "diet_information" {
		t.Fatalf("output = %q, want diet_information", result.Output)
	}
	if result.CandidateStage == nil || *result.CandidateStage != "diet_information" {
		t.Fatalf("candidate_stage = %v, want diet_information", result.CandidateStage)
	}
}

// test_motivation_question_is_no_not_maybe_for_improve_trigger
func TestMotivationQuestionIsNoNotMaybeForImproveTrigger(t *testing.T) {
	triggerStatements := []string{
		"Okay. इसे improve करने के लिए आपने क्या try किया है?",
		"समझ सकती हूँ। इसे improve करने के लिए आपने क्या try किया है?",
	}
	falsePositiveQuestions := []string{
		"Got it. और क्या हाल ही में ऐसा कुछ हुआ जिसने आपको आज Disha try करने के लिए motivate किया?",
		"ठीक है। और क्या हाल ही में ऐसा कुछ हुआ जिसने आपको आज Disha try करने के लिए motivate किया?",
		"समझ सकती हूँ. और क्या हाल ही में ऐसा कुछ हुआ जिसने आपको आज Disha try करने के लिए motivate किया?",
		"और Disha try करने से पहले आपने किससे बात की थी?",
		"इस problem को लेकर आपको सबसे ज्यादा tension कब होती है?",
	}

	for _, response := range falsePositiveQuestions {
		t.Run(response, func(t *testing.T) {
			result := evaluateTransition(t, "problem_rca_discussion", triggerStatements, response, nil)

			if result.Decision != StageTransitionDecisionNo {
				t.Fatalf("decision = %q, want no", result.Decision)
			}
			if result.Output != "no" {
				t.Fatalf("output = %q, want no", result.Output)
			}
			if result.PartialScore >= 60 {
				t.Fatalf("partial_score = %v, want < 60", result.PartialScore)
			}
		})
	}
}

// test_production_improve_question_true_positive_is_yes
func TestProductionImproveQuestionTruePositiveIsYes(t *testing.T) {
	result := evaluateTransition(t, "problem_rca_discussion",
		[]string{"Okay. इसे improve करने के लिए आपने क्या try किया है?"},
		"Okay इसे improve करने के लिए आपने क्या try किया है",
		nil,
	)

	if result.Decision != StageTransitionDecisionYes {
		t.Fatalf("decision = %q, want yes", result.Decision)
	}
	if result.Output != "problem_rca_discussion" {
		t.Fatalf("output = %q, want problem_rca_discussion", result.Output)
	}
	if result.TokenSetScore < 90 {
		t.Fatalf("token_set_score = %v, want >= 90", result.TokenSetScore)
	}
	if result.PartialScore < 85 {
		t.Fatalf("partial_score = %v, want >= 85", result.PartialScore)
	}
	if result.Coverage < 85 {
		t.Fatalf("coverage = %v, want >= 85", result.Coverage)
	}
}

// test_introduction_location_question_with_name_variant_is_yes
func TestIntroductionLocationQuestionWithNameVariantIsYes(t *testing.T) {
	result := evaluateTransition(t, "problem_discovery_and_exploration",
		[]string{"तो [Name], आप कहाँ से हैं और क्या करते हैं?"},
		"तो नौरीना, आप कहाँ से हैं और क्या करती हैं?",
		nil,
	)

	if result.Decision != StageTransitionDecisionYes {
		t.Fatalf("decision = %q, want yes", result.Decision)
	}
	if result.Output != "problem_discovery_and_exploration" {
		t.Fatalf("output = %q, want problem_discovery_and_exploration", result.Output)
	}
	if result.TokenSetScore < 90 {
		t.Fatalf("token_set_score = %v, want >= 90", result.TokenSetScore)
	}
	if result.PartialScore < 85 {
		t.Fatalf("partial_score = %v, want >= 85", result.PartialScore)
	}
	if result.Coverage < 85 {
		t.Fatalf("coverage = %v, want >= 85", result.Coverage)
	}
}

// test_filler_and_punctuation_variants_match_core_action
func TestFillerAndPunctuationVariantsMatchCoreAction(t *testing.T) {
	trigger := "Okay. इसे improve करने के लिए आपने क्या try किया है?"
	validVariants := []string{
		"ठीक है, इसे improve करने के लिए आपने क्या try किया है?",
		"Got it इसे improve करने के लिए आपने क्या try किया है",
		"समझ सकती हूँ — इसे improve करने के लिए आपने क्या try किया है!",
		"इसे improve करने के लिए आपने क्या try किया है",
	}

	for _, response := range validVariants {
		t.Run(response, func(t *testing.T) {
			result := evaluateTransition(t, "problem_rca_discussion", []string{trigger}, response, nil)

			if result.Decision != StageTransitionDecisionYes {
				t.Fatalf("decision = %q, want yes", result.Decision)
			}
			if result.Output != "problem_rca_discussion" {
				t.Fatalf("output = %q, want problem_rca_discussion", result.Output)
			}
		})
	}
}

// test_name_placeholder_variants_match_same_action
func TestNamePlaceholderVariantsMatchSameAction(t *testing.T) {
	trigger := "[Name], आपने जो share किया वो बहुत helpful था। अब मैं simply " +
		"आपको बताती हूँ कि यह क्यों हो रहा है — और फिर हम मिलकर इसे " +
		"better करने की बात करेंगे। सुनना चाहेंगे?"
	validVariants := []string{
		"Divyansh Jain, आपने जो share किया वो बहुत helpful था। अब मैं simply " +
			"आपको बताती हूँ कि यह क्यों हो रहा है और फिर हम मिलकर इसे " +
			"better करने की बात करेंगे। सुनना चाहेंगे?",
		"Divyansh, आपने जो share किया वो बहुत helpful था। अब मैं simply " +
			"आपको बताती हूँ कि यह क्यों हो रहा है और फिर हम मिलकर इसे " +
			"better करने की बात करेंगे। सुनना चाहेंगे?",
		"आपने जो share किया वो बहुत helpful था। अब मैं simply आपको बताती हूँ " +
			"कि यह क्यों हो रहा है और फिर हम मिलकर इसे better करने की बात करेंगे। " +
			"सुनना चाहेंगे?",
	}

	for _, response := range validVariants {
		t.Run(response, func(t *testing.T) {
			result := evaluateTransition(t, "formulation_reassurance", []string{trigger}, response, &evaluateOpts{
				patientInfo: "Name: Divyansh Jain\nGender: Male",
			})

			if result.Decision != StageTransitionDecisionYes {
				t.Fatalf("decision = %q, want yes", result.Decision)
			}
			if result.Output != "formulation_reassurance" {
				t.Fatalf("output = %q, want formulation_reassurance", result.Output)
			}
		})
	}
}

// test_representative_branch_triggers_are_yes
func TestRepresentativeBranchTriggersAreYes(t *testing.T) {
	branchCases := []struct {
		stageName string
		trigger   string
		response  string
	}{
		{
			"diet_information",
			"अब बताइए, आप vegetarian हैं, non-vegetarian हैं, या eggetarian?",
			"अब बताइए, आप vegetarian हैं, non-vegetarian हैं, या eggetarian?",
		},
		{
			"root_cause_diagnosis",
			"अब इस problem के पीछे की वजह समझते हैं, okay?",
			"अब इस problem के पीछे की वजह समझते हैं, okay?",
		},
		{
			"solutions_offered",
			"अब हम solutions पर focus करते हैं।",
			"अब हम solutions पर focus करते हैं",
		},
		{
			"exercise_module_personalization",
			"क्या आपने कभी pelvic floor muscles के बारे में सुना है?",
			"क्या आपने कभी pelvic floor muscles के बारे में सुना है?",
		},
		{
			"closing_and_assurance",
			"मैं आपका personalised workout message कर दूंगी। आपको बस " +
				"'Get Started' button दबाना है और मुझे follow करना है। ठीक है?",
			"मैं आपका personalised workout message कर दूंगी। आपको बस " +
				"Get Started button दबाना है और मुझे follow करना है। ठीक है?",
		},
	}

	for _, c := range branchCases {
		t.Run(c.stageName, func(t *testing.T) {
			result := evaluateTransition(t, c.stageName, []string{c.trigger}, c.response, nil)

			if result.Decision != StageTransitionDecisionYes {
				t.Fatalf("decision = %q, want yes", result.Decision)
			}
			if result.Output != c.stageName {
				t.Fatalf("output = %q, want %q", result.Output, c.stageName)
			}
		})
	}
}

// test_partial_and_same_topic_boundaries
func TestPartialAndSameTopicBoundaries(t *testing.T) {
	trigger := "अब बताइए, आप vegetarian हैं, non-vegetarian हैं, या eggetarian?"
	cases := []struct {
		response         string
		expectedDecision StageTransitionDecision
	}{
		{"अच्छा, यह सब जानकर एक अच्छी picture बन रही है।", StageTransitionDecisionNo},
		{"अब बताइए, आप vegetarian हैं", StageTransitionDecisionMaybe},
		{"आप vegetarian खाना ज्यादा खाते हैं या बाहर का खाना?", StageTransitionDecisionNo},
	}

	for _, c := range cases {
		t.Run(c.response, func(t *testing.T) {
			result := evaluateTransition(t, "diet_information", []string{trigger}, c.response, nil)

			if result.Decision != c.expectedDecision {
				t.Fatalf("decision = %q, want %q", result.Decision, c.expectedDecision)
			}
			if c.expectedDecision == StageTransitionDecisionNo && result.Output != "no" {
				t.Fatalf("output = %q, want no", result.Output)
			}
		})
	}
}

// test_next_stages_config_parses_and_filters_allowed_stages
func TestNextStagesConfigParsesAndFiltersAllowedStages(t *testing.T) {
	result, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig: map[string]any{
			"next_stages": []any{
				map[string]any{
					"stage_name": "problem_rca_discussion",
					"trigger_statements": []any{
						"Okay. इसे improve करने के लिए आपने क्या try किया है?",
						"समझ सकती हूँ। इसे improve करने के लिए आपने क्या try किया है?",
					},
				},
				map[string]any{
					"stage_name":         "unexpected_stage",
					"trigger_statements": []any{"Ignore this"},
				},
			},
		},
		AllowedNextStages:       []string{"problem_rca_discussion"},
		LatestAssistantResponse: "Okay इसे improve करने के लिए आपने क्या try किया है",
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	if result.Decision != StageTransitionDecisionYes {
		t.Fatalf("decision = %q, want yes", result.Decision)
	}
	if result.Output != "problem_rca_discussion" {
		t.Fatalf("output = %q, want problem_rca_discussion", result.Output)
	}
	if len(result.LLMNextStageNames) != 1 || result.LLMNextStageNames[0] != "problem_rca_discussion" {
		t.Fatalf("llm_next_stage_names = %v, want [problem_rca_discussion]", result.LLMNextStageNames)
	}
	if result.Score != result.TokenSetScore {
		t.Fatalf("score (%v) != token_set_score (%v)", result.Score, result.TokenSetScore)
	}

	payload := result.ToPayload()
	if _, ok := payload["token_set_score"]; !ok {
		t.Fatalf("payload missing token_set_score: %v", payload)
	}
	if _, ok := payload["partial_score"]; !ok {
		t.Fatalf("payload missing partial_score: %v", payload)
	}
	if payload["score"] != payload["token_set_score"] {
		t.Fatalf("payload score (%v) != token_set_score (%v)", payload["score"], payload["token_set_score"])
	}
}

// test_transition_condition_comes_from_config_trigger_statements
func TestTransitionConditionComesFromConfigTriggerStatements(t *testing.T) {
	result, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig: map[string]any{
			"next_stages": []any{
				map[string]any{
					"stage_name": "diet_information",
					"trigger_statements": []any{
						"अच्छा, यह सब जानकर एक अच्छी picture बन रही है।",
					},
				},
			},
		},
		AllowedNextStages:       []string{"diet_information"},
		LatestAssistantResponse: "अच्छा, यह सब जानकर एक अच्छी picture बन रही है।",
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	want := "Target stage: diet_information\n" +
		"- Trigger: अच्छा, यह सब जानकर एक अच्छी picture बन रही है।"
	if result.TransitionCondition != want {
		t.Fatalf("transition_condition = %q, want %q", result.TransitionCondition, want)
	}
}

// test_exact_trigger_with_name_is_yes
func TestExactTriggerWithNameIsYes(t *testing.T) {
	result, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig: map[string]any{
			"next_stages": []any{
				map[string]any{
					"stage_name": "formulation_reassurance",
					"trigger_statements": []any{
						"[Name], आपने जो share किया वो बहुत helpful था। अब मैं simply " +
							"आपको बताती हूँ कि यह क्यों हो रहा है — और फिर हम मिलकर इसे " +
							"better करने की बात करेंगे। सुनना चाहेंगे?",
					},
				},
			},
		},
		AllowedNextStages: []string{"formulation_reassurance"},
		LatestAssistantResponse: "Divyansh, आपने जो share किया वो बहुत helpful था। अब मैं simply " +
			"आपको बताती हूँ कि यह क्यों हो रहा है और फिर हम मिलकर इसे " +
			"better करने की बात करेंगे। सुनना चाहेंगे?",
		PatientInfo: "Name: Divyansh Jain\nGender: Male",
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	if result.Decision != StageTransitionDecisionYes {
		t.Fatalf("decision = %q, want yes", result.Decision)
	}
	if result.Output != "formulation_reassurance" {
		t.Fatalf("output = %q, want formulation_reassurance", result.Output)
	}
	if result.Score < 92 {
		t.Fatalf("score = %v, want >= 92", result.Score)
	}
	if result.Coverage < 85 {
		t.Fatalf("coverage = %v, want >= 85", result.Coverage)
	}
}

// test_shared_prefix_is_no_due_low_coverage
func TestSharedPrefixIsNoDueLowCoverage(t *testing.T) {
	result, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig: map[string]any{
			"next_stages": []any{
				map[string]any{
					"stage_name": "history_taking_performance_anxiety",
					"trigger_statements": []any{
						"समझ गई। [Name], अब तक हमने एक side cover कर ली है, अब थोड़ा " +
							"दूसरे angle से भी देखते हैं, क्योंकि पूरी picture समझना ज़रूरी है। ठीक है?",
					},
				},
			},
		},
		AllowedNextStages:       []string{"history_taking_performance_anxiety"},
		LatestAssistantResponse: "समझ गई। Sex से पहले आप कितनी देर foreplay करते हैं?",
		PatientInfo:             "Name: Divyansh Jain\nGender: Male",
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	if result.Decision != StageTransitionDecisionNo {
		t.Fatalf("decision = %q, want no", result.Decision)
	}
	if result.Output != "no" {
		t.Fatalf("output = %q, want no", result.Output)
	}
	if result.Coverage >= 40 {
		t.Fatalf("coverage = %v, want < 40", result.Coverage)
	}
}

// test_partial_trigger_is_maybe
func TestPartialTriggerIsMaybe(t *testing.T) {
	result, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig: map[string]any{
			"next_stages": []any{
				map[string]any{
					"stage_name": "problem_rca_discussion",
					"trigger_statements": []any{
						"Okay. इसे improve करने के लिए आपने क्या try किया है, और अब तक क्या असर दिखा?",
					},
				},
			},
		},
		AllowedNextStages:       []string{"problem_rca_discussion"},
		LatestAssistantResponse: "Okay इसे improve करने के लिए आपने क्या try किया है",
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	if result.Decision != StageTransitionDecisionMaybe {
		t.Fatalf("decision = %q, want maybe", result.Decision)
	}
	if result.Output != "no" {
		t.Fatalf("output = %q, want no", result.Output)
	}
	if result.Score < 70 {
		t.Fatalf("score = %v, want >= 70", result.Score)
	}
	if result.Coverage < 40 {
		t.Fatalf("coverage = %v, want >= 40", result.Coverage)
	}
	if result.Coverage >= 85 {
		t.Fatalf("coverage = %v, want < 85", result.Coverage)
	}
}

// test_long_trigger_reports_info_sentry_message
//
// The Python test mocks sentry_sdk.new_scope/capture_message/capture_exception
// to assert on the exact Sentry payload. This codebase has no Sentry test
// double (sentryutil.Capture calls the real sentry-go SDK, which safely
// no-ops without sentry.Init), so this port instead asserts on the same
// process-wide dedupe key state the Python test's mocked scope indirectly
// proved: reportedLongTriggers gains exactly one entry, keyed on
// (document_name, document_version, target_stage, trigger), for a trigger
// longer than the info threshold.
func TestLongTriggerReportsInfoSentryMessage(t *testing.T) {
	resetReportedLongTriggers(t)

	longTrigger := make([]rune, stageTransitionTriggerLengthInfoThreshold+1)
	for i := range longTrigger {
		longTrigger[i] = 'a'
	}
	trigger := string(longTrigger)

	_, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig: map[string]any{
			"next_stages": []any{
				map[string]any{
					"stage_name":         "problem_rca_discussion",
					"trigger_statements": []any{trigger},
				},
			},
			"id": "config-version-id",
		},
		AllowedNextStages:       []string{"problem_rca_discussion"},
		LatestAssistantResponse: "short response",
		DocumentName:            "OB_Call_Configs/test_config",
		DocumentVersion:         7,
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	reportedLongTriggersMu.Lock()
	defer reportedLongTriggersMu.Unlock()
	if len(reportedLongTriggers) != 1 {
		t.Fatalf("reportedLongTriggers size = %d, want 1", len(reportedLongTriggers))
	}
	key := longTriggerKey{
		documentName:    "OB_Call_Configs/test_config",
		documentVersion: 7,
		toStage:         "problem_rca_discussion",
		trigger:         trigger,
	}
	if !reportedLongTriggers[key] {
		t.Fatalf("reportedLongTriggers missing expected key %+v; got %v", key, reportedLongTriggers)
	}
}

// TestLongTriggerSentryRoutesThroughHub proves the sentry-task-hub wiring
// for the matcher: a caller-supplied hub (the tracker's late-bound
// taskSentryHub in production) receives the long-trigger info capture
// instead of the process-global hub, and a tag set on the hub's own scope
// survives onto the captured event alongside the call's own event-level
// tag — same shape as TestStageManagerInvalidStageSentryRoutesThroughHub.
func TestLongTriggerSentryRoutesThroughHub(t *testing.T) {
	resetReportedLongTriggers(t)

	hubTransport := &sentry.MockTransport{}
	hubClient, err := sentry.NewClient(sentry.ClientOptions{Transport: hubTransport})
	if err != nil {
		t.Fatalf("sentry.NewClient: %v", err)
	}
	hub := sentry.NewHub(hubClient, sentry.NewScope())
	hub.Scope().SetTag("conversation_id", "conv-fuzzy-hub-test")

	longTrigger := make([]rune, stageTransitionTriggerLengthInfoThreshold+1)
	for i := range longTrigger {
		longTrigger[i] = 'a'
	}
	trigger := string(longTrigger)

	_, err = EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig:       stagePromptConfigFixture("problem_rca_discussion", trigger),
		AllowedNextStages:       []string{"problem_rca_discussion"},
		LatestAssistantResponse: "short response",
		DocumentName:            "OB_Call_Configs/test_config",
		DocumentVersion:         7,
		Hub:                     hub,
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	events := hubTransport.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event on the caller-supplied hub's transport, got %d", len(events))
	}
	event := events[0]
	if event.Message != "Long onboarding stage transition trigger statement" {
		t.Fatalf("event message = %q", event.Message)
	}
	if event.Tags["conversation_id"] != "conv-fuzzy-hub-test" {
		t.Fatalf("expected hub-scope tag to survive onto the captured event, got %v", event.Tags)
	}
	if event.Tags["target_stage"] != "problem_rca_discussion" {
		t.Fatalf("expected event-level tag to also apply, got %v", event.Tags)
	}
}

// test_short_trigger_does_not_report_sentry_message
func TestShortTriggerDoesNotReportSentryMessage(t *testing.T) {
	resetReportedLongTriggers(t)

	shortTrigger := make([]rune, stageTransitionTriggerLengthInfoThreshold)
	for i := range shortTrigger {
		shortTrigger[i] = 'a'
	}

	_, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig:       stagePromptConfigFixture("problem_rca_discussion", string(shortTrigger)),
		AllowedNextStages:       []string{"problem_rca_discussion"},
		LatestAssistantResponse: "short response",
		DocumentName:            "OB_Call_Configs/test_config",
		DocumentVersion:         7,
	})
	if err != nil {
		t.Fatalf("EvaluateStageTransitionFromConfig: %v", err)
	}

	reportedLongTriggersMu.Lock()
	defer reportedLongTriggersMu.Unlock()
	if len(reportedLongTriggers) != 0 {
		t.Fatalf("reportedLongTriggers size = %d, want 0", len(reportedLongTriggers))
	}
}

// test_duplicate_long_trigger_reports_once_per_process
func TestDuplicateLongTriggerReportsOncePerProcess(t *testing.T) {
	resetReportedLongTriggers(t)

	longTrigger := make([]rune, stageTransitionTriggerLengthInfoThreshold+1)
	for i := range longTrigger {
		longTrigger[i] = 'a'
	}
	trigger := string(longTrigger)
	cfg := stagePromptConfigFixture("problem_rca_discussion", trigger)

	for i := 0; i < 2; i++ {
		_, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
			StagePromptConfig:       cfg,
			AllowedNextStages:       []string{"problem_rca_discussion"},
			LatestAssistantResponse: "short response",
			DocumentName:            "OB_Call_Configs/test_config",
			DocumentVersion:         7,
		})
		if err != nil {
			t.Fatalf("EvaluateStageTransitionFromConfig[%d]: %v", i, err)
		}
	}

	reportedLongTriggersMu.Lock()
	defer reportedLongTriggersMu.Unlock()
	if len(reportedLongTriggers) != 1 {
		t.Fatalf("reportedLongTriggers size = %d, want 1 (deduped)", len(reportedLongTriggers))
	}
}

// test_malformed_transition_trigger_config_does_not_report_sentry_message
func TestMalformedTransitionTriggerConfigDoesNotReportSentryMessage(t *testing.T) {
	resetReportedLongTriggers(t)

	_, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig: map[string]any{
			"next_stages": []any{
				map[string]any{
					"stage_name":         "problem_rca_discussion",
					"trigger_statements": "not a list",
				},
			},
		},
		AllowedNextStages:       []string{"problem_rca_discussion"},
		LatestAssistantResponse: "short response",
		DocumentName:            "OB_Call_Configs/test_config",
		DocumentVersion:         7,
	})

	var configErr *StageTransitionConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("err = %v, want *StageTransitionConfigError", err)
	}

	reportedLongTriggersMu.Lock()
	defer reportedLongTriggersMu.Unlock()
	if len(reportedLongTriggers) != 0 {
		t.Fatalf("reportedLongTriggers size = %d, want 0 (parse failed before reporting)", len(reportedLongTriggers))
	}
}

// Python parity: pydantic validates `list[str]` before the "after"-mode
// field validator runs, so a trigger_statements list containing any
// non-string (or None) item fails the whole parse with
// StageTransitionConfigError even when other items are valid strings.
func TestNonStringTriggerStatementItemFailsWholeParse(t *testing.T) {
	resetReportedLongTriggers(t)

	for _, badItem := range []any{123, nil} {
		_, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
			StagePromptConfig: map[string]any{
				"next_stages": []any{
					map[string]any{
						"stage_name":         "problem_rca_discussion",
						"trigger_statements": []any{"Okay. इसे improve करने के लिए आपने क्या try किया है?", badItem},
					},
				},
			},
			AllowedNextStages:       []string{"problem_rca_discussion"},
			LatestAssistantResponse: "short response",
			DocumentName:            "OB_Call_Configs/test_config",
			DocumentVersion:         7,
		})

		var configErr *StageTransitionConfigError
		if !errors.As(err, &configErr) {
			t.Fatalf("badItem=%v: err = %v, want *StageTransitionConfigError", badItem, err)
		}
	}

	reportedLongTriggersMu.Lock()
	defer reportedLongTriggersMu.Unlock()
	if len(reportedLongTriggers) != 0 {
		t.Fatalf("reportedLongTriggers size = %d, want 0 (parse failed before reporting)", len(reportedLongTriggers))
	}
}

func resetReportedLongTriggers(t *testing.T) {
	t.Helper()
	reportedLongTriggersMu.Lock()
	reportedLongTriggers = map[longTriggerKey]bool{}
	reportedLongTriggersMu.Unlock()
}

// --- Additional scorer-level parity spot checks (numeric values captured
// directly against the real rapidfuzz==3.14.5 C++ engine, verified equal
// to its pure-Python fallback for this corpus before porting). ---

func TestScorerParitySpotChecks(t *testing.T) {
	cases := []struct {
		name         string
		text         string
		trigger      string
		wantTokenSet float64
		wantPartial  float64
		wantCoverage float64
	}{
		{
			name: "changed_preface_diet_transition",
			text: normalizeForFuzzy("अच्छा, तीन-चार महीने पहले tests हुए हैं, तो next step ये होगा कि आप " +
				"latest blood tests करवाइए - Vitamin D, B12, thyroid TSH, CBC, और HbA1c. " +
				"Report share कीजिए. अब बताइए, आप vegetarian हैं, non-vegetarian हैं, " +
				"या egg-vegetarian?"),
			trigger:      normalizeForFuzzy("अब बताइए, आप vegetarian हैं, non-vegetarian हैं, या eggetarian?"),
			wantTokenSet: 85.7143,
			wantPartial:  93.2203,
			wantCoverage: 100.0,
		},
		{
			name:         "production_improve_question",
			text:         normalizeForFuzzy("Okay इसे improve करने के लिए आपने क्या try किया है"),
			trigger:      normalizeForFuzzy("Okay. इसे improve करने के लिए आपने क्या try किया है?"),
			wantTokenSet: 100.0,
			wantPartial:  100.0,
			wantCoverage: 100.0,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotTokenSet := tokenSetRatio(c.text, c.trigger)
			gotPartial := partialRatio(c.text, c.trigger)
			gotCoverage := triggerTokenCoverage(c.text, c.trigger)

			if !almostEqual(gotTokenSet, c.wantTokenSet) {
				t.Errorf("token_set_ratio = %v, want %v", gotTokenSet, c.wantTokenSet)
			}
			if !almostEqual(gotPartial, c.wantPartial) {
				t.Errorf("partial_ratio = %v, want %v", gotPartial, c.wantPartial)
			}
			if !almostEqual(gotCoverage, c.wantCoverage) {
				t.Errorf("coverage = %v, want %v", gotCoverage, c.wantCoverage)
			}
		})
	}
}

// TestIndelRatioBase is a small direct sanity check of the base rapidfuzz
// fuzz.ratio port (normalized Indel similarity) against known values.
func TestIndelRatioBase(t *testing.T) {
	if got := indelRatioStr("this is a test", "this is a test!"); !almostEqual(got, 96.55172) {
		t.Fatalf("indelRatioStr = %v, want ~96.55172", got)
	}
	if got := indelRatioStr("", ""); got != 100 {
		t.Fatalf("indelRatioStr(empty,empty) = %v, want 100", got)
	}
}
