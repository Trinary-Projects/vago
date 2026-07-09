package disha

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// --- Scenario 8: redundant transition (next == current) still runs ---

func TestStageManagerRedundantTransitionStillProcessed(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})

	// Make the per-stage counter observable: AdvanceStage resets it.
	h.state.IncrementStageTurnCount()
	if h.state.StageTurnCount() != 1 {
		t.Fatalf("stage turn count = %d, want 1", h.state.StageTurnCount())
	}

	h.manager.processTransition(context.Background(), "introduction", "stage_transition_tracker_redundant", "transcript")

	// Python Sentry-messages the redundancy but continues: the stage is
	// re-entered (counters reset), the prompt recompiled, and the debug
	// chunk + enqueues still fire.
	if h.state.CurrentStage().Name != "introduction" {
		t.Fatalf("current stage = %s, want introduction", h.state.CurrentStage().Name)
	}
	if h.state.StageTurnCount() != 0 {
		t.Fatalf("stage turn count = %d, want reset to 0", h.state.StageTurnCount())
	}
	if !h.hasRTVI("[PROCESS] System prompt updated") || !h.hasRTVI("Agenda changed from introduction => introduction") {
		t.Fatalf("RTVI messages = %v", h.rtviMessages())
	}

	waitForCondition(t, 5*time.Second, "redundant-transition debug chunk", func() bool {
		return len(h.chunks()) == 1
	})
	chunk := h.chunks()[0]
	if chunk.Text != "Agenda changed(Stage Transition Tracker) from introduction => introduction" || !chunk.IsDebugLog {
		t.Fatalf("debug chunk = %+v", chunk)
	}
	additional, _ := chunk.AdditionalData.(map[string]any)
	if additional["tool_call_id"] != "stage_transition_tracker_redundant" {
		t.Fatalf("tool_call_id = %v", additional["tool_call_id"])
	}

	waitForCondition(t, 5*time.Second, "redundant-transition enqueues", func() bool {
		return len(h.enqueueRequests(agendaAnalyticsModule)) == 1 &&
			len(h.enqueueRequests(stageTransitionLogModule)) == 1
	})

	if len(h.routerMeta.recorded()) != 1 {
		t.Fatalf("router metadata refreshes = %d, want 1", len(h.routerMeta.recorded()))
	}
}

// --- Invalid stage name: RTVI error + Sentry, no state change ---

func TestStageManagerInvalidStage(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})
	systemBefore := h.pair.MessagesSnapshot()[0].Content

	h.manager.processTransition(context.Background(), "ghost_stage", "stage_transition_tracker_x", "")

	if !h.hasRTVI("[ERROR] Invalid stage: ghost_stage") {
		t.Fatalf("missing invalid-stage RTVI: %v", h.rtviMessages())
	}
	if h.state.CurrentStage().Name != "introduction" {
		t.Fatalf("state changed: %s", h.state.CurrentStage().Name)
	}
	if got := h.pair.MessagesSnapshot()[0].Content; got != systemBefore {
		t.Fatal("system message replaced for invalid stage")
	}
	if got := h.chunks(); len(got) != 0 {
		t.Fatalf("chunks = %d, want 0", len(got))
	}
	if reqs := h.apiRecorder.snapshot(); len(reqs) != 0 {
		t.Fatalf("API requests = %+v, want none", reqs)
	}
}

// --- Invalid stage Sentry capture routes through the late-bound hub ---

// TestStageManagerInvalidStageSentryRoutesThroughHub proves the
// sentry-task-hub wiring: SetInfrastructure's hub argument (harness
// stand-in for taskCtx.SentryHub()) is what the manager's
// sentryutil.Capture calls actually use, not the process-global hub.
// A tag set on the hub's own scope (mirroring what NewTaskHub would set
// from TaskConfig.SentryTags) must survive onto the captured event
// alongside the call's own event-level tags.
func TestStageManagerInvalidStageSentryRoutesThroughHub(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})
	h.hub.Scope().SetTag("conversation_id", stageTestConversationID)

	h.manager.processTransition(context.Background(), "ghost_stage", "stage_transition_tracker_x", "")

	events := h.hubTransport.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event on the late-bound hub's transport, got %d", len(events))
	}
	event := events[0]
	if event.Message != "Invalid stage: ghost_stage" {
		t.Fatalf("event message = %q", event.Message)
	}
	if event.Tags["conversation_id"] != stageTestConversationID {
		t.Fatalf("expected hub-scope tag to survive onto the captured event, got %v", event.Tags)
	}
	if event.Tags["operation"] != "stage_transition" {
		t.Fatalf("expected event-level tag to also apply, got %v", event.Tags)
	}
}

// --- Transition before SetInfrastructure no-ops safely ---

func TestStageManagerNoInfrastructureNoOp(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})
	bare := NewOnboardingStageManager(h.state, h.config, nil, h.manager.callbacks, nil, nil, nil,
		h.manager.logger, stageTestUserID, stageTestConversationID, h.promptKey)

	bare.processTransition(context.Background(), stageTestNextStage, "id", "")

	if h.state.CurrentStage().Name != "introduction" {
		t.Fatalf("state changed without infrastructure: %s", h.state.CurrentStage().Name)
	}
	if !strings.Contains(h.logBuf.String(), "before infrastructure set") {
		t.Fatalf("missing no-infra log: %s", h.logBuf.String())
	}
	if got := h.chunks(); len(got) != 0 {
		t.Fatalf("chunks = %d, want 0", len(got))
	}
}

// --- Careplan gate wiring (phase 5) ---

func TestStageManagerCareplanGateFiresOnlyForSwitcherStageWithoutSelection(t *testing.T) {
	cpFactory := newDTFactoryRecorder(map[string]*dtStubClient{
		"onboarding_callV3_4/care_plan_switcher": {output: `{"selected_care_plan":"general"}`},
	})
	h := newStageMachineHarnessWithManagers(t, &stubStageClassifier{}, nil, cpFactory.factory())

	// Entering a non-switcher stage never calls the careplan client.
	h.manager.processTransition(context.Background(), stageTestNextStage, "tool-1", "transcript")
	waitForCondition(t, 5*time.Second, "non-switcher stage advance", func() bool {
		return h.state.CurrentStage().Name == stageTestNextStage
	})
	if calls := cpFactory.snapshot(); len(calls) != 0 {
		t.Fatalf("careplan client called for non-switcher stage: %d calls", len(calls))
	}
	if h.state.SelectedCarePlan() != nil {
		t.Fatal("care plan selected without entering the switcher stage")
	}

	// Entering the switcher stage without a selection calls Detect, then
	// Activate stores the resolved plan on ConversationState.
	h.manager.processTransition(context.Background(), h.config.CareplanSwitcherStageName, "tool-2", "transcript")
	waitForCondition(t, 5*time.Second, "care plan selected", func() bool {
		return h.state.SelectedCarePlan() != nil
	})
	if calls := cpFactory.snapshot(); len(calls) != 1 {
		t.Fatalf("careplan client calls = %d, want 1", len(calls))
	}
	if got := h.state.SelectedCarePlan().Name; got != "general" {
		t.Fatalf("selected care plan = %s, want general", got)
	}

	// Re-entering the switcher stage with an existing selection must not
	// call Detect again.
	h.manager.processTransition(context.Background(), h.config.CareplanSwitcherStageName, "tool-3", "transcript")
	waitForCondition(t, 5*time.Second, "re-entry timing log enqueue", func() bool {
		return len(h.enqueueRequests(stageTransitionLogModule)) >= 1
	})
	if calls := cpFactory.snapshot(); len(calls) != 1 {
		t.Fatalf("careplan client calls after re-entry = %d, want still 1", len(calls))
	}
}

func TestStageManagerCareplanDetectErrorAbortsTransition(t *testing.T) {
	cpFactory := newDTFactoryRecorder(map[string]*dtStubClient{
		"onboarding_callV3_4/care_plan_switcher": {err: errors.New("both hedged attempts failed")},
	})
	h := newStageMachineHarnessWithManagers(t, &stubStageClassifier{}, nil, cpFactory.factory())

	h.manager.processTransition(context.Background(), h.config.CareplanSwitcherStageName, "tool-1", "transcript")

	if h.state.CurrentStage().Name != "introduction" {
		t.Fatalf("state advanced despite careplan detect error: %s", h.state.CurrentStage().Name)
	}
	if h.state.SelectedCarePlan() != nil {
		t.Fatal("care plan selected despite detect error")
	}
	if got := h.chunks(); len(got) != 0 {
		t.Fatalf("debug chunks = %d, want 0 (transition aborted)", len(got))
	}
	if reqs := h.apiRecorder.snapshot(); len(reqs) != 0 {
		t.Fatalf("API requests = %+v, want none (no persist/timing log)", reqs)
	}
	if h.hasRTVI("[PROCESS] System prompt updated") {
		t.Fatal("prompt updated despite careplan detect error")
	}
}

// --- Blocking deep thinking wiring (phase 5) ---

func TestStageManagerBlockingDTMergedIntoVariableStoreBeforeCompile(t *testing.T) {
	dtFactory := newDTFactoryRecorder(map[string]*dtStubClient{
		"obtest/dt_blocking_prompt": {output: `{"root_cause_summary":"stress-related sleep issue"}`},
	})
	h := newStageMachineHarnessWithManagers(t, &stubStageClassifier{}, dtFactory.factory(), nil)

	seedDocument(t, h.redisServer, "obtest/dt_target_stage_prompt", "latest", 1, "STAGE BODY summary={{ root_cause_summary }}")
	seedDocument(t, h.redisServer, "obtest/dt_blocking_prompt", "latest", 1, "DT PROMPT BODY")
	h.config.CommonStages = append(h.config.CommonStages, StageConfig{
		Name:   "dt_blocking_target",
		Prompt: PromptConfig{Name: "obtest/dt_target_stage_prompt"},
		DeepThinking: []DeepThinkingConfig{
			{Prompt: PromptConfig{Name: "obtest/dt_blocking_prompt"}, Blocking: true},
		},
	})

	h.manager.processTransition(context.Background(), "dt_blocking_target", "tool-1", "transcript")

	waitForCondition(t, 5*time.Second, "stage advance", func() bool {
		return h.state.CurrentStage().Name == "dt_blocking_target"
	})
	if got := h.state.VariableStoreSnapshot()["root_cause_summary"]; got != "stress-related sleep issue" {
		t.Fatalf("variable store = %v, want the merged DT value", got)
	}
	// The blocking DT must have merged BEFORE the stage prompt compiled —
	// otherwise the compiled system message would still show the
	// unrendered placeholder.
	msgs := h.pair.MessagesSnapshot()
	if msgs[0].Role != "system" || !strings.Contains(msgs[0].Content, "summary=stress-related sleep issue") {
		t.Fatalf("compiled prompt missing merged DT value: %.300s", msgs[0].Content)
	}

	waitForCondition(t, 5*time.Second, "timing log enqueue", func() bool {
		return len(h.enqueueRequests(stageTransitionLogModule)) == 1
	})
	timing := h.enqueueRequests(stageTransitionLogModule)[0]
	kwargs := timing.Body["kwargs"].(map[string]any)
	if v, ok := kwargs["dt_blocking_time_ms"].(float64); !ok || v <= 0 {
		t.Fatalf("dt_blocking_time_ms = %#v, want a positive float64", kwargs["dt_blocking_time_ms"])
	}
	if kwargs["is_dt_blocking_llm_call"] != true {
		t.Fatalf("is_dt_blocking_llm_call = %v, want true", kwargs["is_dt_blocking_llm_call"])
	}
}

// --- Non-blocking deep thinking wiring (phase 5) ---

func TestStageManagerNonBlockingDTMergeRecompilesOnChange(t *testing.T) {
	dtFactory := newDTFactoryRecorder(map[string]*dtStubClient{
		"obtest/dt_nonblocking_prompt": {output: `{"diet_intensity_level":"moderate"}`},
	})
	h := newStageMachineHarnessWithManagers(t, &stubStageClassifier{}, dtFactory.factory(), nil)

	seedDocument(t, h.redisServer, "obtest/dt_nb_target_stage_prompt", "latest", 1, "STAGE BODY level={{ diet_intensity_level }}")
	seedDocument(t, h.redisServer, "obtest/dt_nonblocking_prompt", "latest", 1, "DT PROMPT BODY")
	h.config.CommonStages = append(h.config.CommonStages, StageConfig{
		Name:   "dt_nonblocking_target",
		Prompt: PromptConfig{Name: "obtest/dt_nb_target_stage_prompt"},
		DeepThinking: []DeepThinkingConfig{
			{Prompt: PromptConfig{Name: "obtest/dt_nonblocking_prompt"}, Blocking: false},
		},
	})

	h.manager.processTransition(context.Background(), "dt_nonblocking_target", "tool-1", "transcript")

	waitForCondition(t, 5*time.Second, "stage advance", func() bool {
		return h.state.CurrentStage().Name == "dt_nonblocking_target"
	})
	waitForCondition(t, 5*time.Second, "non-blocking variable merged", func() bool {
		return h.state.VariableStoreSnapshot()["diet_intensity_level"] == "moderate"
	})
	h.waitForRTVI("[PROCESS] System prompt recompiled due to variable update")
	waitForCondition(t, 5*time.Second, "prompt recompiled with merged value", func() bool {
		return strings.Contains(h.pair.MessagesSnapshot()[0].Content, "level=moderate")
	})

	waitForCondition(t, 5*time.Second, "timing log enqueue", func() bool {
		return len(h.enqueueRequests(stageTransitionLogModule)) == 1
	})
	kwargs := h.enqueueRequests(stageTransitionLogModule)[0].Body["kwargs"].(map[string]any)
	if kwargs["is_dt_blocking_llm_call"] != false {
		t.Fatalf("is_dt_blocking_llm_call = %v, want false (only a non-blocking DT is configured)", kwargs["is_dt_blocking_llm_call"])
	}
}

// TestStageManagerOnNonBlockingDTCompleteNoOpWhenUnchanged exercises
// onNonBlockingDTComplete directly (white-box) to pin down the
// changed-vs-unchanged merge behavior deterministically, without racing
// a background goroutine.
func TestStageManagerOnNonBlockingDTCompleteNoOpWhenUnchanged(t *testing.T) {
	h := newStageMachineHarnessWithManagers(t, &stubStageClassifier{}, nil, nil)

	seedDocument(t, h.redisServer, "obtest/var_stage_prompt", "latest", 1, "STAGE dt_var={{ dt_var }}")
	stage := &StageConfig{Name: "var_stage", Prompt: PromptConfig{Name: "obtest/var_stage_prompt"}}
	h.state.AdvanceStage(stage)
	h.state.MergeVariables(map[string]any{"dt_var": "hello"})

	// Recompile once so the current system message reflects dt_var=hello,
	// mirroring the compile that processTransition would already have
	// done after AdvanceStage.
	compiled, err := h.manager.compiler.CompileSystemPrompt(context.Background(), stage, h.state.VariableStoreSnapshot())
	if err != nil {
		t.Fatalf("CompileSystemPrompt: %v", err)
	}
	h.pair.ReplaceSystemMessage(compiled.Text)

	before := h.pair.MessagesSnapshot()[0].Content
	beforeRTVICount := len(h.rtviMessages())

	// Same value: MergeVariables reports unchanged, so no recompile and no
	// RTVI message.
	h.manager.onNonBlockingDTComplete(context.Background())(map[string]any{"dt_var": "hello"})
	if got := h.pair.MessagesSnapshot()[0].Content; got != before {
		t.Fatal("system message replaced despite an unchanged variable")
	}
	if len(h.rtviMessages()) != beforeRTVICount {
		t.Fatalf("RTVI messages sent despite an unchanged variable: %v", h.rtviMessages())
	}

	// Different value: MergeVariables reports changed, so the current
	// stage recompiles.
	h.manager.onNonBlockingDTComplete(context.Background())(map[string]any{"dt_var": "world"})
	if !h.hasRTVI("[PROCESS] System prompt recompiled due to variable update") {
		t.Fatalf("missing recompile RTVI: %v", h.rtviMessages())
	}
	if got := h.pair.MessagesSnapshot()[0].Content; !strings.Contains(got, "dt_var=world") {
		t.Fatalf("system message not recompiled: %.300s", got)
	}
}

func TestStageManagerOnNonBlockingDTCompleteNoInfrastructureNoOp(t *testing.T) {
	h := newStageMachineHarnessWithManagers(t, &stubStageClassifier{}, nil, nil)
	bare := NewOnboardingStageManager(h.state, h.config, h.manager.compiler, h.manager.callbacks, nil, nil, nil,
		h.manager.logger, stageTestUserID, stageTestConversationID, h.promptKey)

	// No SetInfrastructure call: pair is nil. A changed merge must not
	// panic when there is no aggregator pair to recompile onto.
	bare.onNonBlockingDTComplete(context.Background())(map[string]any{"dt_var": "hello"})

	if got := h.state.VariableStoreSnapshot()["dt_var"]; got != "hello" {
		t.Fatalf("variable store = %v, want merged even without infrastructure", got)
	}
}
