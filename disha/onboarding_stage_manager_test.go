package disha

import (
	"context"
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

// --- Transition before SetInfrastructure no-ops safely ---

func TestStageManagerNoInfrastructureNoOp(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})
	bare := NewOnboardingStageManager(h.state, h.config, nil, h.manager.callbacks, nil,
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
