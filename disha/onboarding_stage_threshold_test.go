package disha

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// The student_test fixture's start stage (introduction) has
// turn_threshold: 10, and the monitor allows stageThresholdGraceTurns past
// it, so turns up to 13 are silent and the 14th trips the alert.
const thresholdFixtureIntroThreshold = 10

// thresholdFixtureIntroAlertTurn is the first turn count that alerts on the
// introduction stage: threshold + grace + 1.
const thresholdFixtureIntroAlertTurn = thresholdFixtureIntroThreshold + stageThresholdGraceTurns + 1

func (h *stageMachineHarness) commitAssistantTurns(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		h.threshold.OnAssistantTurnCommitted()
	}
}

func (h *stageMachineHarness) tagRequests() []callAPIRequest {
	var out []callAPIRequest
	for _, req := range h.apiRecorder.snapshot() {
		if strings.HasSuffix(req.Path, "/bot/add_tag_to_user") {
			out = append(out, req)
		}
	}
	return out
}

func (h *stageMachineHarness) thresholdSentryMessages() []string {
	var out []string
	for _, ev := range h.hubTransport.Events() {
		if ev.Tags["operation"] == "stage_threshold" {
			out = append(out, ev.Message)
		}
	}
	return out
}

func TestStageThresholdNoActionWithinGrace(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})

	// Crossing the bare threshold is not enough: the grace turns must be
	// used up too. Prod calls were seen passing the raw threshold and then
	// progressing normally, which is why the grace exists.
	h.commitAssistantTurns(thresholdFixtureIntroAlertTurn - 1)

	if got := h.state.StageTurnCount(); got != thresholdFixtureIntroAlertTurn-1 {
		t.Fatalf("stage turn count = %d, want %d", got, thresholdFixtureIntroAlertTurn-1)
	}
	if h.state.StageThresholdAlerted() {
		t.Fatalf("alerted at turn %d; want no alert until turn %d (threshold %d + grace %d)",
			thresholdFixtureIntroAlertTurn-1, thresholdFixtureIntroAlertTurn,
			thresholdFixtureIntroThreshold, stageThresholdGraceTurns)
	}
	if msgs := h.thresholdSentryMessages(); len(msgs) != 0 {
		t.Fatalf("Sentry messages = %v, want none", msgs)
	}
	if reqs := h.tagRequests(); len(reqs) != 0 {
		t.Fatalf("add_tag_to_user requests = %d, want 0", len(reqs))
	}
	if h.hasRTVI("[STAGE] Threshold exceeded") {
		t.Fatalf("RTVI messages = %v, want no threshold line", h.rtviMessages())
	}
}

func TestStageThresholdAlertsOnceWhenExceeded(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})

	// Alert fires at threshold + grace + 1, i.e. turn 14 for a threshold of
	// 10. The Sentry message reports the raw threshold, not the effective
	// one, so the message text matches Python's historical issue grouping.
	h.commitAssistantTurns(thresholdFixtureIntroAlertTurn)

	if !h.state.StageThresholdAlerted() {
		t.Fatal("expected stage_threshold_alerted to be set")
	}

	want := "Stage turn threshold exceeded: introduction had 14 turns (threshold: 10)"
	msgs := h.thresholdSentryMessages()
	if len(msgs) != 1 || msgs[0] != want {
		t.Fatalf("Sentry messages = %v, want exactly [%q]", msgs, want)
	}

	h.waitForRTVI("[STAGE] Threshold exceeded: introduction had 14 turns (threshold: 10)")

	waitForCondition(t, 5*time.Second, "add_tag_to_user request", func() bool {
		return len(h.tagRequests()) == 1
	})
	body := h.tagRequests()[0].Body
	if body["user_id"] != stageTestUserID {
		t.Fatalf("tag user_id = %v, want %s", body["user_id"], stageTestUserID)
	}
	if body["tag_name"] != stageTransitionFailureTag {
		t.Fatalf("tag_name = %v, want %s", body["tag_name"], stageTransitionFailureTag)
	}

	// Further turns on the same stage must stay silent: one alert per
	// stage, matching Python's stage_threshold_alerted guard.
	h.commitAssistantTurns(5)
	if got := h.state.StageTurnCount(); got != thresholdFixtureIntroAlertTurn+5 {
		t.Fatalf("stage turn count = %d, want %d", got, thresholdFixtureIntroAlertTurn+5)
	}
	if msgs := h.thresholdSentryMessages(); len(msgs) != 1 {
		t.Fatalf("Sentry messages = %v, want the alert to fire exactly once", msgs)
	}
	if reqs := h.tagRequests(); len(reqs) != 1 {
		t.Fatalf("add_tag_to_user requests = %d, want 1", len(reqs))
	}
}

func TestStageThresholdResetsOnStageTransition(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})

	h.commitAssistantTurns(thresholdFixtureIntroAlertTurn)
	if !h.state.StageThresholdAlerted() {
		t.Fatal("expected the introduction alert to fire first")
	}

	// A real transition goes through the stage manager, which calls
	// AdvanceStage and so resets both the counter and the alerted flag.
	h.manager.processTransition(context.Background(), "problem_discovery_and_exploration",
		"stage_transition_tracker_threshold", "transcript")

	if got := h.state.CurrentStage().Name; got != "problem_discovery_and_exploration" {
		t.Fatalf("current stage = %s", got)
	}
	if got := h.state.StageTurnCount(); got != 0 {
		t.Fatalf("stage turn count = %d, want reset to 0", got)
	}
	if h.state.StageThresholdAlerted() {
		t.Fatal("stage_threshold_alerted must reset on transition so the new stage can alert")
	}

	// The new stage's threshold is 20 in the fixture, so the old count is
	// genuinely gone rather than merely unread. 20 + grace + 1 = 24.
	h.commitAssistantTurns(20 + stageThresholdGraceTurns + 1)
	want := "Stage turn threshold exceeded: problem_discovery_and_exploration had 24 turns (threshold: 20)"
	msgs := h.thresholdSentryMessages()
	if len(msgs) != 2 || msgs[1] != want {
		t.Fatalf("Sentry messages = %v, want second message %q", msgs, want)
	}
}

func TestStageThresholdSkippedWhenStageHasNoThreshold(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})

	// Python's `if not threshold: return` covers both a missing key and an
	// explicit 0.
	stage := h.state.CurrentStage()
	zero := 0
	stage.TurnThreshold = &zero
	h.commitAssistantTurns(50)
	if h.state.StageThresholdAlerted() {
		t.Fatal("threshold 0 must be treated as unset")
	}

	stage.TurnThreshold = nil
	h.commitAssistantTurns(50)
	if h.state.StageThresholdAlerted() {
		t.Fatal("nil threshold must be treated as unset")
	}
	if msgs := h.thresholdSentryMessages(); len(msgs) != 0 {
		t.Fatalf("Sentry messages = %v, want none", msgs)
	}

	// The counter still advances, so a later stage with a threshold is
	// unaffected by having passed through an unthresholded one.
	if got := h.state.StageTurnCount(); got != 100 {
		t.Fatalf("stage turn count = %d, want 100", got)
	}
}

// The monitor runs for every variant, including the stage-transition-tracker
// variants that the old pipeline-processor version skipped entirely. The
// harness builds a student_test (tracker) state, so this asserts the
// regression that motivated the port.
func TestStageThresholdRunsForTrackerVariant(t *testing.T) {
	h := newStageMachineHarness(t, &stubStageClassifier{})

	if got := h.state.Variant(); got != "student_test" {
		t.Fatalf("harness variant = %s, want the tracker variant student_test", got)
	}

	h.commitAssistantTurns(thresholdFixtureIntroAlertTurn)

	if len(h.thresholdSentryMessages()) != 1 {
		t.Fatalf("tracker variant produced no threshold alert; Sentry messages = %v",
			h.thresholdSentryMessages())
	}
}

// The handler seam must stay bot-agnostic: bots that never register one
// (sales, follow-up) keep the plain chunk-write behavior.
func TestAssistantTurnCommittedHandlerUnsetIsNoOp(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: stageTestConversationID,
		UserID:         stageTestUserID,
		BotType:        SalesCallBotType,
		Logger:         log.New(&syncBuffer{}, "", 0),
	}, redisClient, nil, nil)

	// No SetAssistantTurnCommittedHandler call: the chunk is still written
	// and nothing panics.
	callbacks.OnAssistantTurnCommitted("hello", time.Now(), voicepipelinecore.TurnMetrics{}, "")

	items, err := redisServer.List(conversationChunksKey(stageTestUserID, stageTestConversationID))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("chunks written = %d, want 1", len(items))
	}
}
