package disha

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// stageTransitionFailureTag is the CRM tag Python's
// StageThresholdMonitor._tag_user writes when a stage stalls.
const stageTransitionFailureTag = "Stage Transition Failure"

// OnboardingStageThresholdMonitor is the Go port of Disha's
// bots/onboarding_call/stage_threshold_monitor.py: it counts assistant
// turns spent on the current stage and raises one alert per stage when the
// count passes the stage's configured turn_threshold.
//
// Deliberately not a pipeline processor in either runtime — it observes
// ConversationState only and never touches frames or the LLM context, so
// it needs no placement in the pipeline. Python's earlier version also
// injected a "you skipped the stage transition" reminder into the context;
// that was removed, which is what let both runtimes drop it out of the
// pipeline and run it for every variant (the tracker architecture can stall
// on a stage just as the tool-call architecture can).
//
// The per-stage count and the alerted flag live on ConversationState, so
// ConversationState.AdvanceStage resets both on every transition.
type OnboardingStageThresholdMonitor struct {
	state          *ConversationState
	api            *APIClient
	logger         *log.Logger
	userID         string
	conversationID string

	// Late-bound UI (same nil-safe pattern as the tracker/stage/DT/careplan
	// managers): RTVI sends are skipped until wired. The task-scoped Sentry
	// hub is wired separately via the embedded taskSentryHub.
	infraMu sync.Mutex
	ui      serverMessageEmitter

	taskSentryHub
}

func NewOnboardingStageThresholdMonitor(
	state *ConversationState,
	api *APIClient,
	logger *log.Logger,
	userID, conversationID string,
) *OnboardingStageThresholdMonitor {
	return &OnboardingStageThresholdMonitor{
		state:          state,
		api:            api,
		logger:         logger,
		userID:         userID,
		conversationID: conversationID,
	}
}

// SetUI injects the late-bound RTVI emitter.
func (m *OnboardingStageThresholdMonitor) SetUI(ui serverMessageEmitter) {
	if m == nil {
		return
	}
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	m.ui = ui
}

func (m *OnboardingStageThresholdMonitor) getUI() serverMessageEmitter {
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	return m.ui
}

// OnAssistantTurnCommitted advances the per-stage turn count and alerts if
// the stage has overstayed its threshold. It mirrors Python's
// on_assistant_turn_stopped hook: one call per committed assistant turn,
// interrupted turns included.
//
// The read-modify-write across IncrementStageTurnCount / StageThresholdAlerted
// / MarkStageThresholdAlerted needs no extra lock: the call-events dispatcher
// invokes this on a single FIFO goroutine, so calls never overlap.
func (m *OnboardingStageThresholdMonitor) OnAssistantTurnCommitted() {
	if m == nil || m.state == nil {
		return
	}

	turnCount := m.state.IncrementStageTurnCount()

	stage := m.state.CurrentStage()
	if stage == nil || stage.TurnThreshold == nil || *stage.TurnThreshold == 0 {
		return
	}
	stageName := stage.Name
	threshold := *stage.TurnThreshold

	if m.state.StageThresholdAlerted() {
		m.logger.Printf("Stage threshold already alerted for stage %q, skipping (turn %d)", stageName, turnCount)
		return
	}

	if turnCount <= threshold {
		m.logger.Printf("Stage threshold check for stage %q: turn %d, threshold %d, no action needed", stageName, turnCount, threshold)
		return
	}

	m.logger.Printf("Stage turn threshold exceeded for stage %q (turn %d, threshold %d), handling alert", stageName, turnCount, threshold)
	m.state.MarkStageThresholdAlerted()
	m.handleAlert(stageName, turnCount, threshold)
}

func (m *OnboardingStageThresholdMonitor) handleAlert(stageName string, turns, threshold int) {
	sentryutil.Capture(sentryutil.Event{
		Hub: m.sentryHub(),
		Message: fmt.Sprintf(
			"Stage turn threshold exceeded: %s had %d turns (threshold: %d)",
			stageName, turns, threshold,
		),
		Tags: map[string]string{
			"component": "disha_onboarding",
			"operation": "stage_threshold",
			"stage":     stageName,
			"turns":     strconv.Itoa(turns),
			"threshold": strconv.Itoa(threshold),
		},
		Details: map[string]any{
			"conversation_id": m.conversationID,
			"user_id":         m.userID,
		},
	})

	if ui := m.getUI(); ui != nil {
		ui.ServerMessage(fmt.Sprintf(
			"[STAGE] Threshold exceeded: %s had %d turns (threshold: %d)",
			stageName, turns, threshold,
		), time.Now())
	}

	if m.userID == "" || m.api == nil {
		return
	}
	// Python fires this as an asyncio task so the alert never blocks the
	// turn; the equivalent here is a detached goroutine with its own
	// timeout, because the call-events dispatcher runs this inline.
	go m.tagUser(m.userID)
}

func (m *OnboardingStageThresholdMonitor) tagUser(userID string) {
	ctx, cancel := context.WithTimeout(context.Background(), stageThresholdTagTimeout)
	defer cancel()

	if err := m.api.AddTagToUserWithFallback(ctx, AddTagToUserRequest{
		UserID:  userID,
		TagName: stageTransitionFailureTag,
	}); err != nil {
		// Python logs and captures the tagging failure but never lets it
		// affect the call.
		m.logger.Printf("Failed to tag user with stage transition failure: %v", err)
		sentryutil.Capture(sentryutil.Event{
			Hub: m.sentryHub(),
			Err: err,
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "stage_threshold_tag_user",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         userID,
			},
		})
	}
}

// stageThresholdTagTimeout bounds the detached tag call. The task context
// is deliberately not used: the alert must still land if it fires as the
// call is ending.
var stageThresholdTagTimeout = 15 * time.Second
