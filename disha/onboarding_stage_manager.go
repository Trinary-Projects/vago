package disha

// OnboardingStageManager is the Go port of Disha's
// bots/onboarding_call/stage_manager.py process_transition +
// _persist_and_track, scoped to the tracker-source transition path.
// Phase 5 wires in careplan detection/activation (OnboardingCarePlanManager)
// and deep thinking (OnboardingDeepThinkingManager) around the existing
// phase-4 stage-resolve/compile/persist flow.

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/google/uuid"
	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	// Python: StageTransitionLogService._persist enqueue target.
	stageTransitionLogModule = "llm_logging.stage_transition_log_service"
	stageTransitionLogFunc   = "StageTransitionLogService._save_to_db"
	stageTransitionLogQueue  = "llm-logs-parallel"

	// Python: _persist_and_track analytics enqueue target.
	agendaAnalyticsModule = "users.managers.user_analytics_manager"
	agendaAnalyticsFunc   = "UserAnalyticsManager.track_event_job"
	agendaAnalyticsQueue  = "p1-fast-l1"
)

type OnboardingStageManager struct {
	state          *ConversationState
	config         *OnboardingConfig
	compiler       *onboardingPromptCompiler
	callbacks      *CallEventCallbacks
	api            *APIClient
	dtManager      *OnboardingDeepThinkingManager
	careplan       *OnboardingCarePlanManager
	logger         *log.Logger
	userID         string
	conversationID string
	promptKey      string

	// Late-bound infrastructure (Python's set_infrastructure): the
	// aggregator pair, conversation router handle, RTVI emitter, and
	// task-scoped Sentry hub only exist after NewPipelineTask.
	// processTransition before SetInfrastructure no-ops safely.
	infraMu sync.Mutex
	pair    *voicepipelinecore.ContextAggregatorPair
	router  promptMetadataSetter
	ui      serverMessageEmitter
	hub     *sentry.Hub
}

func NewOnboardingStageManager(
	state *ConversationState,
	config *OnboardingConfig,
	compiler *onboardingPromptCompiler,
	callbacks *CallEventCallbacks,
	api *APIClient,
	dtManager *OnboardingDeepThinkingManager,
	careplan *OnboardingCarePlanManager,
	logger *log.Logger,
	userID, conversationID, promptKey string,
) *OnboardingStageManager {
	return &OnboardingStageManager{
		state:          state,
		config:         config,
		compiler:       compiler,
		callbacks:      callbacks,
		api:            api,
		dtManager:      dtManager,
		careplan:       careplan,
		logger:         logger,
		userID:         userID,
		conversationID: conversationID,
		promptKey:      promptKey,
	}
}

// SetInfrastructure injects the aggregator pair (ReplaceSystemMessage),
// the conversation router handle (SetPromptMetadata refresh after
// recompiles), the RTVI emitter, and the task-scoped Sentry hub (routes
// this manager's Sentry captures through the call's identity tags —
// sentry-task-hub; nil hub is safe and falls back to global capture,
// which covers any call in the window before this is wired).
func (m *OnboardingStageManager) SetInfrastructure(pair *voicepipelinecore.ContextAggregatorPair, router promptMetadataSetter, ui serverMessageEmitter, hub *sentry.Hub) {
	if m == nil {
		return
	}
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	m.pair = pair
	m.router = router
	m.ui = ui
	m.hub = hub
}

// getHub returns the late-bound task-scoped Sentry hub (nil before
// SetInfrastructure runs).
func (m *OnboardingStageManager) getHub() *sentry.Hub {
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	return m.hub
}

// processTransition mirrors StageManager.process_transition (phase-4
// scope: no careplan detection, no deep thinking).
func (m *OnboardingStageManager) processTransition(ctx context.Context, nextStageName, toolCallID, transcript string) {
	_ = transcript // consumed by the phase-5 careplan/deep-thinking port
	start := time.Now()

	m.infraMu.Lock()
	pair, router := m.pair, m.router
	m.infraMu.Unlock()
	if pair == nil {
		// Python raises RuntimeError before set_infrastructure; our wiring
		// guarantees infrastructure before the pipeline runs, so a call in
		// that window can only be a test/teardown artifact — no-op safely.
		m.logf("disha: stage manager transition before infrastructure set; ignoring next_stage=%s", nextStageName)
		return
	}

	currentName := m.state.CurrentStage().Name
	if currentName != "" && strings.EqualFold(currentName, nextStageName) {
		sentryutil.Capture(sentryutil.Event{
			Hub:     m.getHub(),
			Message: "Redundant stage transition attempted",
			Tags: map[string]string{
				"current_agenda": currentName,
				"next_agenda":    nextStageName,
			},
			Details: map[string]any{"user_id": m.userID},
		})
		// Python does not return: the redundant transition still runs.
	}

	// Careplan detection: when the careplan-switcher stage is entered
	// without a selection yet, classify the transcript and activate the
	// resulting plan before resolving the stage (Python: detect/activate
	// run before resolve_stage, so the just-selected plan is visible to
	// it). A detection error aborts the transition entirely — no advance,
	// no compile, no timing log — mirroring Python's exception
	// propagating out of detect() and leaving the plan unset for a later
	// trigger to retry.
	if nextStageName == m.config.CareplanSwitcherStageName && m.state.SelectedCarePlan() == nil {
		name, detected, err := m.careplan.Detect(ctx, transcript)
		if err != nil {
			m.logf("disha: careplan detection failed for stage=%s: %v", nextStageName, err)
			if !isContextCancellation(ctx, err) {
				sentryutil.Capture(sentryutil.Event{
					Hub: m.getHub(),
					Err: fmt.Errorf("disha: careplan detection failed: %w", err),
					Tags: map[string]string{
						"component": "disha_onboarding",
						"operation": "careplan_detect",
					},
					Details: map[string]any{
						"conversation_id": m.conversationID,
						"user_id":         m.userID,
						"stage":           nextStageName,
					},
				})
			}
			return
		}
		m.state.SetSelectedCarePlan(m.careplan.Activate(ctx, name, detected))
	}

	nextStage := m.config.ResolveStage(nextStageName, m.state.SelectedCarePlan())
	if nextStage == nil {
		m.sendRTVI(fmt.Sprintf("[ERROR] Invalid stage: %s", nextStageName))
		sentryutil.Capture(sentryutil.Event{
			Hub:     m.getHub(),
			Message: fmt.Sprintf("Invalid stage: %s", nextStageName),
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "stage_transition",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
				"current_stage":   currentName,
			},
		})
		return
	}

	// Blocking deep thinking for the stage being ENTERED: run and wait
	// before compiling, then merge results into the variable store BEFORE
	// AdvanceStage/CompileSystemPrompt so the new stage's prompt renders
	// with the fresh values (Python: dt_manager.run_blocking + merge_
	// variables happen before advance_stage/compile_and_apply).
	isDTBlockingLLMCall := false
	for _, dt := range nextStage.DeepThinking {
		if dt.Blocking {
			isDTBlockingLLMCall = true
			break
		}
	}
	dtStart := time.Now()
	dtResults := m.dtManager.RunBlocking(ctx, nextStage, transcript, nextStageName, m.state.VariableStoreSnapshot())
	dtBlockingMs := float64(time.Since(dtStart)) / float64(time.Millisecond)
	m.state.MergeVariables(dtResults)

	m.state.AdvanceStage(nextStage)

	compiled, err := m.compiler.CompileSystemPrompt(ctx, nextStage, m.state.VariableStoreSnapshot())
	if err != nil {
		// State has already advanced — Python has the same ordering
		// (advance_stage before compile_and_apply) and does not roll back.
		sentryutil.Capture(sentryutil.Event{
			Hub: m.getHub(),
			Err: fmt.Errorf("disha: stage transition prompt compile failed: %w", err),
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "stage_transition_compile",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
				"from_stage":      currentName,
				"next_stage":      nextStageName,
			},
		})
		m.sendRTVI(fmt.Sprintf("[ERROR] System prompt update failed for stage %s: %s",
			nextStageName, runePrefix(err.Error(), 80)))
		return
	}
	pair.ReplaceSystemMessage(compiled.Text)
	if router != nil {
		router.SetPromptMetadata(buildOnboardingPromptMetadata(m.config, nextStage, compiled))
	}
	m.sendRTVI("[PROCESS] System prompt updated")

	// Non-blocking deep thinking for the stage just entered: fired after
	// the compiled prompt is already applied, on a FRESH variable-store
	// snapshot (which already includes the blocking results merged
	// above — Python takes this snapshot at the equivalent point, after
	// compile_and_apply). Each result is merged into the variable store
	// and, if it actually changed anything, recompiles the CURRENT stage's
	// prompt whenever it lands.
	m.dtManager.RunNonBlocking(ctx, nextStage, transcript, nextStageName, m.state.VariableStoreSnapshot(), m.onNonBlockingDTComplete(ctx))

	m.sendRTVI(fmt.Sprintf("Agenda changed from %s => %s", currentName, nextStageName))

	// Python: asyncio.create_task(self._persist_and_track(...)).
	go m.persistAndTrack(currentName, nextStageName, toolCallID)

	// Python: StageTransitionLogService().log_stage_transition fires the
	// enqueue from a create_task too (fire-and-forget, best-effort).
	totalMs := float64(time.Since(start)) / float64(time.Millisecond)
	go m.enqueueStageTransitionTimingLog(currentName, nextStageName, totalMs, dtBlockingMs, isDTBlockingLLMCall)
}

// onNonBlockingDTComplete returns the RunNonBlocking completion callback
// for the transition that spawned it, mirroring StageManager.
// _on_non_blocking_dt_complete. Merging happens unconditionally; only a
// changed variable store triggers a recompile. Results can land after a
// LATER transition has already advanced the stage further — that's
// correct Python behavior (no stale-guard here, unlike the tracker's
// classifier path): the merge still applies and, if it changes anything,
// the CURRENT (newer) stage's prompt is recompiled with it.
func (m *OnboardingStageManager) onNonBlockingDTComplete(ctx context.Context) func(map[string]any) {
	return func(results map[string]any) {
		if !m.state.MergeVariables(results) {
			return
		}

		m.infraMu.Lock()
		pair, router := m.pair, m.router
		m.infraMu.Unlock()
		if pair == nil {
			// Infrastructure not yet set (or torn down) — no-op safely.
			return
		}

		currentStage := m.state.CurrentStage()
		compiled, err := m.compiler.CompileSystemPrompt(ctx, currentStage, m.state.VariableStoreSnapshot())
		if err != nil {
			sentryutil.Capture(sentryutil.Event{
				Hub: m.getHub(),
				Err: fmt.Errorf("disha: non-blocking DT recompile failed: %w", err),
				Tags: map[string]string{
					"component": "disha_onboarding",
					"operation": "non_blocking_dt_recompile",
				},
				Details: map[string]any{
					"conversation_id": m.conversationID,
					"user_id":         m.userID,
					"current_stage":   currentStage.Name,
				},
			})
			m.logf("disha: non-blocking DT recompile failed for stage=%s: %v", currentStage.Name, err)
			return
		}
		pair.ReplaceSystemMessage(compiled.Text)
		if router != nil {
			router.SetPromptMetadata(buildOnboardingPromptMetadata(m.config, currentStage, compiled))
		}
		m.sendRTVI("[PROCESS] System prompt recompiled due to variable update")
	}
}

// persistAndTrack mirrors StageManager._persist_and_track for the
// tracker source: ONLY the agenda-change debug chunk is persisted — the
// assistant tool_calls / tool chunk pair belongs to the un-ported
// update_stage tool-call path — plus the analytics enqueue.
func (m *OnboardingStageManager) persistAndTrack(fromStage, toStage, toolCallID string) {
	defer m.recoverToSentry("stage_transition_persist_and_track")

	m.callbacks.AppendDebugLogChunk(
		fmt.Sprintf("Agenda changed(Stage Transition Tracker) from %s => %s", fromStage, toStage),
		time.Now(),
		m.promptKey,
		map[string]any{"tool_call_id": toolCallID},
	)

	if m.api == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), postCallRequestTimeout)
	defer cancel()
	err := m.api.EnqueueJob(ctx, EnqueueJobRequest{
		ModuleName: agendaAnalyticsModule,
		FuncName:   agendaAnalyticsFunc,
		Kwargs: map[string]any{
			"user_id":    m.userID,
			"event":      "CallAgendaUpdate-" + toStage,
			"properties": map[string]any{},
		},
		SQSQueue: agendaAnalyticsQueue,
	})
	if err != nil {
		m.logf("disha: agenda analytics enqueue failed conversation=%s: %v", m.conversationID, err)
		sentryutil.Capture(sentryutil.Event{
			Hub: m.getHub(),
			Err: err,
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "stage_transition_analytics_enqueue",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
				"to_stage":        toStage,
			},
		})
	}
}

// enqueueStageTransitionTimingLog mirrors StageTransitionLogService.
// log_stage_transition → _persist: enqueue the unbound-safe staticmethod
// _save_to_db on llm-logs-parallel. Best-effort (failure → Sentry + log).
func (m *OnboardingStageManager) enqueueStageTransitionTimingLog(fromStage, toStage string, totalMs, dtBlockingMs float64, isDTBlockingLLMCall bool) {
	defer m.recoverToSentry("stage_transition_timing_log")
	if m.api == nil {
		return
	}
	logID := uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), postCallRequestTimeout)
	defer cancel()
	err := m.api.EnqueueJob(ctx, EnqueueJobRequest{
		ModuleName: stageTransitionLogModule,
		FuncName:   stageTransitionLogFunc,
		Kwargs: map[string]any{
			"log_id":                  logID,
			"user_id":                 m.userID,
			"conversation_id":         m.conversationID,
			"current_stage":           fromStage,
			"next_stage":              toStage,
			"total_time_ms":           totalMs,
			"dt_blocking_time_ms":     dtBlockingMs,
			"is_dt_blocking_llm_call": isDTBlockingLLMCall,
			"data_s3_key":             nil,
			"bucket_name":             nil,
		},
		SQSQueue: stageTransitionLogQueue,
	})
	if err != nil {
		m.logf("[STAGE_TRANSITION_LOG] %s => %s | Failed to persist: %v", fromStage, toStage, err)
		sentryutil.Capture(sentryutil.Event{
			Hub: m.getHub(),
			Err: err,
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "stage_transition_log_enqueue",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
				"log_id":          logID,
			},
		})
		return
	}
	m.logf("[STAGE_TRANSITION_LOG] %s => %s | conversation=%s | log_id=%s | Queued Save to DB",
		fromStage, toStage, m.conversationID, logID)
}

func (m *OnboardingStageManager) recoverToSentry(operation string) {
	p := recover()
	if p == nil {
		return
	}
	err := fmt.Errorf("disha: panic in %s: %v", operation, p)
	m.logf("%v", err)
	sentryutil.Capture(sentryutil.Event{
		Hub: m.getHub(),
		Err: err,
		Tags: map[string]string{
			"component": "disha_onboarding",
			"operation": operation,
		},
		Details: map[string]any{
			"conversation_id": m.conversationID,
			"user_id":         m.userID,
		},
	})
}

func (m *OnboardingStageManager) sendRTVI(message string) {
	m.infraMu.Lock()
	ui := m.ui
	m.infraMu.Unlock()
	if ui != nil {
		ui.ServerMessage(message, time.Now())
	}
}

func (m *OnboardingStageManager) logf(format string, args ...any) {
	if m.logger != nil {
		m.logger.Printf(format+"\n", args...)
	}
}
