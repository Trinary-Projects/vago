package disha

// OnboardingStageTracker is the Go port of Disha's
// bots/onboarding_call/stage_transition_tracker_processor.py: after every
// completed LLM response it fuzzy-matches the assistant's latest utterance
// against the current stage prompt's configured trigger statements
// (onboarding_stage_fuzzy_matcher.go), runs a one-shot LLM classifier on
// "maybe", and asks the stage manager to transition on "yes"/a valid
// classifier output. By decision the tracker architecture is used for ALL
// variants; the Python update_stage tool-call path is not ported.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	// Python: STAGE_TRANSITION_TRACKER_SYS_PROMPT / _USER_PROMPT.
	stageTrackerSysPromptName  = "obcall_helpers/agenda_tracker/agenda_tracker_sys"
	stageTrackerUserPromptName = "obcall_helpers/agenda_tracker/agenda_tracker_user"

	// Python: STAGE_TRANSITION_TRACKER_TRANSCRIPT_TURNS.
	stageTrackerTranscriptTurns = 5

	// Python: STAGE_TRANSITION_TRACKER_LOG_PREFIX.
	stageTrackerLogPrefix = "[STAGE_TRANSITION_TRACKER]"

	// Python: UsecaseType.ONBOARDING_CALL_STAGE_TRANSITION_TRACKER.
	stageTrackerUsecaseType = "onboarding_call_stage_transition_tracker"

	// transcriptAllTurns is the "no windowing" sentinel for
	// onboardingTranscript (Python's max_turns=None).
	transcriptAllTurns = -1
)

// serverMessageEmitter is the narrow RTVI surface the tracker/manager
// need (Python's rtvi_observer.send_rtvi_message(ServerMessage(...))).
// Satisfied by *voicepipelinecore.UIEventSender.
type serverMessageEmitter interface {
	ServerMessage(data any, at time.Time)
}

// stageTransitionProcessor is the tracker's consumer-side view of the
// stage manager. The manager handles its own failure paths internally
// (Sentry + RTVI), so the method reports nothing back.
type stageTransitionProcessor interface {
	processTransition(ctx context.Context, nextStageName, toolCallID, transcript string)
}

// promptMetadataSetter is satisfied by *llmrouter.Router. The tracker
// type-asserts its classifier against it to attach per-call prompt
// identity; the manager uses it to refresh the conversation router.
type promptMetadataSetter interface {
	SetPromptMetadata(map[string]any)
}

type OnboardingStageTracker struct {
	state          *ConversationState
	config         *OnboardingConfig
	docs           *DocumentStore
	manager        stageTransitionProcessor
	classifier     voicepipelinecore.LLMClient
	logger         *log.Logger
	userID         string
	conversationID string
	patientInfo    string

	// Late-bound infrastructure (Python's set_infrastructure pattern):
	// the aggregator pair and UI emitter only exist after
	// NewPipelineTask, but the tracker must exist before it (the
	// CallEvents mapping is consumed at NewPipelineTask time). Methods
	// firing before SetInfrastructure no-op safely. The task-scoped
	// Sentry hub is wired separately via the embedded taskSentryHub.
	infraMu sync.Mutex
	ctx     context.Context
	pair    *voicepipelinecore.ContextAggregatorPair
	ui      serverMessageEmitter

	taskSentryHub

	// transitionMu is Python's _transition_lock: serializes the
	// stale-check + processTransition critical section.
	transitionMu sync.Mutex

	// classifierMu serializes SetPromptMetadata + Stream on the shared
	// classifier so overlapping runs can't mismatch metadata and call.
	classifierMu sync.Mutex
}

func NewOnboardingStageTracker(
	state *ConversationState,
	config *OnboardingConfig,
	docs *DocumentStore,
	manager stageTransitionProcessor,
	classifier voicepipelinecore.LLMClient,
	logger *log.Logger,
	userID, conversationID, patientInfo string,
) *OnboardingStageTracker {
	return &OnboardingStageTracker{
		state:          state,
		config:         config,
		docs:           docs,
		manager:        manager,
		classifier:     classifier,
		logger:         logger,
		userID:         userID,
		conversationID: conversationID,
		patientInfo:    patientInfo,
	}
}

// SetInfrastructure injects the late-bound pieces before task assembly
// completes: the task context (goroutine lifetime), the aggregator pair
// (transcript source), and the RTVI emitter. The task-scoped Sentry hub
// is wired separately via the embedded taskSentryHub.SetSentryHub.
func (t *OnboardingStageTracker) SetInfrastructure(ctx context.Context, pair *voicepipelinecore.ContextAggregatorPair, ui serverMessageEmitter) {
	if t == nil {
		return
	}
	t.infraMu.Lock()
	defer t.infraMu.Unlock()
	t.ctx = ctx
	t.pair = pair
	t.ui = ui
}

func (t *OnboardingStageTracker) infrastructure() (context.Context, *voicepipelinecore.ContextAggregatorPair, serverMessageEmitter) {
	t.infraMu.Lock()
	defer t.infraMu.Unlock()
	return t.ctx, t.pair, t.ui
}

// OnLLMCallCompleted is the CallEvents.OnLLMCallCompleted hook: the Go
// port of on_llm_call_complete (tracker side of
// onboarding_pipeline_manager.on_llm_call_complete, which computes the
// transcripts, plus the processor's own queueing/skip logic).
func (t *OnboardingStageTracker) OnLLMCallCompleted(text string, interrupted bool) {
	if t == nil {
		return
	}
	ctx, pair, _ := t.infrastructure()
	if ctx == nil || pair == nil {
		// Fired before SetInfrastructure — no-op safely.
		return
	}

	stageName := t.state.CurrentStage().Name
	if interrupted {
		t.sendRTVI(fmt.Sprintf("%s Skipped interrupted LLM response for stage=%s", stageTrackerLogPrefix, stageName))
		return
	}

	latestAssistantResponse := strings.TrimSpace(text)
	messages := pair.MessagesSnapshot()
	latestTranscript := onboardingTranscript(messages, stageTrackerTranscriptTurns-1)
	if latestAssistantResponse != "" {
		latestTranscript = latestTranscript + "\ndisha: " + latestAssistantResponse
	}
	fullTranscript := onboardingTranscript(messages, transcriptAllTurns)

	t.sendRTVI(fmt.Sprintf("%s Queued after LLM response for stage=%s", stageTrackerLogPrefix, stageName))

	// Python queues the evaluation with asyncio.create_task: the work
	// includes a document fetch and possibly an LLM call, and must never
	// block the call-events dispatcher.
	go func() {
		defer t.recoverToSentry("stage_transition_tracker_run")
		if ctx.Err() != nil {
			return
		}
		t.run(ctx, latestTranscript, fullTranscript, latestAssistantResponse)
	}()
}

// run mirrors StageTransitionTrackerProcessor._run.
func (t *OnboardingStageTracker) run(ctx context.Context, latestTranscript, fullTranscript, latestAssistantResponse string) {
	currentStage := t.state.CurrentStage()
	currentStageName := currentStage.Name
	allowedNextStages := append([]string(nil), currentStage.NextStages...)

	if strings.Contains(strings.ToLower(currentStageName), "closing") {
		t.logf("%s Stage=%s is a closing stage, skipping", stageTrackerLogPrefix, currentStageName)
		return
	}
	if len(allowedNextStages) == 0 {
		t.logf("%s Stage=%s has no next stages, skipping", stageTrackerLogPrefix, currentStageName)
		t.sendRTVI(fmt.Sprintf("%s Skipped stage=%s: no next stages", stageTrackerLogPrefix, currentStageName))
		return
	}

	t.sendRTVI(fmt.Sprintf("%s Started for stage=%s", stageTrackerLogPrefix, currentStageName))

	stagePromptConfig, _, err := t.docs.GetDocumentConfig(ctx, currentStage.Prompt.Name, currentStage.Prompt.Version)
	if err != nil {
		t.reportRunError(ctx, currentStageName, err)
		return
	}

	// Python passes prompt.version straight through (None → the matcher
	// falls back to the config's own "version" field). Go's PromptConfig
	// pins with int, 0 == "latest"/unpinned.
	var documentVersion any
	if currentStage.Prompt.Version > 0 {
		documentVersion = currentStage.Prompt.Version
	}
	result, err := EvaluateStageTransitionFromConfig(StageTransitionEvalConfig{
		StagePromptConfig:       stagePromptConfig,
		AllowedNextStages:       allowedNextStages,
		LatestAssistantResponse: latestAssistantResponse,
		PatientInfo:             t.patientInfo,
		DocumentName:            currentStage.Prompt.Name,
		DocumentVersion:         documentVersion,
		Hub:                     t.sentryHub(),
	})
	if err != nil {
		var cfgErr *StageTransitionConfigError
		if errors.As(err, &cfgErr) {
			sentryutil.Capture(sentryutil.Event{
				Hub:     t.sentryHub(),
				Message: cfgErr.Error(),
				Tags: map[string]string{
					"conversation_id": t.conversationID,
					"current_stage":   currentStageName,
				},
				Details: map[string]any{
					"user_id":             t.userID,
					"allowed_next_stages": allowedNextStages,
				},
			})
			t.sendRTVI(fmt.Sprintf("%s Invalid stage transition config for stage=%s: %s",
				stageTrackerLogPrefix, currentStageName, runePrefix(err.Error(), 80)))
			return
		}
		t.reportRunError(ctx, currentStageName, err)
		return
	}

	t.logFuzzyResult(currentStageName, result)

	switch result.Decision {
	case StageTransitionDecisionYes:
		t.sendRTVI(fmt.Sprintf("%s Fuzzy matched %s => %s; skipping LLM",
			stageTrackerLogPrefix, currentStageName, result.Output))
		t.processOutput(ctx, result.Output, currentStageName, allowedNextStages, fullTranscript)
	case StageTransitionDecisionNo:
		t.sendRTVI(fmt.Sprintf("%s Fuzzy no transition for stage=%s; skipping LLM",
			stageTrackerLogPrefix, currentStageName))
	default: // maybe
		t.sendRTVI(fmt.Sprintf("%s Fuzzy maybe for stage=%s; running LLM",
			stageTrackerLogPrefix, currentStageName))
		output, llmErr := t.runLLMTracker(ctx, currentStageName, latestTranscript, result)
		if llmErr != nil {
			t.reportRunError(ctx, currentStageName, llmErr)
			return
		}
		t.processOutput(ctx, output, currentStageName, allowedNextStages, fullTranscript)
	}
}

// runLLMTracker mirrors _run_llm_tracker: render the agenda-tracker
// prompts and run the fixed-endpoint one-shot classifier.
func (t *OnboardingStageTracker) runLLMTracker(ctx context.Context, currentStageName, transcript string, result *StageTransitionFuzzyResult) (string, error) {
	if t.classifier == nil {
		return "", errors.New("disha: stage tracker classifier LLM client is not configured")
	}

	systemVars := DocumentVariables{
		"current_stage":        currentStageName,
		"transition_condition": result.TransitionCondition,
		"next_stages":          formatTrackerNextStages(result.LLMNextStageNames),
	}
	userVars := DocumentVariables{"transcript": transcript}
	for k, v := range systemVars {
		userVars[k] = v
	}

	sysText, sysVersion, err := t.docs.GetDocument(ctx, stageTrackerSysPromptName, 0, systemVars)
	if err != nil {
		return "", fmt.Errorf("disha: render stage tracker system prompt: %w", err)
	}
	userText, userVersion, err := t.docs.GetDocument(ctx, stageTrackerUserPromptName, 0, userVars)
	if err != nil {
		return "", fmt.Errorf("disha: render stage tracker user prompt: %w", err)
	}

	// Prompt-identity fields only (strict repo rule). Python additionally
	// stuffs stage_transitions + fuzzy_result into prompt_metadata —
	// deliberately not ported (no business metadata in prompt_metadata).
	metadata := buildPromptTraceMetadata("system", stageTrackerSysPromptName, sysVersion, systemVars)
	for k, v := range buildPromptTraceMetadata("user", stageTrackerUserPromptName, userVersion, userVars) {
		metadata[k] = v
	}

	req := voicepipelinecore.LLMRequest{Messages: []voicepipelinecore.Message{
		{Role: "system", Content: sysText},
		{Role: "user", Content: userText},
	}}

	start := time.Now()
	t.classifierMu.Lock()
	if setter, ok := t.classifier.(promptMetadataSetter); ok {
		setter.SetPromptMetadata(metadata)
	}
	var out strings.Builder
	_, err = t.classifier.Stream(ctx, req, func(token string) { out.WriteString(token) })
	t.classifierMu.Unlock()
	totalMs := float64(time.Since(start)) / float64(time.Millisecond)

	if err != nil {
		return "", fmt.Errorf("disha: stage tracker classifier LLM call failed: %w", err)
	}
	output := strings.TrimSpace(out.String())
	if output == "" {
		return "", errors.New("disha: stage tracker classifier returned empty output")
	}

	t.logf("%s stage=%s, output=%q, took=%.2fms", stageTrackerLogPrefix, currentStageName, output, totalMs)
	t.sendRTVI(fmt.Sprintf("%s Output=%q for stage=%s in %.2f ms",
		stageTrackerLogPrefix, output, currentStageName, totalMs))
	return output, nil
}

// logFuzzyResult mirrors _log_fuzzy_result.
func (t *OnboardingStageTracker) logFuzzyResult(currentStageName string, result *StageTransitionFuzzyResult) {
	t.logf("%s Fuzzy matcher final output for stage=%s: output=%q, payload=%v",
		stageTrackerLogPrefix, currentStageName, result.Output, result.ToPayload())
	candidate := "None"
	if result.CandidateStage != nil {
		candidate = fmt.Sprintf("%q", *result.CandidateStage)
	}
	t.sendRTVI(fmt.Sprintf("%s Fuzzy matcher final output decision=%s output=%q score=%.2f coverage=%.2f candidate=%s",
		stageTrackerLogPrefix, result.Decision, result.Output, round2(result.Score), round2(result.Coverage), candidate))
}

// processOutput mirrors _process_output.
func (t *OnboardingStageTracker) processOutput(ctx context.Context, output, startedStageName string, allowedNextStages []string, fullTranscript string) {
	if strings.ToLower(output) == "no" {
		t.sendRTVI(fmt.Sprintf("%s No transition for stage=%s", stageTrackerLogPrefix, startedStageName))
		return
	}

	if !stringSet(allowedNextStages)[output] {
		sentryutil.Capture(sentryutil.Event{
			Hub:     t.sentryHub(),
			Message: "Invalid stage transition tracker output",
			Tags: map[string]string{
				"conversation_id": t.conversationID,
				"current_stage":   startedStageName,
			},
			Details: map[string]any{
				"user_id":                         t.userID,
				"stage_transition_tracker_output": output,
				"allowed_next_stages":             allowedNextStages,
			},
		})
		t.sendRTVI(fmt.Sprintf("%s Invalid output=%q for stage=%s", stageTrackerLogPrefix, output, startedStageName))
		return
	}

	t.transitionMu.Lock()
	defer t.transitionMu.Unlock()
	if current := t.state.CurrentStage().Name; current != startedStageName {
		t.logf("%s Stale tracker result ignored: started_stage=%s, current_stage=%s",
			stageTrackerLogPrefix, startedStageName, current)
		t.sendRTVI(fmt.Sprintf("%s Stale output ignored: started=%s, current=%s",
			stageTrackerLogPrefix, startedStageName, current))
		return
	}

	t.sendRTVI(fmt.Sprintf("%s Transitioning %s => %s", stageTrackerLogPrefix, startedStageName, output))
	t.manager.processTransition(ctx, output, "stage_transition_tracker_"+uuid.NewString(), fullTranscript)
	t.sendRTVI(fmt.Sprintf("%s Transition complete %s => %s", stageTrackerLogPrefix, startedStageName, output))
}

// reportRunError is _run's generic `except Exception` arm: log + RTVI +
// Sentry, never crash the call. Context cancellation means the call is
// tearing down — log only, no Sentry noise.
func (t *OnboardingStageTracker) reportRunError(ctx context.Context, currentStageName string, err error) {
	t.logf("%s Error for stage=%s: %v", stageTrackerLogPrefix, currentStageName, err)
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return
	}
	t.sendRTVI(fmt.Sprintf("%s Error for stage=%s: %s",
		stageTrackerLogPrefix, currentStageName, runePrefix(err.Error(), 80)))
	sentryutil.Capture(sentryutil.Event{
		Hub: t.sentryHub(),
		Err: err,
		Tags: map[string]string{
			"component": "disha_onboarding",
			"operation": "stage_transition_tracker",
		},
		Details: map[string]any{
			"conversation_id": t.conversationID,
			"user_id":         t.userID,
			"current_stage":   currentStageName,
		},
	})
}

func (t *OnboardingStageTracker) recoverToSentry(operation string) {
	p := recover()
	if p == nil {
		return
	}
	err := fmt.Errorf("disha: panic in %s: %v", operation, p)
	t.logf("%v", err)
	sentryutil.Capture(sentryutil.Event{
		Hub: t.sentryHub(),
		Err: err,
		Tags: map[string]string{
			"component": "disha_onboarding",
			"operation": operation,
		},
		Details: map[string]any{
			"conversation_id": t.conversationID,
			"user_id":         t.userID,
		},
	})
}

func (t *OnboardingStageTracker) sendRTVI(message string) {
	_, _, ui := t.infrastructure()
	if ui != nil {
		ui.ServerMessage(message, time.Now())
	}
}

func (t *OnboardingStageTracker) logf(format string, args ...any) {
	if t.logger != nil {
		t.logger.Printf(format+"\n", args...)
	}
}

// formatTrackerNextStages mirrors _format_next_stages: "- {stage}" lines.
func formatTrackerNextStages(nextStages []string) string {
	lines := make([]string, 0, len(nextStages))
	for _, stage := range nextStages {
		lines = append(lines, "- "+stage)
	}
	return strings.Join(lines, "\n")
}

// onboardingTranscript mirrors onboarding_pipeline_manager._get_transcript:
// skip the first (system) message unconditionally, keep only user/assistant
// messages with non-empty content, window AFTER filtering, label assistant
// lines "disha: " and user lines "patient: ". maxTurns < 0 means no
// windowing (Python's max_turns=None); 0 yields an empty transcript
// (Python's explicit `else []` branch).
func onboardingTranscript(messages []voicepipelinecore.Message, maxTurns int) string {
	if len(messages) > 0 {
		messages = messages[1:]
	}
	filtered := make([]voicepipelinecore.Message, 0, len(messages))
	for _, m := range messages {
		if m.Content == "" {
			continue
		}
		if m.Role != "assistant" && m.Role != "user" {
			continue
		}
		filtered = append(filtered, m)
	}
	if maxTurns >= 0 {
		if maxTurns == 0 {
			filtered = nil
		} else if len(filtered) > maxTurns {
			filtered = filtered[len(filtered)-maxTurns:]
		}
	}
	lines := make([]string, 0, len(filtered))
	for _, m := range filtered {
		speaker := "patient"
		if m.Role == "assistant" {
			speaker = "disha"
		}
		lines = append(lines, speaker+": "+m.Content)
	}
	return strings.Join(lines, "\n")
}
