package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

const (
	OnboardingCallBotType = "onboarding_call"

	// onboardingUsecaseType matches Python's
	// UsecaseType.ONBOARDING_CALL_CONVERSATION.
	onboardingUsecaseType = "onboarding_call_conversation"

	// onboardingDatetimeFormat mirrors Python's
	// `%B %d, %Y %I:%M %p` (prompt_parser.py) in IST.
	onboardingDatetimeFormat = "January 02, 2006 03:04 PM"

	// onboardingResumeDefaultAgenda mirrors Python's
	// resumed_chunk_state.setdefault("agenda", "Introduction") before
	// ConversationState.from_resume. Note this is Python's literal
	// default value, not necessarily a stage name present in every
	// variant config (student_test's start stage is "introduction",
	// lower-case) — a miss here falls through to the start-stage
	// fallback in ConversationStateFromResume, matching Python's own
	// behavior when the default doesn't resolve either.
	onboardingResumeDefaultAgenda = "Introduction"

	// onboardingTestUserExclusionID mirrors onboarding_pipeline_manager.
	// _is_call_completed's hardcoded QA/test-user exclusion: this user's
	// calls are never reported as onboarding_call_done, even on an end
	// stage with talk time, so QA traffic on this user doesn't trip
	// completion-dependent downstream automation.
	onboardingTestUserExclusionID = "4da9a570-e993-48fe-b1dd-7e1823488325"

	// Onboarding-specific resume texts, byte-exact ports of
	// bots/onboarding_call/conversation_context_manager.py's resume
	// branches (distinct wording/formatting from the shared sales/
	// follow-up texts in call_startup.go, including the "1.If" typo with
	// no space).
	onboardingResumeMessageWithinWindow = "The conversation might have interrupted a few mins ago. Here's how to resume, follow carefully:\n" +
		"1.If the interruption was user initiated(like \"ill call you back, give me a min\") then say something like 'hanji to aap keh rhe the' and resume. Make sure to not acknowledge their interrupt request(this already happened), just continue.\n" +
		"2. If not, make sure to first acknowledge the call being disconnected and then continue."

	onboardingResumeMessageAfterWindow = "This conversation was interrupted because the call ended. Now you have to resume this conversation by saying hi and acknowledge the things that have been discussed very briefly and inform the next agenda. Then ask the user if we should continue further"
)

// buildOnboardingResumeMessage is buildResumeSystemMessage (call_startup.go)
// with onboarding's distinct resume texts (conversation_context_manager.py's
// resume branches differ in wording/formatting from the shared sales/
// follow-up ones). Gate logic — graceful-only, parseable `created`, the
// 5-minute window — is identical and shared via resumeMessageGate. Returns
// "" when no resume nudge is needed; otherwise the text is wrapped in
// <system_instruction>...</system_instruction> and is ready to append
// verbatim via buildInitialMessages.
func buildOnboardingResumeMessage(data *ConversationData, now time.Time) string {
	withinWindow, ok := resumeMessageGate(data, now)
	if !ok {
		return ""
	}
	text := onboardingResumeMessageAfterWindow
	if withinWindow {
		text = onboardingResumeMessageWithinWindow
	}
	return "<system_instruction>" + text + "</system_instruction>"
}

// onboardingResumeS3Getter builds the client used to download onboarding
// resume state from the US bucket. A package var — like sttDialURL/
// ttsDialURL — so tests can inject a fake S3GetClient instead of
// exercising real S3/network.
var onboardingResumeS3Getter = func(logger *log.Logger) S3GetClient {
	return NewS3GetClientFromEnv(logger, "AWS_US_BUCKET_NAME", "AWS_US_REGION")
}

// OnboardingCallBot is the Disha onboarding-call assembly (the tracker
// architecture port of bots/onboarding_call): stage machine (phase 4),
// deep thinking/careplan/resume (phase 5), and post-call state (phase 6:
// onboarding_call_done/latest_onboarding_call_stage/intensity levels/
// conversation_variables) are wired in on top of the phase-3 skeleton.
type OnboardingCallBot struct{}

var _ Bot = OnboardingCallBot{}

func (OnboardingCallBot) BotType() string { return OnboardingCallBotType }

// onboardingPromptCompiler is the Go port of
// bots/onboarding_call/prompt_parser.py: render the current stage's
// prompt, then embed it as `analysis` in the main system prompt. Built
// once per call; the phase-4 stage manager recompiles through the same
// instance on every transition.
type onboardingPromptCompiler struct {
	docs        *DocumentStore
	config      *OnboardingConfig
	patientInfo string
	profileVars map[string]any
}

type compiledOnboardingPrompt struct {
	Text string
	// Resolved document versions. Python's metadata carries the
	// config-pinned versions and omits unpinned ones; we always send the
	// versions Vago actually resolved (identical when pinned, strictly
	// more correct when not).
	MainVersion  int
	StageVersion int
	// MetadataVars is Python's `system_prompt_variables` shape:
	// patient_info + current_datetime + profile variables + variable
	// store. `analysis` is deliberately excluded — the stage prompt's
	// identity travels as stage_prompt_name/stage_prompt_version instead
	// (conversation_context_manager._set_prompt_metadata).
	MetadataVars DocumentVariables
}

func (p *onboardingPromptCompiler) CompileSystemPrompt(ctx context.Context, stage *StageConfig, variables map[string]any) (compiledOnboardingPrompt, error) {
	if p.docs == nil {
		return compiledOnboardingPrompt{}, errors.New("disha: document store is required to compile onboarding prompt")
	}
	merged := make(DocumentVariables, len(p.profileVars)+len(variables))
	for k, v := range p.profileVars {
		merged[k] = v
	}
	for k, v := range variables {
		merged[k] = v
	}

	stageText, stageVersion, err := p.docs.GetDocument(ctx, stage.Prompt.Name, stage.Prompt.Version, merged)
	if err != nil {
		return compiledOnboardingPrompt{}, fmt.Errorf("disha: compile stage prompt %q: %w", stage.Prompt.Name, err)
	}

	currentDatetime := time.Now().In(istLocation()).Format(onboardingDatetimeFormat)
	mainVars := DocumentVariables{
		"analysis":         stageText,
		"patient_info":     p.patientInfo,
		"current_datetime": currentDatetime,
	}
	// Python's `**merged` comes last: store/profile values win even over
	// patient_info/current_datetime on key collision.
	for k, v := range merged {
		mainVars[k] = v
	}

	text, mainVersion, err := p.docs.GetDocument(ctx, p.config.MainSystemPrompt.Name, p.config.MainSystemPrompt.Version, mainVars)
	if err != nil {
		return compiledOnboardingPrompt{}, fmt.Errorf("disha: compile main system prompt %q: %w", p.config.MainSystemPrompt.Name, err)
	}
	if strings.TrimSpace(text) == "" {
		return compiledOnboardingPrompt{}, fmt.Errorf("disha: main system prompt %q rendered empty", p.config.MainSystemPrompt.Name)
	}

	metadataVars := DocumentVariables{
		"patient_info":     p.patientInfo,
		"current_datetime": currentDatetime,
	}
	for k, v := range merged {
		metadataVars[k] = v
	}

	return compiledOnboardingPrompt{
		Text:         text,
		MainVersion:  mainVersion,
		StageVersion: stageVersion,
		MetadataVars: metadataVars,
	}, nil
}

// onboardingCallPlan is the resolved, room-independent configuration for
// an onboarding call, mirroring salesCallPlan/followUpPlan.
type onboardingCallPlan struct {
	Startup         CallStartup
	Config          *OnboardingConfig
	State           *ConversationState
	Compiler        *onboardingPromptCompiler
	InitialMessages []voicepipelinecore.Message
	PhoneticDict    map[string]string
	Callbacks       *CallEventCallbacks
	PromptKey       string
	PromptMetadata  map[string]any
	Tools           []voicepipelinecore.ToolDefinition
}

func (b OnboardingCallBot) plan(ctx context.Context, conversationID string, deps Deps) (*onboardingCallPlan, error) {
	startup, err := collectCallStartup(ctx, conversationID, OnboardingCallBotType, deps)
	if err != nil {
		return nil, err
	}

	// Python raises when the profile has no variant
	// (onboarding_pipeline_manager.load_conversation).
	variant := strings.TrimSpace(derefString(startup.Data.UserProfile.OnboardingCallVariant))
	if variant == "" {
		return nil, errors.New("disha: onboarding call requires onboarding_call_variant on user profile")
	}

	config, err := LoadOnboardingConfig(ctx, deps.Documents, variant)
	if err != nil {
		return nil, err
	}

	state := resolveOnboardingState(ctx, startup.Data, config, variant, startup.Logger)
	stage := state.CurrentStage()
	startup.Logger.Printf("disha: onboarding variant=%s model=%s start_stage=%s\n", variant, config.Model, stage.Name)

	compiler := &onboardingPromptCompiler{
		docs:        deps.Documents,
		config:      config,
		patientInfo: startup.Data.Conversation.PatientInfo,
		profileVars: map[string]any{"gender": startup.Data.UserProfile.Gender},
	}
	compiled, err := compiler.CompileSystemPrompt(ctx, stage, state.VariableStoreSnapshot())
	if err != nil {
		return nil, err
	}

	callbacks := NewCallEventCallbacks(
		startup,
		deps.Redis,
		deps.API,
		NewDebugLogUploaderFromEnv(startup.Logger, startup.ConversationID),
	)
	callbacks.SetChunkDecorator(newOnboardingChunkDecorator(
		state, NewUSBucketJSONUploaderFromEnv(startup.Logger), startup.UserID, startup.ConversationID, startup.Logger,
	))
	callbacks.SetPostCallDecorator(newOnboardingPostCallDecorator(state, startup.UserID))

	resumeMsg := buildOnboardingResumeMessage(startup.Data, time.Now())
	pl := &onboardingCallPlan{
		Startup:         startup,
		Config:          config,
		State:           state,
		Compiler:        compiler,
		InitialMessages: buildInitialMessages(compiled.Text, startup.Data.Chunks, resumeMsg),
		PromptKey:       PromptKey(config.MainSystemPrompt.Name, compiled.MainVersion),
		PromptMetadata:  buildOnboardingPromptMetadata(config, stage, compiled),
		Tools:           []voicepipelinecore.ToolDefinition{onboardingEndCallTool()},
		Callbacks:       callbacks,
	}
	if deps.PhoneticDict != nil {
		pl.PhoneticDict = deps.PhoneticDict.Dictionary(ctx)
	}
	return pl, nil
}

// newOnboardingChunkDecorator returns the onboarding CallEventCallbacks
// chunk_decorator: it stamps the live stage name onto every persisted
// chunk (current_agenda, Python's conversation_state.current_stage.name)
// and, when uploader is available, uploads a conversation-state snapshot
// to S3 BEFORE the chunk is written to Redis (the key must never point to
// a missing object), mirroring Python's ConversationPersistenceProcessor.
// A nil uploader (env incomplete) still sets current_agenda but skips the
// upload — the chunk keeps a null conversation_state_s3_key.
func newOnboardingChunkDecorator(state *ConversationState, uploader JSONUploader, userID, conversationID string, logger *log.Logger) func(*ConversationChunk) {
	return func(chunk *ConversationChunk) {
		agenda := state.CurrentStage().Name
		chunk.CurrentAgenda = &agenda
		if uploader == nil {
			return
		}
		stateDict := state.ToPersistDict()
		stateDict["user_id"] = userID
		stateDict["conversation_id"] = conversationID
		objectKey := fmt.Sprintf("conversation_state/%s/%s.json", conversationID, chunk.ID)
		ctx, cancel := context.WithTimeout(context.Background(), defaultS3UploadTimeout)
		defer cancel()
		if err := uploader.UploadJSON(ctx, objectKey, stateDict); err != nil {
			wrapped := fmt.Errorf("disha: conversation state upload: %w", err)
			sentryutil.Capture(sentryutil.Event{
				Err:  wrapped,
				Tags: map[string]string{"component": "disha_s3", "operation": "conversation_state_upload"},
				Details: map[string]any{
					"conversation_id": conversationID,
					"chunk_id":        chunk.ID,
				},
			})
			if logger != nil {
				logger.Printf("disha: conversation state upload failed conversation=%s chunk=%s: %v\n", conversationID, chunk.ID, wrapped)
			}
			return
		}
		chunk.ConversationStateS3Key = &objectKey
	}
}

// newOnboardingPostCallDecorator returns the onboarding CallEventCallbacks
// post_call_decorator: it reads the live stage/variable-store snapshot at
// invocation time (call end) and fills the onboarding-only
// run_post_call_operations fields, mirroring base_pipeline_manager.
// perform_cleanup + onboarding_pipeline_manager._is_call_completed. Any
// still-in-flight non-blocking deep thinking is racy in Python too, so Go
// does not wait for it either.
func newOnboardingPostCallDecorator(state *ConversationState, userID string) func(*PostCallOperationsRequest) {
	return func(req *PostCallOperationsRequest) {
		stage := state.CurrentStage()
		stageName := stage.Name
		req.LatestOnboardingCallStage = &stageName
		intensity := state.GetIntensityLevels()
		if v, ok := intensity["diet_plan_intensity_level"]; ok {
			req.DietPlanIntensityLevel = &v
		}
		if v, ok := intensity["fitness_plan_intensity_level"]; ok {
			req.FitnessPlanIntensityLevel = &v
		}
		req.ConversationVariables = state.GetConversationVariables()
		req.OnboardingCallDone = stage.IsEndStage &&
			req.TotalUserDuration > 0 &&
			userID != onboardingTestUserExclusionID
	}
}

// BuildTask assembles the onboarding pipeline: the follow-up shape
// (LLM → response timeout → output filter → TTS) with no talk-time
// monitor — Python's onboarding pipeline has no talk-time limit, and the
// StageThresholdProcessor is tool-call-architecture-only (not ported).
func (b OnboardingCallBot) BuildTask(ctx context.Context, req BotTaskRequest, deps Deps) (*voicepipelinecore.PipelineTask, error) {
	pl, err := b.plan(ctx, req.ConversationID, deps)
	if err != nil {
		return nil, err
	}

	// Stage machine (phase 4): manager + tracker are constructed before
	// NewPipelineTask because the CallEvents mapping is consumed there,
	// but they need the aggregator pair / router / UI emitter, which only
	// exist later — those are injected via SetInfrastructure (Python's
	// set_infrastructure pattern). Until then their entry points no-op.
	classifier, err := newOnboardingStageClassifier(deps, pl)
	if err != nil {
		return nil, err
	}
	// Deep thinking + careplan detection (phase 5): built with the
	// production hedged-client factories before the stage manager, which
	// only holds references to them (SetUI is wired on each manager
	// directly below, once the UI event sender exists).
	dtManager := NewOnboardingDeepThinkingManager(
		deps.Documents, pl.Callbacks, newDeepThinkingClientFactory(deps, pl.Startup.Logger, pl.Startup.UserID, pl.Startup.ConversationID),
		pl.Startup.Logger, pl.Startup.UserID, pl.Startup.ConversationID,
		pl.Startup.Data.Conversation.PatientInfo, pl.PromptKey,
	)
	careplanManager := NewOnboardingCarePlanManager(
		pl.Config, deps.Documents, deps.API, newCarePlanClientFactory(deps, pl.Startup.Logger, pl.Startup.UserID, pl.Startup.ConversationID),
		pl.Startup.Logger, pl.Startup.UserID, pl.Startup.ConversationID,
		pl.Startup.Data.Conversation.PatientInfo,
	)
	stageManager := NewOnboardingStageManager(
		pl.State, pl.Config, pl.Compiler, pl.Callbacks, deps.API, dtManager, careplanManager,
		pl.Startup.Logger, pl.Startup.UserID, pl.Startup.ConversationID, pl.PromptKey,
	)
	stageTracker := NewOnboardingStageTracker(
		pl.State, pl.Config, deps.Documents, stageManager, classifier,
		pl.Startup.Logger, pl.Startup.UserID, pl.Startup.ConversationID,
		pl.Startup.Data.Conversation.PatientInfo,
	)
	// CallEventCallbacks owns every callback in Events(); the tracker is
	// wired as its OnLLMCallCompleted handler (Python's pipeline manager
	// receiving on_llm_call_complete and delegating to the tracker) so the
	// mapping below stays the plain Events() used by sales/follow-up.
	pl.Callbacks.SetLLMCallCompletedHandler(stageTracker.OnLLMCallCompleted)

	task, err := voicepipelinecore.NewPipelineTask(ctx, voicepipelinecore.TaskConfig{
		Logger:     pl.Startup.Logger,
		SessionID:  pl.Startup.ConversationID,
		CallEvents: pl.Callbacks.Events(),
	})
	if err != nil {
		return nil, err
	}
	taskCtx := task.TaskCtx

	source := voicepipelinecore.NewPipelineSourceProcessor(taskCtx)
	task.AttachSource(source)
	audioSource := voicepipelinecore.NewAudioSourceProcessor(taskCtx)
	stt := voicepipelinecore.NewSTTProcessor(taskCtx)

	// Onboarding ends the call when the participant leaves, like
	// sales/follow-up — a deliberate delta from Python's rejoin-tolerant
	// onboarding (see AGENTS.md session-cleanup triggers). Onboarding is
	// Daily-only in practice, so the LiveKit branch below is left
	// unchanged/unused.
	var room voicepipelinecore.RoomTransport
	if isDailyRoomURL(req.RoomURL) {
		room, err = voicepipelinecore.JoinDailyRoom(req.RoomURL, req.RoomToken, taskCtx, audioSource, voicepipelinecore.DailyRoomOptions{EndOnParticipantLeft: true})
	} else {
		room, err = voicepipelinecore.JoinLiveKitRoom(req.RoomURL, req.RoomName, req.RoomToken, taskCtx, audioSource)
	}
	if err != nil {
		task.Abort()
		return nil, err
	}
	taskCtx.Room = room
	taskCtx.UIEvents.SetRoom(room)

	userIdle := voicepipelinecore.NewUserIdleProcessor(taskCtx)
	contextAggregators := voicepipelinecore.NewContextAggregatorPair(taskCtx, pl.InitialMessages, pl.PromptKey)
	llmClient, err := newOnboardingLLMClient(deps, pl)
	if err != nil {
		task.Abort()
		return nil, err
	}
	llm := voicepipelinecore.NewLLMProcessorWithClient(taskCtx, llmClient)
	registerOnboardingTools(llm, task, pl)

	// Late-bind the stage machine's infrastructure now that the pair,
	// conversation router, and UI event sender exist — before task
	// assembly completes, so the first LLM completion can already be
	// tracked.
	stageManager.SetInfrastructure(contextAggregators, llmClient, taskCtx.UIEvents)
	stageTracker.SetInfrastructure(taskCtx.Ctx, contextAggregators, taskCtx.UIEvents)
	dtManager.SetUI(taskCtx.UIEvents)
	careplanManager.SetUI(taskCtx.UIEvents)

	llmResponseTimeout := voicepipelinecore.NewLLMResponseTimeoutProcessor(taskCtx)
	llmOutputFilter := voicepipelinecore.NewLLMOutputFilterProcessor(taskCtx)
	tts := voicepipelinecore.NewTTSProcessor(taskCtx, pl.PhoneticDict)
	playback := voicepipelinecore.NewPlaybackSinkProcessor(taskCtx)
	sink := voicepipelinecore.NewPipelineSinkProcessor(taskCtx, task.CompleteEnd)

	pipeline := voicepipelinecore.NewPipeline([]voicepipelinecore.Processor{
		source,
		audioSource,
		stt,
		userIdle,
		contextAggregators.User(),
		llm,
		llmResponseTimeout,
		llmOutputFilter,
		tts,
		playback,
		contextAggregators.Assistant(),
		sink,
	})
	task.SetPipeline(source, pipeline)
	return task, nil
}

func buildOnboardingPromptMetadata(config *OnboardingConfig, stage *StageConfig, compiled compiledOnboardingPrompt) map[string]any {
	metadata := buildPromptTraceMetadata("system", config.MainSystemPrompt.Name, compiled.MainVersion, compiled.MetadataVars)
	metadata["stage_prompt_name"] = stage.Prompt.Name
	metadata["stage_prompt_version"] = compiled.StageVersion
	return metadata
}

// onboardingEndCallTool mirrors conversation_context_manager.
// _build_end_call_tool_schema — onboarding builds its single tool in
// code, not from prompt config_json.tools.
func onboardingEndCallTool() voicepipelinecore.ToolDefinition {
	return voicepipelinecore.ToolDefinition{
		Type: "function",
		Function: voicepipelinecore.ToolFunction{
			Name:        endCallToolName,
			Description: "End the call when the onboarding consultation is complete or the patient asks to end the call.",
			Parameters: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
				"required":   []string{},
			},
		},
	}
}

func registerOnboardingTools(llm *voicepipelinecore.LLMProcessor, task *voicepipelinecore.PipelineTask, pl *onboardingCallPlan) {
	for _, def := range pl.Tools {
		if def.Function.Name == endCallToolName {
			registerEndCallTool(llm, task, def)
		}
	}
}

// newOnboardingLLMClient builds the health-based router on the variant
// config's model group (student_test: grok-4.1-fast). Temperature stays
// the router default 0, matching Python's InputParams(temperature=0).
// It returns the concrete *llmrouter.Router (not the LLMClient
// interface) because the stage manager refreshes its prompt metadata via
// SetPromptMetadata after every stage transition.
func newOnboardingLLMClient(deps Deps, pl *onboardingCallPlan) (*llmrouter.Router, error) {
	return llmrouter.New(llmrouter.Config{
		Group:          pl.Config.Model,
		Region:         "us",
		Redis:          deps.Redis,
		Logger:         pl.Startup.Logger,
		LogSink:        newLLMLogSink(deps.API, pl.Startup.Logger, onboardingUsecaseType, pl.Startup.UserID, pl.Startup.ConversationID),
		PromptMetadata: pl.PromptMetadata,
	})
}

// newOnboardingStageClassifier builds the one-shot fixed-endpoint client
// for the stage tracker's "maybe" classifier, mirroring Python's
// generate_llm_response_with_failover(config_key=onboarding_stage_
// transition_tracker, max_tokens=32, temperature=0). PromptMetadata is
// set per call by the tracker (SetPromptMetadata) with the rendered
// prompt identity, so none is pinned here.
func newOnboardingStageClassifier(deps Deps, pl *onboardingCallPlan) (*llmrouter.Router, error) {
	temperature := 0.0
	maxTokens := 32
	return llmrouter.New(llmrouter.Config{
		FixedEndpoint: llmrouter.EndpointOpenRouterGemini25FlashLite,
		Redis:         deps.Redis,
		Logger:        pl.Startup.Logger,
		LogSink:       newLLMLogSink(deps.API, pl.Startup.Logger, stageTrackerUsecaseType, pl.Startup.UserID, pl.Startup.ConversationID),
		Temperature:   &temperature,
		MaxTokens:     &maxTokens,
	})
}

// resolveOnboardingState is the Go port of onboarding_pipeline_manager's
// resume path: when the conversation was resumed from a chunk carrying a
// conversation_state_s3_key, download and rebuild state from it via
// ConversationStateFromResume; on ANY failure along that path (no
// resumed chunk, missing/blank key, download error, invalid/empty JSON,
// or ConversationStateFromResume itself erroring), fall back to a fresh
// state on the variant's start stage and log + Sentry-report the
// fallback. The call always proceeds — chunk history replay
// (buildInitialMessages over startup.Data.Chunks) is driven
// independently by the caller and happens regardless of which state
// wins here, matching Python's "never abort on a bad resume" behavior.
func resolveOnboardingState(ctx context.Context, data *ConversationData, config *OnboardingConfig, variant string, logger *log.Logger) *ConversationState {
	fresh := func() *ConversationState { return NewConversationState(config, variant) }

	if data == nil {
		return fresh()
	}
	if strings.TrimSpace(derefString(data.Conversation.ResumedFromChunkID)) == "" {
		return fresh()
	}
	if len(data.ResumedChunk) == 0 {
		return fresh()
	}
	s3Key, _ := data.ResumedChunk["conversation_state_s3_key"].(string)
	s3Key = strings.TrimSpace(s3Key)
	if s3Key == "" {
		return fresh()
	}

	getter := onboardingResumeS3Getter(logger)
	resumeData, err := loadOnboardingResumeState(ctx, getter, s3Key, logger)
	if err != nil {
		reportOnboardingResumeFallback(logger, variant, s3Key, err)
		return fresh()
	}
	if len(resumeData) == 0 {
		reportOnboardingResumeFallback(logger, variant, s3Key, errors.New("onboarding resume state is empty"))
		return fresh()
	}
	// Python: resumed_chunk_state.setdefault("agenda", "Introduction").
	if agenda, _ := resumeData["agenda"].(string); strings.TrimSpace(agenda) == "" {
		resumeData["agenda"] = onboardingResumeDefaultAgenda
	}

	// Python's cache_care_plan_prompts pre-warms a local Langfuse doc
	// cache here after a care plan is restored; Go's DocumentStore reads
	// pre-rendered Redis keys on demand with its own TTL cache, so there
	// is nothing to pre-warm.
	state, err := ConversationStateFromResume(config, variant, resumeData, logger)
	if err != nil {
		reportOnboardingResumeFallback(logger, variant, s3Key, err)
		return fresh()
	}
	return state
}

// loadOnboardingResumeState downloads and parses the conversation-state
// JSON blob a prior onboarding call chunk wrote to the US bucket
// (conversation_state/{conversation_id}/{chunk_id}.json). Returns an
// error — never partial state — so callers can uniformly fall back to a
// fresh state.
func loadOnboardingResumeState(ctx context.Context, getter S3GetClient, s3Key string, logger *log.Logger) (map[string]any, error) {
	if getter == nil {
		return nil, errors.New("disha: onboarding resume state client is unavailable")
	}
	s3Key = strings.TrimSpace(s3Key)
	if s3Key == "" {
		return nil, errors.New("disha: onboarding resume state s3 key is empty")
	}
	body, err := getter.GetObject(ctx, "", s3Key)
	if err != nil {
		return nil, fmt.Errorf("disha: download onboarding resume state %q: %w", s3Key, err)
	}
	var state map[string]any
	if err := json.Unmarshal(body, &state); err != nil {
		return nil, fmt.Errorf("disha: parse onboarding resume state %q: %w", s3Key, err)
	}
	if logger != nil {
		logger.Printf("disha: onboarding resume state loaded from %s (%d bytes)\n", s3Key, len(body))
	}
	return state, nil
}

// reportOnboardingResumeFallback logs and Sentry-reports a resume
// failure that falls back to a fresh onboarding state. Never fatal —
// the call proceeds on the start stage with chunk history still
// replayed.
func reportOnboardingResumeFallback(logger *log.Logger, variant, s3Key string, err error) {
	wrapped := fmt.Errorf("disha: onboarding resume state fallback (variant=%s key=%s): %w", variant, s3Key, err)
	if logger != nil {
		logger.Println(wrapped.Error())
	}
	sentryutil.Capture(sentryutil.Event{
		Err: wrapped,
		Tags: map[string]string{
			"component": "disha_onboarding",
			"operation": "onboarding_resume_state",
		},
		Details: map[string]any{
			"variant": variant,
			"s3_key":  s3Key,
		},
	})
}
