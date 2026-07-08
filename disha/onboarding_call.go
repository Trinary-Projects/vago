package disha

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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
)

// OnboardingCallBot is the Disha onboarding-call assembly (the tracker
// architecture port of bots/onboarding_call). Phase 3 scope: fresh calls
// on the variant's start stage — the stage tracker (phase 4), deep
// thinking/careplan/resume (phase 5), and post-call state (phase 6) land
// on top of this skeleton.
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

	// Phase 3: fresh state on the start stage only. Resume
	// (ConversationStateFromResume + chunk replay) lands in phase 5.
	state := NewConversationState(config, variant)
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
	callbacks.SetCurrentAgendaProvider(func() string { return state.CurrentStage().Name })

	pl := &onboardingCallPlan{
		Startup:         startup,
		Config:          config,
		State:           state,
		Compiler:        compiler,
		InitialMessages: buildInitialMessages(compiled.Text, nil, ""),
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

// BuildTask assembles the onboarding pipeline: the follow-up shape
// (LLM → response timeout → output filter → TTS) with no talk-time
// monitor — Python's onboarding pipeline has no talk-time limit, and the
// StageThresholdProcessor is tool-call-architecture-only (not ported).
func (b OnboardingCallBot) BuildTask(ctx context.Context, req BotTaskRequest, deps Deps) (*voicepipelinecore.PipelineTask, error) {
	pl, err := b.plan(ctx, req.ConversationID, deps)
	if err != nil {
		return nil, err
	}

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

	var room voicepipelinecore.RoomTransport
	if isDailyRoomURL(req.RoomURL) {
		room, err = voicepipelinecore.JoinDailyRoom(req.RoomURL, req.RoomToken, taskCtx, audioSource)
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
func newOnboardingLLMClient(deps Deps, pl *onboardingCallPlan) (voicepipelinecore.LLMClient, error) {
	return llmrouter.New(llmrouter.Config{
		Group:          pl.Config.Model,
		Region:         "us",
		Redis:          deps.Redis,
		Logger:         pl.Startup.Logger,
		LogSink:        newLLMLogSink(deps.API, pl.Startup.Logger, onboardingUsecaseType, pl.Startup.UserID, pl.Startup.ConversationID),
		PromptMetadata: pl.PromptMetadata,
	})
}
