package disha

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

const (
	FollowUpBotType = "follow_up"

	followUpUsecaseType     = "followup_call_conversation"
	followUpGuidanceUsecase = "followup_call_get_guidance_tool"

	followUpModelGroup              = "gemini-flash-3.1-lite"
	followUpPhoneOverrideModelGroup = "gpt-4.1"
	followUpDynamicModelGroup       = "followup-dynamic-gemma"
	followUpGuidanceModelGroup      = "gpt-oss120-fast"

	followUpPromptDefault            = "followup_call/system_prompt"
	followUpPromptD1Inactive         = "disha_init_calls/d0_d1_inactive_user/call_main_sys"
	followUpPromptD1InactivePCOS     = "disha_init_calls/d0_d1_inactive_user/weight_loss_pcos_users"
	followUpPromptInvestorDemo       = "misc/investor_demo"
	followUpDynamicMainPrompt        = "disha_init_calls/dynamic_checkin_call/main_sys"
	followUpGetGuidancePrompt        = "disha_init_calls/dynamic_checkin_call/tools/get_guidance"
	followUpPhonePromptOverridePhone = "+916261229421"
)

// FollowUpBot is the Disha follow-up-call assembly. It supports the legacy
// agenda-based follow-up path and the dynamic check-in treatment path gated
// by conversation.call_flow_key.
type FollowUpBot struct{}

var _ Bot = FollowUpBot{}

func (FollowUpBot) BotType() string { return FollowUpBotType }

type followUpPlan struct {
	Startup         CallStartup
	InitialMessages []voicepipelinecore.Message
	PhoneticDict    map[string]string
	Callbacks       *CallEventCallbacks
	PromptKey       string
	PromptMetadata  map[string]any
	ModelGroup      string
	Dynamic         bool
	Tools           []voicepipelinecore.ToolDefinition
}

func (b FollowUpBot) plan(ctx context.Context, conversationID string, deps Deps) (*followUpPlan, error) {
	startup, err := collectCallStartup(ctx, conversationID, FollowUpBotType, deps)
	if err != nil {
		return nil, err
	}

	prompt, promptName, promptVersion, promptConfig, variables, dynamic, err := loadFollowUpPrompt(ctx, deps, startup)
	if err != nil {
		return nil, err
	}
	startup.Logger.Printf("disha: follow-up prompt selected name=%s version=%d dynamic=%v\n", promptName, promptVersion, dynamic)

	resumeMsg := buildResumeSystemMessage(startup.Data, time.Now())
	if resumeMsg != "" {
		startup.Logger.Println("disha: appending follow-up resume system message")
	}

	metadata := buildPromptTraceMetadata("system", promptName, promptVersion, variables)

	modelGroup := followUpModelGroup
	if dynamic {
		modelGroup = followUpDynamicModelGroup
	} else if promptName == followUpPromptInvestorDemo {
		modelGroup = followUpPhoneOverrideModelGroup
	}

	tools, err := buildCallToolDefinitionsFromConfig(promptConfig)
	if err != nil {
		return nil, err
	}

	pl := &followUpPlan{
		Startup:         startup,
		InitialMessages: buildInitialMessages(prompt, startup.Data.Chunks, resumeMsg),
		PromptKey:       PromptKey(promptName, promptVersion),
		PromptMetadata:  metadata,
		ModelGroup:      modelGroup,
		Dynamic:         dynamic,
		Tools:           tools,
		Callbacks: NewCallEventCallbacks(
			startup,
			deps.Redis,
			deps.API,
			NewDebugLogUploaderFromEnv(startup.Logger, startup.ConversationID),
		),
	}
	if deps.PhoneticDict != nil {
		pl.PhoneticDict = deps.PhoneticDict.Dictionary(ctx)
	}
	return pl, nil
}

func (b FollowUpBot) BuildTask(ctx context.Context, req BotTaskRequest, deps Deps) (*voicepipelinecore.PipelineTask, error) {
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
	stt := voicepipelinecore.NewSTTProcessor(taskCtx) // Soniox only, by design.

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
	llmClient, err := newFollowUpLLMClient(deps, pl)
	if err != nil {
		task.Abort()
		return nil, err
	}
	llm := voicepipelinecore.NewLLMProcessorWithClient(taskCtx, llmClient)
	registerFollowUpTools(llm, task, deps, pl)
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

func loadFollowUpPrompt(ctx context.Context, deps Deps, startup CallStartup) (text, name string, version int, config map[string]any, vars DocumentVariables, dynamic bool, err error) {
	if deps.Documents == nil {
		err = errors.New("disha: document store is required to load follow-up prompt")
		return
	}
	dynamic = strings.TrimSpace(startup.Data.Conversation.CallFlowKey) != ""
	var callFlow string
	if dynamic {
		callFlow, err = downloadCompiledCallFlow(ctx, deps.S3, startup.Data.Conversation.CompiledCallFlowS3Key)
		if err != nil {
			return
		}
		name = followUpDynamicMainPrompt
	} else {
		name = followUpPromptName(startup.Data)
	}
	vars = followUpPromptVariables(startup.Data, callFlow)
	text, version, config, err = deps.Documents.GetDocumentWithConfig(ctx, name, 0, vars)
	if err != nil {
		err = fmt.Errorf("disha: load follow-up prompt %q: %w", name, err)
		return
	}
	if strings.TrimSpace(text) == "" {
		err = fmt.Errorf("disha: follow-up prompt %q is empty", name)
	}
	return
}

func followUpPromptName(data *ConversationData) string {
	if data == nil {
		return followUpPromptDefault
	}
	if data.UserProfile.Phone == followUpPhonePromptOverridePhone {
		return followUpPromptInvestorDemo
	}
	switch strings.TrimSpace(data.Conversation.Agenda) {
	case "d1_inactive_checkin":
		return followUpPromptD1Inactive
	case "d1_inactive_checkin_weight_loss_pcos":
		return followUpPromptD1InactivePCOS
	default:
		return followUpPromptDefault
	}
}

func followUpPromptVariables(data *ConversationData, callFlow string) DocumentVariables {
	user := data.UserProfile
	gender := strings.ToLower(strings.TrimSpace(user.Gender))
	name := firstNonEmptyString(user.DevanagariName, user.FirstName, user.Name)
	patientExecutiveProfile := derefString(user.PatientExecutiveProfile)
	var callFlowValue any
	if callFlow != "" {
		callFlowValue = callFlow
	}
	return DocumentVariables{
		"patient_info":              data.Conversation.PatientInfo,
		"patient_memory":            patientExecutiveProfile,
		"current_datetime":          time.Now().In(istLocation()).Format("2 Jan 2006 03:04 PM"),
		"diet_chart_xml":            user.LastDietChartXML,
		"patient_executive_profile": patientExecutiveProfile,
		"active_chat_context":       derefString(user.ActiveChatContext),
		"patient_name":              name,
		"patient_schedule":          patientScheduleFromSlots(user.IdealCallTimeSlots),
		"he_she":                    subjectPronoun(gender),
		"him_her":                   objectPronoun(gender),
		"his_her":                   possessivePronoun(gender),
		"call_flow":                 callFlowValue,
	}
}

// patientScheduleFromSlots mirrors fetch_conversation.py:
// `_slots = ideal_call_time_slots or {}; _slots.get("checkin_slots") or _slots`.
func patientScheduleFromSlots(slots map[string]any) any {
	if v, ok := slots["checkin_slots"]; ok && isTruthyJSON(v) {
		return v
	}
	if slots == nil {
		return map[string]any{}
	}
	return slots
}

func subjectPronoun(gender string) string {
	switch gender {
	case "male":
		return "he"
	case "female":
		return "she"
	default:
		return "them"
	}
}

func objectPronoun(gender string) string {
	switch gender {
	case "male":
		return "him"
	case "female":
		return "her"
	default:
		return "them"
	}
}

func possessivePronoun(gender string) string {
	switch gender {
	case "male":
		return "his"
	case "female":
		return "her"
	default:
		return "their"
	}
}

func downloadCompiledCallFlow(ctx context.Context, s3 S3GetClient, key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", errors.New("disha: dynamic follow-up conversation has no compiled_call_flow_s3_key")
	}
	if s3 == nil {
		return "", errors.New("disha: S3 client is required to load compiled call flow")
	}
	raw, err := s3.GetObject(ctx, "", key)
	if err != nil {
		return "", fmt.Errorf("disha: download compiled call flow %q: %w", key, err)
	}
	if strings.TrimSpace(string(raw)) == "" {
		return "", fmt.Errorf("disha: compiled call flow %q is empty", key)
	}
	return string(raw), nil
}

func newFollowUpLLMClient(deps Deps, pl *followUpPlan) (voicepipelinecore.LLMClient, error) {
	return llmrouter.New(llmrouter.Config{
		Group:          pl.ModelGroup,
		Region:         "us",
		Redis:          deps.Redis,
		Logger:         pl.Startup.Logger,
		LogSink:        newLLMLogSink(deps.API, pl.Startup.Logger, followUpUsecaseType, pl.Startup.UserID, pl.Startup.ConversationID),
		PromptMetadata: pl.PromptMetadata,
	})
}

func registerFollowUpTools(llm *voicepipelinecore.LLMProcessor, task *voicepipelinecore.PipelineTask, deps Deps, pl *followUpPlan) {
	for _, def := range pl.Tools {
		switch def.Function.Name {
		case "get_guidance":
			llm.RegisterTool(def, func(ctx context.Context, req voicepipelinecore.ToolCallRequest) (voicepipelinecore.ToolCallResponse, error) {
				situation, _ := req.Arguments["situation"].(string)
				text, err := getFollowUpGuidance(ctx, deps, pl, situation)
				if err != nil {
					return voicepipelinecore.ToolCallResponse{}, err
				}
				return voicepipelinecore.ToolCallResponse{Result: text, RunLLM: true}, nil
			}, voicepipelinecore.ToolOptions{CancelOnInterruption: false, Timeout: 30 * time.Second})
		case endCallToolName:
			registerEndCallTool(llm, task, def)
		}
	}
}

func getFollowUpGuidance(ctx context.Context, deps Deps, pl *followUpPlan, situation string) (string, error) {
	if strings.TrimSpace(situation) == "" {
		situation = "No situation provided."
	}
	if deps.Documents == nil {
		return "", errors.New("disha: document store is required for get_guidance")
	}
	systemVariables := followUpGuidancePromptVariables(pl, situation)
	systemPrompt, systemVersion, err := deps.Documents.GetDocument(ctx, followUpGetGuidancePrompt, 0, systemVariables)
	if err != nil {
		return "", err
	}
	metadata := buildPromptTraceMetadata("system", followUpGetGuidancePrompt, systemVersion, systemVariables)
	metadata["user_prompt_name"] = "raw_situation"
	metadata["user_prompt_variables"] = DocumentVariables{"situation": situation}
	req := voicepipelinecore.LLMRequest{Messages: []voicepipelinecore.Message{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: situation},
	}}
	client, err := llmrouter.New(llmrouter.Config{
		Group:          followUpGuidanceModelGroup,
		Region:         "us",
		Redis:          deps.Redis,
		Logger:         pl.Startup.Logger,
		LogSink:        newLLMLogSink(deps.API, pl.Startup.Logger, followUpGuidanceUsecase, pl.Startup.UserID, pl.Startup.ConversationID),
		PromptMetadata: metadata,
	})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	_, err = client.Stream(ctx, req, func(token string) { b.WriteString(token) })
	if err != nil {
		reportGuidanceLLMFailure(pl, err)
		return "", err
	}
	if strings.TrimSpace(b.String()) == "" {
		err = errors.New("disha: get_guidance returned empty response")
		reportGuidanceLLMFailure(pl, err)
		return "", err
	}
	return b.String(), nil
}

func followUpGuidancePromptVariables(pl *followUpPlan, situation string) DocumentVariables {
	return DocumentVariables{
		"situation":                 situation,
		"patient_executive_profile": derefString(pl.Startup.Data.UserProfile.PatientExecutiveProfile),
	}
}

// reportGuidanceLLMFailure sends get_guidance LLM failures to Sentry. The
// guidance router has no in-call failover (unlike Python's
// generate_llm_response_with_failover), so a failed call must be visible.
// Context cancellation just means the call ended mid-tool — not an error.
func reportGuidanceLLMFailure(pl *followUpPlan, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	sentryutil.Capture(sentryutil.Event{
		Err: err,
		Tags: map[string]string{
			"component": "disha_followup",
			"operation": "get_guidance_llm",
		},
		Details: map[string]any{
			"conversation_id": pl.Startup.ConversationID,
			"user_id":         pl.Startup.UserID,
			"model_group":     followUpGuidanceModelGroup,
		},
	})
}
