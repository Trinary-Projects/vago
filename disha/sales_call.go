package disha

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

const (
	SalesCallBotType        = "sales_call"
	lifetimeTalkTimeSeconds = 600

	// salesModelGroup is the LLM model group the sales call selects from.
	// Matches Python's model_group="grok-4.1-fast-sales".
	salesModelGroup = "grok-4.1-fast-sales"

	// salesUsecaseType matches Python's
	// UsecaseType.SALES_CALL_CONVERSATION; it tags this call's LLM logs.
	salesUsecaseType = "sales_call_conversation"

	// Prompt names in the Disha document store. Same as Python's
	// bots/sales_call/sales_call.py.
	salesPrompt21Rs    = "sales_call/main_sys-3day_21rs_v2"
	salesPrompt299Rs   = "sales_call/main_sys-30day_299rs_v2"
	salesPromptDefault = "sales_call/main_sys-3day_v2"

	// Campaign-pricing-experiment values, from Python
	// users/models.py:UserProfile.CampaignPricingExperimentFlag.
	campaignFlag21For3Days   = "21_for_3_days"
	campaignFlag299For30Days = "299_for_30_days"
)

// SalesCallBot is the Disha-specific assembly of a sales call. It loads
// the conversation, selects the dynamic prompt, builds the initial LLM
// context (with resume handling), and wires the call-event callbacks
// that talk to Redis + Disha's REST API, then assembles the voice
// pipeline from voicepipelinecore's frame processors.
type SalesCallBot struct{}

var _ Bot = SalesCallBot{}

func (SalesCallBot) BotType() string {
	return SalesCallBotType
}

// salesCallPlan is the resolved, room-independent configuration for a
// sales call. It is computed by plan() (which touches only Redis + the
// document store, so it is unit-testable without a Daily room) and then
// consumed by BuildTask() to construct the processors.
type salesCallPlan struct {
	Startup         CallStartup
	InitialMessages []voicepipelinecore.Message
	MaxTalkTime     time.Duration
	PhoneticDict    map[string]string
	Callbacks       *CallEventCallbacks
	PromptKey       string
	PromptMetadata  map[string]any
	Tools           []voicepipelinecore.ToolDefinition
}

// plan resolves everything that depends only on the conversation data:
// the dynamic prompt, the initial messages (incl. resume), the
// talk-time budget, the phonetic dictionary, and the call-event
// callbacks. It performs no room join, so it is safe to unit-test.
func (b SalesCallBot) plan(ctx context.Context, conversationID string, deps Deps) (*salesCallPlan, error) {
	startup, err := collectCallStartup(ctx, conversationID, SalesCallBotType, deps)
	if err != nil {
		return nil, err
	}

	prompt, promptName, promptVersion, promptConfig, promptVariables, err := loadSalesPrompt(ctx, deps.Documents, startup)
	if err != nil {
		return nil, err
	}
	startup.Logger.Printf("disha: sales prompt selected name=%s version=%d\n", promptName, promptVersion)
	promptKey := PromptKey(promptName, promptVersion)

	tools, err := buildCallToolDefinitionsFromConfig(promptConfig)
	if err != nil {
		return nil, err
	}

	resumeMsg := buildResumeSystemMessage(startup.Data, time.Now())
	if resumeMsg != "" {
		startup.Logger.Println("disha: appending resume system message")
	}

	pl := &salesCallPlan{
		Startup:         startup,
		InitialMessages: buildInitialMessages(prompt, startup.Data.Chunks, resumeMsg),
		MaxTalkTime:     salesTalkTimeLimit(startup.Data.UserProfile.RemainingSalesCallTalktimeSeconds),
		PromptKey:       promptKey,
		PromptMetadata:  buildPromptTraceMetadata("system", promptName, promptVersion, promptVariables),
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

// BuildTask assembles the sales-call pipeline. The processor list here
// is the Go equivalent of the Pipeline([...]) in bots/sales_call/
// sales_call.py: each processor is constructed with its own config and
// then linked into the chain.
func (b SalesCallBot) BuildTask(ctx context.Context, req BotTaskRequest, deps Deps) (*voicepipelinecore.PipelineTask, error) {
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

	// Source + audio input must exist before the room join: the room
	// transport pumps inbound audio frames into the audio source.
	source := voicepipelinecore.NewPipelineSourceProcessor(taskCtx)
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
	talkTime := voicepipelinecore.NewTalkTimeMonitoringProcessorWithMaxTalkTime(taskCtx, pl.MaxTalkTime)
	llmClient, err := newSalesLLMClient(deps, pl)
	if err != nil {
		task.Abort()
		return nil, err
	}
	llm := voicepipelinecore.NewLLMProcessorWithClient(taskCtx, llmClient)
	registerSalesTools(llm, task, pl)
	llmResponseTimeout := voicepipelinecore.NewLLMResponseTimeoutProcessor(taskCtx)
	tts := voicepipelinecore.NewTTSProcessor(taskCtx, pl.PhoneticDict)
	playback := voicepipelinecore.NewPlaybackSinkProcessor(taskCtx)
	sink := voicepipelinecore.NewPipelineSinkProcessor(taskCtx, task.CompleteEnd)

	pipeline := voicepipelinecore.NewPipeline([]voicepipelinecore.Processor{
		source,
		audioSource,
		stt,
		userIdle,
		contextAggregators.User(),
		talkTime,
		llm,
		llmResponseTimeout,
		tts,
		playback,
		contextAggregators.Assistant(),
		sink,
	})
	task.SetPipeline(source, pipeline)
	return task, nil
}

func isDailyRoomURL(roomURL string) bool {
	parsed, err := url.Parse(roomURL)
	if err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(parsed.Host), "daily.co")
}

// loadSalesPrompt picks the prompt name based on
// campaign_pricing_experiment_flag (matching Python) and fetches it
// from the document store. Variables passed to the template mirror
// `sales_call_variables` in sales_call.py.
func loadSalesPrompt(ctx context.Context, store *DocumentStore, startup CallStartup) (string, string, int, map[string]any, DocumentVariables, error) {
	name := salesPromptDefault
	switch derefString(startup.Data.UserProfile.CampaignPricingExperimentFlag) {
	case campaignFlag21For3Days:
		name = salesPrompt21Rs
	case campaignFlag299For30Days:
		name = salesPrompt299Rs
	}

	if store == nil {
		return "", "", 0, nil, nil, fmt.Errorf("disha: document store is required to load %q", name)
	}

	vars := DocumentVariables{
		"patient_info":      startup.Data.Conversation.PatientInfo,
		"current_datetime":  time.Now().In(istLocation()).Format("2 Jan 2006 15:04:05"),
		"short_term_memory": derefString(startup.Data.UserProfile.PatientExecutiveProfile),
	}
	text, version, config, err := store.GetDocumentWithConfig(ctx, name, 0, vars)
	if err != nil {
		return "", "", 0, nil, nil, fmt.Errorf("disha: load sales prompt %q: %w", name, err)
	}
	if strings.TrimSpace(text) == "" {
		return "", "", 0, nil, nil, fmt.Errorf("disha: sales prompt %q is empty", name)
	}
	return text, name, version, config, vars, nil
}

func registerSalesTools(llm *voicepipelinecore.LLMProcessor, task *voicepipelinecore.PipelineTask, pl *salesCallPlan) {
	for _, def := range pl.Tools {
		if def.Function.Name == endCallToolName {
			registerEndCallTool(llm, task, def)
		}
	}
}

// newSalesLLMClient builds the health-based LLM router for the sales
// call (model group grok-4.1-fast-sales, region us, temperature 0).
func newSalesLLMClient(deps Deps, pl *salesCallPlan) (voicepipelinecore.LLMClient, error) {
	startup := pl.Startup
	logger := startup.Logger
	router, err := llmrouter.New(llmrouter.Config{
		Group:          salesModelGroup,
		Region:         "us",
		Redis:          deps.Redis,
		Logger:         logger,
		LogSink:        newLLMLogSink(deps.API, logger, salesUsecaseType, startup.UserID, startup.ConversationID),
		PromptMetadata: pl.PromptMetadata,
	})
	if err != nil {
		if logger != nil {
			logger.Printf("disha: LLM router init failed: %v\n", err)
		}
		return nil, fmt.Errorf("disha: create sales LLM router: %w", err)
	}
	return router, nil
}

func salesTalkTimeLimit(remainingSeconds *float64) time.Duration {
	seconds := float64(lifetimeTalkTimeSeconds)
	if remainingSeconds != nil {
		seconds = *remainingSeconds
		if seconds < 0 {
			seconds = 0
		}
	}
	return time.Duration(seconds * float64(time.Second))
}
