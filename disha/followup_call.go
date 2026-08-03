package disha

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/internal/weaviate"
	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

const (
	FollowUpBotType = "follow_up"

	followUpUsecaseType     = "followup_call_conversation"
	followUpGuidanceUsecase = "followup_call_get_guidance_tool"

	followUpModelGroup              = "gemini-flash-3.1-lite"
	followUpPhoneOverrideModelGroup = "gpt-4.1"
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
	PromptVariables DocumentVariables
	ModelGroup      string
	Dynamic         bool
	Tools           []voicepipelinecore.ToolDefinition

	// ProtocolEnricher performs blocking protocol retrieval before every LLM
	// call, on both follow-up paths. Non-nil only when the feature is enabled
	// and Weaviate is configured; nil leaves the pipeline byte-identical to
	// before.
	ProtocolEnricher *protocolEnricher

	// NewGuardrailChecker builds the non-blocking guardrail checker for this
	// call, given the call's own context. It is a constructor closure rather
	// than a built value because guardrailChecker.spawnAuditJudge needs a
	// call-lifetime context (TaskContext.Ctx) that does not exist yet at
	// plan() time — only BuildTask has it. plan() still does all the
	// env/config work and registers the chunk decorator here; nil means the
	// feature is disabled or Weaviate is unconfigured, leaving the pipeline
	// byte-identical to before.
	NewGuardrailChecker func(callCtx context.Context) *guardrailChecker

	// retrievalClient is the one *weaviate.Client shared by both retrieval
	// steps above (they hit the same Weaviate instance), built once in
	// setupRetrieval whenever either feature is enabled and configured.
	// BuildTask warms it exactly once regardless of which step(s) use it.
	retrievalClient *weaviate.Client
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

	// Dynamic check-in deliberately shares the regular follow-up group
	// (decided 2026-07-23): the gemma-on-OpenRouter split was retired
	// after persistent shared-quota 429 storms — gemini-flash-3.1-lite
	// spans three separate infrastructures and needs no provider pinning.
	modelGroup := followUpModelGroup
	if promptName == followUpPromptInvestorDemo {
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
		PromptVariables: variables,
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
	setupRetrieval(pl, deps)
	return pl, nil
}

// guardrailCheckEnabled reports whether the non-blocking guardrail check is
// switched on. One env var, no fallback chain — mirrors
// protocolRetrievalEnabled exactly.
func guardrailCheckEnabled() bool {
	return strings.TrimSpace(os.Getenv(guardrailCheckEnabledEnv)) == "1"
}

// setupRetrieval wires the two optional retrieval-shaped follow-up steps —
// blocking protocol retrieval (before generation) and the non-blocking
// guardrail check (during generation) — for both follow-up paths. Sales and
// onboarding are untouched: they never call this, so their pipelines are
// unchanged.
//
// Both steps hit the same Weaviate, so the client is built ONCE here,
// whenever either flag is on, and shared. A missing/incomplete Weaviate env
// (ErrNotConfigured) is treated as "that feature is off" rather than a call
// failure — the same posture as the other optional S3-backed features — and
// disables BOTH steps rather than failing the call.
//
// SetChunkDecorator is a single-occupancy slot (disha/call_event_callbacks.go),
// so the chunk decorator is registered AT MOST ONCE here, with whichever
// box(es) the enabled step(s) produced, and not at all when neither step is
// enabled.
func setupRetrieval(pl *followUpPlan, deps Deps) {
	protoEnabled := protocolRetrievalEnabled()
	guardEnabled := guardrailCheckEnabled()
	if !protoEnabled && !guardEnabled {
		return
	}

	client, err := weaviate.NewClientFromEnv(pl.Startup.Logger)
	if err != nil {
		if protoEnabled {
			reportRetrievalConfigFailure(pl, err, "protocol_retrieval_config")
		}
		if guardEnabled {
			reportRetrievalConfigFailure(pl, err, "guardrail_check_config")
		}
		return
	}
	pl.retrievalClient = client

	// protocolBox always exists once we get here, even when protocol
	// retrieval itself is disabled: newRetrievalChunkDecorator calls
	// protocolBox.take() unconditionally with no nil guard on that parameter
	// (unlike guardrailBox, which the decorator does check for nil), so an
	// empty box that nothing ever writes to is the safe stand-in rather than
	// a literal nil. It costs nothing — take() on an empty box always
	// returns nil, exactly like "no protocol step ran".
	protocolBox := &protocolRecordBox{}
	if protoEnabled {
		setupProtocolRetrieval(pl, client, protocolBox, deps.Documents)
	}

	var guardrailBox *guardrailRecordBox
	if guardEnabled {
		guardrailBox = setupGuardrailCheck(pl, client, deps)
	}

	if pl.ProtocolEnricher == nil && guardrailBox == nil {
		return
	}
	pl.Callbacks.SetChunkDecorator(newRetrievalChunkDecorator(
		protocolBox,
		guardrailBox,
		NewUSBucketJSONUploaderFromEnv(pl.Startup.Logger),
		pl.Startup.Logger,
		pl.Startup.UserID,
		pl.Startup.ConversationID,
		FollowUpBotType,
	))
}

// reportRetrievalConfigFailure logs and Sentries a shared-client construction
// failure under the given step's own operation tag, so a call that requested
// only one of the two steps doesn't get the other step's operation name in
// its Sentry event.
func reportRetrievalConfigFailure(pl *followUpPlan, err error, operation string) {
	pl.Startup.Logger.Printf("disha: %s disabled: %v\n", operation, err)
	sentryutil.Capture(sentryutil.Event{
		Err: err,
		Tags: map[string]string{
			"component": "disha_followup",
			"operation": operation,
		},
		Details: map[string]any{
			"conversation_id": pl.Startup.ConversationID,
			"user_id":         pl.Startup.UserID,
		},
	})
}

// setupProtocolRetrieval builds the protocol-retrieval enricher, given the
// shared client and box setupRetrieval already constructed. It does NOT
// touch the chunk decorator — setupRetrieval registers that once, after
// finding out which step(s) actually built something.
func setupProtocolRetrieval(pl *followUpPlan, client *weaviate.Client, box *protocolRecordBox, renderer templateRenderer) {
	pl.ProtocolEnricher = newProtocolEnricher(
		client,
		NewProtocolStore(),
		box,
		pl.Startup.Logger,
		renderer,
		pl.PromptMetadata,
		pl.PromptVariables,
		pl.Startup.UserID,
		pl.Startup.ConversationID,
	)
	pl.Startup.Logger.Printf("disha: protocol retrieval enabled (dynamic=%v)\n", pl.Dynamic)
}

// setupGuardrailCheck builds the plumbing the non-blocking guardrail check
// needs from plan()-time config (the judge client factory, the record box)
// and stores a constructor closure on pl for BuildTask to invoke once the
// call's own context exists — see followUpPlan.NewGuardrailChecker. Returns
// the box so setupRetrieval can register it with the chunk decorator; the
// checker built later from the closure shares this exact box.
func setupGuardrailCheck(pl *followUpPlan, client *weaviate.Client, deps Deps) *guardrailRecordBox {
	box := &guardrailRecordBox{}
	judgeFactory := newGuardrailJudgeClientFactory(deps, pl.Startup.Logger, pl.Startup.UserID, pl.Startup.ConversationID)
	docs := deps.Documents
	logger := pl.Startup.Logger
	userID, conversationID := pl.Startup.UserID, pl.Startup.ConversationID
	pl.NewGuardrailChecker = func(callCtx context.Context) *guardrailChecker {
		return newGuardrailChecker(callCtx, client, docs, box, logger, judgeFactory, userID, conversationID)
	}
	pl.Startup.Logger.Println("disha: guardrail check enabled")
	return box
}

// composeEnrichers threads enrichers together in order, so a
// ContextEnricherProcessor — which holds exactly one MessagesEnricher — can
// run several. Generic and business-free: it knows nothing about protocol
// retrieval or guardrail correction, only that each enricher's output becomes
// the next one's input. nil entries are skipped, so callers can pass an
// always-present slot that happens to be disabled this call. Returns nil for
// zero enrichers and the single enricher unchanged for exactly one, so a call
// with nothing to compose adds no processor and a call with one enricher
// costs nothing beyond that one call.
//
// Order matters at the call site (disha/followup_call.go's BuildTask):
// protocol must run before guardrail. Protocol recomputes its injection point
// (3 assistant turns above the tail) from the message list on every turn;
// guardrail appends its correction as the final message. Running guardrail
// first would shift what protocol sees as the tail.
func composeEnrichers(enrichers ...voicepipelinecore.MessagesEnricher) voicepipelinecore.MessagesEnricher {
	filtered := make([]voicepipelinecore.MessagesEnricher, 0, len(enrichers))
	for _, enrich := range enrichers {
		if enrich != nil {
			filtered = append(filtered, enrich)
		}
	}
	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	}
	return func(ctx context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
		for _, enrich := range filtered {
			messages = enrich(ctx, messages)
		}
		return messages
	}
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
		SentryTags: map[string]string{
			"conversation_id": pl.Startup.ConversationID,
			"user_id":         pl.Startup.UserID,
			"bot_type":        FollowUpBotType,
		},
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

	processors := []voicepipelinecore.Processor{
		source,
		audioSource,
		stt,
		userIdle,
		contextAggregators.User(),
	}

	// Both retrieval-shaped steps are late-bound here because they need
	// infrastructure that only exists once the task is built: the router
	// (protocol's prompt-metadata refresh), UI events, the task-scoped Sentry
	// hub, and — for the guardrail checker specifically — the call's own
	// long-lived context (its fire-and-forget audit judge must outlive the
	// very interrupt its own violation triggers; see
	// guardrailChecker.spawnAuditJudge). Each is nil unless its own env flag
	// was on AND Weaviate was configured back in plan(), so with neither flag
	// set this whole block is a no-op and the pipeline is byte-identical to
	// before.
	var protocolEnrich voicepipelinecore.MessagesEnricher
	if pl.ProtocolEnricher != nil {
		pl.ProtocolEnricher.SetInfrastructure(routerPromptMetadataSetter(llmClient), taskCtx.UIEvents)
		pl.ProtocolEnricher.SetSentryHub(taskCtx.SentryHub())
		protocolEnrich = pl.ProtocolEnricher.Enrich
	}

	var guardChecker *guardrailChecker
	if pl.NewGuardrailChecker != nil {
		guardChecker = pl.NewGuardrailChecker(taskCtx.Ctx)
		guardChecker.SetUI(taskCtx.UIEvents)
		guardChecker.SetSentryHub(taskCtx.SentryHub())
	}
	var guardrailEnrich voicepipelinecore.MessagesEnricher
	if guardChecker != nil {
		guardrailEnrich = guardChecker.Enrich
	}

	// One ContextEnricherProcessor for both enrichers (it holds exactly one
	// MessagesEnricher). Protocol runs first: see composeEnrichers for why
	// order matters here. Sits upstream of LLMProcessor so its latency lands
	// in its own MetricContextEnrich rather than inside llm_ttfb_ms.
	if enrich := composeEnrichers(protocolEnrich, guardrailEnrich); enrich != nil {
		processors = append(processors, voicepipelinecore.NewContextEnricherProcessor(taskCtx, enrich))
	}

	// Exactly one warm-up goroutine regardless of which step(s) are enabled:
	// both hit the same Weaviate through the same shared client, so there is
	// one cold-connection cost to pay per call, not one per step.
	if pl.retrievalClient != nil {
		go warmUpWeaviateClient(taskCtx.Ctx, pl.retrievalClient, pl.Startup.Logger)
	}

	processors = append(processors, llm, llmResponseTimeout, llmOutputFilter)

	// After LLMOutputFilterProcessor (must judge the text the user will
	// actually hear, not raw model output with leaked artifacts) and before
	// TTS (fragments must be observable before they're spoken). Absent
	// unless the guardrail flag was on and Weaviate was configured.
	if guardChecker != nil {
		processors = append(processors, voicepipelinecore.NewResponseGuardProcessor(taskCtx, guardChecker.Check))
	}

	processors = append(processors,
		tts,
		playback,
		contextAggregators.Assistant(),
		sink,
	)

	pipeline := voicepipelinecore.NewPipeline(processors)
	task.SetPipeline(source, pipeline)
	return task, nil
}

// routerPromptMetadataSetter exposes the conversation router's per-call
// prompt-metadata hook when the injected client is one (it always is in
// production; tests may inject a stub that isn't). Returns nil otherwise, which
// the enricher treats as "skip the metadata refresh".
func routerPromptMetadataSetter(client voicepipelinecore.LLMClient) promptMetadataSetter {
	setter, ok := client.(promptMetadataSetter)
	if !ok {
		return nil
	}
	return setter
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
		"recent_1hr_transcript":     derefString(user.Recent1HrTranscript),
		"patient_name":              name,
		"patient_schedule":          patientScheduleFromSlots(user.IdealCallTimeSlots),
		"he_she":                    subjectPronoun(gender),
		"him_her":                   objectPronoun(gender),
		"his_her":                   possessivePronoun(gender),
		"call_flow":                 callFlowValue,

		// Not referenced by any follow-up prompt today — these two exist for
		// retrieved protocol instruction texts, which are rendered against
		// this same store. Python resolves them in
		// user_prompt_variable_resolver (_diet_chart_available /
		// _today_diet_plan) and fetch_conversation forwards them here.
		"diet_chart_available": user.DietChartAvailable,
		"diet_plan_today":      derefString(user.DietPlanToday),
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
				text, err := getFollowUpGuidance(ctx, task.TaskCtx, deps, pl, situation)
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

func getFollowUpGuidance(ctx context.Context, taskCtx *voicepipelinecore.TaskContext, deps Deps, pl *followUpPlan, situation string) (string, error) {
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
		reportGuidanceLLMFailure(taskCtx, pl, err)
		return "", err
	}
	if strings.TrimSpace(b.String()) == "" {
		err = errors.New("disha: get_guidance returned empty response")
		reportGuidanceLLMFailure(taskCtx, pl, err)
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
func reportGuidanceLLMFailure(taskCtx *voicepipelinecore.TaskContext, pl *followUpPlan, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	sentryutil.Capture(sentryutil.Event{
		Hub: taskCtx.SentryHub(),
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
