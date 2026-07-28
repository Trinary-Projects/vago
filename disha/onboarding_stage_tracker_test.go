package disha

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	stageTestConversationID = "conv-stage-1"
	stageTestUserID         = "user-stage"
	stageTestPatientInfo    = "Riya Sharma, age 22, student"
	stageTestNextStage      = "problem_discovery_and_exploration"
)

// syncBuffer is a race-safe bytes.Buffer for capturing tracker logs.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// stubStageClassifier is an injectable classifier LLM client that also
// records SetPromptMetadata calls (the tracker type-asserts for it).
type stubStageClassifier struct {
	mu       sync.Mutex
	output   string
	err      error
	requests []voicepipelinecore.LLMRequest
	metadata []map[string]any
	onCall   func()
}

func (s *stubStageClassifier) Stream(ctx context.Context, req voicepipelinecore.LLMRequest, onToken func(string)) (voicepipelinecore.LLMResult, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	onCall, output, err := s.onCall, s.output, s.err
	s.mu.Unlock()
	if onCall != nil {
		onCall()
	}
	if err != nil {
		return voicepipelinecore.LLMResult{}, err
	}
	if output != "" {
		onToken(output)
	}
	return voicepipelinecore.LLMResult{Model: "stub-classifier"}, nil
}

func (s *stubStageClassifier) SetPromptMetadata(m map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata = append(s.metadata, m)
}

func (s *stubStageClassifier) calls() []voicepipelinecore.LLMRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]voicepipelinecore.LLMRequest, len(s.requests))
	copy(out, s.requests)
	return out
}

func (s *stubStageClassifier) recordedMetadata() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, len(s.metadata))
	copy(out, s.metadata)
	return out
}

// stubMetadataRecorder stands in for the conversation router handle.
type stubMetadataRecorder struct {
	mu       sync.Mutex
	metadata []map[string]any
}

func (s *stubMetadataRecorder) SetPromptMetadata(m map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata = append(s.metadata, m)
}

func (s *stubMetadataRecorder) recorded() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]map[string]any, len(s.metadata))
	copy(out, s.metadata)
	return out
}

type stageMachineHarness struct {
	t           *testing.T
	logBuf      *syncBuffer
	redisServer *miniredis.Miniredis
	redisClient RedisClient
	apiRecorder *callAPIRecorder
	deps        Deps
	config      *OnboardingConfig
	state       *ConversationState
	pair        *voicepipelinecore.ContextAggregatorPair
	ui          *voicepipelinecore.UIEventSender
	routerMeta  *stubMetadataRecorder
	manager     *OnboardingStageManager
	tracker     *OnboardingStageTracker
	threshold   *OnboardingStageThresholdMonitor
	promptKey   string

	// hub is a private, network-free Sentry hub (own client + mock
	// transport, like sentryutil_test.go's newTestHub) wired into every
	// manager via SetInfrastructure/SetSentryHub exactly like BuildTask
	// wires taskCtx.SentryHub(). hubTransport lets tests assert that a
	// manager's Sentry.Capture actually routed through this hub instead
	// of the process-global one.
	hub          *sentry.Hub
	hubTransport *sentry.MockTransport
}

// newStageMachineHarness stands up the real fixtures + real manager +
// real fuzzy matcher around an injectable classifier — the phase-4 stage
// machine wired exactly like BuildTask, minus the pipeline. It builds no
// deep-thinking/careplan managers (both nil), matching every phase-4 test
// that never touches the careplan-switcher stage or a DT-configured stage.
func newStageMachineHarness(t *testing.T, classifier voicepipelinecore.LLMClient) *stageMachineHarness {
	return newStageMachineHarnessWithManagers(t, classifier, nil, nil)
}

// newStageMachineHarnessWithManagers is newStageMachineHarness plus
// injectable one-shot client factories for deep thinking and careplan
// detection (phase 5). A nil factory means "don't build that manager" —
// the stage manager's own nil-safety (dtManager methods self-guard;
// careplan calls are gated on the config's careplan-switcher stage name)
// keeps every phase-4 test working unchanged.
func newStageMachineHarnessWithManagers(t *testing.T, classifier voicepipelinecore.LLMClient, dtFactory, careplanFactory deepThinkingClientFactory) *stageMachineHarness {
	t.Helper()
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedOnboardingFixtures(t, redisServer)
	seedDocument(t, redisServer, stageTrackerSysPromptName, "latest", 2,
		"TRACKER_SYS stage={{ current_stage }} cond={{ transition_condition }} stages={{ next_stages }}")
	seedDocument(t, redisServer, stageTrackerUserPromptName, "latest", 4,
		"TRACKER_USER stage={{ current_stage }} transcript={{ transcript }}")

	apiServer, apiRecorder := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	deps := testDeps(redisClient, api)
	cfg := newOnboardingTestConfig(t, redisClient)

	logBuf := &syncBuffer{}
	logger := log.New(logBuf, "", 0)

	state := NewConversationState(cfg, "student_test")
	compiler := &onboardingPromptCompiler{
		docs:        deps.Documents,
		config:      cfg,
		patientInfo: stageTestPatientInfo,
		profileVars: map[string]any{"gender": "female"},
	}
	compiled, err := compiler.CompileSystemPrompt(context.Background(), state.CurrentStage(), state.VariableStoreSnapshot())
	if err != nil {
		t.Fatalf("CompileSystemPrompt: %v", err)
	}
	promptKey := PromptKey(cfg.MainSystemPrompt.Name, compiled.MainVersion)

	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: stageTestConversationID,
		UserID:         stageTestUserID,
		BotType:        OnboardingCallBotType,
		Logger:         logger,
	}, redisClient, api, nil)
	callbacks.SetChunkDecorator(newOnboardingChunkDecorator(state, nil, stageTestUserID, stageTestConversationID, logger))

	pair := voicepipelinecore.NewContextAggregatorPair(
		&voicepipelinecore.TaskContext{Logger: logger},
		buildInitialMessages(compiled.Text, nil, ""),
		promptKey,
	)
	ui := voicepipelinecore.NewUIEventSender(logger)
	routerMeta := &stubMetadataRecorder{}

	hubTransport := &sentry.MockTransport{}
	hubClient, err := sentry.NewClient(sentry.ClientOptions{Transport: hubTransport})
	if err != nil {
		t.Fatalf("sentry.NewClient: %v", err)
	}
	hub := sentry.NewHub(hubClient, sentry.NewScope())

	var dtManager *OnboardingDeepThinkingManager
	if dtFactory != nil {
		dtManager = NewOnboardingDeepThinkingManager(deps.Documents, callbacks, dtFactory, logger,
			stageTestUserID, stageTestConversationID, stageTestPatientInfo, promptKey)
		dtManager.SetUI(ui)
		dtManager.SetSentryHub(hub)
	}
	var careplanManager *OnboardingCarePlanManager
	if careplanFactory != nil {
		careplanManager = NewOnboardingCarePlanManager(cfg, deps.Documents, api, careplanFactory, logger,
			stageTestUserID, stageTestConversationID, stageTestPatientInfo)
		careplanManager.SetUI(ui)
		careplanManager.SetSentryHub(hub)
	}

	manager := NewOnboardingStageManager(state, cfg, compiler, callbacks, api, dtManager, careplanManager, logger,
		stageTestUserID, stageTestConversationID, promptKey)
	tracker := NewOnboardingStageTracker(state, cfg, deps.Documents, manager, classifier, logger,
		stageTestUserID, stageTestConversationID, stageTestPatientInfo)
	threshold := NewOnboardingStageThresholdMonitor(state, api, logger,
		stageTestUserID, stageTestConversationID)
	callbacks.SetAssistantTurnCommittedHandler(func(string, time.Time) {
		threshold.OnAssistantTurnCommitted()
	})

	manager.SetInfrastructure(pair, routerMeta, ui)
	tracker.SetInfrastructure(context.Background(), pair, ui)
	manager.SetSentryHub(hub)
	tracker.SetSentryHub(hub)
	threshold.SetUI(ui)
	threshold.SetSentryHub(hub)

	return &stageMachineHarness{
		t:            t,
		logBuf:       logBuf,
		redisServer:  redisServer,
		redisClient:  redisClient,
		apiRecorder:  apiRecorder,
		deps:         deps,
		config:       cfg,
		state:        state,
		pair:         pair,
		ui:           ui,
		routerMeta:   routerMeta,
		manager:      manager,
		tracker:      tracker,
		threshold:    threshold,
		promptKey:    promptKey,
		hub:          hub,
		hubTransport: hubTransport,
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func (h *stageMachineHarness) rtviMessages() []string {
	var out []string
	for _, entry := range h.ui.Snapshot() {
		if entry.Type != "server-message" {
			continue
		}
		if s, ok := entry.Data.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (h *stageMachineHarness) hasRTVI(substr string) bool {
	for _, msg := range h.rtviMessages() {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

func (h *stageMachineHarness) waitForRTVI(substr string) {
	h.t.Helper()
	waitForCondition(h.t, 5*time.Second, "RTVI message containing "+substr, func() bool {
		return h.hasRTVI(substr)
	})
}

func (h *stageMachineHarness) chunks() []ConversationChunk {
	h.t.Helper()
	items, err := h.redisServer.List(conversationChunksKey(stageTestUserID, stageTestConversationID))
	if err != nil {
		return nil
	}
	out := make([]ConversationChunk, 0, len(items))
	for _, item := range items {
		var chunk ConversationChunk
		if err := json.Unmarshal([]byte(item), &chunk); err != nil {
			h.t.Fatalf("unmarshal chunk: %v", err)
		}
		out = append(out, chunk)
	}
	return out
}

func (h *stageMachineHarness) enqueueRequests(moduleName string) []callAPIRequest {
	var out []callAPIRequest
	for _, req := range h.apiRecorder.snapshot() {
		if req.Path == "/common/enqueue_job" && req.Body["module_name"] == moduleName {
			out = append(out, req)
		}
	}
	return out
}

// installMaybeStage moves the harness state onto a synthetic stage whose
// prompt config triggers are engineered to land in the fuzzy "maybe"
// band for the response "alpha beta gamma" (token subset → token_set
// 100, coverage 50 → below the yes-gate's 85, above the no-gate's 40).
func installMaybeStage(t *testing.T, h *stageMachineHarness) *StageConfig {
	t.Helper()
	seedStageDocWithConfig(t, h.redisServer, "obtest/maybe_stage", 7, map[string]any{
		"next_stages": []any{map[string]any{
			"stage_name":         stageTestNextStage,
			"trigger_statements": []any{"alpha beta gamma delta epsilon zeta"},
		}},
	})
	stage := &StageConfig{
		Name:       "intro_maybe",
		Prompt:     PromptConfig{Name: "obtest/maybe_stage", Version: 7},
		NextStages: []string{stageTestNextStage},
	}
	h.state.AdvanceStage(stage)
	return stage
}

func seedStageDocWithConfig(t *testing.T, server *miniredis.Miniredis, name string, version int, configJSON map[string]any) {
	t.Helper()
	doc := DocumentVersion{ID: "doc-" + name, PromptText: "stage prompt body", ConfigJSON: configJSON, Version: version}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal stage doc %q: %v", name, err)
	}
	server.Set("document:"+name+":v"+strconv.Itoa(version), string(raw))
}

// startStageTriggerStatement pulls the real trigger string from the
// captured student_test start-stage prompt config fixture.
func startStageTriggerStatement(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/onboarding/student_test_prompts.json")
	if err != nil {
		t.Fatalf("read prompts fixture: %v", err)
	}
	var prompts map[string]struct {
		Key string          `json:"key"`
		Doc json.RawMessage `json:"doc"`
	}
	if err := json.Unmarshal(raw, &prompts); err != nil {
		t.Fatalf("unmarshal prompts fixture: %v", err)
	}
	var doc DocumentVersion
	if err := json.Unmarshal(prompts[onboardingTestStagePrompt].Doc, &doc); err != nil {
		t.Fatalf("unmarshal start-stage prompt doc: %v", err)
	}
	nextStages, ok := doc.ConfigJSON["next_stages"].([]any)
	if !ok || len(nextStages) == 0 {
		t.Fatalf("start-stage config has no next_stages: %v", doc.ConfigJSON)
	}
	entry := nextStages[0].(map[string]any)
	triggers := entry["trigger_statements"].([]any)
	if len(triggers) == 0 {
		t.Fatal("start-stage config has no trigger statements")
	}
	return triggers[0].(string)
}

// --- Scenario 1: fuzzy-yes happy path against real fixtures ---

func TestStageTrackerFuzzyYesTransitionHappyPath(t *testing.T) {
	classifier := &stubStageClassifier{output: stageTestNextStage}
	h := newStageMachineHarness(t, classifier)

	// The assistant says the start stage's trigger statement verbatim
	// (with the [Name] placeholder dropped, one of the matcher's trigger
	// variants) → fuzzy decision "yes", no classifier call.
	response := strings.ReplaceAll(startStageTriggerStatement(t), "[Name]", "")
	h.tracker.OnLLMCallCompleted(response, false)

	waitForCondition(t, 5*time.Second, "stage advance", func() bool {
		return h.state.CurrentStage().Name == stageTestNextStage
	})
	h.waitForRTVI("Fuzzy matched introduction => " + stageTestNextStage)
	h.waitForRTVI("Transition complete introduction => " + stageTestNextStage)

	if calls := classifier.calls(); len(calls) != 0 {
		t.Fatalf("classifier called %d times on fuzzy yes, want 0", len(calls))
	}

	// System message swapped to the new stage's compiled prompt.
	msgs := h.pair.MessagesSnapshot()
	if msgs[0].Role != "system" || !strings.Contains(msgs[0].Content, "<problem_discovery_and_exploration>") {
		t.Fatalf("system message not recompiled for the new stage:\n%.300s", msgs[0].Content)
	}

	// Debug chunk (persisted asynchronously).
	waitForCondition(t, 5*time.Second, "debug chunk", func() bool { return len(h.chunks()) == 1 })
	chunk := h.chunks()[0]
	if chunk.Role != "assistant" || !chunk.IsDebugLog {
		t.Fatalf("debug chunk role/is_debug_log = %q/%v, want assistant/true", chunk.Role, chunk.IsDebugLog)
	}
	if want := "Agenda changed(Stage Transition Tracker) from introduction => " + stageTestNextStage; chunk.Text != want {
		t.Fatalf("debug chunk text = %q, want %q", chunk.Text, want)
	}
	if chunk.CurrentAgenda == nil || *chunk.CurrentAgenda != stageTestNextStage {
		t.Fatalf("debug chunk current_agenda = %v, want new stage %s", chunk.CurrentAgenda, stageTestNextStage)
	}
	if chunk.MainAgentSystemPromptLangfuseKey == nil || *chunk.MainAgentSystemPromptLangfuseKey != h.promptKey {
		t.Fatalf("debug chunk prompt key = %v, want %s", chunk.MainAgentSystemPromptLangfuseKey, h.promptKey)
	}
	additional, ok := chunk.AdditionalData.(map[string]any)
	if !ok {
		t.Fatalf("debug chunk additional_data = %#v, want object", chunk.AdditionalData)
	}
	toolCallID, _ := additional["tool_call_id"].(string)
	if !strings.HasPrefix(toolCallID, "stage_transition_tracker_") {
		t.Fatalf("tool_call_id = %q, want stage_transition_tracker_ prefix", toolCallID)
	}

	// Analytics enqueue.
	waitForCondition(t, 5*time.Second, "analytics enqueue", func() bool {
		return len(h.enqueueRequests(agendaAnalyticsModule)) == 1
	})
	analytics := h.enqueueRequests(agendaAnalyticsModule)[0]
	if analytics.Body["func_name"] != agendaAnalyticsFunc || analytics.Body["sqs_queue"] != agendaAnalyticsQueue {
		t.Fatalf("analytics enqueue = %+v", analytics.Body)
	}
	analyticsKwargs := analytics.Body["kwargs"].(map[string]any)
	if analyticsKwargs["user_id"] != stageTestUserID ||
		analyticsKwargs["event"] != "CallAgendaUpdate-"+stageTestNextStage {
		t.Fatalf("analytics kwargs = %+v", analyticsKwargs)
	}
	if props, ok := analyticsKwargs["properties"].(map[string]any); !ok || len(props) != 0 {
		t.Fatalf("analytics properties = %#v, want empty object", analyticsKwargs["properties"])
	}

	// Stage-transition timing log enqueue.
	waitForCondition(t, 5*time.Second, "timing log enqueue", func() bool {
		return len(h.enqueueRequests(stageTransitionLogModule)) == 1
	})
	timing := h.enqueueRequests(stageTransitionLogModule)[0]
	if timing.Body["func_name"] != stageTransitionLogFunc || timing.Body["sqs_queue"] != stageTransitionLogQueue {
		t.Fatalf("timing enqueue = %+v", timing.Body)
	}
	timingKwargs := timing.Body["kwargs"].(map[string]any)
	if id, _ := timingKwargs["log_id"].(string); id == "" {
		t.Fatalf("timing log_id = %#v, want non-empty", timingKwargs["log_id"])
	}
	if timingKwargs["user_id"] != stageTestUserID ||
		timingKwargs["conversation_id"] != stageTestConversationID ||
		timingKwargs["current_stage"] != "introduction" ||
		timingKwargs["next_stage"] != stageTestNextStage {
		t.Fatalf("timing kwargs identity = %+v", timingKwargs)
	}
	if _, ok := timingKwargs["total_time_ms"].(float64); !ok {
		t.Fatalf("total_time_ms = %#v, want number", timingKwargs["total_time_ms"])
	}
	// The target stage (problem_discovery_and_exploration) has no
	// configured deep thinking, so is_dt_blocking_llm_call is false; the
	// timing value is still a real (near-zero, non-negative) measurement
	// around RunBlocking rather than a hardcoded phase-4 placeholder.
	if v, ok := timingKwargs["dt_blocking_time_ms"].(float64); !ok || v < 0 {
		t.Fatalf("dt_blocking_time_ms = %#v, want non-negative float64", timingKwargs["dt_blocking_time_ms"])
	}
	if timingKwargs["is_dt_blocking_llm_call"] != false {
		t.Fatalf("deep-thinking timing fields = %v/%v, want .../false",
			timingKwargs["dt_blocking_time_ms"], timingKwargs["is_dt_blocking_llm_call"])
	}
	for _, key := range []string{"data_s3_key", "bucket_name"} {
		if v, present := timingKwargs[key]; !present || v != nil {
			t.Fatalf("timing kwargs %s = %#v (present=%v), want explicit null", key, v, present)
		}
	}

	// Conversation router metadata refreshed with the new stage identity.
	recorded := h.routerMeta.recorded()
	if len(recorded) != 1 {
		t.Fatalf("router metadata refreshes = %d, want 1", len(recorded))
	}
	if recorded[0]["stage_prompt_name"] != "onboarding_callV3_4/02_problem_discovery_and_exploration" {
		t.Fatalf("router metadata stage prompt = %v", recorded[0]["stage_prompt_name"])
	}
}

// --- Scenario 2: fuzzy-maybe path runs the classifier ---

func TestStageTrackerMaybePathRunsClassifier(t *testing.T) {
	classifier := &stubStageClassifier{output: stageTestNextStage}
	h := newStageMachineHarness(t, classifier)
	installMaybeStage(t, h)

	h.tracker.OnLLMCallCompleted("alpha beta gamma", false)

	waitForCondition(t, 5*time.Second, "maybe-path stage advance", func() bool {
		return h.state.CurrentStage().Name == stageTestNextStage
	})
	h.waitForRTVI("Fuzzy maybe for stage=intro_maybe; running LLM")
	h.waitForRTVI("Transition complete intro_maybe => " + stageTestNextStage)

	calls := classifier.calls()
	if len(calls) != 1 {
		t.Fatalf("classifier calls = %d, want 1", len(calls))
	}
	wantCondition := "Target stage: " + stageTestNextStage + "\n- Trigger: alpha beta gamma delta epsilon zeta"
	wantSys := "TRACKER_SYS stage=intro_maybe cond=" + wantCondition + " stages=- " + stageTestNextStage
	if calls[0].Messages[0].Role != "system" || calls[0].Messages[0].Content != wantSys {
		t.Fatalf("classifier system message = %q, want %q", calls[0].Messages[0].Content, wantSys)
	}
	wantTranscript := "patient: hello?\ndisha: alpha beta gamma"
	wantUser := "TRACKER_USER stage=intro_maybe transcript=" + wantTranscript
	if calls[0].Messages[1].Role != "user" || calls[0].Messages[1].Content != wantUser {
		t.Fatalf("classifier user message = %q, want %q", calls[0].Messages[1].Content, wantUser)
	}

	// Prompt-identity metadata set on the classifier before the call:
	// exact variables used to render, resolved versions, and no business
	// fields (Python's stage_transitions/fuzzy_result deliberately absent).
	metas := classifier.recordedMetadata()
	if len(metas) != 1 {
		t.Fatalf("classifier metadata sets = %d, want 1", len(metas))
	}
	meta := metas[0]
	if meta["system_prompt_name"] != stageTrackerSysPromptName || meta["system_prompt_version"] != 2 ||
		meta["user_prompt_name"] != stageTrackerUserPromptName || meta["user_prompt_version"] != 4 {
		t.Fatalf("classifier metadata identity = %+v", meta)
	}
	sysVars := meta["system_prompt_variables"].(DocumentVariables)
	if sysVars["current_stage"] != "intro_maybe" ||
		sysVars["transition_condition"] != wantCondition ||
		sysVars["next_stages"] != "- "+stageTestNextStage {
		t.Fatalf("classifier system variables = %+v", sysVars)
	}
	userVars := meta["user_prompt_variables"].(DocumentVariables)
	if userVars["transcript"] != wantTranscript || userVars["current_stage"] != "intro_maybe" {
		t.Fatalf("classifier user variables = %+v", userVars)
	}
	for _, forbidden := range []string{"stage_transitions", "fuzzy_result"} {
		if _, ok := meta[forbidden]; ok {
			t.Fatalf("classifier metadata carries business field %q: %+v", forbidden, meta)
		}
	}
}

// --- Scenario 3: classifier output not in allowed_next_stages ---

func TestStageTrackerInvalidClassifierOutput(t *testing.T) {
	classifier := &stubStageClassifier{output: "bogus_stage"}
	h := newStageMachineHarness(t, classifier)
	installMaybeStage(t, h)

	h.tracker.OnLLMCallCompleted("alpha beta gamma", false)

	h.waitForRTVI(`Invalid output="bogus_stage" for stage=intro_maybe`)
	if h.state.CurrentStage().Name != "intro_maybe" {
		t.Fatalf("state advanced on invalid output: %s", h.state.CurrentStage().Name)
	}
	if got := h.chunks(); len(got) != 0 {
		t.Fatalf("chunks = %d, want 0", len(got))
	}
	if reqs := h.apiRecorder.snapshot(); len(reqs) != 0 {
		t.Fatalf("API requests = %+v, want none", reqs)
	}
	if h.hasRTVI("Transitioning") {
		t.Fatal("unexpected Transitioning RTVI on invalid output")
	}
}

// --- Scenario 4: classifier says "no" ---

func TestStageTrackerClassifierNoOutput(t *testing.T) {
	classifier := &stubStageClassifier{output: "no"}
	h := newStageMachineHarness(t, classifier)
	installMaybeStage(t, h)

	h.tracker.OnLLMCallCompleted("alpha beta gamma", false)

	h.waitForRTVI("No transition for stage=intro_maybe")
	if h.state.CurrentStage().Name != "intro_maybe" {
		t.Fatalf("state advanced on classifier no: %s", h.state.CurrentStage().Name)
	}
	if got := h.chunks(); len(got) != 0 {
		t.Fatalf("chunks = %d, want 0", len(got))
	}
	if reqs := h.apiRecorder.snapshot(); len(reqs) != 0 {
		t.Fatalf("API requests = %+v, want none", reqs)
	}
}

// --- Scenario 5: stale result after a concurrent transition ---

func TestStageTrackerStaleResultDropped(t *testing.T) {
	classifier := &stubStageClassifier{output: stageTestNextStage}
	h := newStageMachineHarness(t, classifier)
	installMaybeStage(t, h)

	// While the classifier is "in flight", another transition advances
	// the stage — the tracker's started-stage check must drop the result.
	classifier.onCall = func() {
		h.state.AdvanceStage(&h.config.CommonStages[0])
	}

	systemBefore := h.pair.MessagesSnapshot()[0].Content
	h.tracker.OnLLMCallCompleted("alpha beta gamma", false)

	h.waitForRTVI("Stale output ignored: started=intro_maybe, current=" + stageTestNextStage)
	if h.hasRTVI("Transitioning") {
		t.Fatal("stale output still transitioned")
	}
	if got := h.chunks(); len(got) != 0 {
		t.Fatalf("chunks = %d, want 0", len(got))
	}
	if reqs := h.apiRecorder.snapshot(); len(reqs) != 0 {
		t.Fatalf("API requests = %+v, want none", reqs)
	}
	if got := h.pair.MessagesSnapshot()[0].Content; got != systemBefore {
		t.Fatal("system message replaced despite stale result")
	}
}

// --- Scenario 6: interrupted LLM response is skipped entirely ---

func TestStageTrackerSkipsInterruptedResponse(t *testing.T) {
	classifier := &stubStageClassifier{output: stageTestNextStage}
	h := newStageMachineHarness(t, classifier)

	h.tracker.OnLLMCallCompleted("half a sentence", true)

	// The skip path is synchronous — no goroutine is spawned.
	if !h.hasRTVI("Skipped interrupted LLM response for stage=introduction") {
		t.Fatalf("missing skip RTVI: %v", h.rtviMessages())
	}
	if msgs := h.rtviMessages(); len(msgs) != 1 {
		t.Fatalf("RTVI messages = %v, want only the skip entry", msgs)
	}
	if h.state.CurrentStage().Name != "introduction" {
		t.Fatalf("state changed on interrupted response: %s", h.state.CurrentStage().Name)
	}
	if len(classifier.calls()) != 0 {
		t.Fatal("classifier called for interrupted response")
	}
}

// --- Scenario 7: closing stage is skipped ---

func TestStageTrackerSkipsClosingStage(t *testing.T) {
	classifier := &stubStageClassifier{output: stageTestNextStage}
	h := newStageMachineHarness(t, classifier)
	h.state.AdvanceStage(&StageConfig{
		Name:       "closing_and_assurance",
		Prompt:     PromptConfig{Name: "obtest/closing_stage", Version: 1},
		NextStages: []string{stageTestNextStage},
	})

	h.tracker.OnLLMCallCompleted("dhanyavaad, call khatam karte hain", false)

	waitForCondition(t, 5*time.Second, "closing-stage skip log", func() bool {
		return strings.Contains(h.logBuf.String(), "Stage=closing_and_assurance is a closing stage, skipping")
	})
	if h.hasRTVI("Started for stage=") {
		t.Fatal("closing stage still started the tracker run")
	}
	if h.state.CurrentStage().Name != "closing_and_assurance" {
		t.Fatalf("state changed: %s", h.state.CurrentStage().Name)
	}
	if len(classifier.calls()) != 0 {
		t.Fatal("classifier called for closing stage")
	}
}

// --- Scenario 9: stage prompt config without next_stages triggers ---

func TestStageTrackerConfigErrorPath(t *testing.T) {
	classifier := &stubStageClassifier{output: stageTestNextStage}
	h := newStageMachineHarness(t, classifier)
	seedStageDocWithConfig(t, h.redisServer, "obtest/no_triggers_stage", 2, map[string]any{})
	h.state.AdvanceStage(&StageConfig{
		Name:       "no_trigger",
		Prompt:     PromptConfig{Name: "obtest/no_triggers_stage", Version: 2},
		NextStages: []string{stageTestNextStage},
	})

	h.tracker.OnLLMCallCompleted("hello there", false)

	h.waitForRTVI("Invalid stage transition config for stage=no_trigger: next_stages config has no valid trigger statements")
	if h.state.CurrentStage().Name != "no_trigger" {
		t.Fatalf("state advanced on config error: %s", h.state.CurrentStage().Name)
	}
	if got := h.chunks(); len(got) != 0 {
		t.Fatalf("chunks = %d, want 0", len(got))
	}
	if len(classifier.calls()) != 0 {
		t.Fatal("classifier called on config error")
	}
}

// --- GetDocumentConfig: resolve without rendering ---

func TestGetDocumentConfigReturnsUnrenderedConfig(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedOnboardingFixtures(t, redisServer)
	deps := testDeps(redisClient, nil)

	config, version, err := deps.Documents.GetDocumentConfig(context.Background(), onboardingTestStagePrompt, 3)
	if err != nil {
		t.Fatalf("GetDocumentConfig: %v", err)
	}
	if version != 3 {
		t.Fatalf("version = %d, want 3", version)
	}
	if config["version"] != 3 {
		t.Fatalf("config[version] = %#v, want 3", config["version"])
	}
	// The fixture doc carries no id — Python injects it only when present.
	if _, ok := config["id"]; ok {
		t.Fatalf("config has unexpected id (fixture doc has none): %v", config)
	}
	if _, ok := config["next_stages"].([]any); !ok {
		t.Fatalf("config next_stages = %#v, want list", config["next_stages"])
	}
}
