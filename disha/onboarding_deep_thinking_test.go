package disha

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	dtTestConversationID = "conv-dt-1"
	dtTestUserID         = "user-dt"
	dtTestPatientInfo    = "Asha Patel, age 30, diabetic"
	dtTestPromptKey      = "dt-prompt-key"
)

// dtStubClient is an injectable one-shot LLMClient for deep-thinking
// calls: configurable output/error, and an optional artificial delay so
// tests can exercise ctx cancellation mid-flight.
type dtStubClient struct {
	output string
	err    error
	delay  time.Duration
}

func (c *dtStubClient) Stream(ctx context.Context, req voicepipelinecore.LLMRequest, onToken func(string)) (voicepipelinecore.LLMResult, error) {
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return voicepipelinecore.LLMResult{}, ctx.Err()
		}
	}
	if c.err != nil {
		return voicepipelinecore.LLMResult{}, c.err
	}
	if c.output != "" {
		onToken(c.output)
	}
	return voicepipelinecore.LLMResult{Model: "stub-deep-thinking"}, nil
}

type dtFactoryCall struct {
	promptMetadata map[string]any
	usecaseType    string
}

// dtFactoryRecorder is the test double for deepThinkingClientFactory: it
// records every call and dispatches a stub client keyed by the
// system_prompt_name carried in promptMetadata (so tests can give each
// configured DT prompt its own canned response).
type dtFactoryRecorder struct {
	mu       sync.Mutex
	calls    []dtFactoryCall
	byPrompt map[string]*dtStubClient
}

func newDTFactoryRecorder(byPrompt map[string]*dtStubClient) *dtFactoryRecorder {
	return &dtFactoryRecorder{byPrompt: byPrompt}
}

func (r *dtFactoryRecorder) factory() deepThinkingClientFactory {
	return func(promptMetadata map[string]any, usecaseType string) voicepipelinecore.LLMClient {
		r.mu.Lock()
		r.calls = append(r.calls, dtFactoryCall{promptMetadata: promptMetadata, usecaseType: usecaseType})
		name, _ := promptMetadata["system_prompt_name"].(string)
		client := r.byPrompt[name]
		r.mu.Unlock()
		if client == nil {
			return &dtStubClient{}
		}
		return client
	}
}

func (r *dtFactoryRecorder) snapshot() []dtFactoryCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]dtFactoryCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// dtHarness wires a real DocumentStore + CallEventCallbacks (backed by
// miniredis) and a real UIEventSender around an injectable client
// factory — everything RunBlocking/RunNonBlocking touch except the LLM
// call itself.
type dtHarness struct {
	t           *testing.T
	redisServer *miniredis.Miniredis
	redisClient RedisClient
	callbacks   *CallEventCallbacks
	ui          *voicepipelinecore.UIEventSender
	manager     *OnboardingDeepThinkingManager
	factory     *dtFactoryRecorder
	logBuf      *syncBuffer
}

func newDTHarness(t *testing.T, byPrompt map[string]*dtStubClient) *dtHarness {
	t.Helper()
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)

	logBuf := &syncBuffer{}
	logger := log.New(logBuf, "", 0)

	docs := newDocumentStore(redisClient, logger, simpleTemplateRenderer{})
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: dtTestConversationID,
		UserID:         dtTestUserID,
		BotType:        OnboardingCallBotType,
		Logger:         logger,
	}, redisClient, nil, nil)

	ui := voicepipelinecore.NewUIEventSender(logger)
	factory := newDTFactoryRecorder(byPrompt)

	manager := NewOnboardingDeepThinkingManager(
		docs, callbacks, factory.factory(), logger,
		dtTestUserID, dtTestConversationID, dtTestPatientInfo, dtTestPromptKey,
	)
	manager.SetUI(ui)

	return &dtHarness{
		t:           t,
		redisServer: redisServer,
		redisClient: redisClient,
		callbacks:   callbacks,
		ui:          ui,
		manager:     manager,
		factory:     factory,
		logBuf:      logBuf,
	}
}

func (h *dtHarness) seedPrompt(name, body string) {
	h.t.Helper()
	seedDocument(h.t, h.redisServer, name, "latest", 3, body)
}

func (h *dtHarness) rtviMessages() []string {
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

func (h *dtHarness) hasRTVI(substr string) bool {
	for _, msg := range h.rtviMessages() {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

func (h *dtHarness) countRTVI(substr string) int {
	n := 0
	for _, msg := range h.rtviMessages() {
		if strings.Contains(msg, substr) {
			n++
		}
	}
	return n
}

func (h *dtHarness) chunks() []ConversationChunk {
	h.t.Helper()
	items, err := h.redisServer.List(conversationChunksKey(dtTestUserID, dtTestConversationID))
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

func waitForDTCondition(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

// --- RunBlocking ---

func TestDeepThinkingRunBlockingMergesInConfigOrder(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obtest/dt_a": {output: `{"x":1,"common":"from_a"}`},
		"obtest/dt_b": {output: `{"y":2,"common":"from_b"}`},
	})
	h.seedPrompt("obtest/dt_a", "DT_A prompt")
	h.seedPrompt("obtest/dt_b", "DT_B prompt")

	stage := &StageConfig{
		Name: "diet_information",
		DeepThinking: []DeepThinkingConfig{
			{Prompt: PromptConfig{Name: "obtest/dt_a"}, Blocking: true},
			{Prompt: PromptConfig{Name: "obtest/dt_b"}, Blocking: true},
		},
	}

	merged := h.manager.RunBlocking(context.Background(), stage, "patient: hi\ndisha: hello", "diet_information", map[string]any{"gender": "female"})

	if merged["common"] != "from_b" {
		t.Fatalf("key collision: common = %v, want from_b (later config wins)", merged["common"])
	}
	if merged["x"] != float64(1) || merged["y"] != float64(2) {
		t.Fatalf("merged = %v, missing non-colliding keys", merged)
	}

	if !h.hasRTVI("[PROCESS] Starting blocking deep thinking for diet_information...") {
		t.Fatalf("missing start RTVI message: %v", h.rtviMessages())
	}
	if !h.hasRTVI("[PROCESS] Blocking DT for diet_information took ") {
		t.Fatalf("missing finish RTVI message: %v", h.rtviMessages())
	}
	if got := h.countRTVI("deep_thinking_diet_information LLM call took "); got != 2 {
		t.Fatalf("per-call RTVI count = %d, want 2: %v", got, h.rtviMessages())
	}

	chunks := h.chunks()
	if len(chunks) != 1 {
		t.Fatalf("chunk count = %d, want 1", len(chunks))
	}
	var content map[string]any
	if err := json.Unmarshal([]byte(chunks[0].Text), &content); err != nil {
		t.Fatalf("unmarshal debug chunk: %v", err)
	}
	if content["common"] != "from_b" {
		t.Fatalf("debug chunk content = %v", content)
	}
	if !chunks[0].IsDebugLog {
		t.Fatal("chunk not marked is_debug_log")
	}
}

func TestDeepThinkingRunBlockingIsolatesErrors(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obtest/dt_ok": {output: `{"ok":"value"}`},
	})
	h.seedPrompt("obtest/dt_ok", "OK prompt")
	// obtest/dt_missing is never seeded, so GetDocument fails inside
	// executeSingle — the isolated-error path, no panic expected.

	stage := &StageConfig{
		Name: "problem_rca_discussion",
		DeepThinking: []DeepThinkingConfig{
			{Prompt: PromptConfig{Name: "obtest/dt_missing"}, Blocking: true},
			{Prompt: PromptConfig{Name: "obtest/dt_ok"}, Blocking: true},
		},
	}

	merged := h.manager.RunBlocking(context.Background(), stage, "transcript", "problem_rca_discussion", nil)

	if merged["ok"] != "value" {
		t.Fatalf("merged = %v, want ok=value despite the other DT failing", merged)
	}
	if len(merged) != 1 {
		t.Fatalf("merged = %v, want exactly the surviving key", merged)
	}
}

func TestDeepThinkingRunBlockingZeroConfigsStillSendsRTVI(t *testing.T) {
	h := newDTHarness(t, nil)
	stage := &StageConfig{Name: "introduction"}

	merged := h.manager.RunBlocking(context.Background(), stage, "", "introduction", nil)

	if len(merged) != 0 {
		t.Fatalf("merged = %v, want empty", merged)
	}
	if !h.hasRTVI("[PROCESS] Starting blocking deep thinking for introduction...") {
		t.Fatal("missing start RTVI message with zero blocking DTs")
	}
	if !h.hasRTVI("[PROCESS] Blocking DT for introduction took ") {
		t.Fatal("missing finish RTVI message with zero blocking DTs")
	}
	if len(h.chunks()) != 0 {
		t.Fatal("debug chunk written for empty merged result")
	}
}

// --- executeSingle output parsing (white-box) ---

func TestDeepThinkingExecuteSingleStripsThinkAndParsesJSONObject(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obtest/dt_think": {output: "<think>reasoning about the patient...</think>\n{\"diet_intensity_level\":\"moderate\"}"},
	})
	h.seedPrompt("obtest/dt_think", "prompt body")

	dt := DeepThinkingConfig{Prompt: PromptConfig{Name: "obtest/dt_think"}}
	result, err := h.manager.executeSingle(context.Background(), dt, "transcript", "stageX", nil)
	if err != nil {
		t.Fatalf("executeSingle: %v", err)
	}
	if result["diet_intensity_level"] != "moderate" {
		t.Fatalf("result = %v", result)
	}
}

func TestDeepThinkingExecuteSingleValidJSONNonObjectSkipped(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obtest/dt_array": {output: "[1,2,3]"},
	})
	h.seedPrompt("obtest/dt_array", "prompt body")

	dt := DeepThinkingConfig{Prompt: PromptConfig{Name: "obtest/dt_array"}}
	result, err := h.manager.executeSingle(context.Background(), dt, "transcript", "stageX", nil)
	if err != nil {
		t.Fatalf("executeSingle: %v", err)
	}
	if result != nil {
		t.Fatalf("result = %v, want nil for a valid-JSON-non-object output", result)
	}
}

func TestDeepThinkingExecuteSingleNonJSONFallsBackToPromptPathKey(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obcall_helpers/deep_thinking/free_text": {output: "just some free-form guidance text"},
	})
	h.seedPrompt("obcall_helpers/deep_thinking/free_text", "prompt body")

	dt := DeepThinkingConfig{Prompt: PromptConfig{Name: "obcall_helpers/deep_thinking/free_text"}}
	result, err := h.manager.executeSingle(context.Background(), dt, "transcript", "stageX", nil)
	if err != nil {
		t.Fatalf("executeSingle: %v", err)
	}
	wantKey := PromptPathToVarName("obcall_helpers/deep_thinking/free_text")
	if result[wantKey] != "just some free-form guidance text" {
		t.Fatalf("result = %v, want key %q", result, wantKey)
	}
}

// --- RunNonBlocking ---

func TestDeepThinkingRunNonBlockingCallsOnCompleteAndPersistsChunk(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obtest/dt_nb": {output: `{"fitness_intensity_level":"high"}`},
	})
	h.seedPrompt("obtest/dt_nb", "prompt body")

	stage := &StageConfig{
		Name: "diet_information",
		DeepThinking: []DeepThinkingConfig{
			{Prompt: PromptConfig{Name: "obtest/dt_nb"}, Blocking: false},
		},
	}

	var mu sync.Mutex
	var got map[string]any
	h.manager.RunNonBlocking(context.Background(), stage, "transcript", "diet_information", nil, func(result map[string]any) {
		mu.Lock()
		got = result
		mu.Unlock()
	})

	waitForDTCondition(t, "onComplete callback", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got != nil
	})
	if got["fitness_intensity_level"] != "high" {
		t.Fatalf("onComplete result = %v", got)
	}

	waitForDTCondition(t, "debug chunk written", func() bool { return len(h.chunks()) == 1 })
	var content map[string]any
	if err := json.Unmarshal([]byte(h.chunks()[0].Text), &content); err != nil {
		t.Fatalf("unmarshal chunk: %v", err)
	}
	if content["fitness_intensity_level"] != "high" {
		t.Fatalf("chunk content = %v", content)
	}
}

func TestDeepThinkingRunNonBlockingErrorSkipsOnCompleteAndSendsRTVI(t *testing.T) {
	h := newDTHarness(t, nil)
	// obtest/dt_missing is never seeded so GetDocument fails inside
	// executeSingle, exercising the generic (non-cancellation) error path.
	stage := &StageConfig{
		Name: "diet_information",
		DeepThinking: []DeepThinkingConfig{
			{Prompt: PromptConfig{Name: "obtest/dt_missing"}, Blocking: false},
		},
	}

	called := make(chan struct{}, 1)
	h.manager.RunNonBlocking(context.Background(), stage, "transcript", "diet_information", nil, func(result map[string]any) {
		called <- struct{}{}
	})

	waitForDTCondition(t, "error RTVI message", func() bool {
		return h.hasRTVI("Error in non-blocking DT diet_information: ")
	})
	select {
	case <-called:
		t.Fatal("onComplete called despite executeSingle error")
	case <-time.After(200 * time.Millisecond):
	}
	if len(h.chunks()) != 0 {
		t.Fatal("debug chunk written despite error")
	}
}

func TestDeepThinkingRunNonBlockingContextCancelledIsSilent(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obtest/dt_slow": {delay: 500 * time.Millisecond},
	})
	h.seedPrompt("obtest/dt_slow", "prompt body")

	stage := &StageConfig{
		Name: "diet_information",
		DeepThinking: []DeepThinkingConfig{
			{Prompt: PromptConfig{Name: "obtest/dt_slow"}, Blocking: false},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan struct{}, 1)
	h.manager.RunNonBlocking(ctx, stage, "transcript", "diet_information", nil, func(result map[string]any) {
		called <- struct{}{}
	})
	cancel()

	select {
	case <-called:
		t.Fatal("onComplete called for a cancelled DT")
	case <-time.After(700 * time.Millisecond):
	}
	if h.hasRTVI("Error in non-blocking DT") {
		t.Fatalf("cancelled DT recorded an error RTVI message: %v", h.rtviMessages())
	}
	if len(h.chunks()) != 0 {
		t.Fatal("debug chunk written for a cancelled DT")
	}
}

// --- prompt metadata passed to the client factory ---

func TestDeepThinkingPromptMetadataCarriesNameVersionAndVariables(t *testing.T) {
	h := newDTHarness(t, map[string]*dtStubClient{
		"obtest/dt_meta": {output: `{"k":"v"}`},
	})
	h.seedPrompt("obtest/dt_meta", "prompt body")

	dt := DeepThinkingConfig{Prompt: PromptConfig{Name: "obtest/dt_meta"}}
	vars := map[string]any{"gender": "female", "diet_intensity_level": "moderate"}
	if _, err := h.manager.executeSingle(context.Background(), dt, "transcript", "diet_information", vars); err != nil {
		t.Fatalf("executeSingle: %v", err)
	}

	calls := h.factory.snapshot()
	if len(calls) != 1 {
		t.Fatalf("factory calls = %d, want 1", len(calls))
	}
	call := calls[0]
	if call.usecaseType != "deep_thinking_diet_information" {
		t.Fatalf("usecaseType = %q", call.usecaseType)
	}
	if call.promptMetadata["system_prompt_name"] != "obtest/dt_meta" {
		t.Fatalf("system_prompt_name = %v", call.promptMetadata["system_prompt_name"])
	}
	if call.promptMetadata["system_prompt_version"] != 3 {
		t.Fatalf("system_prompt_version = %v, want the resolved seeded version 3", call.promptMetadata["system_prompt_version"])
	}
	gotVars, ok := call.promptMetadata["system_prompt_variables"].(DocumentVariables)
	if !ok {
		t.Fatalf("system_prompt_variables type = %T", call.promptMetadata["system_prompt_variables"])
	}
	if gotVars["gender"] != "female" || gotVars["diet_intensity_level"] != "moderate" {
		t.Fatalf("system_prompt_variables = %v", gotVars)
	}
	if _, hasUserFields := call.promptMetadata["user_prompt_name"]; hasUserFields {
		t.Fatal("prompt metadata carries user_prompt_name, want prompt-identity-only fields")
	}
}

// --- factory error handling ---

func TestNewDeepThinkingClientFactoryBuildsHedgedClient(t *testing.T) {
	logBuf := &syncBuffer{}
	logger := log.New(logBuf, "", 0)
	_, redisClient := newRedisTestClient(t)
	deps := Deps{Logger: logger, Redis: redisClient}

	// The production pair (GroupGPTOSS120FastHedged) is always
	// registered in llmrouter's groups table, so construction should
	// always succeed and yield a non-nil client satisfying LLMClient.
	factory := newDeepThinkingClientFactory(deps, logger, dtTestUserID, dtTestConversationID)
	client := factory(map[string]any{"system_prompt_name": "x"}, "deep_thinking_stage")
	if client == nil {
		t.Fatal("expected a non-nil hedged client for the registered production pair")
	}
}
