package disha

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	onboardingTestMainPrompt  = "onboarding_callV3_4/00_main_call_agent_sys"
	onboardingTestStagePrompt = "onboarding_callV3_4/01_introduction_and_call_overview"
)

// TestBuildOnboardingResumeMessage pins onboarding's distinct resume
// texts (conversation_context_manager.py) and confirms it shares
// buildResumeSystemMessage's gate exactly (same false cases), while the
// returned text is byte-exact to onboarding's wording — notably "1.If"
// with no space, unlike the shared "1. If" sales/follow-up text — and
// wrapped in <system_instruction> (not the shared <system_message> tag),
// ready to append verbatim via buildInitialMessages.
func TestBuildOnboardingResumeMessage(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	chunkID := "chunk-1"
	boolPtr := func(v bool) *bool { return &v }
	resumedChunk := func(age time.Duration) map[string]any {
		return map[string]any{"created": now.Add(-age).Format(time.RFC3339Nano)}
	}
	wrap := func(text string) string { return "<system_instruction>" + text + "</system_instruction>" }
	cases := []struct {
		name string
		data *ConversationData
		want string
	}{
		{"nil data", nil, ""},
		{"no resumed chunk id", &ConversationData{
			ResumedChunk: resumedChunk(time.Minute),
		}, ""},
		{"gracefully nil emits nothing", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID},
			ResumedChunk: resumedChunk(time.Minute),
		}, ""},
		{"gracefully false emits nothing", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(false)},
			ResumedChunk: resumedChunk(time.Minute),
		}, ""},
		{"missing created timestamp emits nothing", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(true)},
			ResumedChunk: map[string]any{},
		}, ""},
		{"gracefully true within window", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(true)},
			ResumedChunk: resumedChunk(time.Minute),
		}, wrap(onboardingResumeMessageWithinWindow)},
		{"gracefully true after window", &ConversationData{
			Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(true)},
			ResumedChunk: resumedChunk(10 * time.Minute),
		}, wrap(onboardingResumeMessageAfterWindow)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildOnboardingResumeMessage(tc.data, now); got != tc.want {
				t.Fatalf("buildOnboardingResumeMessage = %q, want %q", got, tc.want)
			}
		})
	}

	within := buildOnboardingResumeMessage(&ConversationData{
		Conversation: ConversationRow{ResumedFromChunkID: &chunkID, ResumeGracefully: boolPtr(true)},
		ResumedChunk: resumedChunk(time.Minute),
	}, now)
	if !strings.Contains(within, "1.If the interruption was user initiated(like") {
		t.Fatalf("onboarding within-window text missing the byte-exact '1.If' line: %q", within)
	}
	if strings.Contains(within, "1. If the interruption") {
		t.Fatalf("onboarding within-window text must not use the shared sales/follow-up '1. If' wording: %q", within)
	}
}

// seedOnboardingFixtures loads the captured student_test staging config
// and all its stage prompts into miniredis under their real document
// keys (pinned-version keys like document:{name}:v{n}).
func seedOnboardingFixtures(t *testing.T, server *miniredis.Miniredis) {
	t.Helper()
	configRaw, err := os.ReadFile("testdata/onboarding/student_test_config.json")
	if err != nil {
		t.Fatalf("read config fixture: %v", err)
	}
	server.Set("document:OB_Call_Configs/student_test_config:latest", string(configRaw))

	promptsRaw, err := os.ReadFile("testdata/onboarding/student_test_prompts.json")
	if err != nil {
		t.Fatalf("read prompts fixture: %v", err)
	}
	var prompts map[string]struct {
		Key string          `json:"key"`
		Doc json.RawMessage `json:"doc"`
	}
	if err := json.Unmarshal(promptsRaw, &prompts); err != nil {
		t.Fatalf("unmarshal prompts fixture: %v", err)
	}
	for name, entry := range prompts {
		if entry.Key == "" {
			t.Fatalf("prompt fixture %q has no redis key", name)
		}
		server.Set(entry.Key, string(entry.Doc))
	}
}

func newOnboardingTestConfig(t *testing.T, redisClient RedisClient) *OnboardingConfig {
	t.Helper()
	store := newDocumentStore(redisClient, testDeps(redisClient, nil).Logger, simpleTemplateRenderer{})
	cfg, err := LoadOnboardingConfig(context.Background(), store, "student_test")
	if err != nil {
		t.Fatalf("LoadOnboardingConfig: %v", err)
	}
	return cfg
}

func TestOnboardingPromptCompilerEmbedsStageAnalysis(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedOnboardingFixtures(t, redisServer)
	deps := testDeps(redisClient, nil)
	cfg := newOnboardingTestConfig(t, redisClient)

	compiler := &onboardingPromptCompiler{
		docs:        deps.Documents,
		config:      cfg,
		patientInfo: "PATIENT_INFO_SENTINEL",
		profileVars: map[string]any{"gender": "female"},
	}
	compiled, err := compiler.CompileSystemPrompt(context.Background(), &cfg.StartStage, map[string]any{})
	if err != nil {
		t.Fatalf("CompileSystemPrompt: %v", err)
	}

	if !containsAll(compiled.Text, "You are Disha", "Introduction and Call Overview", "PATIENT_INFO_SENTINEL") {
		t.Fatalf("compiled text missing main prompt, stage analysis, or patient info:\n%.500s", compiled.Text)
	}
	if compiled.MainVersion != 1 || compiled.StageVersion != 3 {
		t.Fatalf("versions = main %d stage %d, want 1 and 3", compiled.MainVersion, compiled.StageVersion)
	}
	if _, ok := compiled.MetadataVars["analysis"]; ok {
		t.Fatalf("MetadataVars must not carry analysis (Python parity): %v", compiled.MetadataVars)
	}
	if compiled.MetadataVars["patient_info"] != "PATIENT_INFO_SENTINEL" ||
		compiled.MetadataVars["gender"] != "female" {
		t.Fatalf("MetadataVars mismatch: %v", compiled.MetadataVars)
	}
	if s, ok := compiled.MetadataVars["current_datetime"].(string); !ok || s == "" {
		t.Fatalf("current_datetime = %#v, want non-empty string", compiled.MetadataVars["current_datetime"])
	}
}

func TestOnboardingPromptCompilerVariableStoreWins(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedOnboardingFixtures(t, redisServer)
	deps := testDeps(redisClient, nil)
	cfg := newOnboardingTestConfig(t, redisClient)

	compiler := &onboardingPromptCompiler{
		docs:        deps.Documents,
		config:      cfg,
		patientInfo: "FROM_CONVERSATION",
		profileVars: map[string]any{"gender": "female"},
	}
	// Python's `**merged` comes last in the main render vars, so a
	// variable-store value overrides even patient_info on collision.
	compiled, err := compiler.CompileSystemPrompt(context.Background(), &cfg.StartStage, map[string]any{
		"patient_info": "FROM_VARIABLE_STORE",
		"gender":       "male",
	})
	if err != nil {
		t.Fatalf("CompileSystemPrompt: %v", err)
	}
	if !strings.Contains(compiled.Text, "FROM_VARIABLE_STORE") || strings.Contains(compiled.Text, "FROM_CONVERSATION") {
		t.Fatalf("variable store should override patient_info in render:\n%.300s", compiled.Text)
	}
	if compiled.MetadataVars["patient_info"] != "FROM_VARIABLE_STORE" || compiled.MetadataVars["gender"] != "male" {
		t.Fatalf("MetadataVars merge order mismatch: %v", compiled.MetadataVars)
	}
}

func TestOnboardingPromptCompilerMissingStagePrompt(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedOnboardingFixtures(t, redisServer)
	deps := testDeps(redisClient, nil)
	cfg := newOnboardingTestConfig(t, redisClient)

	compiler := &onboardingPromptCompiler{
		docs:        deps.Documents,
		config:      cfg,
		patientInfo: "info",
		profileVars: map[string]any{"gender": ""},
	}
	missing := &StageConfig{
		Name:   "ghost",
		Prompt: PromptConfig{Name: "onboarding_callV3_4/does_not_exist", Version: 9},
	}
	if _, err := compiler.CompileSystemPrompt(context.Background(), missing, nil); err == nil {
		t.Fatal("missing stage prompt: want error")
	}
}

func seedOnboardingConversation(t *testing.T, server *miniredis.Miniredis, conversationID string, variant *string) {
	t.Helper()
	edPeRXVariant := "test"
	seedConversationData(t, server, conversationID, ConversationData{
		Conversation: ConversationRow{
			ID:          conversationID,
			UserID:      "user-ob",
			BotType:     OnboardingCallBotType,
			PatientInfo: "Riya, age 32, wants better sleep",
		},
		UserProfile: UserProfileData{
			UserID:                "user-ob",
			OnboardingCallVariant: variant,
			EDPeRXVariant:         &edPeRXVariant,
			Gender:                "female",
		},
	})
}

func TestOnboardingCallBotPlanBuildsFreshStartStageCall(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	seedOnboardingFixtures(t, redisServer)
	variant := "student_test"
	conversationID := "conv-ob-1"
	seedOnboardingConversation(t, redisServer, conversationID, &variant)

	pl, err := OnboardingCallBot{}.plan(context.Background(), conversationID, testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("OnboardingCallBot.plan: %v", err)
	}

	if pl.Config.Model != "grok-4.1-fast" {
		t.Fatalf("Config.Model = %q, want grok-4.1-fast", pl.Config.Model)
	}
	if pl.State.CurrentStage().Name != "introduction" {
		t.Fatalf("start stage = %q, want introduction", pl.State.CurrentStage().Name)
	}

	if len(pl.InitialMessages) != 2 ||
		pl.InitialMessages[0].Role != "system" ||
		!containsAll(pl.InitialMessages[0].Content, "You are Disha", "Introduction and Call Overview", "Riya, age 32, wants better sleep") ||
		pl.InitialMessages[1].Role != "user" ||
		pl.InitialMessages[1].Content != "hello?" {
		t.Fatalf("InitialMessages mismatch: roles=%v", messageRoles(pl.InitialMessages))
	}

	if pl.PromptKey != onboardingTestMainPrompt+"_v1" {
		t.Fatalf("PromptKey = %q, want %s_v1", pl.PromptKey, onboardingTestMainPrompt)
	}

	if pl.PromptMetadata["system_prompt_name"] != onboardingTestMainPrompt ||
		pl.PromptMetadata["system_prompt_version"] != 1 ||
		pl.PromptMetadata["stage_prompt_name"] != onboardingTestStagePrompt ||
		pl.PromptMetadata["stage_prompt_version"] != 3 {
		t.Fatalf("PromptMetadata identity = %+v", pl.PromptMetadata)
	}
	promptVars, ok := pl.PromptMetadata["system_prompt_variables"].(DocumentVariables)
	if !ok {
		t.Fatalf("system_prompt_variables = %#v, want DocumentVariables", pl.PromptMetadata["system_prompt_variables"])
	}
	if promptVars["gender"] != "female" ||
		promptVars["ed_pe_rx_variant"] != "test" ||
		promptVars["patient_info"] != "Riya, age 32, wants better sleep" {
		t.Fatalf("system_prompt_variables mismatch: %v", promptVars)
	}
	if _, ok := promptVars["analysis"]; ok {
		t.Fatalf("system_prompt_variables must not carry analysis: %v", promptVars)
	}

	if len(pl.Tools) != 1 || pl.Tools[0].Function.Name != endCallToolName {
		t.Fatalf("Tools = %+v, want single end_call", pl.Tools)
	}

	// Committed turns must persist current_agenda from the live stage.
	events := pl.Callbacks.Events()
	turnAt := time.Date(2026, 7, 7, 10, 0, 0, 0, time.UTC)
	events.OnUserTurnCommitted("mujhe neend nahi aati", turnAt, pl.PromptKey)
	events.OnAssistantTurnCommitted("samajh gayi", turnAt.Add(time.Second), voicepipelinecore.TurnMetrics{}, pl.PromptKey)

	chunkItems, err := redisServer.List(conversationChunksKey("user-ob", conversationID))
	if err != nil {
		t.Fatalf("List chunks: %v", err)
	}
	if len(chunkItems) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunkItems))
	}
	for i, item := range chunkItems {
		var chunk ConversationChunk
		if err := json.Unmarshal([]byte(item), &chunk); err != nil {
			t.Fatalf("Unmarshal chunk %d: %v", i, err)
		}
		if chunk.CurrentAgenda == nil || *chunk.CurrentAgenda != "introduction" {
			t.Fatalf("chunk %d current_agenda = %v, want introduction", i, chunk.CurrentAgenda)
		}
		if chunk.BotType != OnboardingCallBotType {
			t.Fatalf("chunk %d bot_type = %q, want %s", i, chunk.BotType, OnboardingCallBotType)
		}
		if chunk.MainAgentSystemPromptLangfuseKey == nil || *chunk.MainAgentSystemPromptLangfuseKey != pl.PromptKey {
			t.Fatalf("chunk %d prompt key = %v, want %s", i, chunk.MainAgentSystemPromptLangfuseKey, pl.PromptKey)
		}
	}

	// The agenda provider tracks the live stage, not a snapshot.
	pl.State.AdvanceStage(&pl.Config.CommonStages[0])
	events.OnUserTurnCommitted("aage badhte hain", turnAt.Add(2*time.Second), pl.PromptKey)
	chunkItems, err = redisServer.List(conversationChunksKey("user-ob", conversationID))
	if err != nil {
		t.Fatalf("List chunks after advance: %v", err)
	}
	var advanced ConversationChunk
	if err := json.Unmarshal([]byte(chunkItems[len(chunkItems)-1]), &advanced); err != nil {
		t.Fatalf("Unmarshal advanced chunk: %v", err)
	}
	if advanced.CurrentAgenda == nil || *advanced.CurrentAgenda != pl.Config.CommonStages[0].Name {
		t.Fatalf("advanced chunk current_agenda = %v, want %s", advanced.CurrentAgenda, pl.Config.CommonStages[0].Name)
	}
}

func TestOnboardingCallBotPlanRequiresVariant(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	seedOnboardingFixtures(t, redisServer)

	seedOnboardingConversation(t, redisServer, "conv-no-variant", nil)
	if _, err := (OnboardingCallBot{}).plan(context.Background(), "conv-no-variant", testDeps(redisClient, api)); err == nil {
		t.Fatal("missing onboarding_call_variant: want error")
	}

	empty := "  "
	seedOnboardingConversation(t, redisServer, "conv-blank-variant", &empty)
	if _, err := (OnboardingCallBot{}).plan(context.Background(), "conv-blank-variant", testDeps(redisClient, api)); err == nil {
		t.Fatal("blank onboarding_call_variant: want error")
	}
}

func TestNewBotReturnsOnboardingCallBot(t *testing.T) {
	bot, err := NewBot(OnboardingCallBotType)
	if err != nil {
		t.Fatalf("NewBot: %v", err)
	}
	if bot.BotType() != OnboardingCallBotType {
		t.Fatalf("BotType = %q, want %s", bot.BotType(), OnboardingCallBotType)
	}
	if _, ok := bot.(OnboardingCallBot); !ok {
		t.Fatalf("bot type = %T, want OnboardingCallBot", bot)
	}
}

func messageRoles(msgs []voicepipelinecore.Message) []string {
	roles := make([]string, len(msgs))
	for i, m := range msgs {
		roles[i] = m.Role
	}
	return roles
}

// --- loadOnboardingResumeState ---------------------------------------

func TestLoadOnboardingResumeStateHappyPath(t *testing.T) {
	getter := fakeS3GetClient{objects: map[string][]byte{
		"conversation_state/conv-1/chunk-1.json": []byte(`{"agenda":"introduction","variable_store":{"k":"v"}}`),
	}}
	state, err := loadOnboardingResumeState(context.Background(), getter, "conversation_state/conv-1/chunk-1.json", nil)
	if err != nil {
		t.Fatalf("loadOnboardingResumeState: %v", err)
	}
	if state["agenda"] != "introduction" {
		t.Fatalf("state[agenda] = %v, want introduction", state["agenda"])
	}
	store, _ := state["variable_store"].(map[string]any)
	if store["k"] != "v" {
		t.Fatalf("state[variable_store] = %v", state["variable_store"])
	}
}

func TestLoadOnboardingResumeStateDownloadError(t *testing.T) {
	getter := fakeS3GetClient{objects: map[string][]byte{}}
	if _, err := loadOnboardingResumeState(context.Background(), getter, "missing.json", nil); err == nil {
		t.Fatal("download error: want error")
	}
}

func TestLoadOnboardingResumeStateInvalidJSON(t *testing.T) {
	getter := fakeS3GetClient{objects: map[string][]byte{
		"bad.json": []byte("not json"),
	}}
	if _, err := loadOnboardingResumeState(context.Background(), getter, "bad.json", nil); err == nil {
		t.Fatal("invalid JSON: want error")
	}
}

func TestLoadOnboardingResumeStateNilGetter(t *testing.T) {
	if _, err := loadOnboardingResumeState(context.Background(), nil, "key.json", nil); err == nil {
		t.Fatal("nil getter: want error")
	}
}

// --- plan()-level resume ----------------------------------------------

// stubOnboardingResumeGetter overrides the package-level S3-getter seam
// (like sttDialURL/ttsDialURL) so plan() exercises a fake client instead
// of building one from AWS env/network.
func stubOnboardingResumeGetter(t *testing.T, getter S3GetClient) {
	t.Helper()
	prev := onboardingResumeS3Getter
	onboardingResumeS3Getter = func(*log.Logger) S3GetClient { return getter }
	t.Cleanup(func() { onboardingResumeS3Getter = prev })
}

// seedOnboardingResumeConversation seeds conversation_data for a resumed
// onboarding call: a resumed_chunk carrying conversation_state_s3_key
// plus prior transcript chunks to replay.
func seedOnboardingResumeConversation(t *testing.T, server *miniredis.Miniredis, conversationID string, resumedID string, gracefully *bool, created string, s3Key string) {
	t.Helper()
	variant := "student_test"
	resumedChunk := map[string]any{
		"id":      resumedID,
		"created": created,
	}
	if s3Key != "" {
		resumedChunk["conversation_state_s3_key"] = s3Key
	}
	seedConversationData(t, server, conversationID, ConversationData{
		Conversation: ConversationRow{
			ID:                 conversationID,
			UserID:             "user-ob",
			BotType:            OnboardingCallBotType,
			PatientInfo:        "Riya, age 32, wants better sleep",
			ResumedFromChunkID: &resumedID,
			ResumeGracefully:   gracefully,
		},
		Chunks: [][]any{
			{"chunk-prev-1", "user", "I was telling you about my sleep", false, nil},
			{"chunk-prev-2", "assistant", "Got it, let's continue", false, nil},
		},
		ResumedChunk: resumedChunk,
		UserProfile: UserProfileData{
			UserID:                "user-ob",
			OnboardingCallVariant: &variant,
			Gender:                "female",
		},
	})
}

func TestOnboardingCallBotPlanResumesFromValidState(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	seedOnboardingFixtures(t, redisServer)

	gracefully := true
	recent := time.Now().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	conversationID := "conv-ob-resume"
	s3Key := "conversation_state/" + conversationID + "/chunk-prev.json"
	seedOnboardingResumeConversation(t, redisServer, conversationID, "chunk-prev", &gracefully, recent, s3Key)

	getter := fakeS3GetClient{objects: map[string][]byte{
		s3Key: []byte(`{"agenda":"problem_rca_discussion","variable_store":{"x":"y"}}`),
	}}
	stubOnboardingResumeGetter(t, getter)

	pl, err := OnboardingCallBot{}.plan(context.Background(), conversationID, testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	if pl.State.CurrentStage().Name != "problem_rca_discussion" {
		t.Fatalf("resumed stage = %q, want problem_rca_discussion", pl.State.CurrentStage().Name)
	}
	if plan := pl.State.SelectedCarePlan(); plan == nil || plan.Name != "general" {
		t.Fatalf("resumed care plan = %v, want general (discovered)", plan)
	}
	if pl.PromptMetadata["stage_prompt_name"] != "onboarding_callV3_4/general/01_problem_rca_discussion" {
		t.Fatalf("stage_prompt_name = %v, want the resumed stage's prompt, not the start stage's", pl.PromptMetadata["stage_prompt_name"])
	}

	// Compiled system prompt embeds the RESUMED stage's analysis, not the
	// start stage's introduction prompt.
	if !containsAll(pl.InitialMessages[0].Content, "Problem RCA Discussion") ||
		strings.Contains(pl.InitialMessages[0].Content, "Introduction and Call Overview") {
		t.Fatalf("compiled system prompt did not switch to the resumed stage:\n%.500s", pl.InitialMessages[0].Content)
	}

	// Chunk history is replayed regardless of which state won.
	if len(pl.InitialMessages) != 4 ||
		pl.InitialMessages[1].Role != "user" || pl.InitialMessages[1].Content != "I was telling you about my sleep" ||
		pl.InitialMessages[2].Role != "assistant" || pl.InitialMessages[2].Content != "Got it, let's continue" {
		t.Fatalf("InitialMessages replay mismatch: %+v", pl.InitialMessages)
	}

	// The onboarding resume nudge is the final message, wrapped in
	// <system_instruction> (not the shared <system_message> tag).
	last := pl.InitialMessages[3]
	if last.Role != "user" ||
		!containsAll(last.Content, "<system_instruction>", "hanji to aap keh", "</system_instruction>") {
		t.Fatalf("resume nudge missing or wrongly wrapped: %+v", last)
	}
}

func TestOnboardingCallBotPlanResumeGracefullyFalseSkipsNudge(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	seedOnboardingFixtures(t, redisServer)

	gracefully := false
	recent := time.Now().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	conversationID := "conv-ob-resume-explicit-chunk"
	s3Key := "conversation_state/" + conversationID + "/chunk-prev.json"
	seedOnboardingResumeConversation(t, redisServer, conversationID, "chunk-prev", &gracefully, recent, s3Key)

	getter := fakeS3GetClient{objects: map[string][]byte{
		s3Key: []byte(`{"agenda":"problem_rca_discussion"}`),
	}}
	stubOnboardingResumeGetter(t, getter)

	pl, err := OnboardingCallBot{}.plan(context.Background(), conversationID, testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	// State still resumes (resume_gracefully only gates the nudge, not
	// state restoration) and chunks are still replayed...
	if pl.State.CurrentStage().Name != "problem_rca_discussion" {
		t.Fatalf("resumed stage = %q, want problem_rca_discussion", pl.State.CurrentStage().Name)
	}
	if len(pl.InitialMessages) != 3 {
		t.Fatalf("InitialMessages = %+v, want system + 2 replayed chunks with no nudge", pl.InitialMessages)
	}
	for _, msg := range pl.InitialMessages {
		if strings.Contains(msg.Content, "system_instruction") {
			t.Fatalf("resume nudge must not be emitted when resume_gracefully=false: %+v", pl.InitialMessages)
		}
	}
}

func TestOnboardingCallBotPlanFallsBackToFreshStateOnDownloadFailure(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	seedOnboardingFixtures(t, redisServer)

	gracefully := true
	recent := time.Now().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	conversationID := "conv-ob-resume-download-fail"
	s3Key := "conversation_state/" + conversationID + "/chunk-prev.json"
	seedOnboardingResumeConversation(t, redisServer, conversationID, "chunk-prev", &gracefully, recent, s3Key)

	// The fake client has no object for s3Key, so GetObject errors and
	// resolveOnboardingState must fall back to a fresh state rather than
	// failing plan() outright.
	getter := fakeS3GetClient{objects: map[string][]byte{}}
	stubOnboardingResumeGetter(t, getter)

	pl, err := OnboardingCallBot{}.plan(context.Background(), conversationID, testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("plan: want no error on resume-state download failure, got %v", err)
	}
	if pl.State.CurrentStage().Name != "introduction" {
		t.Fatalf("fallback stage = %q, want fresh start stage introduction", pl.State.CurrentStage().Name)
	}
	// Chunk history is still replayed even though state resume failed.
	if len(pl.InitialMessages) != 4 ||
		pl.InitialMessages[1].Content != "I was telling you about my sleep" ||
		pl.InitialMessages[2].Content != "Got it, let's continue" {
		t.Fatalf("InitialMessages replay mismatch after fallback: %+v", pl.InitialMessages)
	}
}

func TestOnboardingCallBotPlanMissingAgendaFallsBackToStartStage(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	seedOnboardingFixtures(t, redisServer)

	gracefully := true
	recent := time.Now().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	conversationID := "conv-ob-resume-no-agenda"
	s3Key := "conversation_state/" + conversationID + "/chunk-prev.json"
	seedOnboardingResumeConversation(t, redisServer, conversationID, "chunk-prev", &gracefully, recent, s3Key)

	// No "agenda" key at all: Python's setdefault("agenda", "Introduction")
	// applies, but student_test's start stage is named "introduction"
	// (lower-case), so the capitalized default does not resolve either —
	// ConversationStateFromResume's own fallback to the configured start
	// stage is what actually wins here.
	getter := fakeS3GetClient{objects: map[string][]byte{
		s3Key: []byte(`{"variable_store":{"k":"v"}}`),
	}}
	stubOnboardingResumeGetter(t, getter)

	pl, err := OnboardingCallBot{}.plan(context.Background(), conversationID, testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if pl.State.CurrentStage().Name != pl.Config.StartStage.Name {
		t.Fatalf("stage = %q, want start-stage fallback %q", pl.State.CurrentStage().Name, pl.Config.StartStage.Name)
	}
}

func TestResolveOnboardingStateNoResumedChunkIsFresh(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedOnboardingFixtures(t, redisServer)
	cfg := newOnboardingTestConfig(t, redisClient)

	state := resolveOnboardingState(context.Background(), &ConversationData{}, cfg, "student_test", nil)
	if state.CurrentStage().Name != cfg.StartStage.Name {
		t.Fatalf("no resumed chunk: stage = %q, want start stage %q", state.CurrentStage().Name, cfg.StartStage.Name)
	}
}

func TestResolveOnboardingStateEmptyBodyFallsBack(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedOnboardingFixtures(t, redisServer)
	cfg := newOnboardingTestConfig(t, redisClient)

	resumedID := "chunk-prev"
	s3Key := "conversation_state/conv/chunk-prev.json"
	data := &ConversationData{
		Conversation: ConversationRow{ResumedFromChunkID: &resumedID},
		ResumedChunk: map[string]any{
			"id":                        resumedID,
			"conversation_state_s3_key": s3Key,
		},
	}
	getter := fakeS3GetClient{objects: map[string][]byte{s3Key: []byte(`{}`)}}
	stubOnboardingResumeGetter(t, getter)

	state := resolveOnboardingState(context.Background(), data, cfg, "student_test", nil)
	if state.CurrentStage().Name != cfg.StartStage.Name {
		t.Fatalf("empty resume state: stage = %q, want start stage fallback %q", state.CurrentStage().Name, cfg.StartStage.Name)
	}
}
