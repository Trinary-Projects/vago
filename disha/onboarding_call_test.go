package disha

import (
	"context"
	"encoding/json"
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
	if promptVars["gender"] != "female" || promptVars["patient_info"] != "Riya, age 32, wants better sleep" {
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
