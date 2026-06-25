package disha

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

type fakeS3GetClient struct {
	objects map[string][]byte
}

func (f fakeS3GetClient) GetObject(_ context.Context, _, objectKey string) ([]byte, error) {
	if body, ok := f.objects[objectKey]; ok {
		return append([]byte(nil), body...), nil
	}
	return nil, errors.New("not found")
}

func seedDocumentWithConfig(t *testing.T, server *miniredis.Miniredis, name, env string, version int, body string, config map[string]any) {
	t.Helper()
	doc := DocumentVersion{
		ID:         "doc-" + name,
		PromptText: body,
		ConfigJSON: config,
		Version:    version,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal document %q: %v", name, err)
	}
	server.Set("document:"+name+":"+env, string(raw))
}

func endCallToolConfig() []any {
	return []any{
		map[string]any{"function": map[string]any{
			"name":        "end_call",
			"description": "End the call.",
			"parameters": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
				"required":   []any{},
			},
		}},
	}
}

func TestFollowUpBotPlanSelectsAgendaPrompt(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)

	conversationID := "follow-1"
	userID := "user-1"
	shortTerm := "Sleeps late"
	unprocessed := "Recent chat note"
	schedule := map[string]any{
		"checkin_slots": map[string]any{"morning": "8 AM"},
	}
	seedDocument(
		t,
		redisServer,
		followUpPromptD1Inactive,
		"production",
		9,
		"FOLLOWUP patient={{ patient_info }} memory={{ patient_memory }} name={{ patient_name }} pronoun={{ he_she }} schedule={{ patient_schedule }}",
	)
	seedConversationData(t, redisServer, conversationID, ConversationData{
		Conversation: ConversationRow{
			ID:          conversationID,
			UserID:      userID,
			BotType:     FollowUpBotType,
			PatientInfo: "Riya, age 32",
			Agenda:      "d1_inactive_checkin",
		},
		Chunks: [][]any{
			{"chunk-1", "user", "hello", false, nil},
		},
		UserProfile: UserProfileData{
			UserID:             userID,
			ShortTermMemory:    &shortTerm,
			IdealCallTimeSlots: schedule,
			DevanagariName:     "रिया",
			FirstName:          "Riya",
			Gender:             "female",
		},
		UnprocessedChatContext: &unprocessed,
	})

	pl, err := FollowUpBot{}.plan(context.Background(), conversationID, testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("FollowUpBot.plan: %v", err)
	}
	if pl.Dynamic {
		t.Fatal("Dynamic = true, want false")
	}
	if pl.ModelGroup != followUpModelGroup {
		t.Fatalf("ModelGroup = %q, want %q", pl.ModelGroup, followUpModelGroup)
	}
	if pl.PromptKey != "disha_init_calls/d0_d1_inactive_user/call_main_sys_v9" {
		t.Fatalf("PromptKey = %q", pl.PromptKey)
	}
	if len(pl.Tools) != 0 {
		t.Fatalf("Tools = %+v, want none for regular follow-up", pl.Tools)
	}
	if len(pl.InitialMessages) != 2 ||
		pl.InitialMessages[0].Role != "system" ||
		!containsAll(pl.InitialMessages[0].Content, "FOLLOWUP", "Riya, age 32", "Sleeps late\n\nRecent chat note", "रिया", "she") ||
		pl.InitialMessages[1].Role != "user" ||
		pl.InitialMessages[1].Content != "hello" {
		t.Fatalf("InitialMessages = %+v", pl.InitialMessages)
	}
	if pl.PromptMetadata["system_prompt_name"] != followUpPromptD1Inactive ||
		pl.PromptMetadata["system_prompt_version"] != 9 {
		t.Fatalf("PromptMetadata identity = %+v", pl.PromptMetadata)
	}
	if len(pl.PromptMetadata) != 3 {
		t.Fatalf("PromptMetadata keys = %+v, want only system prompt triplet", pl.PromptMetadata)
	}

	vars, ok := pl.PromptMetadata["system_prompt_variables"].(DocumentVariables)
	if !ok {
		t.Fatalf("system_prompt_variables = %#v, want DocumentVariables", pl.PromptMetadata["system_prompt_variables"])
	}
	if vars["patient_memory"] != "Sleeps late\n\nRecent chat note" {
		t.Fatalf("patient_memory = %#v", vars["patient_memory"])
	}
	// Python extracts checkin_slots: `_slots.get("checkin_slots") or _slots`.
	if !reflect.DeepEqual(vars["patient_schedule"], schedule["checkin_slots"]) {
		t.Fatalf("patient_schedule = %#v, want inner checkin_slots", vars["patient_schedule"])
	}
}

func TestFollowUpBotPlanLoadsEndCallToolForRegularPrompt(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)

	seedDocumentWithConfig(
		t,
		redisServer,
		followUpPromptDefault,
		"production",
		4,
		"FOLLOWUP",
		map[string]any{"tools": endCallToolConfig()},
	)
	seedConversationData(t, redisServer, "follow-regular-tools", ConversationData{
		Conversation: ConversationRow{
			ID:      "follow-regular-tools",
			UserID:  "user-1",
			BotType: FollowUpBotType,
		},
		UserProfile: UserProfileData{UserID: "user-1"},
	})

	pl, err := FollowUpBot{}.plan(context.Background(), "follow-regular-tools", testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("FollowUpBot.plan: %v", err)
	}
	if pl.Dynamic {
		t.Fatal("Dynamic = true, want false")
	}
	if len(pl.Tools) != 1 || pl.Tools[0].Function.Name != endCallToolName {
		t.Fatalf("Tools = %+v, want end_call", pl.Tools)
	}
}

func TestPatientScheduleFromSlots(t *testing.T) {
	checkin := map[string]any{"morning": "8 AM"}
	cases := []struct {
		name  string
		slots map[string]any
		want  any
	}{
		{"nil slots", nil, map[string]any{}},
		{"checkin_slots extracted", map[string]any{"checkin_slots": checkin, "daily_routine": "x"}, checkin},
		{"empty checkin_slots falls back to whole map", map[string]any{"checkin_slots": map[string]any{}, "evening": "7 PM"}, map[string]any{"checkin_slots": map[string]any{}, "evening": "7 PM"}},
		{"no checkin_slots key", map[string]any{"evening": "7 PM"}, map[string]any{"evening": "7 PM"}},
	}
	for _, tc := range cases {
		if got := patientScheduleFromSlots(tc.slots); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: patientScheduleFromSlots = %#v, want %#v", tc.name, got, tc.want)
		}
	}
}

func TestFollowUpBotPlanPhoneOverrideUsesGPT41Group(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)

	seedDocumentWithConfig(t, redisServer, followUpPromptInvestorDemo, "production", 3, "INVESTOR", map[string]any{"tools": endCallToolConfig()})
	seedConversationData(t, redisServer, "follow-phone", ConversationData{
		Conversation: ConversationRow{
			ID:      "follow-phone",
			UserID:  "user-1",
			BotType: FollowUpBotType,
		},
		UserProfile: UserProfileData{
			UserID: "user-1",
			Phone:  followUpPhonePromptOverridePhone,
		},
	})

	pl, err := FollowUpBot{}.plan(context.Background(), "follow-phone", testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("FollowUpBot.plan: %v", err)
	}
	if pl.PromptKey != "misc/investor_demo_v3" {
		t.Fatalf("PromptKey = %q, want investor prompt", pl.PromptKey)
	}
	if pl.ModelGroup != followUpPhoneOverrideModelGroup {
		t.Fatalf("ModelGroup = %q, want %q", pl.ModelGroup, followUpPhoneOverrideModelGroup)
	}
	if len(pl.Tools) != 1 || pl.Tools[0].Function.Name != endCallToolName {
		t.Fatalf("Tools = %+v, want end_call", pl.Tools)
	}
}

func TestFollowUpBotPlanDynamicLoadsCallFlowAndTools(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)

	toolsConfig := []any{
		map[string]any{"function": map[string]any{
			"name":        "get_guidance",
			"description": "Get guidance for the next step.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"situation": map[string]any{"type": "string"},
				},
				"required": []any{"situation"},
			},
		}},
		map[string]any{"function": map[string]any{
			"name":        "end_call",
			"description": "End the call.",
			"parameters": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
				"required":   []any{},
			},
		}},
	}
	seedDocumentWithConfig(
		t,
		redisServer,
		followUpDynamicMainPrompt,
		"production",
		12,
		"DYNAMIC call_flow={{ call_flow }} patient={{ patient_name }}",
		map[string]any{"tools": toolsConfig},
	)
	seedConversationData(t, redisServer, "follow-dynamic", ConversationData{
		Conversation: ConversationRow{
			ID:                    "follow-dynamic",
			UserID:                "user-1",
			BotType:               FollowUpBotType,
			CallFlowKey:           "weekly_checkin",
			CompiledCallFlowS3Key: "compiled/flow.json",
		},
		UserProfile: UserProfileData{
			UserID:    "user-1",
			FirstName: "Riya",
		},
	})
	deps := testDeps(redisClient, api)
	deps.S3 = fakeS3GetClient{objects: map[string][]byte{"compiled/flow.json": []byte("CALL FLOW BODY")}}

	pl, err := FollowUpBot{}.plan(context.Background(), "follow-dynamic", deps)
	if err != nil {
		t.Fatalf("FollowUpBot.plan: %v", err)
	}
	if !pl.Dynamic {
		t.Fatal("Dynamic = false, want true")
	}
	if pl.ModelGroup != followUpDynamicModelGroup {
		t.Fatalf("ModelGroup = %q, want %q", pl.ModelGroup, followUpDynamicModelGroup)
	}
	if pl.PromptKey != "disha_init_calls/dynamic_checkin_call/main_sys_v12" {
		t.Fatalf("PromptKey = %q", pl.PromptKey)
	}
	if len(pl.InitialMessages) != 2 ||
		pl.InitialMessages[0].Role != "system" ||
		!containsAll(pl.InitialMessages[0].Content, "DYNAMIC", "CALL FLOW BODY", "Riya") ||
		pl.InitialMessages[1].Role != "user" ||
		pl.InitialMessages[1].Content != "hello?" {
		t.Fatalf("InitialMessages = %+v", pl.InitialMessages)
	}
	if len(pl.Tools) != 2 ||
		pl.Tools[0].Function.Name != "get_guidance" ||
		pl.Tools[1].Function.Name != "end_call" {
		t.Fatalf("Tools = %+v", pl.Tools)
	}
	required, _ := pl.Tools[0].Function.Parameters["required"].([]any)
	if len(required) != 1 || required[0] != "situation" {
		t.Fatalf("get_guidance required = %#v, want situation", pl.Tools[0].Function.Parameters["required"])
	}
	if pl.PromptMetadata["system_prompt_name"] != followUpDynamicMainPrompt ||
		pl.PromptMetadata["system_prompt_version"] != 12 {
		t.Fatalf("PromptMetadata identity = %+v", pl.PromptMetadata)
	}
	if len(pl.PromptMetadata) != 3 {
		t.Fatalf("PromptMetadata keys = %+v, want only system prompt triplet", pl.PromptMetadata)
	}
	if _, ok := pl.PromptMetadata["call_flow_key"]; ok {
		t.Fatalf("PromptMetadata should not include call_flow_key: %+v", pl.PromptMetadata)
	}
	if _, ok := pl.PromptMetadata["compiled_call_flow_s3_key"]; ok {
		t.Fatalf("PromptMetadata should not include compiled_call_flow_s3_key: %+v", pl.PromptMetadata)
	}
	vars, ok := pl.PromptMetadata["system_prompt_variables"].(DocumentVariables)
	if !ok || vars["call_flow"] != "CALL FLOW BODY" {
		t.Fatalf("call_flow vars = %#v", pl.PromptMetadata["system_prompt_variables"])
	}
}
