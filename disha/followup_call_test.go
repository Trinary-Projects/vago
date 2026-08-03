package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jaideep329/talk-go/voicepipelinecore"
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
	patientExecutiveProfile := "Formatted patient executive profile"
	activeChatContext := "Recent active chat context"
	recent1HrTranscript := "Patient asked about dinner in chat 20 minutes ago"
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
		"FOLLOWUP patient={{ patient_info }} memory={{ patient_memory }} active={{ active_chat_context }} recent={{ recent_1hr_transcript }} when={{ current_datetime }} name={{ patient_name }} pronoun={{ he_she }} schedule={{ patient_schedule }}",
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
			UserID:                  userID,
			PatientExecutiveProfile: &patientExecutiveProfile,
			ActiveChatContext:       &activeChatContext,
			Recent1HrTranscript:     &recent1HrTranscript,
			IdealCallTimeSlots:      schedule,
			DevanagariName:          "रिया",
			FirstName:               "Riya",
			Gender:                  "female",
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
		!containsAll(pl.InitialMessages[0].Content, "FOLLOWUP", "Riya, age 32", "Formatted patient executive profile", "Recent active chat context", "Patient asked about dinner in chat 20 minutes ago", "रिया", "she") ||
		pl.InitialMessages[1].Role != "user" ||
		pl.InitialMessages[1].Content != "hello" {
		t.Fatalf("InitialMessages = %+v", pl.InitialMessages)
	}
	if strings.Contains(pl.InitialMessages[0].Content, "Recent chat note") {
		t.Fatalf("InitialMessages should not append unprocessed chat context into patient memory: %+v", pl.InitialMessages)
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
	if vars["patient_memory"] != patientExecutiveProfile {
		t.Fatalf("patient_memory = %#v", vars["patient_memory"])
	}
	if vars["patient_executive_profile"] != patientExecutiveProfile {
		t.Fatalf("patient_executive_profile = %#v", vars["patient_executive_profile"])
	}
	if vars["active_chat_context"] != activeChatContext {
		t.Fatalf("active_chat_context = %#v", vars["active_chat_context"])
	}
	if vars["recent_1hr_transcript"] != recent1HrTranscript {
		t.Fatalf("recent_1hr_transcript = %#v", vars["recent_1hr_transcript"])
	}
	currentDatetime, ok := vars["current_datetime"].(string)
	if !ok {
		t.Fatalf("current_datetime = %#v, want string", vars["current_datetime"])
	}
	if _, err := time.Parse("2 Jan 2006 03:04 PM", currentDatetime); err != nil {
		t.Fatalf("current_datetime = %q, want AM/PM IST prompt format: %v", currentDatetime, err)
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
	if pl.ModelGroup != followUpModelGroup {
		t.Fatalf("ModelGroup = %q, want %q (dynamic shares the regular follow-up group)", pl.ModelGroup, followUpModelGroup)
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
	if vars["active_chat_context"] != "" {
		t.Fatalf("active_chat_context = %#v, want empty fallback", vars["active_chat_context"])
	}
	if vars["recent_1hr_transcript"] != "" {
		t.Fatalf("recent_1hr_transcript = %#v, want empty fallback", vars["recent_1hr_transcript"])
	}
}

func TestFollowUpGuidancePromptVariablesIncludePatientExecutiveProfile(t *testing.T) {
	patientExecutiveProfile := "Formatted patient executive profile"
	pl := &followUpPlan{
		Startup: CallStartup{
			Data: &ConversationData{
				UserProfile: UserProfileData{
					PatientExecutiveProfile: &patientExecutiveProfile,
				},
			},
		},
	}

	vars := followUpGuidancePromptVariables(pl, "patient is worried about dinner")

	if vars["situation"] != "patient is worried about dinner" {
		t.Fatalf("situation = %#v", vars["situation"])
	}
	if vars["patient_executive_profile"] != patientExecutiveProfile {
		t.Fatalf("patient_executive_profile = %#v", vars["patient_executive_profile"])
	}
}

// ------------------------------------------------------- composeEnrichers

func TestComposeEnrichersReturnsNilForZeroEnrichers(t *testing.T) {
	if got := composeEnrichers(); got != nil {
		t.Errorf("composeEnrichers() = %v, want nil", got)
	}
	if got := composeEnrichers(nil, nil, nil); got != nil {
		t.Errorf("composeEnrichers(nil, nil, nil) = %v, want nil", got)
	}
}

func TestComposeEnrichersReturnsTheSingleEnricherUnchanged(t *testing.T) {
	calls := 0
	fn := func(_ context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
		calls++
		return messages
	}

	got := composeEnrichers(nil, fn, nil)
	if got == nil {
		t.Fatal("composeEnrichers(nil, fn, nil) = nil, want fn")
	}
	got(context.Background(), nil)
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (should be fn itself, not a wrapper)", calls)
	}
}

// Order matters: protocol must run before guardrail (design note §6.2 /
// AGENTS.md) because protocol recomputes its injection point from the
// message list on every call, and guardrail appends its correction last.
// This test proves composeEnrichers threads output into input in call order
// generically, without knowing about either feature.
func TestComposeEnrichersThreadsOutputIntoNextInput(t *testing.T) {
	var order []string
	first := func(_ context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
		order = append(order, "first")
		return append(messages, voicepipelinecore.Message{Role: "user", Content: "from-first"})
	}
	second := func(_ context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
		order = append(order, "second")
		if len(messages) == 0 || messages[len(messages)-1].Content != "from-first" {
			t.Fatalf("second enricher did not see first's output: %+v", messages)
		}
		return append(messages, voicepipelinecore.Message{Role: "user", Content: "from-second"})
	}

	composed := composeEnrichers(nil, first, nil, second, nil)
	if composed == nil {
		t.Fatal("composeEnrichers with two enrichers = nil")
	}
	out := composed(context.Background(), []voicepipelinecore.Message{{Role: "system", Content: "sys"}})

	if !reflect.DeepEqual(order, []string{"first", "second"}) {
		t.Fatalf("order = %v, want [first second]", order)
	}
	if len(out) != 3 || out[1].Content != "from-first" || out[2].Content != "from-second" {
		t.Fatalf("out = %+v", out)
	}
}

// -------------------------------------------------------- setupRetrieval

func newRetrievalWiringPlan(dynamic bool) *followUpPlan {
	return &followUpPlan{
		Startup: CallStartup{
			Logger:         log.New(io.Discard, "", 0),
			UserID:         "user-1",
			ConversationID: "conv-1",
		},
		Dynamic:         dynamic,
		PromptMetadata:  map[string]any{},
		PromptVariables: DocumentVariables{},
		Callbacks:       &CallEventCallbacks{},
	}
}

// The two env flags are independent gates. All four combinations must wire
// correctly: which enricher(s) exist, whether the guardrail checker
// constructor exists (BuildTask's proxy for "does the guard processor get
// built"), whether the shared client/warm-up exists, and whether the chunk
// decorator is registered exactly once — with the boxes belonging to
// whichever step(s) are actually enabled, verified by writing directly into
// each side's own box (same package, unexported field access) and reading it
// back through the ONE registered decorator.
func TestSetupRetrievalFourFlagCombinations(t *testing.T) {
	tests := []struct {
		name         string
		protoFlag    string
		guardFlag    string
		wantProtocol bool
		wantGuardian bool
	}{
		{"neither enabled", "", "", false, false},
		{"protocol only (unchanged from today)", "1", "", true, false},
		{"guardrail only", "", "1", false, true},
		{"both enabled", "1", "1", true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(protocolRetrievalEnabledEnv, tc.protoFlag)
			t.Setenv(guardrailCheckEnabledEnv, tc.guardFlag)
			t.Setenv("WEAVIATE_URL", "http://weaviate.staging.svc.cluster.local:8080")
			t.Setenv("WEAVIATE_API_KEY", "key")
			t.Setenv("AWS_US_BUCKET_NAME", "")
			t.Setenv("AWS_US_REGION", "")

			pl := newRetrievalWiringPlan(true)
			setupRetrieval(pl, Deps{})

			if got := pl.ProtocolEnricher != nil; got != tc.wantProtocol {
				t.Errorf("ProtocolEnricher present = %v, want %v", got, tc.wantProtocol)
			}
			if got := pl.NewGuardrailChecker != nil; got != tc.wantGuardian {
				t.Errorf("NewGuardrailChecker present = %v, want %v", got, tc.wantGuardian)
			}
			wantWired := tc.wantProtocol || tc.wantGuardian
			if got := pl.Callbacks.chunkDecorator != nil; got != wantWired {
				t.Errorf("chunk decorator registered = %v, want %v", got, wantWired)
			}
			if got := pl.retrievalClient != nil; got != wantWired {
				t.Errorf("shared weaviate client built = %v, want %v (guardrail-only must still get a client to warm up)", got, wantWired)
			}

			if !wantWired {
				return
			}

			// Prove the decorator was registered with the SAME boxes the
			// enricher/checker write into: put a record straight into each
			// side's own box, then read it back through the one registered
			// decorator.
			chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
			if pl.ProtocolEnricher != nil {
				pl.ProtocolEnricher.box.put(protocolRetrievalRecord{Status: "ok", QueryText: "q"})
			}
			if pl.NewGuardrailChecker != nil {
				checker := pl.NewGuardrailChecker(context.Background())
				checker.box.offer(guardrailCheckRecord{Status: "ok", SelectedIndex: 0})
			}
			pl.Callbacks.chunkDecorator(chunk)

			gotProtocol := chunk.ChunkRetrievalMetrics != nil && chunk.ChunkRetrievalMetrics.Protocol != nil
			if gotProtocol != tc.wantProtocol {
				t.Errorf("chunk protocol metrics present = %v, want %v", gotProtocol, tc.wantProtocol)
			}
			gotGuardian := chunk.ChunkRetrievalMetrics != nil && chunk.ChunkRetrievalMetrics.Guardrail != nil
			if gotGuardian != tc.wantGuardian {
				t.Errorf("chunk guardrail metrics present = %v, want %v", gotGuardian, tc.wantGuardian)
			}
		})
	}
}

// An unconfigured Weaviate means "feature off" for BOTH steps, not a failed
// call — regardless of which flag(s) requested them.
func TestSetupRetrievalMissingWeaviateConfigDisablesBothSteps(t *testing.T) {
	tests := []struct {
		name      string
		protoFlag string
		guardFlag string
	}{
		{"protocol only requested", "1", ""},
		{"guardrail only requested", "", "1"},
		{"both requested", "1", "1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(protocolRetrievalEnabledEnv, tc.protoFlag)
			t.Setenv(guardrailCheckEnabledEnv, tc.guardFlag)
			t.Setenv("WEAVIATE_URL", "")
			t.Setenv("WEAVIATE_API_KEY", "")

			pl := newRetrievalWiringPlan(true)
			setupRetrieval(pl, Deps{})

			if pl.ProtocolEnricher != nil {
				t.Error("ProtocolEnricher should not be built without Weaviate config")
			}
			if pl.NewGuardrailChecker != nil {
				t.Error("NewGuardrailChecker should not be built without Weaviate config")
			}
			if pl.Callbacks.chunkDecorator != nil {
				t.Error("chunk decorator should not be registered without Weaviate config")
			}
			if pl.retrievalClient != nil {
				t.Error("retrievalClient should not be set without Weaviate config")
			}
		})
	}
}

// setupGuardrailCheck's constructor closure must actually build a usable
// checker once BuildTask supplies the call context — Check and Enrich are
// the two methods BuildTask wires as ResponseGuard/MessagesEnricher.
func TestSetupGuardrailCheckConstructorBuildsAUsableChecker(t *testing.T) {
	t.Setenv(guardrailCheckEnabledEnv, "1")
	t.Setenv(protocolRetrievalEnabledEnv, "")
	t.Setenv("WEAVIATE_URL", "http://weaviate.staging.svc.cluster.local:8080")
	t.Setenv("WEAVIATE_API_KEY", "key")
	t.Setenv("AWS_US_BUCKET_NAME", "")
	t.Setenv("AWS_US_REGION", "")

	pl := newRetrievalWiringPlan(false)
	setupRetrieval(pl, Deps{})

	if pl.NewGuardrailChecker == nil {
		t.Fatal("NewGuardrailChecker should be set when the guardrail flag is on and Weaviate is configured")
	}
	checker := pl.NewGuardrailChecker(context.Background())
	if checker == nil {
		t.Fatal("NewGuardrailChecker(ctx) returned nil")
	}
	var _ voicepipelinecore.ResponseGuard = checker.Check
	var _ voicepipelinecore.MessagesEnricher = checker.Enrich
}

// Sanity check that the shared warm-up entry point used for a guardrail-only
// call (pl.retrievalClient with no protocolEnricher) behaves like the
// protocol-only path: same function, same client, no panic.
func TestSharedWarmUpWorksForGuardrailOnlyClient(t *testing.T) {
	server := newStubWeaviate(t, fmt.Sprintf(anchorResponseTemplate, ""), nil)
	warmUpWeaviateClient(context.Background(), server, log.New(io.Discard, "", 0))
}
