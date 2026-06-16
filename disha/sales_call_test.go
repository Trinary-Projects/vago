package disha

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

type callAPIRequest struct {
	Method string
	Path   string
	Body   map[string]any
}

type callAPIRecorder struct {
	mu       sync.Mutex
	requests []callAPIRequest
}

func newCallAPIServer(t *testing.T) (*httptest.Server, *callAPIRecorder) {
	t.Helper()
	recorder := &callAPIRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var body map[string]any
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("Unmarshal request: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want absent", got)
		}
		recorder.mu.Lock()
		recorder.requests = append(recorder.requests, callAPIRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Body:   body,
		})
		recorder.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)
	return server, recorder
}

func (r *callAPIRecorder) snapshot() []callAPIRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]callAPIRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

func seedConversationData(t *testing.T, server *miniredis.Miniredis, conversationID string, data ConversationData) {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal conversation data: %v", err)
	}
	server.Set(conversationDataKey(conversationID), string(raw))
}

// testSalesPrompt is the body the test harness seeds into the document
// store. Production reads the real Langfuse prompt from Redis.
const testSalesPrompt = "TEST_SYSTEM_PROMPT patient={{ patient_info }} when={{ current_datetime }}"

func seedDocument(t *testing.T, server *miniredis.Miniredis, name, env string, version int, body string) {
	t.Helper()
	doc := DocumentVersion{ID: "doc-" + name, PromptText: body, Version: version}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal document %q: %v", name, err)
	}
	server.Set("document:"+name+":"+env, string(raw))
}

func testDeps(redis RedisClient, api *APIClient) Deps {
	logger := log.New(io.Discard, "", 0)
	return Deps{
		Logger:    logger,
		Redis:     redis,
		API:       api,
		Documents: newDocumentStore(redis, logger, simpleTemplateRenderer{}),
	}
}

func TestCallEndedUploadsDebugLogsAndQueuesDailyMetrics(t *testing.T) {
	apiServer, apiRecorder := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	var uploaded []voicepipelinecore.RTVIDebugLogEntry
	callbacks := NewCallEventCallbacks(CallStartup{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        SalesCallBotType,
		Logger:         log.New(io.Discard, "", 0),
	}, nil, api, func(_ context.Context, entries []voicepipelinecore.RTVIDebugLogEntry) (string, error) {
		uploaded = append(uploaded, entries...)
		return "debug_log_data/conv-1/log_data.json", nil
	})
	endedAt := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)
	callbacks.OnCallEnded(voicepipelinecore.EndReasonClientDisconnect, voicepipelinecore.CallStats{
		TotalUserDurationSec: 12.9,
		EndedAt:              endedAt,
		MeetingID:            "meeting-1",
		BotSessionID:         "bot-session-1",
		UserSessionID:        "user-session-1",
		DebugLogs: []voicepipelinecore.RTVIDebugLogEntry{{
			Label:     "rtvi-ai",
			Type:      "server-message",
			Data:      "Participant left: leftCall",
			Timestamp: 1.23,
		}},
	})

	if len(uploaded) != 1 || uploaded[0].Type != "server-message" {
		t.Fatalf("uploaded logs = %+v", uploaded)
	}

	requests := apiRecorder.snapshot()
	if len(requests) != 3 {
		t.Fatalf("request count = %d, want 3: %+v", len(requests), requests)
	}
	assertRequest(t, requests[0], http.MethodPost, "/bot/run_post_call_operations")
	if requests[0].Body["log_data_s3_key"] != "debug_log_data/conv-1/log_data.json" {
		t.Fatalf("post-call log key mismatch: %+v", requests[0].Body)
	}
	assertRequest(t, requests[1], http.MethodPost, "/common/enqueue_job")
	if requests[1].Body["module_name"] != "bots.webhooks" ||
		requests[1].Body["func_name"] != "fetch_and_store_daily_metrics" ||
		requests[1].Body["sqs_queue"] != "p1-fast-l1" {
		t.Fatalf("Daily metrics enqueue mismatch: %+v", requests[1].Body)
	}
	kwargs, ok := requests[1].Body["kwargs"].(map[string]any)
	if !ok ||
		kwargs["conversation_id"] != "conv-1" ||
		kwargs["meeting_id"] != "meeting-1" ||
		kwargs["bot_session_id"] != "bot-session-1" ||
		kwargs["user_session_id"] != "user-session-1" {
		t.Fatalf("Daily metrics kwargs mismatch: %+v", requests[1].Body["kwargs"])
	}
	assertRequest(t, requests[2], http.MethodPost, "/common/enqueue_job")
	if requests[2].Body["module_name"] != "services.conversation_chunk_manager" {
		t.Fatalf("chunk sync enqueue mismatch: %+v", requests[2].Body)
	}
}

func TestUploadDebugLogsUsesUploader(t *testing.T) {
	logs := []voicepipelinecore.RTVIDebugLogEntry{{
		Label:     "rtvi-ai",
		Type:      "server-message",
		Data:      "Participant left: leftCall",
		Timestamp: 1.23,
	}}
	var uploaded []voicepipelinecore.RTVIDebugLogEntry
	key := uploadDebugLogs(log.New(io.Discard, "", 0), func(_ context.Context, entries []voicepipelinecore.RTVIDebugLogEntry) (string, error) {
		uploaded = append(uploaded, entries...)
		return "debug_log_data/conv-1/log_data.json", nil
	}, logs)

	if key != "debug_log_data/conv-1/log_data.json" {
		t.Fatalf("debug log key = %q", key)
	}
	if len(uploaded) != 1 || uploaded[0].Type != "server-message" {
		t.Fatalf("uploaded logs = %+v", uploaded)
	}
}

func TestNewBotReturnsSalesCallBot(t *testing.T) {
	bot, err := NewBot(SalesCallBotType)
	if err != nil {
		t.Fatalf("NewBot: %v", err)
	}
	if bot.BotType() != SalesCallBotType {
		t.Fatalf("BotType = %q, want %q", bot.BotType(), SalesCallBotType)
	}
	if _, ok := bot.(SalesCallBot); !ok {
		t.Fatalf("bot type = %T, want SalesCallBot", bot)
	}

	followUpBot, err := NewBot(FollowUpBotType)
	if err != nil {
		t.Fatalf("NewBot follow-up: %v", err)
	}
	if followUpBot.BotType() != FollowUpBotType {
		t.Fatalf("FollowUp BotType = %q, want %q", followUpBot.BotType(), FollowUpBotType)
	}
	if _, ok := followUpBot.(FollowUpBot); !ok {
		t.Fatalf("follow-up bot type = %T, want FollowUpBot", followUpBot)
	}
}

func TestSalesCallBotPlanAssemblesDishaCall(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, apiRecorder := newCallAPIServer(t)
	api := NewAPIClient(apiServer.URL, 10*time.Second, nil)
	remainingTalkTime := 2.5
	conversationID := "conv-1"
	userID := "user-1"
	seedDocument(t, redisServer, salesPromptDefault, "production", 17, testSalesPrompt)
	seedConversationData(t, redisServer, conversationID, ConversationData{
		Conversation: ConversationRow{
			ID:          conversationID,
			UserID:      userID,
			BotType:     SalesCallBotType,
			PatientInfo: "Riya, age 32",
		},
		Chunks: [][]any{
			{"chunk-1", "user", "hello", false, nil},
			{"chunk-2", "assistant", "hi", false, nil},
			{"debug", "assistant", "debug", true, nil},                        // dropped: is_debug_log
			{"meta", "assistant", "fn call", false, map[string]any{"x": "y"}}, // dropped: additional_data present
			{"empty-data", "tool", "tool turn", false, map[string]any{}},      // kept: empty additional_data is falsy, role unrestricted
		},
		UserProfile: UserProfileData{
			UserID:                            userID,
			Phone:                             "+15551234567",
			RemainingSalesCallTalktimeSeconds: &remainingTalkTime,
		},
	})

	pl, err := SalesCallBot{}.plan(context.Background(), conversationID, testDeps(redisClient, api))
	if err != nil {
		t.Fatalf("SalesCallBot.plan: %v", err)
	}
	if pl.Startup.ConversationID != conversationID {
		t.Fatalf("ConversationID = %q, want %q", pl.Startup.ConversationID, conversationID)
	}
	if pl.MaxTalkTime != 2500*time.Millisecond {
		t.Fatalf("MaxTalkTime = %v, want 2.5s", pl.MaxTalkTime)
	}
	if len(pl.InitialMessages) != 4 ||
		pl.InitialMessages[0].Role != "system" ||
		!containsAll(pl.InitialMessages[0].Content, "TEST_SYSTEM_PROMPT", "patient=Riya, age 32") ||
		pl.InitialMessages[1].Role != "user" ||
		pl.InitialMessages[1].Content != "hello" ||
		pl.InitialMessages[2].Role != "assistant" ||
		pl.InitialMessages[2].Content != "hi" ||
		pl.InitialMessages[3].Role != "tool" ||
		pl.InitialMessages[3].Content != "tool turn" {
		t.Fatalf("InitialMessages mismatch: %+v", pl.InitialMessages)
	}
	events := pl.Callbacks.Events()
	turnAt := time.Date(2026, 5, 22, 1, 2, 3, 0, time.UTC)
	events.OnUserTurnCommitted("new user", turnAt, pl.PromptKey)
	events.OnAssistantTurnCommitted("new assistant", turnAt.Add(time.Second), voicepipelinecore.TurnMetrics{
		LLMTTFBMs:            11.1,
		TTSTTFBMs:            22.2,
		E2ELatencyMs:         33.3,
		TTSTextAggregationMs: 44.4,
	}, pl.PromptKey)
	chunkItems, err := redisServer.List(conversationChunksKey(userID, conversationID))
	if err != nil {
		t.Fatalf("List chunks: %v", err)
	}
	if len(chunkItems) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunkItems))
	}
	var userChunk, assistantChunk ConversationChunk
	if err := json.Unmarshal([]byte(chunkItems[0]), &userChunk); err != nil {
		t.Fatalf("Unmarshal user chunk: %v", err)
	}
	if err := json.Unmarshal([]byte(chunkItems[1]), &assistantChunk); err != nil {
		t.Fatalf("Unmarshal assistant chunk: %v", err)
	}
	if userChunk.Role != "user" || userChunk.Text != "new user" || userChunk.LLMTTFBMs != nil {
		t.Fatalf("user chunk mismatch: %+v", userChunk)
	}
	if userChunk.MainAgentSystemPromptLangfuseKey == nil || *userChunk.MainAgentSystemPromptLangfuseKey != "sales_call/main_sys-3day_v2_v17" {
		t.Fatalf("user prompt key = %v, want sales_call/main_sys-3day_v2_v17", userChunk.MainAgentSystemPromptLangfuseKey)
	}
	if assistantChunk.Role != "assistant" || assistantChunk.Text != "new assistant" {
		t.Fatalf("assistant chunk mismatch: %+v", assistantChunk)
	}
	if assistantChunk.MainAgentSystemPromptLangfuseKey == nil || *assistantChunk.MainAgentSystemPromptLangfuseKey != "sales_call/main_sys-3day_v2_v17" {
		t.Fatalf("assistant prompt key = %v, want sales_call/main_sys-3day_v2_v17", assistantChunk.MainAgentSystemPromptLangfuseKey)
	}
	if !floatPtrNear(assistantChunk.LLMTTFBMs, 0.0111) ||
		!floatPtrNear(assistantChunk.TTSTTFBMs, 0.0222) ||
		!floatPtrNear(assistantChunk.V2VLatencyMs, 0.0333) ||
		!floatPtrNear(assistantChunk.TextAggregationMs, 0.0444) {
		t.Fatalf("assistant metrics mismatch: %+v", assistantChunk)
	}

	eventAt := time.Date(2026, 5, 22, 2, 3, 4, 0, time.UTC)
	events.OnBotJoined(eventAt)
	events.OnUserJoined(eventAt.Add(time.Second))
	events.OnUserFirstSpeech(eventAt.Add(2 * time.Second))
	events.OnBotFirstSpeech(eventAt.Add(3 * time.Second))
	events.OnCallEnded(voicepipelinecore.EndReasonClientDisconnect, voicepipelinecore.CallStats{
		TotalUserDurationSec:  12.9,
		FirstUserAudioFrameAt: eventAt.Add(4 * time.Second),
		EndedAt:               eventAt.Add(5 * time.Second),
	})

	requests := apiRecorder.snapshot()
	if len(requests) != 6 {
		t.Fatalf("request count = %d, want 6: %+v", len(requests), requests)
	}
	assertRequest(t, requests[0], http.MethodPatch, "/bot/update_conversation")
	if requests[0].Body["conversation_id"] != conversationID || requests[0].Body["bot_joined_at"] != eventAt.Format(time.RFC3339) {
		t.Fatalf("bot joined body mismatch: %+v", requests[0].Body)
	}
	assertRequest(t, requests[4], http.MethodPost, "/bot/run_post_call_operations")
	if value, ok := requests[4].Body["end_reason"]; !ok || value != nil {
		t.Fatalf("end_reason = %v (present=%v), want explicit null", value, ok)
	}
	if requests[4].Body["total_user_duration"] != float64(12) || requests[4].Body["log_data_s3_key"] != "" {
		t.Fatalf("post-call body mismatch: %+v", requests[4].Body)
	}
	assertRequest(t, requests[5], http.MethodPost, "/common/enqueue_job")
	kwargs, ok := requests[5].Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", requests[5].Body["kwargs"])
	}
	if requests[5].Body["module_name"] != "services.conversation_chunk_manager" ||
		requests[5].Body["func_name"] != "sync_conversation_chunks_to_db" ||
		requests[5].Body["sqs_queue"] != "p1-fast-l1" ||
		kwargs["user_id"] != userID ||
		kwargs["conversation_id"] != conversationID ||
		kwargs["bot_type"] != SalesCallBotType {
		t.Fatalf("enqueue body mismatch: %+v", requests[5].Body)
	}
}

func assertRequest(t *testing.T, got callAPIRequest, method, path string) {
	t.Helper()
	if got.Method != method || got.Path != path {
		t.Fatalf("request = %s %s, want %s %s", got.Method, got.Path, method, path)
	}
}

func TestSalesCallBotPlanSeedsHelloWhenNoPriorChunks(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	conversationID := "fresh"
	seedDocument(t, redisServer, salesPromptDefault, "production", 1, testSalesPrompt)
	seedConversationData(t, redisServer, conversationID, ConversationData{
		Conversation: ConversationRow{
			ID:      conversationID,
			UserID:  "user-1",
			BotType: SalesCallBotType,
		},
		Chunks: [][]any{
			{"debug", "user", "ignore", true, nil},
		},
		UserProfile: UserProfileData{UserID: "user-1"},
	})

	pl, err := SalesCallBot{}.plan(context.Background(), conversationID, testDeps(redisClient, NewAPIClient(apiServer.URL, 10*time.Second, nil)))
	if err != nil {
		t.Fatalf("SalesCallBot.plan: %v", err)
	}
	if len(pl.InitialMessages) != 2 ||
		pl.InitialMessages[0].Role != "system" ||
		pl.InitialMessages[1].Role != "user" ||
		pl.InitialMessages[1].Content != "hello?" {
		t.Fatalf("InitialMessages = %+v, want system + hello seed", pl.InitialMessages)
	}
	if pl.MaxTalkTime != lifetimeTalkTimeSeconds*time.Second {
		t.Fatalf("MaxTalkTime = %v, want lifetime default", pl.MaxTalkTime)
	}
}

func TestSalesCallBotPlanPicksCampaignPrompt(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	flag := campaignFlag21For3Days
	seedDocument(t, redisServer, salesPrompt21Rs, "production", 3, "21RS_PROMPT")
	seedConversationData(t, redisServer, "conv-21", ConversationData{
		Conversation: ConversationRow{ID: "conv-21", UserID: "user-1", BotType: SalesCallBotType},
		UserProfile: UserProfileData{
			UserID:                        "user-1",
			CampaignPricingExperimentFlag: &flag,
		},
	})

	pl, err := SalesCallBot{}.plan(context.Background(), "conv-21", testDeps(redisClient, NewAPIClient(apiServer.URL, 10*time.Second, nil)))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if pl.InitialMessages[0].Content != "21RS_PROMPT" {
		t.Fatalf("expected 21RS prompt, got %q", pl.InitialMessages[0].Content)
	}
}

func TestSalesCallBotPlanLoadsEndCallToolFromPromptConfig(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	seedDocumentWithConfig(
		t,
		redisServer,
		salesPromptDefault,
		"production",
		5,
		testSalesPrompt,
		map[string]any{"tools": []any{
			map[string]any{
				"name":        "end_call",
				"description": "End the call.",
				"properties":  map[string]any{},
				"required":    []any{},
			},
		}},
	)
	seedConversationData(t, redisServer, "conv-tools", ConversationData{
		Conversation: ConversationRow{ID: "conv-tools", UserID: "user-1", BotType: SalesCallBotType},
		UserProfile:  UserProfileData{UserID: "user-1"},
	})

	pl, err := SalesCallBot{}.plan(context.Background(), "conv-tools", testDeps(redisClient, NewAPIClient(apiServer.URL, 10*time.Second, nil)))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(pl.Tools) != 1 || pl.Tools[0].Function.Name != endCallToolName {
		t.Fatalf("Tools = %+v, want end_call", pl.Tools)
	}
	if got, _ := pl.Tools[0].Function.Parameters["properties"].(map[string]any); got == nil || len(got) != 0 {
		t.Fatalf("end_call properties = %#v, want empty object", pl.Tools[0].Function.Parameters["properties"])
	}
}

func TestSalesCallBotPlanAppendsResumeMessage(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	seedDocument(t, redisServer, salesPromptDefault, "production", 1, "SYS")
	resumedID := "chunk-prev"
	// The backend cache always serializes resume_gracefully (model default
	// True); only explicit-chunkId rebuilds set it False.
	gracefully := true
	recent := time.Now().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	seedConversationData(t, redisServer, "conv-resume", ConversationData{
		Conversation: ConversationRow{
			ID:                 "conv-resume",
			UserID:             "user-1",
			BotType:            SalesCallBotType,
			ResumedFromChunkID: &resumedID,
			ResumeGracefully:   &gracefully,
		},
		Chunks: [][]any{
			{"chunk-1", "user", "I was saying", false, nil},
		},
		ResumedChunk: map[string]any{
			"id":      resumedID,
			"created": recent,
		},
		UserProfile: UserProfileData{UserID: "user-1"},
	})

	pl, err := SalesCallBot{}.plan(context.Background(), "conv-resume", testDeps(redisClient, NewAPIClient(apiServer.URL, 10*time.Second, nil)))
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(pl.InitialMessages) != 3 {
		t.Fatalf("InitialMessages = %+v, want 3", pl.InitialMessages)
	}
	if pl.InitialMessages[2].Role != "system" || !containsAll(pl.InitialMessages[2].Content, "hanji to aap keh") {
		t.Fatalf("resume message missing: %+v", pl.InitialMessages[2])
	}
}

func TestSalesCallBotPlanRejectsUnsupportedBotType(t *testing.T) {
	redisServer, redisClient := newRedisTestClient(t)
	apiServer, _ := newCallAPIServer(t)
	seedConversationData(t, redisServer, "conv-1", ConversationData{
		Conversation: ConversationRow{
			ID:      "conv-1",
			UserID:  "user-1",
			BotType: "followup_call",
		},
		UserProfile: UserProfileData{UserID: "user-1"},
	})

	_, err := SalesCallBot{}.plan(context.Background(), "conv-1", testDeps(redisClient, NewAPIClient(apiServer.URL, 10*time.Second, nil)))
	if err == nil {
		t.Fatal("SalesCallBot.plan returned nil error for unsupported bot type")
	}
}

func containsAll(haystack string, needles ...string) bool {
	for _, n := range needles {
		if !strings.Contains(haystack, n) {
			return false
		}
	}
	return true
}

func floatPtrNear(got *float64, want float64) bool {
	if got == nil {
		return false
	}
	return math.Abs(*got-want) < 1e-9
}

func TestSalesTalkTimeLimit(t *testing.T) {
	if got := salesTalkTimeLimit(nil); got != lifetimeTalkTimeSeconds*time.Second {
		t.Fatalf("nil talktime = %s, want default", got)
	}
	zero := 0.0
	if got := salesTalkTimeLimit(&zero); got != 0 {
		t.Fatalf("zero talktime = %s, want immediate timeout", got)
	}
	negative := -10.0
	if got := salesTalkTimeLimit(&negative); got != 0 {
		t.Fatalf("negative talktime = %s, want clamped zero", got)
	}
}

func TestMapEndReason(t *testing.T) {
	if got := mapEndReason(voicepipelinecore.EndReasonTalkTimeExhausted); got == nil || *got != "talktime_exhausted" {
		t.Fatalf("talktime reason = %v, want talktime_exhausted", got)
	}
	if got := mapEndReason(voicepipelinecore.EndReasonUserIdle); got == nil || *got != "user_idle" {
		t.Fatalf("idle reason = %v, want user_idle", got)
	}
	if got := mapEndReason(voicepipelinecore.EndReasonClientDisconnect); got != nil {
		t.Fatalf("client disconnect reason = %v, want nil", *got)
	}
	if got := mapEndReason(voicepipelinecore.EndReasonError); got != nil {
		t.Fatalf("error reason = %v, want nil", *got)
	}
}
