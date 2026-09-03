package disha

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	callEventRequestTimeout = 10 * time.Second
	postCallRequestTimeout  = 10 * time.Second
	chunkWriteTimeout       = 5 * time.Second
)

type lastPersistedChunk struct {
	role    string
	id      string
	created string
	index   int64
	valid   bool
}

type CallEventCallbacks struct {
	redis            RedisClient
	api              *APIClient
	logger           *log.Logger
	debugLogUploader DebugLogUploader

	conversationID string
	userID         string
	botType        string

	// lastChunk is read and written only by CallEvents.On* methods on the
	// dispatcher's single FIFO goroutine, so it needs no locking. Direct
	// AppendDebugLogChunk calls never read or update it.
	lastChunk lastPersistedChunk

	// llmCallCompleted receives each finished LLM generation (Python's
	// OnboardingPipelineManager.on_llm_call_complete delegating to the
	// stage-transition tracker). Nil for bots without a stage tracker;
	// OnLLMCallCompleted then no-ops.
	llmCallCompleted func(text string, interrupted bool)

	// assistantTurnCommitted receives each committed assistant turn
	// (Python's OnboardingPipelineManager.on_assistant_turn_stopped
	// delegating to the stage-threshold monitor). Nil for bots without a
	// stage machine; OnAssistantTurnCommitted then only persists the chunk.
	assistantTurnCommitted func(text string, at time.Time)

	// chunkDecorator lets bot-specific code enrich a chunk immediately
	// before it is written to Redis (e.g. onboarding calls attach the
	// current stage name and a conversation-state S3 key to every
	// persisted chunk). Nil for bots that need no enrichment; the
	// decorator may block briefly, since it runs inline on the call-events
	// dispatcher goroutine, same as the rest of appendChunk's work.
	chunkDecorator func(*ConversationChunk)

	// postCallDecorator lets bot-specific code enrich the
	// run_post_call_operations request immediately before it is sent (e.g.
	// onboarding calls fill onboarding_call_done and the stage/variable-
	// store snapshot fields). Nil for bots that need no enrichment, which
	// leaves those request fields at their zero value (explicit JSON
	// null).
	postCallDecorator func(*PostCallOperationsRequest)
}

func NewCallEventCallbacks(startup CallStartup, redis RedisClient, api *APIClient, debugLogUploader DebugLogUploader) *CallEventCallbacks {
	return &CallEventCallbacks{
		redis:            redis,
		api:              api,
		logger:           startup.Logger,
		debugLogUploader: debugLogUploader,
		conversationID:   startup.ConversationID,
		userID:           startup.UserID,
		botType:          startup.BotType,
	}
}

// SetChunkDecorator wires bot-specific chunk enrichment, invoked on every
// persisted chunk (committed turns, tool-context chunks, and debug-log
// chunks) right before the Redis write. Example: onboarding calls use this
// to attach the current stage name and a per-chunk conversation-state S3
// key.
func (c *CallEventCallbacks) SetChunkDecorator(fn func(*ConversationChunk)) {
	if c == nil {
		return
	}
	c.chunkDecorator = fn
}

// SetPostCallDecorator wires bot-specific enrichment of the
// run_post_call_operations request, invoked once all shared fields are
// set. Example: onboarding calls use this to fill onboarding_call_done,
// latest_onboarding_call_stage, the intensity-level fields, and
// conversation_variables.
func (c *CallEventCallbacks) SetPostCallDecorator(fn func(*PostCallOperationsRequest)) {
	if c == nil {
		return
	}
	c.postCallDecorator = fn
}

// SetLLMCallCompletedHandler wires the onboarding stage tracker's
// LLM-completion hook; other bots leave it unset.
func (c *CallEventCallbacks) SetLLMCallCompletedHandler(fn func(text string, interrupted bool)) {
	if c == nil {
		return
	}
	c.llmCallCompleted = fn
}

// OnLLMCallCompleted delegates a finished LLM generation to the
// registered handler (the onboarding stage tracker); no-op when unset.
func (c *CallEventCallbacks) OnLLMCallCompleted(text string, interrupted bool) {
	if c.llmCallCompleted == nil {
		return
	}
	c.llmCallCompleted(text, interrupted)
}

// SetAssistantTurnCommittedHandler wires bot-specific work that must run
// after a committed assistant turn is persisted; other bots leave it unset.
// Example: onboarding calls use this to advance the per-stage turn count
// for stuck-stage alerting.
func (c *CallEventCallbacks) SetAssistantTurnCommittedHandler(fn func(text string, at time.Time)) {
	if c == nil {
		return
	}
	c.assistantTurnCommitted = fn
}

func (c *CallEventCallbacks) Events() voicepipelinecore.CallEvents {
	if c == nil {
		return voicepipelinecore.CallEvents{}
	}
	return voicepipelinecore.CallEvents{
		OnBotJoined:              c.OnBotJoined,
		OnUserJoined:             c.OnUserJoined,
		OnUserFirstSpeech:        c.OnUserFirstSpeech,
		OnBotFirstSpeech:         c.OnBotFirstSpeech,
		OnFirstUserAudio:         c.OnFirstUserAudio,
		OnUserTurnCommitted:      c.OnUserTurnCommitted,
		OnAssistantTurnCommitted: c.OnAssistantTurnCommitted,
		OnToolResultCommitted:    c.OnToolResultCommitted,
		OnLLMCallCompleted:       c.OnLLMCallCompleted,
		OnCallEnded:              c.OnCallEnded,
	}
}

func (c *CallEventCallbacks) OnBotJoined(at time.Time) {
	c.updateConversation(UpdateConversationRequest{ConversationID: c.conversationID, BotJoinedAt: &at})
}

func (c *CallEventCallbacks) OnUserJoined(at time.Time) {
	c.updateConversation(UpdateConversationRequest{ConversationID: c.conversationID, UserJoinedAt: &at})
}

func (c *CallEventCallbacks) OnUserFirstSpeech(at time.Time) {
	c.updateConversation(UpdateConversationRequest{ConversationID: c.conversationID, UserFirstSpeechAt: &at})
}

func (c *CallEventCallbacks) OnBotFirstSpeech(at time.Time) {
	c.updateConversation(UpdateConversationRequest{ConversationID: c.conversationID, BotFirstSpeechAt: &at})
}

func (c *CallEventCallbacks) OnFirstUserAudio(time.Time) {}

func (c *CallEventCallbacks) OnUserTurnCommitted(text string, at time.Time, promptKey string) {
	if c.lastChunk.valid && c.lastChunk.role == "user" {
		c.rewriteLastUserChunk(text, at, promptKey)
		return
	}
	c.appendConversationChunk(text, "user", at, voicepipelinecore.TurnMetrics{}, promptKey)
}

func (c *CallEventCallbacks) OnAssistantTurnCommitted(text string, at time.Time, metrics voicepipelinecore.TurnMetrics, promptKey string) {
	c.appendConversationChunk(text, "assistant", at, metrics, promptKey)
	if c.assistantTurnCommitted != nil {
		c.assistantTurnCommitted(text, at)
	}
}

func (c *CallEventCallbacks) OnToolResultCommitted(assistantToolCall voicepipelinecore.Message, toolResult voicepipelinecore.Message, at time.Time) {
	c.appendConversationChunkWithAdditionalData(
		assistantToolCall.Content,
		assistantToolCall.Role,
		at,
		voicepipelinecore.TurnMetrics{},
		"",
		toolContextAdditionalData(assistantToolCall),
	)
	c.appendConversationChunkWithAdditionalData(
		toolResult.Content,
		toolResult.Role,
		at.Add(time.Nanosecond),
		voicepipelinecore.TurnMetrics{},
		"",
		toolContextAdditionalData(toolResult),
	)
}

func (c *CallEventCallbacks) OnCallEnded(reason voicepipelinecore.EndReason, stats voicepipelinecore.CallStats) {
	logDataS3Key := uploadDebugLogs(c.logger, c.debugLogUploader, stats.DebugLogs)
	c.runPostCallOperations(reason, stats, logDataS3Key)
	c.enqueueDailyMetrics(stats)
	c.enqueueChunkSync()
}

func (c *CallEventCallbacks) updateConversation(req UpdateConversationRequest) {
	if c == nil || c.api == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), callEventRequestTimeout)
	defer cancel()
	if err := c.api.UpdateConversationWithFallback(ctx, req); err != nil && c.logger != nil {
		c.logger.Printf("disha: update_conversation failed: %v\n", err)
	}
}

func (c *CallEventCallbacks) appendConversationChunk(text, role string, at time.Time, metrics voicepipelinecore.TurnMetrics, promptKey string) {
	c.appendConversationChunkWithAdditionalData(text, role, at, metrics, promptKey, nil)
}

func (c *CallEventCallbacks) appendConversationChunkWithAdditionalData(text, role string, at time.Time, metrics voicepipelinecore.TurnMetrics, promptKey string, additionalData any) {
	c.appendChunk(text, role, at, metrics, promptKey, additionalData, false)
}

// AppendDebugLogChunk persists an is_debug_log=true assistant chunk,
// mirroring Python's conversation_persistence_processor.on_debug_log.
// The onboarding stage manager uses it for the tracker-source
// agenda-change debug chunk (additional_data.tool_call_id). The chunk's
// bot-specific fields (e.g. onboarding's current_agenda) are filled by the
// registered chunkDecorator, which reads live state at decoration time, so
// they carry the NEW stage — the state has already advanced, same as
// Python.
func (c *CallEventCallbacks) AppendDebugLogChunk(text string, at time.Time, promptKey string, additionalData any) {
	c.appendChunk(text, "assistant", at, voicepipelinecore.TurnMetrics{}, promptKey, additionalData, true)
}

func (c *CallEventCallbacks) appendChunk(text, role string, at time.Time, metrics voicepipelinecore.TurnMetrics, promptKey string, additionalData any, isDebugLog bool) {
	if c == nil || c.redis == nil {
		return
	}
	chunk := c.buildChunk(text, role, at, metrics, promptKey, additionalData, isDebugLog, "", "")
	ctx, cancel := context.WithTimeout(context.Background(), chunkWriteTimeout)
	defer cancel()
	index, err := c.redis.AppendChunk(ctx, c.userID, c.conversationID, *chunk)
	if err != nil {
		if c.logger != nil {
			c.logger.Printf("disha: chunk persist failed conversation=%s role=%s: %v\n", c.conversationID, role, err)
		}
		return
	}
	if !isDebugLog {
		c.lastChunk = lastPersistedChunk{
			role:    chunk.Role,
			id:      chunk.ID,
			created: chunk.Created,
			index:   index,
			valid:   true,
		}
	}
}

func (c *CallEventCallbacks) buildChunk(text, role string, at time.Time, metrics voicepipelinecore.TurnMetrics, promptKey string, additionalData any, isDebugLog bool, chunkID, created string) *ConversationChunk {
	var promptKeyPtr *string
	if promptKey != "" {
		promptKeyPtr = &promptKey
	}
	if chunkID == "" {
		chunkID = uuid.NewString()
	}
	if created == "" {
		created = at.Format(time.RFC3339Nano)
	}
	chunk := &ConversationChunk{
		ID:                               chunkID,
		Text:                             text,
		Role:                             role,
		BotType:                          c.botType,
		ConversationID:                   c.conversationID,
		UserID:                           c.userID,
		LLMTTFBMs:                        assistantMetricSeconds(role, metrics.LLMTTFBMs),
		TTSTTFBMs:                        assistantMetricSeconds(role, metrics.TTSTTFBMs),
		V2VLatencyMs:                     assistantMetricSeconds(role, metrics.E2ELatencyMs),
		TextAggregationMs:                assistantMetricSeconds(role, metrics.TTSTextAggregationMs),
		Created:                          created,
		IsDebugLog:                       isDebugLog,
		AdditionalData:                   additionalData,
		MainAgentSystemPromptLangfuseKey: promptKeyPtr,
	}
	if c.chunkDecorator != nil {
		c.chunkDecorator(chunk)
	}
	return chunk
}

func (c *CallEventCallbacks) rewriteLastUserChunk(text string, at time.Time, promptKey string) {
	if c == nil || c.redis == nil {
		return
	}
	chunk := c.buildChunk(
		text,
		"user",
		at,
		voicepipelinecore.TurnMetrics{},
		promptKey,
		nil,
		false,
		c.lastChunk.id,
		c.lastChunk.created,
	)
	ctx, cancel := context.WithTimeout(context.Background(), chunkWriteTimeout)
	defer cancel()
	if err := c.redis.SetChunk(ctx, c.userID, c.conversationID, c.lastChunk.index, *chunk); err != nil && c.logger != nil {
		c.logger.Printf("disha: chunk persist failed conversation=%s role=%s: %v\n", c.conversationID, chunk.Role, err)
	}
}

func (c *CallEventCallbacks) runPostCallOperations(reason voicepipelinecore.EndReason, stats voicepipelinecore.CallStats, logDataS3Key string) {
	if c == nil || c.api == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), postCallRequestTimeout)
	defer cancel()
	req := PostCallOperationsRequest{
		ConversationID:                 c.conversationID,
		EndReason:                      mapEndReason(reason),
		TotalUserDuration:              int(stats.TotalUserDurationSec),
		FirstUserAudioFramesReceivedAt: optionalTime(stats.FirstUserAudioFrameAt),
		EndedAt:                        stats.EndedAt,
		LogDataS3Key:                   logDataS3Key,
		OnboardingCallDone:             false,
	}
	if req.EndedAt.IsZero() {
		req.EndedAt = time.Now()
	}
	if c.postCallDecorator != nil {
		c.postCallDecorator(&req)
	}
	if err := c.api.RunPostCallOperationsWithFallback(ctx, req); err != nil && c.logger != nil {
		c.logger.Printf("disha: run_post_call_operations failed conversation=%s user=%s: %v\n", c.conversationID, c.userID, err)
	}
}

func (c *CallEventCallbacks) enqueueDailyMetrics(stats voicepipelinecore.CallStats) {
	if c == nil || c.api == nil || stats.MeetingID == "" || stats.UserSessionID == "" {
		return
	}
	if stats.TransportType != "" && stats.TransportType != "daily" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), postCallRequestTimeout)
	defer cancel()
	req := EnqueueJobRequest{
		ModuleName: "bots.webhooks",
		FuncName:   "fetch_and_store_daily_metrics",
		Kwargs: map[string]any{
			"conversation_id": c.conversationID,
			"meeting_id":      stats.MeetingID,
			"bot_session_id":  stats.BotSessionID,
			"user_session_id": stats.UserSessionID,
		},
		SQSQueue: "p1-fast-l1",
	}
	if err := c.api.EnqueueJob(ctx, req); err != nil && c.logger != nil {
		c.logger.Printf("disha: enqueue Daily metrics failed conversation=%s user=%s: %v\n", c.conversationID, c.userID, err)
	}
}

func (c *CallEventCallbacks) enqueueChunkSync() {
	if c == nil || c.api == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), postCallRequestTimeout)
	defer cancel()
	req := EnqueueJobRequest{
		ModuleName: "services.conversation_chunk_manager",
		FuncName:   "sync_conversation_chunks_to_db",
		Kwargs: map[string]any{
			"user_id":         c.userID,
			"conversation_id": c.conversationID,
			"bot_type":        c.botType,
		},
		SQSQueue: "p1-fast-l1",
	}
	if err := c.api.EnqueueJob(ctx, req); err != nil && c.logger != nil {
		c.logger.Printf("disha: enqueue chunk sync failed conversation=%s user=%s: %v\n", c.conversationID, c.userID, err)
	}
}

func optionalTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// mapEndReason maps core end reasons to Disha's end_reason enum, which only
// supports talktime_exhausted and user_idle. Everything else, including the
// 120s-watchdog EndReasonIdleTimeout, deliberately falls to the nil default
// (Python's Pipecat cancel_on_idle_timeout parity: an unlabeled task cancel).
func mapEndReason(reason voicepipelinecore.EndReason) *string {
	switch reason {
	case voicepipelinecore.EndReasonTalkTimeExhausted:
		value := "talktime_exhausted"
		return &value
	case voicepipelinecore.EndReasonUserIdle:
		value := "user_idle"
		return &value
	default:
		return nil
	}
}

func assistantMetricSeconds(role string, valueMs float64) *float64 {
	if role != "assistant" || valueMs == 0 {
		return nil
	}
	valueSeconds := valueMs / 1000
	return &valueSeconds
}
