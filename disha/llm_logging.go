package disha

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

const (
	// llmLogModule/llmLogFunc target a module-level Disha job wrapper.
	// Do not enqueue the llm_logging_service singleton method directly:
	// SQS serialization stores __qualname__ and loses the bound self.
	llmLogModule = "services.llm_logging_service"
	llmLogFunc   = "log_llm_call_job"
	llmLogQueue  = "p1-fast-l1"

	// Matches services.llm_logging_service.EntityType.CALL_CONVERSATION.
	// The usecase_type is supplied per call so this sink is not bot-specific.
	callConversationEntityType = "callconversation"

	// maxLLMLogPayloadBytes guards SQS's 256KB message limit; if the
	// assembled kwargs exceed this the log is dropped (best-effort).
	maxLLMLogPayloadBytes = 200 * 1024
)

// newLLMLogSink returns an llmrouter.LogSink that enqueues each LLM call
// to Disha's llm_logging_service (S3 + DB) through the existing
// /common/enqueue_job worker. usecaseType differentiates call types
// (e.g. sales_call_conversation), so this sink is reusable across bots.
// It is best-effort: failures are logged and swallowed so they never
// affect the live call. Returns nil when no API client is available.
func newLLMLogSink(api *APIClient, logger *log.Logger, usecaseType, userID, conversationID string) func(llmrouter.CallLog) {
	if api == nil {
		return nil
	}
	return func(c llmrouter.CallLog) {
		requestPayload := map[string]any{
			"model":          c.Model,
			"messages":       c.Request.Messages,
			"stream":         true,
			"stream_options": map[string]any{"include_usage": true},
		}
		if len(c.Request.Tools) > 0 {
			requestPayload["tools"] = c.Request.Tools
		}
		if c.Request.ToolChoice != nil {
			requestPayload["tool_choice"] = c.Request.ToolChoice
		}
		responsePayload := map[string]any{
			"content":        c.ResponseContent,
			"functions_list": toolCallNames(c.ToolCalls),
			"arguments_list": toolCallArguments(c.ToolCalls),
			"tool_id_list":   toolCallIDs(c.ToolCalls),
			"usage": map[string]any{
				"prompt_tokens":     c.PromptTokens,
				"completion_tokens": c.CompletionTokens,
				"total_tokens":      c.PromptTokens + c.CompletionTokens,
			},
			"completed":   c.Completed,
			"status_code": c.StatusCode,
			// finish_reason nests inside the existing response_payload
			// dict rather than becoming a new top-level kwarg: Python's
			// llm_logging_service.log_llm_call has a strict typed
			// parameter list with no **kwargs catch-all, so an unknown
			// TOP-LEVEL kwarg would TypeError the SQS job. response_payload
			// itself is Dict[str, Any] passed straight through into the S3
			// "response" blob with no field-level validation anywhere
			// (verified: _save_to_db's DB row doesn't even reference
			// response_payload), so adding a key here is safe today with
			// no backend deploy required.
			"finish_reason": c.FinishReason,
		}
		if c.ErrorMessage != "" {
			responsePayload["error"] = map[string]any{
				"type":        c.ErrorType,
				"message":     c.ErrorMessage,
				"status_code": c.StatusCode,
			}
		}
		kwargs := map[string]any{
			"model":              c.Model,
			"request_payload":    requestPayload,
			"response_payload":   responsePayload,
			"prompt_tokens":      c.PromptTokens,
			"completion_tokens":  c.CompletionTokens,
			"ttfb_ms":            c.TTFBMs,
			"total_time_ms":      c.TotalMs,
			"user_id":            userID,
			"entity_type":        callConversationEntityType,
			"entity_id":          conversationID,
			"usecase_type":       usecaseType,
			"deployment":         c.Deployment,
			"llm_call_completed": c.Completed,
			"status_code":        statusCodeOrNil(c.StatusCode),
			// The sink runs synchronously at call completion, so Now() is
			// the real completion time. Without this the Python job stamps
			// the log when IT runs, skewing Go rows by the SQS queue lag
			// (4-60s+ under load) and making llm-log timelines unusable.
			// TOP-LEVEL kwarg: requires disha-backend's log_llm_call to
			// declare call_completed_at (deployed 2026-07-18) — an unknown
			// kwarg TypeErrors the SQS job, so backend deploys first.
			"call_completed_at": time.Now().UTC().Format("2006-01-02T15:04:05.000000"),
		}
		if len(c.PromptMetadata) > 0 {
			kwargs["prompt_metadata"] = c.PromptMetadata
		}

		raw, err := json.Marshal(kwargs)
		if err != nil || len(raw) > maxLLMLogPayloadBytes {
			if logger != nil {
				logger.Printf("disha: skipping LLM call log (bytes=%d err=%v)\n", len(raw), err)
			}
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := api.EnqueueJob(ctx, EnqueueJobRequest{
			ModuleName: llmLogModule,
			FuncName:   llmLogFunc,
			Kwargs:     kwargs,
			SQSQueue:   llmLogQueue,
		}); err != nil && logger != nil {
			logger.Printf("disha: LLM call log enqueue failed: %v\n", err)
		}
	}
}

func toolCallNames(calls []voicepipelinecore.ToolCall) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, call.Function.Name)
	}
	return out
}

func toolCallArguments(calls []voicepipelinecore.ToolCall) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, call.Function.Arguments)
	}
	return out
}

func toolCallIDs(calls []voicepipelinecore.ToolCall) []string {
	out := make([]string, 0, len(calls))
	for _, call := range calls {
		out = append(out, call.ID)
	}
	return out
}

func statusCodeOrNil(code int) any {
	if code == 0 {
		return nil
	}
	return code
}
