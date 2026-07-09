package disha

import (
	"net/http"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

func TestNewLLMLogSinkQueuesModuleLevelWrapper(t *testing.T) {
	server, requests := captureAPIRequest(t, http.StatusOK)
	api := NewAPIClient(server.URL, time.Second, nil)
	sink := newLLMLogSink(api, nil, "sales_call_conversation", "user-1", "conv-1")
	if sink == nil {
		t.Fatal("newLLMLogSink returned nil")
	}

	sink(llmrouter.CallLog{
		Model:           "grok-4-1-fast-non-reasoning",
		Deployment:      "GROK_4_1_FNR_EASTUS",
		Request:         voicepipelinecore.LLMRequest{Messages: []voicepipelinecore.Message{{Role: "user", Content: "hello"}}},
		ResponseContent: "hi",
		ToolCalls: []voicepipelinecore.ToolCall{{
			ID:   "call_1",
			Type: "function",
			Function: voicepipelinecore.ToolCallFunction{
				Name:      "get_guidance",
				Arguments: `{"situation":"pain"}`,
			},
		}},
		TTFBMs:  12.5,
		TotalMs: 48.25,
		PromptMetadata: map[string]any{
			"system_prompt_name":      "sales_call/main_sys-3day_v2",
			"system_prompt_version":   17,
			"system_prompt_variables": DocumentVariables{"patient_info": "Riya"},
		},
		PromptTokens:     11,
		CompletionTokens: 7,
		StatusCode:       http.StatusOK,
		Completed:        true,
		FinishReason:     "stop",
	})

	var got capturedAPIRequest
	select {
	case got = <-requests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for enqueue request")
	}
	if got.Method != http.MethodPost || got.Path != "/common/enqueue_job" {
		t.Fatalf("request = %s %s, want POST /common/enqueue_job", got.Method, got.Path)
	}
	if got.Body["module_name"] != "services.llm_logging_service" ||
		got.Body["func_name"] != "log_llm_call_job" ||
		got.Body["sqs_queue"] != "p1-fast-l1" {
		t.Fatalf("enqueue target mismatch: %+v", got.Body)
	}

	kwargs, ok := got.Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", got.Body["kwargs"])
	}
	if kwargs["entity_id"] != "conv-1" ||
		kwargs["user_id"] != "user-1" ||
		kwargs["entity_type"] != "callconversation" ||
		kwargs["usecase_type"] != "sales_call_conversation" ||
		kwargs["deployment"] != "GROK_4_1_FNR_EASTUS" ||
		kwargs["llm_call_completed"] != true ||
		kwargs["prompt_tokens"] != float64(11) ||
		kwargs["completion_tokens"] != float64(7) {
		t.Fatalf("kwargs mismatch: %+v", kwargs)
	}
	promptMetadata, ok := kwargs["prompt_metadata"].(map[string]any)
	if !ok {
		t.Fatalf("prompt_metadata = %#v, want object", kwargs["prompt_metadata"])
	}
	if promptMetadata["system_prompt_name"] != "sales_call/main_sys-3day_v2" ||
		promptMetadata["system_prompt_version"] != float64(17) {
		t.Fatalf("prompt_metadata identity = %+v", promptMetadata)
	}
	promptVars, ok := promptMetadata["system_prompt_variables"].(map[string]any)
	if !ok || promptVars["patient_info"] != "Riya" {
		t.Fatalf("system_prompt_variables = %#v, want patient_info", promptMetadata["system_prompt_variables"])
	}
	responsePayload, ok := kwargs["response_payload"].(map[string]any)
	if !ok {
		t.Fatalf("response_payload = %#v, want object", kwargs["response_payload"])
	}
	if _, hasToolCalls := responsePayload["tool_calls"]; hasToolCalls {
		t.Fatalf("response_payload should match live Disha shape without raw tool_calls: %+v", responsePayload)
	}
	functions, ok := responsePayload["functions_list"].([]any)
	if !ok || len(functions) != 1 || functions[0] != "get_guidance" {
		t.Fatalf("functions_list = %#v, want [get_guidance]", responsePayload["functions_list"])
	}
	arguments, ok := responsePayload["arguments_list"].([]any)
	if !ok || len(arguments) != 1 || arguments[0] != `{"situation":"pain"}` {
		t.Fatalf("arguments_list = %#v, want situation JSON", responsePayload["arguments_list"])
	}
	toolIDs, ok := responsePayload["tool_id_list"].([]any)
	if !ok || len(toolIDs) != 1 || toolIDs[0] != "call_1" {
		t.Fatalf("tool_id_list = %#v, want [call_1]", responsePayload["tool_id_list"])
	}
	usage, ok := responsePayload["usage"].(map[string]any)
	if !ok || usage["prompt_tokens"] != float64(11) || usage["completion_tokens"] != float64(7) || usage["total_tokens"] != float64(18) {
		t.Fatalf("usage = %#v, want 11/7/18", responsePayload["usage"])
	}
	if got := responsePayload["finish_reason"]; got != "stop" {
		t.Fatalf("response_payload[finish_reason] = %#v, want %q", got, "stop")
	}
}
