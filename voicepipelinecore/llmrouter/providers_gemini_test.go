package llmrouter

import (
	"net/http"
	"testing"

	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

func toolCallHistoryRequest() vpc.LLMRequest {
	return vpc.LLMRequest{
		Messages: []vpc.Message{
			{Role: "user", Content: "hi"},
			{Role: "assistant", ToolCalls: []vpc.ToolCall{{
				ID: "call_1", Type: "function",
				Function: vpc.ToolCallFunction{Name: "get_guidance", Arguments: `{"situation":"x"}`},
			}}},
			{Role: "tool", Content: `{"ok":true}`, ToolCallID: "call_1"},
			{Role: "user", Content: "and then?"},
		},
	}
}

// Gemini 3 rejects assistant functionCall history without a
// thought_signature; the router injects Google's bypass value into every
// replayed tool call, mirroring Python's
// _inject_gemini_thought_signatures (Sentry issue 7615109021).
func TestBuildRequestGeminiInjectsThoughtSignature(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "or-key")
	r := &Router{cfg: Config{}, httpClient: &http.Client{}}

	req, err := r.buildRequest(ctx(), endpointConfigs["openrouter_gemini_flash_3_1_lite"], toolCallHistoryRequest())
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	body := readBody(t, req)
	msgs := body["messages"].([]any)
	if len(msgs) != 4 {
		t.Fatalf("messages len = %d, want 4", len(msgs))
	}
	assistant := msgs[1].(map[string]any)
	calls := assistant["tool_calls"].([]any)
	call := calls[0].(map[string]any)
	extra, ok := call["extra_content"].(map[string]any)
	if !ok {
		t.Fatalf("tool call missing extra_content: %#v", call)
	}
	google := extra["google"].(map[string]any)
	if google["thought_signature"] != geminiThoughtSignatureSkip {
		t.Fatalf("thought_signature = %#v", google["thought_signature"])
	}
	fn := call["function"].(map[string]any)
	if fn["name"] != "get_guidance" || fn["arguments"] != `{"situation":"x"}` {
		t.Fatalf("tool call function mangled: %#v", fn)
	}
	if tool := msgs[2].(map[string]any); tool["tool_call_id"] != "call_1" {
		t.Fatalf("tool result message mangled: %#v", tool)
	}
}

func TestBuildRequestNonGeminiLeavesToolCallsUntouched(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "or-key")
	r := &Router{cfg: Config{}, httpClient: &http.Client{}}

	req, err := r.buildRequest(ctx(), endpointConfigs["openrouter_gemma_4_31b_it_modelrun"], toolCallHistoryRequest())
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	body := readBody(t, req)
	assistant := body["messages"].([]any)[1].(map[string]any)
	call := assistant["tool_calls"].([]any)[0].(map[string]any)
	if _, present := call["extra_content"]; present {
		t.Fatalf("non-gemini request should not carry extra_content: %#v", call)
	}
}
