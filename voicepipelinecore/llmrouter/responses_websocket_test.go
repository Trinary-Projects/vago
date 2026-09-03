package llmrouter

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

type responsesWSTestServer struct {
	t           *testing.T
	server      *httptest.Server
	respond     func(requestIndex, connectionIndex int, request map[string]any, conn *websocket.Conn)
	mu          sync.Mutex
	requests    []map[string]any
	headers     []http.Header
	queries     []url.Values
	connections int
}

func newResponsesWSTestServer(t *testing.T, respond func(requestIndex, connectionIndex int, request map[string]any, conn *websocket.Conn)) *responsesWSTestServer {
	t.Helper()
	harness := &responsesWSTestServer{t: t, respond: respond}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	harness.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer conn.Close()

		harness.mu.Lock()
		connectionIndex := harness.connections
		harness.connections++
		harness.mu.Unlock()
		for {
			var request map[string]any
			if err := conn.ReadJSON(&request); err != nil {
				return
			}
			harness.mu.Lock()
			requestIndex := len(harness.requests)
			harness.requests = append(harness.requests, request)
			harness.headers = append(harness.headers, r.Header.Clone())
			harness.queries = append(harness.queries, r.URL.Query())
			harness.mu.Unlock()
			respond(requestIndex, connectionIndex, request, conn)
		}
	}))
	t.Cleanup(harness.server.Close)
	return harness
}

func (s *responsesWSTestServer) snapshot() ([]map[string]any, []http.Header, []url.Values, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	requests := append([]map[string]any(nil), s.requests...)
	headers := append([]http.Header(nil), s.headers...)
	queries := append([]url.Values(nil), s.queries...)
	return requests, headers, queries, s.connections
}

func installResponsesTestEndpoint(t *testing.T, key string, endpoint endpointConfig) {
	t.Helper()
	previous, existed := endpointConfigs[key]
	endpoint.Key = key
	endpoint.APIMode = apiModeResponsesWebSocket
	if endpoint.Model == "" {
		endpoint.Model = "gpt-5.6-luna"
	}
	if endpoint.Region == "" {
		endpoint.Region = "us"
	}
	if endpoint.ReasoningEffort == "" {
		endpoint.ReasoningEffort = "none"
	}
	endpointConfigs[key] = endpoint
	t.Cleanup(func() {
		if existed {
			endpointConfigs[key] = previous
		} else {
			delete(endpointConfigs, key)
		}
	})
}

func installResponsesTestGroup(t *testing.T, key string, endpointKeys ...string) {
	t.Helper()
	previous, existed := responsesWebSocketGroups[key]
	responsesWebSocketGroups[key] = modelGroup{Configs: endpointKeys}
	t.Cleanup(func() {
		if existed {
			responsesWebSocketGroups[key] = previous
		} else {
			delete(responsesWebSocketGroups, key)
		}
	})
}

func openAIResponsesTestEndpoint(serverURL string) endpointConfig {
	return endpointConfig{
		Provider:  providerOpenAI,
		APIKeyEnv: "RESPONSES_WS_TEST_API_KEY",
		BaseURL:   serverURL + "/v1",
	}
}

func sendResponsesText(t *testing.T, conn *websocket.Conn, id, text string) {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{
		"type":  "response.output_text.delta",
		"delta": text,
	}); err != nil {
		t.Errorf("write text delta: %v", err)
		return
	}
	if err := conn.WriteJSON(map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"id":     id,
			"status": "completed",
			"model":  "gpt-5.6-luna",
			"output": []any{map[string]any{
				"type":    "message",
				"content": []any{map[string]any{"type": "output_text", "text": text}},
			}},
			"usage": map[string]any{
				"input_tokens": 10, "output_tokens": 2, "total_tokens": 12,
				"output_tokens_details": map[string]any{"reasoning_tokens": 0},
			},
		},
	}); err != nil {
		t.Errorf("write completed response: %v", err)
	}
}

func TestResponsesWebSocketUsesIncrementalInputAndResetsOnRewrite(t *testing.T) {
	logs := make(chan CallLog, 3)
	texts := []string{"ACK", "COBALT", "AMBER"}
	server := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-"+string(rune('1'+requestIndex)), texts[requestIndex])
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_incremental_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))

	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey, LogSink: func(entry CallLog) { logs <- entry }})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	firstMessages := []vpc.Message{
		{Role: "system", Content: "Remember the code word."},
		{Role: "user", Content: "The code word is COBALT."},
	}
	var firstText strings.Builder
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: firstMessages}, func(token string) { firstText.WriteString(token) }); err != nil {
		t.Fatal(err)
	}
	if firstText.String() != "ACK" {
		t.Fatalf("first text = %q, want ACK", firstText.String())
	}

	secondMessages := append(cloneRouterMessages(firstMessages),
		vpc.Message{Role: "assistant", Content: "ACK"},
		vpc.Message{Role: "user", Content: "What is the code word?"},
	)
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: secondMessages}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	rewrittenMessages := []vpc.Message{
		{Role: "system", Content: "Use only this rewritten history."},
		{Role: "user", Content: "The code word is AMBER."},
		{Role: "assistant", Content: "ACK"},
		{Role: "user", Content: "What is the code word?"},
	}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: rewrittenMessages}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	requests, headers, _, connections := server.snapshot()
	if connections != 1 {
		t.Fatalf("connections = %d, want one persistent socket", connections)
	}
	if len(requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(requests))
	}
	if got := headers[0].Get("Authorization"); got != "Bearer secret" {
		t.Fatalf("Authorization = %q", got)
	}
	if _, exists := requests[0]["previous_response_id"]; exists {
		t.Fatal("first request unexpectedly used previous_response_id")
	}
	assertReasoningNone(t, requests[0])
	if requests[0]["instructions"] != "Remember the code word." {
		t.Fatalf("first instructions = %v", requests[0]["instructions"])
	}
	if requests[1]["previous_response_id"] != "resp-1" {
		t.Fatalf("second previous_response_id = %v, want resp-1", requests[1]["previous_response_id"])
	}
	secondInput := requestInput(t, requests[1])
	if len(secondInput) != 1 || secondInput[0]["role"] != "user" || secondInput[0]["content"] != "What is the code word?" {
		t.Fatalf("second input = %#v, want only new user item", secondInput)
	}
	if _, exists := requests[2]["previous_response_id"]; exists {
		t.Fatal("rewritten request unexpectedly used previous_response_id")
	}
	if requests[2]["instructions"] != "Use only this rewritten history." {
		t.Fatalf("rewrite instructions = %v", requests[2]["instructions"])
	}
	if got := len(requestInput(t, requests[2])); got != 3 {
		t.Fatalf("rewritten input items = %d, want full non-system history", got)
	}

	modes := make(map[string]string, 3)
	for range 3 {
		select {
		case entry := <-logs:
			modes[entry.ResponseContent] = entry.ResponseInputMode
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for input-mode call logs")
		}
	}
	if modes["ACK"] != "full" || modes["COBALT"] != "incremental" || modes["AMBER"] != "full" {
		t.Fatalf("response input modes = %#v, want ACK/AMBER full and COBALT incremental", modes)
	}
}

func TestResponsesWebSocketIgnoresUnknownEventWithIncompatibleFields(t *testing.T) {
	server := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		if err := conn.WriteJSON(map[string]any{
			"type": "response.audio.delta", "delta": map[string]any{"audio": "ignored"}, "status": "queued",
		}); err != nil {
			t.Errorf("write unknown event: %v", err)
			return
		}
		sendResponsesText(t, conn, "resp-after-unknown", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_unknown_event_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var text strings.Builder
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hi"}}}, func(token string) { text.WriteString(token) }); err != nil {
		t.Fatal(err)
	}
	if text.String() != "OK" {
		t.Fatalf("text = %q, want OK", text.String())
	}
}

func TestResponsesWebSocketContinuesToolCallWithOnlyToolOutput(t *testing.T) {
	server := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		if requestIndex == 0 {
			item := map[string]any{
				"type": "function_call", "call_id": "call-1", "name": "lookup", "arguments": `{"key":"vago"}`,
			}
			if err := conn.WriteJSON(map[string]any{"type": "response.output_item.done", "item": item}); err != nil {
				t.Errorf("write tool item: %v", err)
				return
			}
			if err := conn.WriteJSON(map[string]any{
				"type": "response.completed",
				"response": map[string]any{
					"id": "resp-tool", "status": "completed", "model": "gpt-5.6-luna",
					"output": []any{item},
					"usage":  map[string]any{"input_tokens": 12, "output_tokens": 4, "total_tokens": 16, "output_tokens_details": map[string]any{"reasoning_tokens": 0}},
				},
			}); err != nil {
				t.Errorf("write tool completion: %v", err)
			}
			return
		}
		sendResponsesText(t, conn, "resp-tool-result", "TOOL_OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_tool_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	strict := true
	request := vpc.LLMRequest{
		Messages: []vpc.Message{{Role: "system", Content: "Use the tool."}, {Role: "user", Content: "Lookup vago."}},
		Tools: []vpc.ToolDefinition{{
			Type: "function",
			Function: vpc.ToolFunction{
				Name: "lookup", Description: "Lookup a key.", Strict: &strict,
				Parameters: map[string]any{"type": "object", "properties": map[string]any{"key": map[string]any{"type": "string"}}, "required": []string{"key"}},
			},
		}},
		ToolChoice: map[string]any{"type": "function", "function": map[string]any{"name": "lookup"}},
	}
	result, err := client.Stream(context.Background(), request, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ToolCalls) != 1 || result.ToolCalls[0].ID != "call-1" || result.ToolCalls[0].Function.Name != "lookup" {
		t.Fatalf("tool calls = %#v", result.ToolCalls)
	}

	continued := cloneRouterMessages(request.Messages)
	continued = append(continued,
		vpc.Message{Role: "assistant", ToolCalls: result.ToolCalls},
		vpc.Message{Role: "tool", ToolCallID: "call-1", Content: `{"value":"TOOL_OK"}`},
	)
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: continued, Tools: request.Tools}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	requests, _, _, _ := server.snapshot()
	tools, ok := requests[0]["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %#v", requests[0]["tools"])
	}
	flatTool := tools[0].(map[string]any)
	if flatTool["name"] != "lookup" || flatTool["strict"] != true {
		t.Fatalf("flattened tool = %#v", flatTool)
	}
	if _, nested := flatTool["function"]; nested {
		t.Fatalf("Responses tool retained Chat-Completions function wrapper: %#v", flatTool)
	}
	if !reflect.DeepEqual(requests[0]["tool_choice"], map[string]any{"type": "function", "name": "lookup"}) {
		t.Fatalf("tool_choice = %#v", requests[0]["tool_choice"])
	}
	if requests[1]["previous_response_id"] != "resp-tool" {
		t.Fatalf("tool result previous_response_id = %v", requests[1]["previous_response_id"])
	}
	input := requestInput(t, requests[1])
	if len(input) != 1 || input[0]["type"] != "function_call_output" || input[0]["call_id"] != "call-1" || input[0]["output"] != `{"value":"TOOL_OK"}` {
		t.Fatalf("tool result input = %#v", input)
	}
}

func TestResponsesWebSocketRetriesEvictedPreviousResponseWithFullHistory(t *testing.T) {
	server := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		switch requestIndex {
		case 0:
			sendResponsesText(t, conn, "resp-1", "ACK")
		case 1:
			if err := conn.WriteJSON(map[string]any{"type": "error", "code": "previous_response_not_found", "message": "evicted"}); err != nil {
				t.Errorf("write eviction error: %v", err)
			}
		case 2:
			sendResponsesText(t, conn, "resp-2", "COBALT")
		}
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_eviction_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	first := []vpc.Message{{Role: "system", Content: "Remember."}, {Role: "user", Content: "COBALT"}}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: first}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	second := append(cloneRouterMessages(first), vpc.Message{Role: "assistant", Content: "ACK"}, vpc.Message{Role: "user", Content: "Repeat it."})
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: second}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	requests, _, _, connections := server.snapshot()
	if connections != 2 || len(requests) != 3 {
		t.Fatalf("connections/requests = %d/%d, want 2/3", connections, len(requests))
	}
	if requests[1]["previous_response_id"] != "resp-1" {
		t.Fatalf("evicted attempt previous id = %v", requests[1]["previous_response_id"])
	}
	if _, exists := requests[2]["previous_response_id"]; exists {
		t.Fatal("full-history retry unexpectedly used previous_response_id")
	}
	if got := len(requestInput(t, requests[2])); got != 3 {
		t.Fatalf("retry input items = %d, want full non-system history", got)
	}
}

func TestResponsesWebSocketInterruptReconnectsAndReplaysPartialAssistant(t *testing.T) {
	server := newResponsesWSTestServer(t, func(_ int, connectionIndex int, _ map[string]any, conn *websocket.Conn) {
		if connectionIndex == 0 {
			if err := conn.WriteJSON(map[string]any{"type": "response.output_text.delta", "delta": "PARTIAL"}); err != nil {
				t.Errorf("write partial delta: %v", err)
			}
			return
		}
		sendResponsesText(t, conn, "resp-after-interrupt", "INTERRUPT_OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_interrupt_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	first := []vpc.Message{{Role: "system", Content: "Speak."}, {Role: "user", Content: "Start."}}
	result, err := client.Stream(ctx, vpc.LLMRequest{Messages: first}, func(string) { cancel() })
	if err == nil || !result.Interrupted {
		t.Fatalf("interrupted result/error = %#v / %v", result, err)
	}

	second := append(cloneRouterMessages(first), vpc.Message{Role: "assistant", Content: "PARTIAL"}, vpc.Message{Role: "user", Content: "Stop."})
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: second}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	requests, _, _, connections := server.snapshot()
	if connections != 2 {
		t.Fatalf("connections = %d, want reconnect after interrupt", connections)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	if _, exists := requests[1]["previous_response_id"]; exists {
		t.Fatal("post-interrupt request unexpectedly used previous_response_id")
	}
	input := requestInput(t, requests[1])
	if len(input) != 3 || input[1]["role"] != "assistant" || input[1]["content"] != "PARTIAL" {
		t.Fatalf("post-interrupt full replay = %#v", input)
	}
}

func TestResponsesWebSocketOrderedDialFailover(t *testing.T) {
	server := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-fallback", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_MISSING_KEY", "")
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_failover_first"
		secondKey = "responses_ws_failover_second"
		groupKey  = "responses_ws_failover_group"
	)
	installResponsesTestEndpoint(t, firstKey, endpointConfig{
		Provider: providerOpenAI, APIKeyEnv: "RESPONSES_WS_TEST_MISSING_KEY", BaseURL: server.server.URL + "/first/v1",
	})
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(server.server.URL))
	previousGroup, existed := responsesWebSocketGroups[groupKey]
	responsesWebSocketGroups[groupKey] = modelGroup{Configs: []string{firstKey, secondKey}, Fallback: secondKey}
	t.Cleanup(func() {
		if existed {
			responsesWebSocketGroups[groupKey] = previousGroup
		} else {
			delete(responsesWebSocketGroups, groupKey)
		}
	})

	logs := make(chan CallLog, 1)
	client, err := NewClient(Config{Group: groupKey, LogSink: func(entry CallLog) { logs <- entry }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.(*ResponsesWebSocketClient).Close() }()
	result, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hi"}}}, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	if result.Model != "gpt-5.6-luna" {
		t.Fatalf("model = %q", result.Model)
	}
	select {
	case entry := <-logs:
		if entry.ConfigKey != secondKey || !entry.UsingFallback {
			t.Fatalf("fallback log = %#v", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for call log")
	}
}

func TestResponsesWebSocketDialTimeoutFallsThroughToNextCandidate(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			defer conn.Close()
			<-time.After(3 * responsesWebSocketDialTimeout)
		}
	}()

	working := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-after-dial-timeout", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_hung_handshake_first"
		secondKey = "responses_ws_hung_handshake_second"
		groupKey  = "responses_ws_hung_handshake_group"
	)
	installResponsesTestEndpoint(t, firstKey, endpointConfig{
		Provider: providerOpenAI, APIKeyEnv: "RESPONSES_WS_TEST_API_KEY", BaseURL: "http://" + listener.Addr().String() + "/v1",
	})
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(working.server.URL))
	installResponsesTestGroup(t, groupKey, firstKey, secondKey)
	client, err := NewResponsesWebSocket(Config{Group: groupKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	started := time.Now()
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hi"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 2*responsesWebSocketDialTimeout {
		t.Fatalf("dial failover took %s, want bounded per-candidate timeout", elapsed)
	}
	_, _, _, workingConnections := working.snapshot()
	if workingConnections != 1 {
		t.Fatalf("working endpoint connections = %d, want 1", workingConnections)
	}
}

func TestResponsesWebSocketEventDeadlineAdvancesCandidate(t *testing.T) {
	silent := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, _ *websocket.Conn) {})
	working := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-after-event-timeout", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_event_timeout_first"
		secondKey = "responses_ws_event_timeout_second"
		groupKey  = "responses_ws_event_timeout_group"
	)
	installResponsesTestEndpoint(t, firstKey, openAIResponsesTestEndpoint(silent.server.URL))
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(working.server.URL))
	installResponsesTestGroup(t, groupKey, firstKey, secondKey)
	client, err := NewResponsesWebSocket(Config{Group: groupKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	started := time.Now()
	result, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hang"}}}, func(string) {})
	if err == nil || result.Interrupted {
		t.Fatalf("silent result/error = %#v / %v, want transport timeout", result, err)
	}
	if elapsed := time.Since(started); elapsed < responsesWebSocketEventTimeout || elapsed > 2*responsesWebSocketEventTimeout {
		t.Fatalf("event timeout took %s, want approximately %s", elapsed, responsesWebSocketEventTimeout)
	}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "retry"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	_, _, _, workingConnections := working.snapshot()
	if workingConnections != 1 {
		t.Fatalf("working endpoint connections = %d, want 1 after event-timeout advance", workingConnections)
	}
}

func TestResponsesWebSocketAzureUsesQueryAuthentication(t *testing.T) {
	server := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-azure", "OK")
	})
	t.Setenv("RESPONSES_WS_AZURE_ENDPOINT", server.server.URL)
	t.Setenv("RESPONSES_WS_AZURE_KEY", "azure-secret")
	const endpointKey = "responses_ws_azure_auth_test"
	installResponsesTestEndpoint(t, endpointKey, endpointConfig{
		Provider: providerAzure, APIKeyEnv: "RESPONSES_WS_AZURE_KEY", EndpointEnv: "RESPONSES_WS_AZURE_ENDPOINT",
	})
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hi"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	_, headers, queries, _ := server.snapshot()
	if got := queries[0].Get("api-key"); got != "azure-secret" {
		t.Fatalf("api-key query = %q", got)
	}
	if got := headers[0].Get("Authorization"); got != "" {
		t.Fatalf("Azure Authorization header = %q, want empty", got)
	}
}

func TestResponsesWebSocketRetriesAStaleReusedSocketWithFullHistory(t *testing.T) {
	logs := make(chan CallLog, 2)
	server := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		if requestIndex == 0 {
			sendResponsesText(t, conn, "resp-1", "ACK")
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "idle"), time.Now().Add(time.Second))
			_ = conn.Close()
			return
		}
		sendResponsesText(t, conn, "resp-2", "SECOND")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_stale_reuse_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey, LogSink: func(entry CallLog) { logs <- entry }})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	first := []vpc.Message{{Role: "system", Content: "prompt"}, {Role: "user", Content: "first"}}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: first}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	second := append(cloneRouterMessages(first), vpc.Message{Role: "assistant", Content: "ACK"}, vpc.Message{Role: "user", Content: "second"})
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: second}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	requests, _, _, connections := server.snapshot()
	if connections != 2 || len(requests) != 2 {
		t.Fatalf("connections/requests = %d/%d, want transparent reconnect 2/2", connections, len(requests))
	}
	if _, exists := requests[1]["previous_response_id"]; exists {
		t.Fatal("stale-socket retry unexpectedly retained the connection-local response id")
	}
	if got := len(requestInput(t, requests[1])); got != 3 {
		t.Fatalf("stale-socket retry input items = %d, want full history", got)
	}

	var secondLog CallLog
	for range 2 {
		select {
		case entry := <-logs:
			if entry.ResponseContent == "SECOND" {
				secondLog = entry
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for stale-socket call logs")
		}
	}
	if secondLog.ResponseInputMode != "full" {
		t.Fatalf("stale-socket input mode = %q, want full", secondLog.ResponseInputMode)
	}
}

func TestResponsesWebSocketCancellationWithoutOutputAdvancesCandidate(t *testing.T) {
	silent := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, _ *websocket.Conn) {})
	working := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-working", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_silent_first"
		secondKey = "responses_ws_working_second"
		groupKey  = "responses_ws_silent_group"
	)
	installResponsesTestEndpoint(t, firstKey, openAIResponsesTestEndpoint(silent.server.URL))
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(working.server.URL))
	installResponsesTestGroup(t, groupKey, firstKey, secondKey)
	client, err := NewResponsesWebSocket(Config{Group: groupKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if result, err := client.Stream(ctx, vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hang"}}}, func(string) {}); err == nil || !result.Interrupted {
		t.Fatalf("silent result/error = %#v / %v, want interrupted timeout", result, err)
	}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "retry"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	_, _, _, silentConnections := silent.snapshot()
	_, _, _, workingConnections := working.snapshot()
	if silentConnections != 1 || workingConnections != 1 {
		t.Fatalf("silent/working connections = %d/%d, want 1/1", silentConnections, workingConnections)
	}
}

func TestResponsesWebSocketIncompleteIsSuccessfulAndClearsChain(t *testing.T) {
	logs := make(chan CallLog, 2)
	server := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		if requestIndex == 0 {
			if err := conn.WriteJSON(map[string]any{"type": "response.output_text.delta", "delta": "PARTIAL"}); err != nil {
				t.Errorf("write incomplete delta: %v", err)
				return
			}
			if err := conn.WriteJSON(map[string]any{
				"type": "response.incomplete",
				"response": map[string]any{
					"id": "resp-incomplete", "status": "incomplete", "model": "gpt-5.6-luna",
					"incomplete_details": map[string]any{"reason": "max_output_tokens"},
					"usage":              map[string]any{"input_tokens": 10, "output_tokens": 4, "total_tokens": 14, "output_tokens_details": map[string]any{"reasoning_tokens": 3}},
				},
			}); err != nil {
				t.Errorf("write incomplete response: %v", err)
			}
			return
		}
		sendResponsesText(t, conn, "resp-after-incomplete", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_incomplete_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey, LogSink: func(entry CallLog) { logs <- entry }})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	first := []vpc.Message{{Role: "system", Content: "prompt"}, {Role: "user", Content: "first"}}
	var text strings.Builder
	result, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: first}, func(token string) { text.WriteString(token) })
	if err != nil || result.Interrupted || text.String() != "PARTIAL" {
		t.Fatalf("incomplete result/error/text = %#v / %v / %q", result, err, text.String())
	}
	select {
	case entry := <-logs:
		if !entry.Completed || entry.ResponseContent != "PARTIAL" || entry.FinishReason != "max_output_tokens" || entry.StatusCode != http.StatusOK || entry.ReasoningTokens != 3 {
			t.Fatalf("incomplete log = %#v", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for incomplete call log")
	}

	second := append(cloneRouterMessages(first), vpc.Message{Role: "assistant", Content: "PARTIAL"}, vpc.Message{Role: "user", Content: "again"})
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: second}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	requests, _, _, connections := server.snapshot()
	if connections != 1 || len(requests) != 2 {
		t.Fatalf("connections/requests = %d/%d, want 1/2", connections, len(requests))
	}
	if _, exists := requests[1]["previous_response_id"]; exists {
		t.Fatal("post-incomplete request unexpectedly used previous_response_id")
	}
}

func TestResponsesWebSocketFailedResponseLogsPartialTextAndAdvancesOnServerError(t *testing.T) {
	logs := make(chan CallLog, 2)
	failing := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		_ = conn.WriteJSON(map[string]any{"type": "response.output_text.delta", "delta": "HEARD"})
		_ = conn.WriteJSON(map[string]any{
			"type": "response.failed", "status": 500,
			"response": map[string]any{"status": "failed", "model": "gpt-5.6-luna", "error": map[string]any{"code": "server_error", "message": "failed"}},
		})
	})
	working := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-recovered", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_server_error_first"
		secondKey = "responses_ws_server_error_second"
		groupKey  = "responses_ws_server_error_group"
	)
	installResponsesTestEndpoint(t, firstKey, openAIResponsesTestEndpoint(failing.server.URL))
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(working.server.URL))
	installResponsesTestGroup(t, groupKey, firstKey, secondKey)
	client, err := NewResponsesWebSocket(Config{Group: groupKey, LogSink: func(entry CallLog) { logs <- entry }})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "first"}}}, func(string) {}); err == nil {
		t.Fatal("server failure unexpectedly succeeded")
	}
	select {
	case entry := <-logs:
		if entry.ResponseContent != "HEARD" || entry.StatusCode != 500 || entry.Completed {
			t.Fatalf("failed response log = %#v", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed-response log")
	}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "second"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	_, _, _, workingConnections := working.snapshot()
	if workingConnections != 1 {
		t.Fatalf("working endpoint connections = %d, want 1 after server-error advance", workingConnections)
	}
}

func TestResponsesWebSocketClientErrorKeepsConnectionAndCandidate(t *testing.T) {
	first := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		if requestIndex == 0 {
			_ = conn.WriteJSON(map[string]any{"type": "error", "status": 400, "code": "invalid_request_error", "message": "bad request"})
			return
		}
		sendResponsesText(t, conn, "resp-fixed", "OK")
	})
	second := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-unexpected", "WRONG")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_client_error_first"
		secondKey = "responses_ws_client_error_second"
		groupKey  = "responses_ws_client_error_group"
	)
	installResponsesTestEndpoint(t, firstKey, openAIResponsesTestEndpoint(first.server.URL))
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(second.server.URL))
	installResponsesTestGroup(t, groupKey, firstKey, secondKey)
	client, err := NewResponsesWebSocket(Config{Group: groupKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "bad"}}}, func(string) {}); err == nil {
		t.Fatal("client error unexpectedly succeeded")
	}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "fixed"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	_, _, _, firstConnections := first.snapshot()
	_, _, _, secondConnections := second.snapshot()
	if firstConnections != 1 || secondConnections != 0 {
		t.Fatalf("first/second connections = %d/%d, want 1/0", firstConnections, secondConnections)
	}
}

func TestResponsesWebSocketCompletedTurnInterruptRetainsSocketButClearsChain(t *testing.T) {
	server := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-"+string(rune('1'+requestIndex)), "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_playback_interrupt_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	first := []vpc.Message{{Role: "user", Content: "first"}}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: first}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	client.Interrupt()
	second := append(cloneRouterMessages(first), vpc.Message{Role: "assistant", Content: "O"}, vpc.Message{Role: "user", Content: "stop"})
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: second}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	requests, _, _, connections := server.snapshot()
	if connections != 1 || len(requests) != 2 {
		t.Fatalf("connections/requests = %d/%d, want retained socket 1/2", connections, len(requests))
	}
	if _, exists := requests[1]["previous_response_id"]; exists {
		t.Fatal("playback-interrupted turn unexpectedly retained response chain")
	}
}

func TestResponsesWebSocketDirectInterruptDoesNotRetryReusedSocket(t *testing.T) {
	secondRequestSeen := make(chan struct{}, 1)
	server := newResponsesWSTestServer(t, func(requestIndex, _ int, _ map[string]any, conn *websocket.Conn) {
		switch requestIndex {
		case 0:
			sendResponsesText(t, conn, "resp-first", "FIRST")
		case 1:
			secondRequestSeen <- struct{}{}
		default:
			sendResponsesText(t, conn, "resp-unexpected-retry", "RETRIED")
		}
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_direct_interrupt_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	first := []vpc.Message{{Role: "user", Content: "first"}}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: first}, func(string) {}); err != nil {
		t.Fatal(err)
	}
	second := append(cloneRouterMessages(first), vpc.Message{Role: "assistant", Content: "FIRST"}, vpc.Message{Role: "user", Content: "second"})
	type streamOutcome struct {
		result vpc.LLMResult
		err    error
	}
	done := make(chan streamOutcome, 1)
	go func() {
		result, streamErr := client.Stream(context.Background(), vpc.LLMRequest{Messages: second}, func(string) {})
		done <- streamOutcome{result: result, err: streamErr}
	}()
	select {
	case <-secondRequestSeen:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second request")
	}
	client.Interrupt()
	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, context.Canceled) || !outcome.result.Interrupted {
			t.Fatalf("direct interrupt outcome = %#v / %v, want cancellation", outcome.result, outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("direct interrupt did not release Stream")
	}
	requests, _, _, connections := server.snapshot()
	if connections != 1 || len(requests) != 2 {
		t.Fatalf("connections/requests = %d/%d, want no retry on direct interrupt", connections, len(requests))
	}
}

func TestIncrementalResponsesMessagesAlignmentCases(t *testing.T) {
	call1 := vpc.ToolCall{ID: "call-1", Type: "function", Function: vpc.ToolCallFunction{Name: "lookup", Arguments: ""}}
	call2 := vpc.ToolCall{ID: "call-2", Type: "function", Function: vpc.ToolCallFunction{Name: "lookup", Arguments: `{"key":"two"}`}}
	chain := responsesChainState{
		ResponseID: "resp-1",
		RequestMessages: []vpc.Message{
			{Role: "system", Content: "old prompt"},
			{Role: "user", Content: "lookup"},
		},
		Text:      "spoken text",
		ToolCalls: []vpc.ToolCall{call1, call2},
	}
	toolFirst := []vpc.Message{
		{Role: "system", Content: "new prompt"},
		{Role: "user", Content: "lookup"},
		{Role: "assistant", ToolCalls: []vpc.ToolCall{{ID: "call-1", Type: "function", Function: vpc.ToolCallFunction{Name: "lookup", Arguments: "{}"}}}},
		{Role: "tool", ToolCallID: "call-1", Content: "one"},
		{Role: "assistant", ToolCalls: []vpc.ToolCall{call2}},
		{Role: "tool", ToolCallID: "call-2", Content: "two"},
		{Role: "assistant", Content: "spoken text"},
		{Role: "user", Content: "next"},
	}
	incremental, ok := incrementalResponsesMessages(chain, toolFirst)
	if !ok || len(incremental) != 3 || incremental[0].Role != "tool" || incremental[1].Role != "tool" || incremental[2].Content != "next" {
		t.Fatalf("tool-first alignment = %#v / %v", incremental, ok)
	}

	whitespaceRewrite := cloneRouterMessages(toolFirst)
	whitespaceRewrite[6].Content = "spoken  text"
	if _, ok := incrementalResponsesMessages(chain, whitespaceRewrite); ok {
		t.Fatal("played-text whitespace rewrite unexpectedly aligned")
	}

	protocolRewrite := cloneRouterMessages(toolFirst)
	protocolRewrite[1] = vpc.Message{Role: "system", Content: "injected protocol"}
	if _, ok := incrementalResponsesMessages(chain, protocolRewrite); ok {
		t.Fatal("mid-history protocol rewrite unexpectedly aligned")
	}

	textOnlyChain := responsesChainState{
		ResponseID:      "resp-text",
		RequestMessages: []vpc.Message{{Role: "user", Content: "hello"}},
		Text:            "hi",
	}
	unchanged := []vpc.Message{{Role: "user", Content: "hello"}, {Role: "assistant", Content: "hi"}}
	if _, ok := incrementalResponsesMessages(textOnlyChain, unchanged); ok {
		t.Fatal("unchanged history unexpectedly produced empty incremental input")
	}
}

func TestResponsesWebSocketURLNormalizesAzurePaths(t *testing.T) {
	t.Setenv("RESPONSES_WS_AZURE_PATH_KEY", "secret")
	for _, tc := range []struct {
		name string
		base string
		want string
	}{
		{name: "root", base: "https://example.test", want: "wss://example.test/openai/v1/responses"},
		{name: "openai", base: "https://example.test/openai", want: "wss://example.test/openai/v1/responses"},
		{name: "v1", base: "https://example.test/openai/v1", want: "wss://example.test/openai/v1/responses"},
		{name: "responses", base: "https://example.test/openai/v1/responses?api-version=ignored", want: "wss://example.test/openai/v1/responses"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RESPONSES_WS_AZURE_PATH_ENDPOINT", tc.base)
			got, err := responsesWebSocketURL(endpointConfig{Provider: providerAzure, EndpointEnv: "RESPONSES_WS_AZURE_PATH_ENDPOINT"})
			if err != nil || got != tc.want {
				t.Fatalf("URL = %q / %v, want %q", got, err, tc.want)
			}
		})
	}
}

func TestResponsesWebSocketHandshakeErrorRedactsAzureSecret(t *testing.T) {
	const secret = "azure-super-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://example.test/openai/v1/responses?api-key="+secret)
		http.Error(w, "reflected api-key="+secret, http.StatusFound)
	}))
	defer server.Close()
	t.Setenv("RESPONSES_WS_REDACT_ENDPOINT", server.URL)
	t.Setenv("RESPONSES_WS_REDACT_KEY", secret)
	_, err := dialResponsesWebSocket(context.Background(), endpointConfig{
		Provider: providerAzure, EndpointEnv: "RESPONSES_WS_REDACT_ENDPOINT", APIKeyEnv: "RESPONSES_WS_REDACT_KEY",
	})
	if err == nil {
		t.Fatal("handshake unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "api-key=") {
		t.Fatalf("handshake error leaked Azure credential: %v", err)
	}
}

func TestResponsesWebSocketCloseIsIdempotentAndStreamReturnsCancellation(t *testing.T) {
	server := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-close", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_close_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "after close"}}}, func(string) {})
	if !errors.Is(err, context.Canceled) || !result.Interrupted {
		t.Fatalf("post-close result/error = %#v / %v, want interrupted context cancellation", result, err)
	}
}

func TestResponsesWebSocketDialLogsFirstConnectAndInsideTurnOffset(t *testing.T) {
	server := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-dial-log-first", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const endpointKey = "responses_ws_dial_log_first_test"
	installResponsesTestEndpoint(t, endpointKey, openAIResponsesTestEndpoint(server.server.URL))

	var logBuf bytes.Buffer
	client, err := NewResponsesWebSocket(Config{FixedEndpoint: endpointKey, Logger: log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hi"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	logged := logBuf.String()
	if !strings.Contains(logged, "Responses WebSocket dial connected") ||
		!strings.Contains(logged, "dial_seq=1") ||
		!strings.Contains(logged, "redial=false") ||
		!strings.Contains(logged, "drop_reason=first") {
		t.Fatalf("connected log missing expected first-dial fields: %q", logged)
	}
	if !strings.Contains(logged, "Responses WebSocket dial inside turn") || !strings.Contains(logged, "dial_seq=1") {
		t.Fatalf("inside-turn log missing expected fields: %q", logged)
	}
	if !regexp.MustCompile(`connect_offset_ms=\d+(\.\d+)?`).MatchString(logged) {
		t.Fatalf("inside-turn log missing well-formed connect_offset_ms: %q", logged)
	}
}

func TestResponsesWebSocketDialLogsRedialReasonAfterCancellation(t *testing.T) {
	silent := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, _ *websocket.Conn) {})
	working := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-dial-log-redial", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_dial_log_redial_first"
		secondKey = "responses_ws_dial_log_redial_second"
		groupKey  = "responses_ws_dial_log_redial_group"
	)
	installResponsesTestEndpoint(t, firstKey, openAIResponsesTestEndpoint(silent.server.URL))
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(working.server.URL))
	installResponsesTestGroup(t, groupKey, firstKey, secondKey)

	var logBuf bytes.Buffer
	client, err := NewResponsesWebSocket(Config{Group: groupKey, Logger: log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if result, err := client.Stream(ctx, vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hang"}}}, func(string) {}); err == nil || !result.Interrupted {
		t.Fatalf("silent result/error = %#v / %v, want interrupted timeout", result, err)
	}
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "retry"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	logged := logBuf.String()
	if !strings.Contains(logged, "dial_seq=2") || !strings.Contains(logged, "redial=true") || !strings.Contains(logged, "drop_reason=cancellation") {
		t.Fatalf("redial log missing expected fields for cancellation drop: %q", logged)
	}
}

func TestResponsesWebSocketDialLogsFailedAttemptsBeforeSuccess(t *testing.T) {
	server := newResponsesWSTestServer(t, func(_ int, _ int, _ map[string]any, conn *websocket.Conn) {
		sendResponsesText(t, conn, "resp-dial-log-failed-attempt", "OK")
	})
	t.Setenv("RESPONSES_WS_TEST_MISSING_KEY", "")
	t.Setenv("RESPONSES_WS_TEST_API_KEY", "secret")
	const (
		firstKey  = "responses_ws_dial_log_failed_first"
		secondKey = "responses_ws_dial_log_failed_second"
		groupKey  = "responses_ws_dial_log_failed_group"
	)
	installResponsesTestEndpoint(t, firstKey, endpointConfig{
		Provider: providerOpenAI, APIKeyEnv: "RESPONSES_WS_TEST_MISSING_KEY", BaseURL: server.server.URL + "/first/v1",
	})
	installResponsesTestEndpoint(t, secondKey, openAIResponsesTestEndpoint(server.server.URL))
	installResponsesTestGroup(t, groupKey, firstKey, secondKey)

	var logBuf bytes.Buffer
	client, err := NewResponsesWebSocket(Config{Group: groupKey, Logger: log.New(&logBuf, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: []vpc.Message{{Role: "user", Content: "hi"}}}, func(string) {}); err != nil {
		t.Fatal(err)
	}

	logged := logBuf.String()
	if !strings.Contains(logged, "Responses WebSocket dial failed cfg="+firstKey) {
		t.Fatalf("failed-attempt log missing for first candidate: %q", logged)
	}
	if !strings.Contains(logged, "Responses WebSocket dial connected") || !strings.Contains(logged, "failed_attempts=1") {
		t.Fatalf("connected log missing failed_attempts=1: %q", logged)
	}
}

// TestLiveGPT56LunaResponsesWebSocketClient is opt-in because it uses real
// credentials and billable endpoints. It exercises the production client (not
// the standalone wire probe), including a fresh-socket replay of tool history.
func TestLiveGPT56LunaResponsesWebSocketClient(t *testing.T) {
	if os.Getenv("VAGO_RUN_LUNA_WS_LIVE_TEST") != "1" {
		t.Skip("set VAGO_RUN_LUNA_WS_LIVE_TEST=1 to run")
	}
	client, err := NewResponsesWebSocket(Config{Group: GroupGPT56LunaNonReasoning, Region: "us"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	strict := true
	tool := vpc.ToolDefinition{
		Type: "function",
		Function: vpc.ToolFunction{
			Name: "lookup_probe_value", Description: "Return the probe value.", Strict: &strict,
			Parameters: map[string]any{
				"type": "object", "properties": map[string]any{"key": map[string]any{"type": "string"}},
				"required": []string{"key"}, "additionalProperties": false,
			},
		},
	}
	firstMessages := []vpc.Message{
		{Role: "system", Content: "Use lookup_probe_value for lookup requests. After its output, reply exactly with the returned value."},
		{Role: "user", Content: "Look up key vago."},
	}
	first, err := client.Stream(context.Background(), vpc.LLMRequest{
		Messages: firstMessages,
		Tools:    []vpc.ToolDefinition{tool},
		ToolChoice: map[string]any{
			"type": "function", "function": map[string]any{"name": "lookup_probe_value"},
		},
	}, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ToolCalls) != 1 || first.ToolCalls[0].Function.Name != "lookup_probe_value" {
		t.Fatalf("tool calls = %#v", first.ToolCalls)
	}

	// Force a fresh socket so the next request must manually replay the
	// function_call + function_call_output pair rather than use the cached ID.
	client.mu.Lock()
	activeConn := client.conn
	client.mu.Unlock()
	client.dropConnection(activeConn, false, "test_forced_fresh_socket")
	toolCall := first.ToolCalls[0]
	continuedMessages := append(cloneRouterMessages(firstMessages),
		vpc.Message{Role: "assistant", ToolCalls: []vpc.ToolCall{toolCall}},
		vpc.Message{Role: "tool", ToolCallID: toolCall.ID, Content: `{"value":"TOOL_OK"}`},
	)
	var replayed strings.Builder
	_, err = client.Stream(context.Background(), vpc.LLMRequest{
		Messages: continuedMessages,
	}, func(token string) { replayed.WriteString(token) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(replayed.String()), "TOOL_OK") {
		t.Fatalf("fresh-socket tool replay text = %q", replayed.String())
	}

	incrementalMessages := append(cloneRouterMessages(continuedMessages),
		vpc.Message{Role: "assistant", Content: replayed.String()},
		vpc.Message{Role: "user", Content: "Reply exactly AGAIN_OK."},
	)
	var incremental strings.Builder
	if _, err := client.Stream(context.Background(), vpc.LLMRequest{Messages: incrementalMessages}, func(token string) { incremental.WriteString(token) }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToUpper(incremental.String()), "AGAIN_OK") {
		t.Fatalf("incremental text = %q", incremental.String())
	}
}

func assertReasoningNone(t *testing.T, request map[string]any) {
	t.Helper()
	reasoning, ok := request["reasoning"].(map[string]any)
	if !ok || reasoning["effort"] != "none" {
		t.Fatalf("reasoning = %#v, want effort none", request["reasoning"])
	}
}

func requestInput(t *testing.T, request map[string]any) []map[string]any {
	t.Helper()
	raw, ok := request["input"].([]any)
	if !ok {
		t.Fatalf("input = %#v", request["input"])
	}
	items := make([]map[string]any, 0, len(raw))
	for _, value := range raw {
		item, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("input item = %#v", value)
		}
		items = append(items, item)
	}
	return items
}
