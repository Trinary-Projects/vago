// Command luna-ws-probe exercises GPT-5.6 Luna through the Responses
// WebSocket API without touching the live-call pipeline.
//
// It deliberately covers the state transitions the production client needs:
// completed-turn continuation, tool-call continuation, full-history rewrite,
// reconnect/resume, and interrupt/reconnect with only the observed assistant
// prefix replayed.
//
// Example:
//
//	go run ./cmd/luna-ws-probe --env-file ../disha-backend/.env
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultModel    = "gpt-5.6-luna"
	openAIWSURL     = "wss://api.openai.com/v1/responses"
	probeCodeWord   = "COBALT"
	rewriteCodeWord = "AMBER"
	toolProbeValue  = "TOOL_OK"
)

type probeTarget struct {
	Name        string
	Provider    string
	EndpointEnv string
	APIKeyEnv   string
}

var probeTargets = []probeTarget{
	{
		Name: "azure-eastus", Provider: "azure",
		EndpointEnv: "GROK_US_EAST_ENDPOINT", APIKeyEnv: "GROK_US_EAST_API_KEY",
	},
	{
		Name: "azure-eastus2", Provider: "azure",
		EndpointEnv: "GROK_US_EAST_2_ENDPOINT", APIKeyEnv: "GROK_US_EAST_2_API_KEY",
	},
	{
		Name: "azure-westus", Provider: "azure",
		EndpointEnv: "GROK_US_WEST_ENDPOINT", APIKeyEnv: "GROK_US_WEST_API_KEY",
	},
	{
		Name: "azure-northcentralus", Provider: "azure",
		EndpointEnv: "AZURE_OPENAI_US_NORTH_CENTRAL_ENDPOINT", APIKeyEnv: "AZURE_OPENAI_US_NORTH_CENTRAL_API_KEY",
	},
	{
		Name: "openai", Provider: "openai", APIKeyEnv: "OPENAI_API_KEY",
	},
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type usage struct {
	InputTokens         int `json:"input_tokens"`
	OutputTokens        int `json:"output_tokens"`
	TotalTokens         int `json:"total_tokens"`
	OutputTokensDetails struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

type outputContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type outputItem struct {
	Type      string          `json:"type"`
	CallID    string          `json:"call_id"`
	Name      string          `json:"name"`
	Arguments string          `json:"arguments"`
	Content   []outputContent `json:"content"`
}

type responseObject struct {
	ID                string       `json:"id"`
	Status            string       `json:"status"`
	Model             string       `json:"model"`
	Output            []outputItem `json:"output"`
	Usage             usage        `json:"usage"`
	Error             *apiError    `json:"error"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
}

type serverEvent struct {
	Type     string         `json:"type"`
	Delta    string         `json:"delta"`
	Code     string         `json:"code"`
	Message  string         `json:"message"`
	Error    *apiError      `json:"error"`
	Item     outputItem     `json:"item"`
	Response responseObject `json:"response"`
}

type responseResult struct {
	ID              string
	Status          string
	Model           string
	Text            string
	Output          []outputItem
	Usage           usage
	TTFB            time.Duration
	Total           time.Duration
	ReasoningTokens int
}

type probeRunner struct {
	model   string
	timeout time.Duration
}

func main() {
	envFile := flag.String("env-file", "", "optional dotenv file containing endpoint credentials")
	targetFlag := flag.String("target", "all", "all or a comma-separated target list")
	model := flag.String("model", defaultModel, "OpenAI model or Azure deployment name")
	timeout := flag.Duration("timeout", 45*time.Second, "per-response timeout")
	flag.Parse()

	if *envFile != "" {
		if err := loadEnvFile(*envFile); err != nil {
			fmt.Fprintln(os.Stderr, "load env file:", err)
			os.Exit(2)
		}
	}

	targets, err := selectedTargets(*targetFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	runner := probeRunner{model: *model, timeout: *timeout}
	failures := 0
	for _, target := range targets {
		fmt.Printf("\n[%s] starting Responses WebSocket probe\n", target.Name)
		if err := runner.runTarget(context.Background(), target); err != nil {
			failures++
			fmt.Printf("[%s] FAIL: %v\n", target.Name, err)
			continue
		}
		fmt.Printf("[%s] PASS: all scenarios\n", target.Name)
	}

	if failures > 0 {
		fmt.Printf("\nprobe failed for %d/%d targets\n", failures, len(targets))
		os.Exit(1)
	}
	fmt.Printf("\nprobe passed for %d/%d targets\n", len(targets), len(targets))
}

func (r probeRunner) runTarget(ctx context.Context, target probeTarget) error {
	conn, err := r.dial(ctx, target)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	fmt.Printf("[%s] connect PASS\n", target.Name)

	remember, err := r.request(conn, responseCreate(
		r.model,
		"Remember the code word in the user message. Reply exactly ACK.",
		[]any{message("user", "The code word is "+probeCodeWord+".")},
	))
	if err != nil {
		return fmt.Errorf("remember turn: %w", err)
	}
	if err := validateText(remember.Text, "ACK"); err != nil {
		return fmt.Errorf("remember turn: %w", err)
	}
	if err := validateNoReasoning(remember); err != nil {
		return fmt.Errorf("remember turn: %w", err)
	}
	printResult(target, "remember", remember)

	incrementalPayload := responseCreate(
		r.model,
		"Reply with only the remembered code word.",
		[]any{message("user", "What is the code word?")},
	)
	incrementalPayload["previous_response_id"] = remember.ID
	incremental, err := r.request(conn, incrementalPayload)
	if err != nil {
		return fmt.Errorf("incremental continuation: %w", err)
	}
	if err := validateText(incremental.Text, probeCodeWord); err != nil {
		return fmt.Errorf("incremental continuation: %w", err)
	}
	if err := validateNoReasoning(incremental); err != nil {
		return fmt.Errorf("incremental continuation: %w", err)
	}
	printResult(target, "incremental", incremental)

	toolPayload := responseCreate(
		r.model,
		"Always use the supplied function for the requested lookup.",
		[]any{message("user", "Look up the probe value for key vago.")},
	)
	toolPayload["tools"] = []any{map[string]any{
		"type":        "function",
		"name":        "lookup_probe_value",
		"description": "Return a deterministic probe value for a key.",
		"strict":      true,
		"parameters": map[string]any{
			"type":                 "object",
			"properties":           map[string]any{"key": map[string]any{"type": "string"}},
			"required":             []string{"key"},
			"additionalProperties": false,
		},
	}}
	toolPayload["tool_choice"] = map[string]any{"type": "function", "name": "lookup_probe_value"}
	toolResponse, err := r.request(conn, toolPayload)
	if err != nil {
		return fmt.Errorf("tool call: %w", err)
	}
	toolCall, err := findFunctionCall(toolResponse.Output, "lookup_probe_value")
	if err != nil {
		return fmt.Errorf("tool call: %w", err)
	}
	var toolArguments map[string]any
	if err := json.Unmarshal([]byte(toolCall.Arguments), &toolArguments); err != nil {
		return fmt.Errorf("tool call arguments: %w", err)
	}
	if toolArguments["key"] != "vago" {
		return fmt.Errorf("tool call arguments key = %v, want vago", toolArguments["key"])
	}
	if err := validateNoReasoning(toolResponse); err != nil {
		return fmt.Errorf("tool call: %w", err)
	}
	printResult(target, "tool-call", toolResponse)

	toolResultPayload := responseCreate(
		r.model,
		"Reply exactly with the value returned by the tool.",
		[]any{map[string]any{
			"type":    "function_call_output",
			"call_id": toolCall.CallID,
			"output":  `{"value":"` + toolProbeValue + `"}`,
		}},
	)
	toolResultPayload["previous_response_id"] = toolResponse.ID
	toolResult, err := r.request(conn, toolResultPayload)
	if err != nil {
		return fmt.Errorf("tool result continuation: %w", err)
	}
	if err := validateText(toolResult.Text, toolProbeValue); err != nil {
		return fmt.Errorf("tool result continuation: %w", err)
	}
	if err := validateNoReasoning(toolResult); err != nil {
		return fmt.Errorf("tool result continuation: %w", err)
	}
	printResult(target, "tool-result", toolResult)

	// Omit previous_response_id and replay a rewritten canonical history.
	// This must forget COBALT and answer from the replacement AMBER history.
	rewrite, err := r.request(conn, responseCreate(
		r.model,
		"Answer with only the code word established by this input history.",
		[]any{
			message("user", "The code word is "+rewriteCodeWord+"."),
			message("assistant", "ACK"),
			message("user", "What is the code word?"),
		},
	))
	if err != nil {
		return fmt.Errorf("history rewrite: %w", err)
	}
	if err := validateText(rewrite.Text, rewriteCodeWord); err != nil {
		return fmt.Errorf("history rewrite: %w", err)
	}
	if strings.Contains(strings.ToUpper(rewrite.Text), probeCodeWord) {
		return errors.New("history rewrite leaked the previous COBALT chain")
	}
	if err := validateNoReasoning(rewrite); err != nil {
		return fmt.Errorf("history rewrite: %w", err)
	}
	printResult(target, "rewrite", rewrite)

	// A call resume cannot depend on connection-local store=false state. Close
	// the socket and replay the canonical conversation on a fresh connection.
	_ = conn.Close()
	conn = nil
	conn, err = r.dial(ctx, target)
	if err != nil {
		return fmt.Errorf("resume reconnect: %w", err)
	}
	resume, err := r.request(conn, responseCreate(
		r.model,
		"This is a resumed call. Answer with only the established code word.",
		[]any{
			message("user", "The code word is "+rewriteCodeWord+"."),
			message("assistant", "ACK"),
			message("user", "What is the code word?"),
			message("assistant", strings.TrimSpace(rewrite.Text)),
			message("user", "After resuming, what is the code word?"),
		},
	))
	if err != nil {
		return fmt.Errorf("resume replay: %w", err)
	}
	if err := validateText(resume.Text, rewriteCodeWord); err != nil {
		return fmt.Errorf("resume replay: %w", err)
	}
	if err := validateNoReasoning(resume); err != nil {
		return fmt.Errorf("resume replay: %w", err)
	}
	printResult(target, "resume", resume)

	interruptPayload := responseCreate(
		r.model,
		"Start answering immediately and continue until the output limit.",
		[]any{message("user", "Repeat the word ALPHA separated by spaces many times.")},
	)
	interruptPayload["max_output_tokens"] = 512
	partial, interruptTTFB, err := r.interruptAfterFirstText(conn, interruptPayload)
	if err != nil {
		return fmt.Errorf("interrupt: %w", err)
	}
	fmt.Printf("[%s] interrupt PASS partial_chars=%d ttfb=%s\n", target.Name, len(partial), durationMS(interruptTTFB))
	conn = nil // interruptAfterFirstText closes it.

	conn, err = r.dial(ctx, target)
	if err != nil {
		return fmt.Errorf("post-interrupt reconnect: %w", err)
	}
	postInterrupt, err := r.request(conn, responseCreate(
		r.model,
		"The caller interrupted the assistant. Follow the latest user instruction exactly.",
		[]any{
			message("user", "Repeat the word ALPHA separated by spaces many times."),
			message("assistant", partial),
			message("user", "Stop. Reply exactly INTERRUPT_OK."),
		},
	))
	if err != nil {
		return fmt.Errorf("post-interrupt replay: %w", err)
	}
	if err := validateText(postInterrupt.Text, "INTERRUPT_OK"); err != nil {
		return fmt.Errorf("post-interrupt replay: %w", err)
	}
	if err := validateNoReasoning(postInterrupt); err != nil {
		return fmt.Errorf("post-interrupt replay: %w", err)
	}
	printResult(target, "post-interrupt", postInterrupt)

	return nil
}

func responseCreate(model, instructions string, input []any) map[string]any {
	return map[string]any{
		"type":              "response.create",
		"model":             model,
		"store":             false,
		"instructions":      instructions,
		"input":             input,
		"reasoning":         map[string]any{"effort": "none"},
		"max_output_tokens": 128,
	}
}

func message(role, content string) map[string]any {
	return map[string]any{"type": "message", "role": role, "content": content}
}

func (r probeRunner) request(conn *websocket.Conn, payload map[string]any) (responseResult, error) {
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return responseResult{}, err
	}
	if err := conn.WriteJSON(payload); err != nil {
		return responseResult{}, err
	}
	return readResponse(conn, r.timeout)
}

func readResponse(conn *websocket.Conn, timeout time.Duration) (responseResult, error) {
	started := time.Now()
	if err := conn.SetReadDeadline(started.Add(timeout)); err != nil {
		return responseResult{}, err
	}
	var result responseResult
	var text strings.Builder
	var completedItems []outputItem

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return result, err
		}
		var event serverEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return result, fmt.Errorf("decode event: %w", err)
		}

		switch event.Type {
		case "response.created", "response.in_progress":
			if result.ID == "" {
				result.ID = event.Response.ID
			}
		case "response.output_text.delta":
			if event.Delta != "" {
				if result.TTFB == 0 {
					result.TTFB = time.Since(started)
				}
				text.WriteString(event.Delta)
			}
		case "response.function_call_arguments.delta":
			if event.Delta != "" && result.TTFB == 0 {
				result.TTFB = time.Since(started)
			}
		case "response.output_item.done":
			completedItems = append(completedItems, event.Item)
		case "response.completed":
			result.ID = firstNonEmpty(event.Response.ID, result.ID)
			result.Status = event.Response.Status
			result.Model = event.Response.Model
			result.Output = event.Response.Output
			if len(result.Output) == 0 {
				result.Output = completedItems
			}
			result.Usage = event.Response.Usage
			result.ReasoningTokens = event.Response.Usage.OutputTokensDetails.ReasoningTokens
			result.Text = text.String()
			if result.Text == "" {
				result.Text = outputText(result.Output)
			}
			result.Total = time.Since(started)
			if result.ID == "" {
				return result, errors.New("completed response had no id")
			}
			return result, nil
		case "response.incomplete":
			reason := "unknown"
			if event.Response.IncompleteDetails != nil && event.Response.IncompleteDetails.Reason != "" {
				reason = event.Response.IncompleteDetails.Reason
			}
			return result, fmt.Errorf("response incomplete: %s", reason)
		case "response.failed":
			if event.Response.Error != nil {
				return result, fmt.Errorf("response failed: %s: %s", event.Response.Error.Code, event.Response.Error.Message)
			}
			return result, errors.New("response failed")
		case "error":
			if event.Error != nil {
				return result, fmt.Errorf("connection error: %s: %s", event.Error.Code, event.Error.Message)
			}
			return result, fmt.Errorf("connection error: %s: %s", event.Code, event.Message)
		}
	}
}

func (r probeRunner) interruptAfterFirstText(conn *websocket.Conn, payload map[string]any) (string, time.Duration, error) {
	started := time.Now()
	if err := conn.SetWriteDeadline(started.Add(10 * time.Second)); err != nil {
		return "", 0, err
	}
	if err := conn.WriteJSON(payload); err != nil {
		return "", 0, err
	}
	if err := conn.SetReadDeadline(started.Add(r.timeout)); err != nil {
		return "", 0, err
	}
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return "", 0, err
		}
		var event serverEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return "", 0, err
		}
		switch event.Type {
		case "response.output_text.delta":
			if strings.TrimSpace(event.Delta) != "" {
				ttfb := time.Since(started)
				_ = conn.Close()
				return event.Delta, ttfb, nil
			}
		case "response.failed", "response.incomplete", "response.completed", "error":
			return "", 0, fmt.Errorf("response became terminal before an interruptible text delta: %s", event.Type)
		}
	}
}

func (r probeRunner) dial(ctx context.Context, target probeTarget) (*websocket.Conn, error) {
	endpoint, err := target.websocketURL()
	if err != nil {
		return nil, err
	}
	apiKey := strings.TrimSpace(os.Getenv(target.APIKeyEnv))
	if apiKey == "" {
		return nil, fmt.Errorf("missing %s", target.APIKeyEnv)
	}

	headers := http.Header{}
	if target.Provider == "azure" {
		// Azure's WebSocket front door redirects a Bearer-authenticated
		// upgrade to the same URL with api-key in the query. Gorilla does not
		// follow WebSocket redirects, so send the observed wire shape on the
		// first upgrade and never expose the resulting URL in logs.
		parsed, parseErr := url.Parse(endpoint)
		if parseErr != nil {
			return nil, parseErr
		}
		query := parsed.Query()
		query.Set("api-key", apiKey)
		parsed.RawQuery = query.Encode()
		endpoint = parsed.String()
	} else {
		headers.Set("Authorization", "Bearer "+apiKey)
	}
	headers.Set("User-Agent", "vago-luna-ws-probe/1")
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, response, err := dialer.DialContext(ctx, endpoint, headers)
	if err == nil {
		return conn, nil
	}
	if response == nil {
		return nil, err
	}
	defer response.Body.Close()
	redirect := sanitizedRedirect(endpoint, response.Header.Get("Location"))
	// Do not print the response body: a provider is allowed to reflect request
	// details, and Azure authenticates this upgrade with an api-key query.
	return nil, fmt.Errorf("websocket handshake status %d%s: %w", response.StatusCode, redirect, err)
}

func sanitizedRedirect(from, raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return " redirect=<unparseable>"
	}
	fromURL, _ := url.Parse(from)
	hostKind := "other"
	switch {
	case strings.HasSuffix(parsed.Hostname(), ".services.ai.azure.com"):
		hostKind = "services.ai.azure.com"
	case strings.HasSuffix(parsed.Hostname(), ".openai.azure.com"):
		hostKind = "openai.azure.com"
	case parsed.Hostname() == "api.openai.com":
		hostKind = "api.openai.com"
	}
	queryKeys := make([]string, 0, len(parsed.Query()))
	for key := range parsed.Query() {
		queryKeys = append(queryKeys, key)
	}
	sort.Strings(queryKeys)
	return fmt.Sprintf(
		" redirect_scheme=%s redirect_host=%s same_host=%v redirect_path=%s query_keys=%v",
		parsed.Scheme,
		hostKind,
		fromURL != nil && strings.EqualFold(fromURL.Hostname(), parsed.Hostname()),
		parsed.EscapedPath(),
		queryKeys,
	)
}

func (t probeTarget) websocketURL() (string, error) {
	if t.Provider == "openai" {
		return openAIWSURL, nil
	}
	raw := strings.TrimSpace(os.Getenv(t.EndpointEnv))
	if raw == "" {
		return "", fmt.Errorf("missing %s", t.EndpointEnv)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		// url.Error can embed the raw endpoint. Keep probe errors credential-safe
		// because Azure endpoints may already contain query authentication.
		return "", fmt.Errorf("invalid endpoint in %s", t.EndpointEnv)
	}
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	case "wss", "ws":
	default:
		return "", fmt.Errorf("unsupported scheme in %s", t.EndpointEnv)
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	path := strings.TrimRight(parsed.Path, "/")
	switch {
	case strings.HasSuffix(path, "/openai/v1/responses"):
	case strings.HasSuffix(path, "/openai/v1"):
		path += "/responses"
	case strings.HasSuffix(path, "/openai"):
		path += "/v1/responses"
	default:
		path += "/openai/v1/responses"
	}
	parsed.Path = path
	return parsed.String(), nil
}

func findFunctionCall(items []outputItem, name string) (outputItem, error) {
	for _, item := range items {
		if item.Type == "function_call" && item.Name == name && item.CallID != "" {
			return item, nil
		}
	}
	return outputItem{}, fmt.Errorf("function call %q not found in %d output items", name, len(items))
}

func outputText(items []outputItem) string {
	var text strings.Builder
	for _, item := range items {
		for _, content := range item.Content {
			if content.Type == "output_text" {
				text.WriteString(content.Text)
			}
		}
	}
	return text.String()
}

func validateText(got, want string) error {
	if !strings.Contains(strings.ToUpper(strings.TrimSpace(got)), strings.ToUpper(want)) {
		return fmt.Errorf("text %q does not contain %q", compact(got), want)
	}
	return nil
}

func validateNoReasoning(result responseResult) error {
	if result.ReasoningTokens != 0 {
		return fmt.Errorf("reasoning tokens = %d, want 0", result.ReasoningTokens)
	}
	return nil
}

func printResult(target probeTarget, stage string, result responseResult) {
	fmt.Printf(
		"[%s] %-14s PASS ttfb=%s total=%s tokens=%d/%d reasoning=%d\n",
		target.Name,
		stage,
		durationMS(result.TTFB),
		durationMS(result.Total),
		result.Usage.InputTokens,
		result.Usage.OutputTokens,
		result.ReasoningTokens,
	)
}

func durationMS(value time.Duration) string {
	if value == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1fms", float64(value.Microseconds())/1000)
}

func compact(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > 120 {
		return value[:120] + "..."
	}
	return value
}

func selectedTargets(raw string) ([]probeTarget, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "all" {
		return append([]probeTarget(nil), probeTargets...), nil
	}
	wanted := map[string]bool{}
	for _, item := range strings.Split(raw, ",") {
		if name := strings.TrimSpace(item); name != "" {
			wanted[name] = true
		}
	}
	var selected []probeTarget
	for _, target := range probeTargets {
		if wanted[target.Name] {
			selected = append(selected, target)
			delete(wanted, target.Name)
		}
	}
	if len(wanted) > 0 {
		unknown := make([]string, 0, len(wanted))
		for name := range wanted {
			unknown = append(unknown, name)
		}
		sort.Strings(unknown)
		return nil, fmt.Errorf("unknown targets %v; valid targets are %v", unknown, targetNames())
	}
	if len(selected) == 0 {
		return nil, errors.New("no targets selected")
	}
	return selected, nil
}

func targetNames() []string {
	names := make([]string, 0, len(probeTargets))
	for _, target := range probeTargets {
		names = append(names, target.Name)
	}
	return names
}

func loadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		value := strings.TrimSpace(parts[1])
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
