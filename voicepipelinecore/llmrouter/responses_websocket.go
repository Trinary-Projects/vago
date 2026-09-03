package llmrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	responsesWebSocketDialTimeout  = 2 * time.Second
	responsesWebSocketWriteTimeout = 2 * time.Second
	responsesWebSocketEventTimeout = 4 * time.Second
	openAIResponsesWebSocketURL    = "wss://api.openai.com/v1/responses"
)

var errResponsesWebSocketClosed = errors.New("llmrouter: Responses WebSocket client is closed")

// ResponsesWebSocketClient is a call-scoped LLM client for OpenAI's Responses
// WebSocket API. It owns one socket at a time, keeps completed response-chain
// state private, and sends only incremental input when Vago's canonical
// history still matches the preceding response. Rewrites and replayed resumes
// automatically start a new chain from the full canonical history.
//
// Stream calls are serialized because one call uses the implicit default lane.
// Interrupt may run concurrently: it always clears the chain, closes the socket
// while generation is active, and otherwise retains the idle socket so the next
// turn can replay canonical history without an unnecessary reconnect.
type ResponsesWebSocketClient struct {
	cfg        Config
	candidates []endpointConfig
	turnMu     sync.Mutex

	mu                sync.Mutex
	conn              *websocket.Conn
	activeIndex       int
	nextIndex         int
	closed            bool
	streamActive      bool
	streamSawOutput   bool
	streamInterrupted bool
	chain             responsesChainState
	promptMetadata    map[string]any
}

var _ Client = (*ResponsesWebSocketClient)(nil)
var _ interface{ Interrupt() } = (*ResponsesWebSocketClient)(nil)
var _ interface{ Close() error } = (*ResponsesWebSocketClient)(nil)

type responsesChainState struct {
	ResponseID      string
	RequestMessages []vpc.Message
	Text            string
	ToolCalls       []vpc.ToolCall
}

type responsesAPIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"status"`
}

type responsesUsage struct {
	InputTokens         int `json:"input_tokens"`
	OutputTokens        int `json:"output_tokens"`
	TotalTokens         int `json:"total_tokens"`
	OutputTokensDetails struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

type responsesOutputContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type responsesOutputItem struct {
	Type      string                   `json:"type"`
	CallID    string                   `json:"call_id"`
	Name      string                   `json:"name"`
	Arguments string                   `json:"arguments"`
	Content   []responsesOutputContent `json:"content"`
}

type responsesObject struct {
	ID                string                `json:"id"`
	Status            string                `json:"status"`
	Model             string                `json:"model"`
	Output            []responsesOutputItem `json:"output"`
	Usage             responsesUsage        `json:"usage"`
	Error             *responsesAPIError    `json:"error"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
}

type responsesServerEvent struct {
	Type     string              `json:"type"`
	Delta    string              `json:"delta"`
	Code     string              `json:"code"`
	Message  string              `json:"message"`
	Status   int                 `json:"status"`
	Error    *responsesAPIError  `json:"error"`
	Item     responsesOutputItem `json:"item"`
	Response responsesObject     `json:"response"`
}

type responsesStreamResult struct {
	ID           string
	Model        string
	Text         string
	ToolCalls    []vpc.ToolCall
	Usage        responsesUsage
	TTFB         time.Duration
	Total        time.Duration
	FinishReason string
	ErrorCode    string
	SawOutput    bool
	SawResponse  bool
	Incomplete   bool
	TransportErr bool
	StatusCode   int
}

// NewResponsesWebSocket builds the dedicated client for a Responses
// WebSocket group or fixed endpoint. Unlike Router, Redis is not required:
// these endpoint lists are ordered Disha failover lists and are deliberately
// outside the Python poller's health-ranked modelGroups registry.
func NewResponsesWebSocket(cfg Config) (*ResponsesWebSocketClient, error) {
	candidates, err := responsesCandidates(cfg)
	if err != nil {
		return nil, err
	}
	return &ResponsesWebSocketClient{
		cfg:            cfg,
		candidates:     candidates,
		activeIndex:    -1,
		promptMetadata: cfg.PromptMetadata,
	}, nil
}

func responsesCandidates(cfg Config) ([]endpointConfig, error) {
	if cfg.FixedEndpoint != "" {
		endpoint, ok := endpointConfigs[cfg.FixedEndpoint]
		if !ok {
			return nil, fmt.Errorf("llmrouter: unknown fixed endpoint %q", cfg.FixedEndpoint)
		}
		if endpoint.APIMode != apiModeResponsesWebSocket {
			return nil, fmt.Errorf("llmrouter: endpoint %q does not use Responses WebSocket", cfg.FixedEndpoint)
		}
		return []endpointConfig{endpoint}, nil
	}
	if cfg.Group == "" {
		return nil, errors.New("llmrouter: Config.Group is required")
	}
	group, ok := responsesWebSocketGroups[cfg.Group]
	if !ok {
		if _, chatGroup := modelGroups[cfg.Group]; chatGroup {
			return nil, fmt.Errorf("llmrouter: model group %q does not use Responses WebSocket", cfg.Group)
		}
		return nil, fmt.Errorf("llmrouter: unknown model group %q", cfg.Group)
	}
	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}
	candidates := make([]endpointConfig, 0, len(group.Configs))
	for _, key := range group.Configs {
		endpoint, exists := endpointConfigs[key]
		if !exists || endpoint.APIMode != apiModeResponsesWebSocket {
			continue
		}
		if region != "" && endpoint.Region != region {
			continue
		}
		candidates = append(candidates, endpoint)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("llmrouter: no Responses WebSocket endpoints for group %q in region %q", cfg.Group, region)
	}
	return candidates, nil
}

// SetPromptMetadata replaces the metadata attached to subsequent call logs.
func (c *ResponsesWebSocketClient) SetPromptMetadata(metadata map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.promptMetadata = metadata
}

func (c *ResponsesWebSocketClient) currentPromptMetadata() map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.promptMetadata
}

// Stream sends one Responses request. A completed, exactly aligned prior turn
// uses previous_response_id plus only new user/tool items. Any history change
// (including a partial/filtered assistant turn) omits previous_response_id and
// sends the full canonical history instead.
func (c *ResponsesWebSocketClient) Stream(ctx context.Context, llmReq vpc.LLMRequest, onToken func(string)) (res vpc.LLMResult, err error) {
	c.turnMu.Lock()
	defer c.turnMu.Unlock()

	if ctx.Err() != nil {
		return vpc.LLMResult{Interrupted: true}, ctx.Err()
	}
	if !c.beginStream() {
		return vpc.LLMResult{Interrupted: true}, context.Canceled
	}
	defer c.endStream()

	var (
		servedConfig      endpointConfig
		streamResult      responsesStreamResult
		statusCode        int
		responseInputMode string
	)
	started := time.Now()

	defer func() {
		if servedConfig.Model != "" && res.Model == "" {
			res.Model = servedConfig.Model
		}
		if c.cfg.LogSink == nil {
			return
		}
		entry := CallLog{
			Model:             res.Model,
			ConfigKey:         servedConfig.Key,
			Deployment:        deploymentName(servedConfig),
			Request:           llmReq,
			ResponseContent:   streamResult.Text,
			ToolCalls:         res.ToolCalls,
			PromptMetadata:    c.currentPromptMetadata(),
			PromptTokens:      streamResult.Usage.InputTokens,
			CompletionTokens:  streamResult.Usage.OutputTokens,
			TTFBMs:            msFromDuration(res.TTFB),
			TotalMs:           msFromDuration(res.Total),
			StatusCode:        statusCode,
			Completed:         err == nil && !res.Interrupted,
			Interrupted:       res.Interrupted,
			FinishReason:      streamResult.FinishReason,
			SelectedGroup:     c.cfg.Group,
			ResponseInputMode: responseInputMode,
			ReasoningTokens:   streamResult.Usage.OutputTokensDetails.ReasoningTokens,
		}
		if servedConfig.Key != "" {
			entry.UsingFallback = c.candidateIndex(servedConfig.Key) > 0
		}
		if err != nil {
			entry.ErrorMessage = err.Error()
			entry.ErrorType = classifyError(statusCode, err.Error())
		}
		sink := c.cfg.LogSink
		go func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					c.logf("log sink panic: %v", recovered)
				}
			}()
			sink(entry)
		}()
	}()

	conn, endpoint, reused, dialErr := c.ensureConnection(ctx)
	servedConfig = endpoint
	if dialErr != nil {
		res.Model = firstCandidateModel(c.candidates)
		res.Total = time.Since(started)
		res.Interrupted = ctx.Err() != nil || errors.Is(dialErr, errResponsesWebSocketClosed)
		if errors.Is(dialErr, errResponsesWebSocketClosed) {
			return res, context.Canceled
		}
		return res, dialErr
	}
	res.Model = endpoint.Model

	retriedStaleSocket := false
	retriedEvictedChain := false
	for {
		payload, previous := c.buildResponseCreate(endpoint, llmReq)
		if previous == "" {
			responseInputMode = "full"
		} else {
			responseInputMode = "incremental"
		}
		requestOffset := time.Since(started)
		streamResult, err = c.request(ctx, conn, payload, onToken)
		if streamResult.TTFB > 0 {
			res.TTFB = requestOffset + streamResult.TTFB
		}

		// A reused connection can be closed by the provider or an intermediary
		// while no read loop is active between turns. If it fails before the
		// server acknowledges this request, no output or tool can have escaped;
		// reconnect once and replay the canonical full history.
		if err != nil && reused && !retriedStaleSocket && streamResult.TransportErr && !streamResult.SawResponse && !streamResult.SawOutput && ctx.Err() == nil && !c.wasStreamInterrupted() {
			retriedStaleSocket = true
			c.dropConnection(conn, false)
			conn, endpoint, reused, dialErr = c.ensureConnection(ctx)
			servedConfig = endpoint
			if dialErr != nil {
				err = dialErr
				break
			}
			res.Model = endpoint.Model
			continue
		}

		// store=false response IDs are connection-local. Reconnect before the
		// one full-history retry so no late event from the rejected request can
		// be mistaken for the replacement request.
		if err != nil && previous != "" && !retriedEvictedChain && !streamResult.SawOutput && streamResult.ErrorCode == "previous_response_not_found" && ctx.Err() == nil {
			retriedEvictedChain = true
			c.dropConnection(conn, false)
			conn, endpoint, reused, dialErr = c.ensureConnection(ctx)
			servedConfig = endpoint
			if dialErr != nil {
				err = dialErr
				break
			}
			res.Model = endpoint.Model
			continue
		}
		break
	}

	res.Total = time.Since(started)
	res.ToolCalls = streamResult.ToolCalls
	res.Interrupted = ctx.Err() != nil || c.wasStreamInterrupted()
	if res.Interrupted {
		// The response-timeout processor retries the same canonical turn. If
		// this endpoint produced nothing before cancellation, advance so that
		// retry cannot loop forever on a silent candidate. A real barge-in has
		// already observed text/tool output and reconnects to the same endpoint.
		c.dropConnection(conn, !streamResult.SawOutput)
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		return res, context.Canceled
	}
	if err != nil {
		statusCode = streamResult.StatusCode
		if errors.Is(err, errResponsesWebSocketClosed) {
			res.Interrupted = true
			return res, context.Canceled
		}
		if streamResult.TransportErr || responsesFailureIsRetryable(streamResult.StatusCode, streamResult.ErrorCode) {
			c.dropConnection(conn, true)
		}
		return res, err
	}
	if streamResult.Model != "" {
		res.Model = streamResult.Model
	}
	statusCode = http.StatusOK
	if reasoningTokens := streamResult.Usage.OutputTokensDetails.ReasoningTokens; reasoningTokens > 0 {
		c.logf("reasoning.effort=none returned reasoning_tokens=%d cfg=%s", reasoningTokens, servedConfig.Key)
	}
	if streamResult.Incomplete {
		c.clearChain()
	} else {
		c.rememberCompleted(conn, llmReq.Messages, streamResult)
	}
	return res, nil
}

func (c *ResponsesWebSocketClient) beginStream() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.streamActive = true
	c.streamSawOutput = false
	c.streamInterrupted = false
	return true
}

func (c *ResponsesWebSocketClient) endStream() {
	c.mu.Lock()
	c.streamActive = false
	c.streamSawOutput = false
	c.streamInterrupted = false
	c.mu.Unlock()
}

func firstCandidateModel(candidates []endpointConfig) string {
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0].Model
}

func (c *ResponsesWebSocketClient) candidateIndex(key string) int {
	for index, endpoint := range c.candidates {
		if endpoint.Key == key {
			return index
		}
	}
	return -1
}

func (c *ResponsesWebSocketClient) logf(format string, args ...any) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Printf("llmrouter: responses websocket: "+format, args...)
	}
}

func (c *ResponsesWebSocketClient) wasStreamInterrupted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streamInterrupted
}

func (c *ResponsesWebSocketClient) ensureConnection(ctx context.Context) (*websocket.Conn, endpointConfig, bool, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, endpointConfig{}, false, errResponsesWebSocketClosed
	}
	if c.conn != nil && c.activeIndex >= 0 && c.activeIndex < len(c.candidates) {
		conn := c.conn
		endpoint := c.candidates[c.activeIndex]
		c.mu.Unlock()
		return conn, endpoint, true, nil
	}
	startIndex := c.nextIndex
	c.mu.Unlock()

	var dialErrors []error
	for offset := 0; offset < len(c.candidates); offset++ {
		index := (startIndex + offset) % len(c.candidates)
		endpoint := c.candidates[index]
		dialCtx, cancel := context.WithTimeout(ctx, responsesWebSocketDialTimeout)
		conn, err := dialResponsesWebSocket(dialCtx, endpoint)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				c.setNextCandidate(index + 1)
				return nil, endpoint, false, ctx.Err()
			}
			c.logf("connect failed cfg=%s: %v", endpoint.Key, err)
			dialErrors = append(dialErrors, fmt.Errorf("%s: %w", endpoint.Key, err))
			continue
		}

		c.mu.Lock()
		if c.closed || ctx.Err() != nil {
			c.mu.Unlock()
			_ = conn.Close()
			if ctx.Err() != nil {
				c.setNextCandidate(index + 1)
				return nil, endpoint, false, ctx.Err()
			}
			return nil, endpoint, false, errResponsesWebSocketClosed
		}
		c.conn = conn
		c.activeIndex = index
		c.nextIndex = index
		c.chain = responsesChainState{}
		c.mu.Unlock()
		c.logf("connected cfg=%s model=%s", endpoint.Key, endpoint.Model)
		return conn, endpoint, false, nil
	}

	c.mu.Lock()
	if len(c.candidates) > 0 {
		c.nextIndex = (startIndex + 1) % len(c.candidates)
	}
	c.mu.Unlock()
	return nil, endpointConfig{}, false, fmt.Errorf("llmrouter: all Responses WebSocket endpoints failed: %w", errors.Join(dialErrors...))
}

func (c *ResponsesWebSocketClient) setNextCandidate(index int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.candidates) == 0 {
		return
	}
	c.nextIndex = index % len(c.candidates)
}

func dialResponsesWebSocket(ctx context.Context, endpoint endpointConfig) (*websocket.Conn, error) {
	address, err := responsesWebSocketURL(endpoint)
	if err != nil {
		return nil, err
	}
	apiKey := strings.TrimSpace(os.Getenv(endpoint.APIKeyEnv))
	if apiKey == "" {
		return nil, fmt.Errorf("missing API key env %s", endpoint.APIKeyEnv)
	}

	headers := http.Header{}
	if endpoint.Provider == providerAzure {
		parsed, parseErr := url.Parse(address)
		if parseErr != nil {
			return nil, errors.New("invalid Azure Responses WebSocket URL")
		}
		query := parsed.Query()
		query.Set("api-key", apiKey)
		parsed.RawQuery = query.Encode()
		address = parsed.String()
	} else {
		headers.Set("Authorization", "Bearer "+apiKey)
	}
	headers.Set("User-Agent", "vago-responses-websocket/1")
	dialer := websocket.Dialer{HandshakeTimeout: responsesWebSocketDialTimeout}
	conn, response, dialErr := dialer.DialContext(ctx, address, headers)
	if dialErr == nil {
		return conn, nil
	}
	if response == nil {
		return nil, fmt.Errorf("websocket handshake failed: %w", dialErr)
	}
	defer response.Body.Close()
	// Do not include the body or Location header: Azure may reflect its
	// query-authenticated redirect URL, which would expose the API key.
	return nil, fmt.Errorf("websocket handshake status %d: %w", response.StatusCode, dialErr)
}

func responsesWebSocketURL(endpoint endpointConfig) (string, error) {
	if endpoint.Provider != providerAzure && endpoint.Provider != providerOpenAI {
		return "", fmt.Errorf("unsupported Responses WebSocket provider %q", endpoint.Provider)
	}
	raw := endpoint.BaseURL
	if endpoint.Provider == providerAzure {
		raw = strings.TrimSpace(os.Getenv(endpoint.EndpointEnv))
		if raw == "" {
			return "", fmt.Errorf("missing endpoint env %s", endpoint.EndpointEnv)
		}
	} else if strings.TrimSpace(raw) == "" {
		return openAIResponsesWebSocketURL, nil
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", errors.New("invalid Responses WebSocket endpoint")
	}
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	case "wss", "ws":
	default:
		return "", errors.New("unsupported Responses WebSocket endpoint scheme")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	path := strings.TrimRight(parsed.Path, "/")
	if endpoint.Provider == providerAzure {
		switch {
		case strings.HasSuffix(path, "/openai/v1/responses"):
		case strings.HasSuffix(path, "/openai/v1"):
			path += "/responses"
		case strings.HasSuffix(path, "/openai"):
			path += "/v1/responses"
		default:
			path += "/openai/v1/responses"
		}
	} else if !strings.HasSuffix(path, "/responses") {
		path += "/responses"
	}
	parsed.Path = path
	return parsed.String(), nil
}

func (c *ResponsesWebSocketClient) buildResponseCreate(endpoint endpointConfig, req vpc.LLMRequest) (map[string]any, string) {
	instructions := responsesInstructions(req.Messages)
	input := responsesInput(req.Messages, true)
	previousID := ""

	c.mu.Lock()
	chain := c.chain
	c.mu.Unlock()
	if chain.ResponseID != "" {
		if incremental, ok := incrementalResponsesMessages(chain, req.Messages); ok {
			input = responsesInput(incremental, false)
			previousID = chain.ResponseID
		}
	}

	payload := map[string]any{
		"type":         "response.create",
		"model":        endpoint.Model,
		"store":        false,
		"instructions": instructions,
		"input":        input,
	}
	// GPT-5.6 Luna accepts only its default temperature. Config.Temperature is
	// intentionally ignored by this transport; reasoning.effort controls the
	// requested non-reasoning mode independently.
	if endpoint.ReasoningEffort != "" {
		payload["reasoning"] = map[string]any{"effort": endpoint.ReasoningEffort}
	}
	if maxTokens := c.maxTokensFor(endpoint); maxTokens != nil {
		payload["max_output_tokens"] = *maxTokens
	}
	if len(req.Tools) > 0 {
		payload["tools"] = responsesTools(req.Tools)
	}
	if req.ToolChoice != nil {
		payload["tool_choice"] = responsesToolChoice(req.ToolChoice)
	}
	if previousID != "" {
		payload["previous_response_id"] = previousID
	}
	return payload, previousID
}

func (c *ResponsesWebSocketClient) maxTokensFor(endpoint endpointConfig) *int {
	if c.cfg.MaxTokens != nil {
		return c.cfg.MaxTokens
	}
	return endpoint.MaxTokens
}

func responsesInstructions(messages []vpc.Message) string {
	if len(messages) > 0 && messages[0].Role == "system" {
		return messages[0].Content
	}
	return ""
}

func responsesInput(messages []vpc.Message, omitLeadingSystem bool) []any {
	input := make([]any, 0, len(messages))
	for index, message := range messages {
		if omitLeadingSystem && index == 0 && message.Role == "system" {
			continue
		}
		if message.Content != "" && message.Role != "tool" {
			input = append(input, map[string]any{
				"type":    "message",
				"role":    message.Role,
				"content": message.Content,
			})
		}
		for _, call := range message.ToolCalls {
			input = append(input, map[string]any{
				"type":      "function_call",
				"call_id":   call.ID,
				"name":      call.Function.Name,
				"arguments": call.Function.Arguments,
			})
		}
		if message.Role == "tool" {
			input = append(input, map[string]any{
				"type":    "function_call_output",
				"call_id": message.ToolCallID,
				"output":  message.Content,
			})
		}
	}
	return input
}

func responsesTools(tools []vpc.ToolDefinition) []any {
	out := make([]any, 0, len(tools))
	for _, tool := range tools {
		item := map[string]any{
			"type":        "function",
			"name":        tool.Function.Name,
			"description": tool.Function.Description,
			"parameters":  tool.Function.Parameters,
		}
		if tool.Function.Strict != nil {
			item["strict"] = *tool.Function.Strict
		}
		out = append(out, item)
	}
	return out
}

func responsesToolChoice(choice any) any {
	mapping, ok := choice.(map[string]any)
	if !ok {
		return choice
	}
	function, nested := mapping["function"].(map[string]any)
	if !nested {
		return choice
	}
	name, _ := function["name"].(string)
	if name == "" {
		return choice
	}
	return map[string]any{"type": "function", "name": name}
}

func incrementalResponsesMessages(chain responsesChainState, current []vpc.Message) ([]vpc.Message, bool) {
	chainRequest := withoutLeadingSystem(chain.RequestMessages)
	currentHistory := withoutLeadingSystem(current)
	if len(currentHistory) < len(chainRequest) || !responsesMessagesEqual(currentHistory[:len(chainRequest)], chainRequest) {
		return nil, false
	}
	rest := currentHistory[len(chainRequest):]
	if incremental, ok := matchResponsesResult(rest, chain, true); ok {
		return nonEmptyIncremental(incremental)
	}
	incremental, ok := matchResponsesResult(rest, chain, false)
	if !ok {
		return nil, false
	}
	return nonEmptyIncremental(incremental)
}

func nonEmptyIncremental(messages []vpc.Message) ([]vpc.Message, bool) {
	if len(messages) == 0 {
		return nil, false
	}
	return messages, true
}

func withoutLeadingSystem(messages []vpc.Message) []vpc.Message {
	if len(messages) > 0 && messages[0].Role == "system" {
		return messages[1:]
	}
	return messages
}

// matchResponsesResult accepts both response commit orders Vago can expose:
// natural text before tool pairs, and tool pairs before text when tool frames
// reach the upstream context before playback commits the spoken words.
func matchResponsesResult(rest []vpc.Message, chain responsesChainState, textFirst bool) ([]vpc.Message, bool) {
	index := 0
	if textFirst && chain.Text != "" {
		if !canonicalAssistantText(rest, index, chain.Text) {
			return nil, false
		}
		index++
	}
	incremental := make([]vpc.Message, 0, len(rest))
	for _, call := range chain.ToolCalls {
		if index >= len(rest) || !canonicalToolCallMessage(rest[index], call) {
			return nil, false
		}
		index++
		if index >= len(rest) || rest[index].Role != "tool" || rest[index].ToolCallID != call.ID {
			return nil, false
		}
		incremental = append(incremental, rest[index])
		index++
	}
	if !textFirst && chain.Text != "" {
		if !canonicalAssistantText(rest, index, chain.Text) {
			return nil, false
		}
		index++
	}
	incremental = append(incremental, rest[index:]...)
	return incremental, true
}

func canonicalAssistantText(messages []vpc.Message, index int, text string) bool {
	return index < len(messages) && messages[index].Role == "assistant" && messages[index].Content == text && len(messages[index].ToolCalls) == 0
}

func canonicalToolCallMessage(message vpc.Message, call vpc.ToolCall) bool {
	return message.Role == "assistant" && message.Content == "" && len(message.ToolCalls) == 1 && responsesToolCallEqual(message.ToolCalls[0], call)
}

func responsesMessagesEqual(left, right []vpc.Message) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Role != right[index].Role || left[index].Content != right[index].Content || left[index].ToolCallID != right[index].ToolCallID {
			return false
		}
		if len(left[index].ToolCalls) != len(right[index].ToolCalls) {
			return false
		}
		for callIndex := range left[index].ToolCalls {
			if !responsesToolCallEqual(left[index].ToolCalls[callIndex], right[index].ToolCalls[callIndex]) {
				return false
			}
		}
	}
	return true
}

func responsesToolCallEqual(left, right vpc.ToolCall) bool {
	return left.ID == right.ID && left.Type == right.Type && left.Function.Name == right.Function.Name && responsesArgumentsEqual(left.Function.Arguments, right.Function.Arguments)
}

func responsesArgumentsEqual(left, right string) bool {
	if left == right {
		return true
	}
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	return (left == "" && right == "{}") || (left == "{}" && right == "")
}

func (c *ResponsesWebSocketClient) request(ctx context.Context, conn *websocket.Conn, payload map[string]any, onToken func(string)) (result responsesStreamResult, err error) {
	started := time.Now()
	if err := conn.SetWriteDeadline(started.Add(responsesWebSocketWriteTimeout)); err != nil {
		return responsesStreamResult{TransportErr: true}, err
	}
	if err := conn.WriteJSON(payload); err != nil {
		return responsesStreamResult{Total: time.Since(started), TransportErr: true}, err
	}
	_ = conn.SetWriteDeadline(time.Time{})

	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c.dropConnectionForCancellation(conn)
		case <-watchDone:
		}
	}()
	defer close(watchDone)

	var text strings.Builder
	var completedItems []responsesOutputItem
	defer func() {
		_ = conn.SetReadDeadline(time.Time{})
		if result.Text == "" {
			result.Text = text.String()
		}
		if len(result.ToolCalls) == 0 && len(completedItems) > 0 {
			result.ToolCalls = responsesToolCalls(completedItems)
		}
		if result.Total == 0 {
			result.Total = time.Since(started)
		}
	}()
	for {
		if deadlineErr := conn.SetReadDeadline(time.Now().Add(responsesWebSocketEventTimeout)); deadlineErr != nil {
			result.TransportErr = true
			return result, deadlineErr
		}
		_, raw, readErr := conn.ReadMessage()
		if readErr != nil {
			result.Total = time.Since(started)
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			result.TransportErr = true
			return result, readErr
		}
		var envelope struct {
			Type string `json:"type"`
		}
		if decodeErr := json.Unmarshal(raw, &envelope); decodeErr != nil || envelope.Type == "" {
			c.logf("ignored undecodable Responses WebSocket event")
			continue
		}
		switch envelope.Type {
		case "response.created", "response.in_progress", "response.output_text.delta", "response.function_call_arguments.delta", "response.output_item.done", "response.completed", "response.incomplete", "response.failed", "error":
		default:
			continue
		}
		var event responsesServerEvent
		if decodeErr := json.Unmarshal(raw, &event); decodeErr != nil {
			c.logf("ignored malformed %s event", envelope.Type)
			continue
		}

		switch event.Type {
		case "response.created", "response.in_progress":
			result.SawResponse = true
			if result.ID == "" {
				result.ID = event.Response.ID
			}
		case "response.output_text.delta":
			if event.Delta == "" {
				continue
			}
			result.SawOutput = true
			c.markStreamOutput(conn)
			if result.TTFB == 0 {
				result.TTFB = time.Since(started)
			}
			text.WriteString(event.Delta)
			if onToken != nil {
				onToken(event.Delta)
			}
		case "response.function_call_arguments.delta":
			if event.Delta != "" {
				result.SawOutput = true
				c.markStreamOutput(conn)
				if result.TTFB == 0 {
					result.TTFB = time.Since(started)
				}
			}
		case "response.output_item.done":
			completedItems = append(completedItems, event.Item)
			if event.Item.Type == "function_call" {
				result.SawOutput = true
				c.markStreamOutput(conn)
				if result.TTFB == 0 {
					result.TTFB = time.Since(started)
				}
			}
		case "response.completed":
			result.SawResponse = true
			result.ID = firstNonEmptyString(event.Response.ID, result.ID)
			result.Model = event.Response.Model
			result.Usage = event.Response.Usage
			items := event.Response.Output
			if len(items) == 0 {
				items = completedItems
			}
			result.ToolCalls = responsesToolCalls(items)
			result.Text = text.String()
			if result.Text == "" {
				result.Text = responsesOutputText(items)
				if result.Text != "" {
					result.SawOutput = true
					c.markStreamOutput(conn)
					if result.TTFB == 0 {
						result.TTFB = time.Since(started)
					}
					if onToken != nil {
						onToken(result.Text)
					}
				}
			}
			result.Total = time.Since(started)
			result.FinishReason = "stop"
			if len(result.ToolCalls) > 0 {
				result.FinishReason = "tool_calls"
			}
			if result.ID == "" {
				return result, errors.New("completed Responses response had no id")
			}
			return result, nil
		case "response.incomplete":
			result.SawResponse = true
			result.Incomplete = true
			result.ID = firstNonEmptyString(event.Response.ID, result.ID)
			result.Model = event.Response.Model
			result.Usage = event.Response.Usage
			items := event.Response.Output
			if len(items) == 0 {
				items = completedItems
			}
			result.ToolCalls = responsesToolCalls(items)
			result.Text = text.String()
			if result.Text == "" {
				result.Text = responsesOutputText(items)
				if result.Text != "" {
					result.SawOutput = true
					c.markStreamOutput(conn)
					if result.TTFB == 0 {
						result.TTFB = time.Since(started)
					}
					if onToken != nil {
						onToken(result.Text)
					}
				}
			}
			result.Total = time.Since(started)
			reason := "unknown"
			if event.Response.IncompleteDetails != nil && event.Response.IncompleteDetails.Reason != "" {
				reason = event.Response.IncompleteDetails.Reason
			}
			result.FinishReason = reason
			return result, nil
		case "response.failed":
			result.SawResponse = true
			result.StatusCode = event.Status
			result.Model = event.Response.Model
			result.Usage = event.Response.Usage
			result.Total = time.Since(started)
			if event.Response.Error != nil {
				result.ErrorCode = event.Response.Error.Code
				if result.StatusCode == 0 {
					result.StatusCode = event.Response.Error.Status
				}
				return result, fmt.Errorf("Responses response failed: %s: %s", event.Response.Error.Code, event.Response.Error.Message)
			}
			return result, errors.New("Responses response failed")
		case "error":
			result.StatusCode = event.Status
			result.Total = time.Since(started)
			if event.Error != nil {
				result.ErrorCode = event.Error.Code
				if result.StatusCode == 0 {
					result.StatusCode = event.Error.Status
				}
				return result, fmt.Errorf("Responses connection error: %s: %s", event.Error.Code, event.Error.Message)
			}
			result.ErrorCode = event.Code
			return result, fmt.Errorf("Responses connection error: %s: %s", event.Code, event.Message)
		}
	}
}

func responsesFailureIsRetryable(statusCode int, code string) bool {
	if statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		return true
	}
	code = strings.ToLower(strings.TrimSpace(code))
	for _, marker := range []string{"timeout", "rate_limit", "server_error", "internal_error", "overloaded", "service_unavailable", "connection_error"} {
		if strings.Contains(code, marker) {
			return true
		}
	}
	return false
}

func responsesToolCalls(items []responsesOutputItem) []vpc.ToolCall {
	out := make([]vpc.ToolCall, 0)
	for _, item := range items {
		if item.Type != "function_call" || item.CallID == "" {
			continue
		}
		out = append(out, vpc.ToolCall{
			ID:   item.CallID,
			Type: "function",
			Function: vpc.ToolCallFunction{
				Name:      item.Name,
				Arguments: item.Arguments,
			},
		})
	}
	return out
}

func responsesOutputText(items []responsesOutputItem) string {
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

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (c *ResponsesWebSocketClient) rememberCompleted(conn *websocket.Conn, requestMessages []vpc.Message, result responsesStreamResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != conn || c.closed {
		return
	}
	c.chain = responsesChainState{
		ResponseID:      result.ID,
		RequestMessages: cloneRouterMessages(requestMessages),
		Text:            result.Text,
		ToolCalls:       append([]vpc.ToolCall(nil), result.ToolCalls...),
	}
}

func cloneRouterMessages(messages []vpc.Message) []vpc.Message {
	out := make([]vpc.Message, len(messages))
	for index, message := range messages {
		out[index] = message
		out[index].ToolCalls = append([]vpc.ToolCall(nil), message.ToolCalls...)
	}
	return out
}

func (c *ResponsesWebSocketClient) clearChain() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.chain = responsesChainState{}
}

func (c *ResponsesWebSocketClient) markStreamOutput(conn *websocket.Conn) {
	c.mu.Lock()
	if c.conn == conn && c.streamActive {
		c.streamSawOutput = true
	}
	c.mu.Unlock()
}

func (c *ResponsesWebSocketClient) dropConnectionForCancellation(expected *websocket.Conn) {
	c.mu.Lock()
	if c.conn != expected {
		c.mu.Unlock()
		return
	}
	conn := c.conn
	activeIndex := c.activeIndex
	advance := c.streamActive && !c.streamSawOutput
	c.conn = nil
	c.activeIndex = -1
	c.chain = responsesChainState{}
	if activeIndex >= 0 {
		c.nextIndex = activeIndex
		if advance && len(c.candidates) > 1 {
			c.nextIndex = (activeIndex + 1) % len(c.candidates)
		}
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (c *ResponsesWebSocketClient) dropConnection(expected *websocket.Conn, advance bool) {
	c.mu.Lock()
	if c.conn != expected {
		c.mu.Unlock()
		return
	}
	conn := c.conn
	activeIndex := c.activeIndex
	c.conn = nil
	c.activeIndex = -1
	c.chain = responsesChainState{}
	if advance && len(c.candidates) > 1 && activeIndex >= 0 {
		c.nextIndex = (activeIndex + 1) % len(c.candidates)
	} else if activeIndex >= 0 {
		c.nextIndex = activeIndex
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// Interrupt clears the response chain so the next turn sends full canonical
// history, whose assistant tail contains only played audio. An active response
// is stopped by closing its socket; when generation already finished and only
// playback remained, the idle socket is safe to retain.
func (c *ResponsesWebSocketClient) Interrupt() {
	c.mu.Lock()
	c.chain = responsesChainState{}
	if !c.streamActive {
		c.mu.Unlock()
		return
	}
	c.streamInterrupted = true
	conn := c.conn
	activeIndex := c.activeIndex
	advance := !c.streamSawOutput
	c.conn = nil
	c.activeIndex = -1
	if activeIndex >= 0 {
		c.nextIndex = activeIndex
		if advance && len(c.candidates) > 1 {
			c.nextIndex = (activeIndex + 1) % len(c.candidates)
		}
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// Close permanently closes the call-scoped client and releases its socket.
func (c *ResponsesWebSocketClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.activeIndex = -1
	c.chain = responsesChainState{}
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}
