package llmrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

// geminiThoughtSignatureSkip is Google's documented bypass value for
// Gemini 3's thought-signature validation. Gemini 3 rejects any request
// whose history contains assistant functionCall parts without a
// thought_signature (400 "Function call is missing a thought_signature"),
// which hits every turn after a tool call (get_guidance/end_call) and
// resume replays. Mirrors Python's
// custom_llm_service._inject_gemini_thought_signatures.
const geminiThoughtSignatureSkip = "skip_thought_signature_validator"

// messagesForModel returns the request messages, injecting the
// thought-signature bypass into assistant tool_calls for gemini-3
// models (any provider — vertex, google_ai_studio, and openrouter all
// route to the same Google backends). Other models get the messages
// unchanged.
func messagesForModel(model string, messages []vpc.Message) any {
	if !strings.Contains(strings.ToLower(model), "gemini-3") {
		return messages
	}
	needsInjection := false
	for _, m := range messages {
		if m.Role == "assistant" && len(m.ToolCalls) > 0 {
			needsInjection = true
			break
		}
	}
	if !needsInjection {
		return messages
	}
	out := make([]any, 0, len(messages))
	for _, m := range messages {
		if m.Role != "assistant" || len(m.ToolCalls) == 0 {
			out = append(out, m)
			continue
		}
		msg := map[string]any{"role": m.Role}
		if m.Content != "" {
			msg["content"] = m.Content
		}
		if m.ToolCallID != "" {
			msg["tool_call_id"] = m.ToolCallID
		}
		calls := make([]map[string]any, 0, len(m.ToolCalls))
		for _, tc := range m.ToolCalls {
			calls = append(calls, map[string]any{
				"id":   tc.ID,
				"type": tc.Type,
				"function": map[string]any{
					"name":      tc.Function.Name,
					"arguments": tc.Function.Arguments,
				},
				"extra_content": map[string]any{
					"google": map[string]any{"thought_signature": geminiThoughtSignatureSkip},
				},
			})
		}
		msg["tool_calls"] = calls
		out = append(out, msg)
	}
	return out
}

// buildRequest assembles an OpenAI Chat-Completions streaming request
// for the chosen endpoint. Every provider speaks the same body shape;
// only the base URL and auth header differ (Azure uses an api-key
// header, everyone else uses Bearer). Grok models get the
// reasoning-disabled extra field, matching custom_llm_service.py.
func (r *Router) buildRequest(ctx context.Context, cfg endpointConfig, llmReq vpc.LLMRequest) (*http.Request, error) {
	endpoint, err := r.completionsURL(cfg)
	if err != nil {
		return nil, err
	}

	body := map[string]any{
		"model":          cfg.Model,
		"stream":         true,
		"stream_options": map[string]any{"include_usage": true},
		"messages":       messagesForModel(cfg.Model, llmReq.Messages),
		"temperature":    r.temperatureFor(cfg),
	}
	if len(llmReq.Tools) > 0 {
		body["tools"] = llmReq.Tools
	}
	if llmReq.ToolChoice != nil {
		body["tool_choice"] = llmReq.ToolChoice
	}
	switch {
	case r.cfg.MaxTokens != nil:
		body["max_tokens"] = *r.cfg.MaxTokens
	case cfg.MaxTokens != nil:
		body["max_tokens"] = *cfg.MaxTokens
	}
	// Provider/model-specific extra request fields (e.g. reasoning toggles),
	// merged at the top level like the OpenAI SDK's extra_body. Mirrors
	// OpenAIConfigHandler.get_extra_params: only OpenRouter Grok / the
	// Gemini providers carry these — the Azure-hosted Grok endpoints reject
	// a top-level "reasoning" argument with a 400.
	for k, v := range extraBodyFor(cfg) {
		body[k] = v
	}
	for k, v := range cfg.ExtraBody {
		body[k] = v
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	apiKey, err := r.apiKeyFor(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Provider == providerAzure {
		req.Header.Set("api-key", apiKey)
	} else {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	return req, nil
}

// azureAPIVersion matches Python's AsyncAzureOpenAI(api_version=...).
const azureAPIVersion = "2024-02-01"

// completionsURL builds the chat-completions endpoint for a config.
// Azure uses the deployment-path API (matching Python's AsyncAzureOpenAI:
// {endpoint}/openai/deployments/{deployment}/chat/completions?api-version=...);
// every other provider uses the OpenAI-compatible {base}/chat/completions.
func (r *Router) completionsURL(cfg endpointConfig) (string, error) {
	base, err := r.baseURL(cfg)
	if err != nil {
		return "", err
	}
	base = strings.TrimRight(base, "/")
	if cfg.Provider == providerAzure {
		return fmt.Sprintf("%s/openai/deployments/%s/chat/completions?api-version=%s", base, url.PathEscape(cfg.Model), azureAPIVersion), nil
	}
	return base + "/chat/completions", nil
}

func (r *Router) baseURL(cfg endpointConfig) (string, error) {
	switch cfg.Provider {
	case providerGrok:
		v := os.Getenv(cfg.EndpointEnv)
		if v == "" {
			return "", fmt.Errorf("llmrouter: missing endpoint env %s for %s", cfg.EndpointEnv, cfg.Key)
		}
		return v, nil
	case providerAzure:
		v := os.Getenv(cfg.EndpointEnv)
		if v == "" {
			return "", fmt.Errorf("llmrouter: missing endpoint env %s for %s", cfg.EndpointEnv, cfg.Key)
		}
		return v, nil
	case providerOpenAI:
		return "https://api.openai.com/v1", nil
	case providerOpenRouter, providerGoogleAIStudio, providerCerebras:
		if cfg.BaseURL == "" {
			return "", fmt.Errorf("llmrouter: missing base URL for %s", cfg.Key)
		}
		return cfg.BaseURL, nil
	case providerVertex:
		return vertexBaseURL(cfg.VertexProject, cfg.VertexLocation), nil
	default:
		return "", fmt.Errorf("llmrouter: unknown provider %q for %s", cfg.Provider, cfg.Key)
	}
}

func (r *Router) apiKeyFor(ctx context.Context, cfg endpointConfig) (string, error) {
	if cfg.Provider == providerVertex {
		// Process-wide per-credential token source; lazily loads the
		// service-account key from S3 on first use.
		return vertexTokenSourceForEnv(vertexCredsEnvForConfig(cfg)).Token(ctx)
	}
	key := os.Getenv(cfg.APIKeyEnv)
	if key == "" {
		return "", fmt.Errorf("llmrouter: missing API key env %s for %s", cfg.APIKeyEnv, cfg.Key)
	}
	return key, nil
}

// temperatureFor returns the temperature for a call: the per-client
// Config override when set (Python passes temperature per call on the
// failover paths), else the endpoint registry's value, else 0.
func (r *Router) temperatureFor(cfg endpointConfig) float64 {
	if r.cfg.Temperature != nil {
		return *r.cfg.Temperature
	}
	if cfg.Temperature != nil {
		return *cfg.Temperature
	}
	return 0
}

func vertexBaseURL(project, location string) string {
	host := "aiplatform.googleapis.com"
	if location != "" && location != "global" {
		host = location + "-aiplatform.googleapis.com"
	}
	return fmt.Sprintf("https://%s/v1/projects/%s/locations/%s/endpoints/openapi", host, project, location)
}

// extraBodyFor returns provider/model-specific request fields, merged at
// the top level of the chat-completions body. Port of
// OpenAIConfigHandler.get_extra_params. Crucially, the Azure-hosted `grok`
// provider and Vertex grok get nothing (they reject `reasoning`); only
// OpenRouter Grok and the Gemini providers carry tuning fields.
func extraBodyFor(cfg endpointConfig) map[string]any {
	model := strings.ToLower(cfg.Model)
	switch cfg.Provider {
	case providerOpenRouter:
		if strings.Contains(model, "grok-4-fast") || strings.Contains(model, "grok-4.1-fast") {
			return map[string]any{"reasoning": map[string]any{"enabled": false}}
		}
		if strings.Contains(model, "gemini") {
			if strings.Contains(model, "gemini-2.5") {
				return map[string]any{"reasoning": map[string]any{"effort": "none"}}
			}
			return map[string]any{"reasoning": map[string]any{"effort": "minimal"}}
		}
	case providerVertex:
		if strings.Contains(model, "gemini") {
			if strings.Contains(model, "gemini-2.5") {
				return map[string]any{"google": map[string]any{"thinking_config": map[string]any{"thinking_budget": 0}}}
			}
			return map[string]any{"google": map[string]any{"thinking_config": map[string]any{"thinking_level": "MINIMAL"}}}
		}
	case providerGoogleAIStudio:
		if strings.Contains(model, "gemini") {
			if strings.Contains(model, "gemini-2.5") {
				return map[string]any{"reasoning_effort": "none"}
			}
			return map[string]any{"reasoning_effort": "minimal"}
		}
	}
	return nil
}

// apiError is a non-2xx response from an LLM endpoint, carrying the
// status code so the caller can classify it (rate limit vs other).
type apiError struct {
	statusCode int
	body       string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("llmrouter: endpoint returned status %d: %s", e.statusCode, e.body)
}

// classifyError mirrors is_rate_limit_error/classify_error: 429 or
// rate-limit-ish text is "rate_limit", everything else "other".
func classifyError(statusCode int, msg string) string {
	if statusCode == 429 {
		return errorTypeRateLimit
	}
	m := strings.ToLower(msg)
	if strings.Contains(m, "ratelimit") ||
		strings.Contains(m, "rate_limit") ||
		strings.Contains(m, "rate limit") ||
		strings.Contains(m, "too many requests") ||
		strings.Contains(m, "resource_exhausted") {
		return errorTypeRateLimit
	}
	return errorTypeOther
}

const (
	errorTypeRateLimit = "rate_limit"
	errorTypeOther     = "other"
)
