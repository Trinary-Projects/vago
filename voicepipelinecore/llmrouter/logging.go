package llmrouter

import (
	"strings"
	"time"

	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

// CallLog is the per-call record handed to Config.LogSink after every
// completion (success, error, or interruption). The sink (provided by
// the bot) decides how to persist it — e.g. enqueue Disha's
// llm_logging_service. The router stays free of S3/DB/usecase concerns.
type CallLog struct {
	Model            string
	ConfigKey        string
	Deployment       string
	Request          vpc.LLMRequest
	ResponseContent  string
	ToolCalls        []vpc.ToolCall
	PromptMetadata   map[string]any
	PromptTokens     int
	CompletionTokens int
	TTFBMs           float64
	TotalMs          float64
	StatusCode       int
	Completed        bool
	Interrupted      bool
	ErrorType        string
	ErrorMessage     string
	FinishReason     string
	UsingFallback    bool
	SelectedGroup    string
	// ResponseInputMode is set by Responses WebSocket clients to "full" or
	// "incremental". It deliberately records the mode, not the response ID.
	ResponseInputMode string
	ReasoningTokens   int
}

// deploymentName mirrors OpenAIConfigHandler.get_deployment_name so the
// logged deployment matches what Python records.
func deploymentName(cfg endpointConfig) string {
	if cfg.APIKeyEnv != "" {
		name := cfg.APIKeyEnv
		if i := strings.Index(name, "_API_KEY"); i >= 0 {
			name = name[:i]
		}
		// When we pin the OpenRouter provider ourselves, suffix it so
		// llm logs distinguish e.g. OPENROUTER_MODELRUN from
		// OPENROUTER_CEREBRAS. Configs without an explicit pin (no
		// provider.only, e.g. throughput-sorted) stay plain OPENROUTER.
		if cfg.Provider == providerOpenRouter {
			if pinned := pinnedProviderSlug(cfg); pinned != "" {
				// Hyphens become underscores (open-inference →
				// OPEN_INFERENCE) so deployment names stay one flat
				// underscore-delimited token. Must match Python's
				// get_deployment_name.
				name = name + "_" + strings.ToUpper(strings.ReplaceAll(pinned, "-", "_"))
			}
		}
		return name
	}
	if cfg.Provider == providerVertex {
		project := cfg.VertexProject
		if project == "gen-lang-client-0439239631" {
			project = "dishaai"
		}
		if project == "" {
			project = "curelinkai"
		}
		parts := []string{string(cfg.Provider), project}
		if cfg.VertexLocation != "" {
			parts = append(parts, cfg.VertexLocation)
		}
		return strings.Join(parts, "_")
	}
	return string(cfg.Provider)
}

// pinnedProviderSlug returns the base slug (e.g. "modelrun" from
// "modelrun/fp4") of a config's self-pinned OpenRouter provider — the
// single entry of ExtraBody provider.only — or "" when the config does
// not pin one.
func pinnedProviderSlug(cfg endpointConfig) string {
	provider, _ := cfg.ExtraBody["provider"].(map[string]any)
	only, _ := provider["only"].([]string)
	if len(only) != 1 {
		return ""
	}
	slug := only[0]
	if i := strings.Index(slug, "/"); i >= 0 {
		slug = slug[:i]
	}
	return slug
}

func msFromDuration(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
