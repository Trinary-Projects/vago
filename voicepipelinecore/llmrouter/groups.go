// Package llmrouter is the live-call LLM resilience layer for the voice
// pipeline. It selects the fastest healthy endpoint within a model group
// (using endpoint health written to Redis by Disha's Python poller),
// speaks to every provider in OpenAI Chat-Completions format over plain
// HTTP (no provider SDKs), blacklists endpoints that error during a live
// call, and triggers a re-poll when an endpoint errors or is slow.
//
// It implements voicepipelinecore.LLMClient structurally (the Stream
// method) without importing voicepipelinecore, so the core pipeline
// package stays free of Redis/provider concerns. Callers select a group
// by name; the registry of groups and endpoints lives here.
//
// This is the Go port of Disha's bots/onboarding_call/custom_llm_service.py
// + bots/llm_switching_service.py + services/openai_config_manager.py,
// scoped to the model groups the Disha call bots need.
package llmrouter

// provider identifies how to build the HTTP request and auth header for
// an endpoint. All providers are spoken in OpenAI Chat-Completions
// format; the only differences are the base URL and the auth header
// (Azure uses an api-key header, everyone else uses Bearer).
type provider string

const (
	providerOpenAI         provider = "openai"
	providerAzure          provider = "azure"
	providerGrok           provider = "grok"
	providerVertex         provider = "vertex"
	providerOpenRouter     provider = "openrouter"
	providerGoogleAIStudio provider = "google_ai_studio"
	providerCerebras       provider = "cerebras"
)

// endpointConfig is one selectable LLM endpoint. The Key must match the
// Python OpenAIModels enum value because it forms the Redis health key
// (live_call_modal_health:{Key}) the Python poller writes.
type endpointConfig struct {
	Key       string
	Provider  provider
	Model     string
	Region    string
	APIKeyEnv string // env var holding the API key (Bearer / azure api-key)

	// EndpointEnv holds the OpenAI-compatible base URL for grok/azure
	// endpoints (e.g. https://<resource>.openai.azure.com/openai/v1).
	EndpointEnv string
	// BaseURL is a literal base URL for providers whose URL is fixed
	// (openai/openrouter/google_ai_studio).
	BaseURL string

	// Vertex-only: the OpenAI-compatible Vertex endpoint is derived from
	// project + location, and the Bearer token is minted from a service
	// account (see vertex_token.go).
	VertexProject  string
	VertexLocation string
	VertexCredsEnv string

	// Temperature/MaxTokens override the router default when non-nil.
	Temperature *float64
	MaxTokens   *int

	// ExtraBody holds provider/model-specific top-level request fields
	// that Python passes via the OpenAI SDK's extra_body argument.
	ExtraBody map[string]any
}

// modelGroup is an ordered set of interchangeable endpoints plus a
// last-resort fallback config key.
type modelGroup struct {
	Configs       []string
	Fallback      string
	FallbackGroup string
}

const (
	groupGrokFast        = "grok-4.1-fast" // onboarding variant configs' model field.
	groupGrokSales       = "grok-4.1-fast-sales"
	groupGPT41           = "gpt-4.1" // the sales cross-group fallback target.
	groupGemini31        = "gemini-flash-3.1-lite"
	groupGemini35        = "gemini-flash-3.5-lite"
	groupFollowUpDynamic = "followup-dynamic-gemma"
	groupGPTOSS120Fast   = "gpt-oss120-fast"

	grokModel  = "grok-4-1-fast-non-reasoning"
	gpt41Model = "gpt-4.1"
)

// EndpointOpenRouterGemini25FlashLite is the fixed-endpoint config key for
// the onboarding stage-transition tracker's one-shot classifier (used via
// Config.FixedEndpoint, never in a health-selected group).
const EndpointOpenRouterGemini25FlashLite = "openrouter_gemini_2_5_flash_lite"

func floatPtr(v float64) *float64 { return &v }
func intPtr(v int) *int           { return &v }

// endpointConfigs is the Go port of OPEN_AI_MODEL_CONFIG, scoped to the
// configs reachable from the sales model group and its gpt-4.1 fallback.
var endpointConfigs = map[string]endpointConfig{
	// --- grok-4.1-fast-non-reasoning, Azure-hosted (OpenAI-compatible) ---
	"grok_4_1_fnr_eastus": {
		Key: "grok_4_1_fnr_eastus", Provider: providerGrok, Model: grokModel, Region: "us",
		APIKeyEnv: "GROK_4_1_FNR_EASTUS_API_KEY", EndpointEnv: "GROK_4_1_FNR_EASTUS_ENDPOINT",
	},
	"grok_4_1_fnr_eastus2": {
		Key: "grok_4_1_fnr_eastus2", Provider: providerGrok, Model: grokModel, Region: "us",
		APIKeyEnv: "GROK_4_1_FNR_EASTUS2_API_KEY", EndpointEnv: "GROK_4_1_FNR_EASTUS2_ENDPOINT",
	},
	"grok_4_1_fnr_westus": {
		Key: "grok_4_1_fnr_westus", Provider: providerGrok, Model: grokModel, Region: "us",
		APIKeyEnv: "GROK_4_1_FNR_WESTUS_API_KEY", EndpointEnv: "GROK_4_1_FNR_WESTUS_ENDPOINT",
	},
	"grok_4_1_fnr_westus2": {
		Key: "grok_4_1_fnr_westus2", Provider: providerGrok, Model: grokModel, Region: "us",
		APIKeyEnv: "GROK_4_1_FNR_WESTUS2_API_KEY", EndpointEnv: "GROK_4_1_FNR_WESTUS2_ENDPOINT",
	},
	"grok_4_1_fnr_westcentralus": {
		Key: "grok_4_1_fnr_westcentralus", Provider: providerGrok, Model: grokModel, Region: "us",
		APIKeyEnv: "GROK_4_1_FNR_WESTCENTRALUS_API_KEY", EndpointEnv: "GROK_4_1_FNR_WESTCENTRALUS_ENDPOINT",
	},

	// --- Vertex grok (OpenAI-compatible endpoint, OAuth Bearer token) ---
	"vertex_dishaai_grok_4_1_fast_non_reasoning": {
		Key: "vertex_dishaai_grok_4_1_fast_non_reasoning", Provider: providerVertex,
		Model: "xai/grok-4.1-fast-non-reasoning", Region: "us",
		VertexProject: "gen-lang-client-0439239631", VertexLocation: "global",
	},

	// --- gpt-4.1 fallback group (Azure + OpenAI) ---
	"azure_gpt_4_1_us_east": {
		Key: "azure_gpt_4_1_us_east", Provider: providerAzure, Model: gpt41Model, Region: "us",
		APIKeyEnv: "AZURE_OPENAI_US_EAST_API_KEY", EndpointEnv: "AZURE_OPENAI_US_EAST_ENDPOINT",
	},
	"azure_gpt_4_1_us_east_2": {
		Key: "azure_gpt_4_1_us_east_2", Provider: providerAzure, Model: gpt41Model, Region: "us",
		APIKeyEnv: "AZURE_OPENAI_US_EAST_2_API_KEY", EndpointEnv: "AZURE_OPENAI_US_EAST_2_ENDPOINT",
	},
	"azure_gpt_4_1_us_north_central": {
		Key: "azure_gpt_4_1_us_north_central", Provider: providerAzure, Model: gpt41Model, Region: "us",
		APIKeyEnv: "AZURE_OPENAI_US_NORTH_CENTRAL_API_KEY", EndpointEnv: "AZURE_OPENAI_US_NORTH_CENTRAL_ENDPOINT",
	},
	"azure_gpt_4_1_us_south_central": {
		Key: "azure_gpt_4_1_us_south_central", Provider: providerAzure, Model: gpt41Model, Region: "us",
		APIKeyEnv: "AZURE_OPENAI_US_SOUTH_CENTRAL_API_KEY", EndpointEnv: "AZURE_OPENAI_US_SOUTH_CENTRAL_ENDPOINT",
	},
	"azure_gpt_4_1_us_west": {
		Key: "azure_gpt_4_1_us_west", Provider: providerAzure, Model: gpt41Model, Region: "us",
		APIKeyEnv: "AZURE_OPENAI_US_WEST_API_KEY", EndpointEnv: "AZURE_OPENAI_US_WEST_ENDPOINT",
	},
	"azure_gpt_4_1_us_west_3": {
		Key: "azure_gpt_4_1_us_west_3", Provider: providerAzure, Model: gpt41Model, Region: "us",
		APIKeyEnv: "AZURE_OPENAI_US_WEST_3_API_KEY", EndpointEnv: "AZURE_OPENAI_US_WEST_3_ENDPOINT",
	},
	"azure_gpt_4_1_south_india": {
		Key: "azure_gpt_4_1_south_india", Provider: providerAzure, Model: gpt41Model, Region: "south_india",
		APIKeyEnv: "AZURE_OPENAI_SOUTH_INDIA_API_KEY", EndpointEnv: "AZURE_OPENAI_SOUTH_INDIA_ENDPOINT",
	},
	"openai_gpt_4_1": {
		Key: "openai_gpt_4_1", Provider: providerOpenAI, Model: gpt41Model, Region: "us",
		APIKeyEnv: "OPENAI_API_KEY",
	},
	"openai_gpt_4_1_priority": {
		Key: "openai_gpt_4_1_priority", Provider: providerOpenAI, Model: gpt41Model, Region: "us",
		APIKeyEnv: "OPENAI_PRIORITY_API_KEY",
	},

	// --- gemini-flash-3.1-lite follow-up group ---
	// Python's follow-up call runs this group at temperature 0.5
	// (bots/followup_call/followup_call.py InputParams(temperature=0.5)).
	"vertex_gemini_flash_3_1_lite": {
		Key: "vertex_gemini_flash_3_1_lite", Provider: providerVertex,
		Model: "google/gemini-3.1-flash-lite", Region: "us",
		VertexProject: "disha-ai3", VertexLocation: "global", VertexCredsEnv: "VERTEX_DISHA_AI2_CREDS_FILE",
		Temperature: floatPtr(0.5),
	},
	"google_ai_studio_gemini_flash_3_1_lite": {
		Key: "google_ai_studio_gemini_flash_3_1_lite", Provider: providerGoogleAIStudio,
		Model: "gemini-3.1-flash-lite", Region: "us",
		APIKeyEnv: "GEMINI_API_KEY_DISHAAI2", BaseURL: "https://generativelanguage.googleapis.com/v1beta/openai/",
		Temperature: floatPtr(0.5),
	},
	"openrouter_gemini_flash_3_1_lite": {
		Key: "openrouter_gemini_flash_3_1_lite", Provider: providerOpenRouter,
		Model: "google/gemini-3.1-flash-lite", Region: "us",
		APIKeyEnv: "OPENROUTER_API_KEY", BaseURL: "https://openrouter.ai/api/v1",
		Temperature: floatPtr(0.5),
	},

	// --- gemini-flash-3.5-lite group (Python LLMFailoverConfigName.gemini_flash_3_5_lite) ---
	// Vertex model carries the Go "google/" prefix like the 3.1 Vertex
	// config (Python stores it plain "gemini-3.5-flash-lite"); creds env is
	// left to vertexCredsEnvForConfig's disha-ai3 default
	// (VERTEX_DISHA_AI2_CREDS_FILE), matching Python's target.
	"vertex_gemini_flash_3_5_lite": {
		Key: "vertex_gemini_flash_3_5_lite", Provider: providerVertex,
		Model: "google/gemini-3.5-flash-lite", Region: "us",
		VertexProject: "disha-ai3", VertexLocation: "global", VertexCredsEnv: "VERTEX_DISHA_AI2_CREDS_FILE",
		Temperature: floatPtr(0.5),
	},
	"google_ai_studio_gemini_flash_3_5_lite": {
		Key: "google_ai_studio_gemini_flash_3_5_lite", Provider: providerGoogleAIStudio,
		Model: "gemini-3.5-flash-lite", Region: "us",
		APIKeyEnv: "GEMINI_API_KEY_DISHAAI2", BaseURL: "https://generativelanguage.googleapis.com/v1beta/openai/",
		Temperature: floatPtr(0.5),
	},

	// --- follow-up dynamic treatment main model ---
	// Split into one config per OpenRouter provider (instead of the old
	// single "openrouter_gemma_4_31b_it" config that pinned both
	// providers via ExtraBody and let OpenRouter's own internal routing
	// pick between them). Now talk-go's own health selection — fed by
	// Disha's Python poller — chooses the provider directly. Each
	// config's "only"+"allow_fallbacks:false" already excludes every
	// other provider, so the old "ignore" list is dropped as redundant.
	// Keys must match the Python OpenAIModels enum values in
	// disha-backend services/openai_config_manager.py exactly, since
	// they form the Redis health keys.
	//
	// Any provider pinned here MUST support tool calling on OpenRouter
	// (check supported_parameters on the /models/{id}/endpoints API):
	// the dynamic-checkin call always sends tools, and OpenRouter
	// returns 404 "No endpoints found" when the pinned provider lacks
	// tool support. wandb/bf16 was removed for exactly that (2026-07-17:
	// 152/152 live calls failed while polls — which send no tools —
	// kept passing). cerebras/fp16 carries an extra_latency_padding_ms
	// handicap in the Python poller config so selection prefers the
	// cheaper modelrun unless Cerebras is meaningfully faster.
	"openrouter_gemma_4_31b_it_modelrun": {
		Key: "openrouter_gemma_4_31b_it_modelrun", Provider: providerOpenRouter,
		Model: "google/gemma-4-31b-it", Region: "us",
		APIKeyEnv: "OPENROUTER_API_KEY", BaseURL: "https://openrouter.ai/api/v1",
		Temperature: floatPtr(0.5),
		ExtraBody: map[string]any{
			"provider": map[string]any{
				"order":           []string{"modelrun/fp4"},
				"only":            []string{"modelrun/fp4"},
				"allow_fallbacks": false,
			},
		},
	},
	"openrouter_gemma_4_31b_it_cerebras": {
		Key: "openrouter_gemma_4_31b_it_cerebras", Provider: providerOpenRouter,
		Model: "google/gemma-4-31b-it", Region: "us",
		APIKeyEnv: "OPENROUTER_API_KEY", BaseURL: "https://openrouter.ai/api/v1",
		Temperature: floatPtr(0.5),
		ExtraBody: map[string]any{
			"provider": map[string]any{
				"order":           []string{"cerebras/fp16"},
				"only":            []string{"cerebras/fp16"},
				"allow_fallbacks": false,
			},
		},
	},
	// open-inference/bf16 is cheaper than modelrun ($0.10/$0.35 vs
	// $0.22/$0.55 per M) and supports tools, so it carries no
	// extra_latency_padding_ms handicap — it competes with modelrun on
	// latency alone.
	"openrouter_gemma_4_31b_it_openinference": {
		Key: "openrouter_gemma_4_31b_it_openinference", Provider: providerOpenRouter,
		Model: "google/gemma-4-31b-it", Region: "us",
		APIKeyEnv: "OPENROUTER_API_KEY", BaseURL: "https://openrouter.ai/api/v1",
		Temperature: floatPtr(0.5),
		ExtraBody: map[string]any{
			"provider": map[string]any{
				"order":           []string{"open-inference/bf16"},
				"only":            []string{"open-inference/bf16"},
				"allow_fallbacks": false,
			},
		},
	},

	// --- follow-up get_guidance tool failover endpoints ---
	"cerebras_gpt_oss_120b": {
		Key: "cerebras_gpt_oss_120b", Provider: providerCerebras,
		Model: "gpt-oss-120b", Region: "us",
		APIKeyEnv: "CEREBRAS_ENTERPRISE_API_KEY", BaseURL: "https://api.cerebras.ai/v1",
		MaxTokens: intPtr(500),
	},
	"openrouter_gpt_oss_120b": {
		Key: "openrouter_gpt_oss_120b", Provider: providerOpenRouter,
		Model: "openai/gpt-oss-120b", Region: "us",
		APIKeyEnv: "OPENROUTER_API_KEY", BaseURL: "https://openrouter.ai/api/v1",
		MaxTokens: intPtr(500),
	},

	// --- onboarding stage-transition tracker classifier ---
	// Fixed-endpoint-only: used via Config.FixedEndpoint by the Disha
	// onboarding stage tracker's one-shot "maybe" classifier and never a
	// member of a health-selected group's Configs list, so Python-side
	// polling for it is optional (same note as the hedged throughput
	// config below).
	EndpointOpenRouterGemini25FlashLite: {
		Key: EndpointOpenRouterGemini25FlashLite, Provider: providerOpenRouter,
		Model: "google/gemini-2.5-flash-lite", Region: "us",
		APIKeyEnv: "OPENROUTER_API_KEY", BaseURL: "https://openrouter.ai/api/v1",
	},

	// --- hedged one-shot hedge endpoint ---
	// Python's gpt_oss120_fast_hedged pair uses OpenRouter with
	// provider_sort="throughput" for the hedge leg. It gets its own key
	// (instead of reusing openrouter_gpt_oss_120b) so blacklist
	// write-back for hedge attempts lands on the config that actually
	// misbehaved and get_guidance's health entries stay untouched.
	"openrouter_gpt_oss_120b_throughput": {
		Key: "openrouter_gpt_oss_120b_throughput", Provider: providerOpenRouter,
		Model: "openai/gpt-oss-120b", Region: "us",
		APIKeyEnv: "OPENROUTER_API_KEY", BaseURL: "https://openrouter.ai/api/v1",
		MaxTokens: intPtr(500),
		ExtraBody: map[string]any{
			"provider": map[string]any{"sort": "throughput"},
		},
	},
}

// hedgedPair is a fixed ordered primary/hedge endpoint pair for the
// hedged one-shot client (Python FAILOVER_CONFIGS: config_list[0]/[1],
// not health-ranked).
type hedgedPair struct {
	Primary string
	Hedge   string
}

// GroupGPTOSS120FastHedged mirrors Python's gpt_oss120_fast_hedged
// failover config: Cerebras primary, OpenRouter throughput-sorted hedge.
const GroupGPTOSS120FastHedged = "gpt-oss120-fast-hedged"

var hedgedPairs = map[string]hedgedPair{
	GroupGPTOSS120FastHedged: {
		Primary: "cerebras_gpt_oss_120b",
		Hedge:   "openrouter_gpt_oss_120b_throughput",
	},
}

// modelGroups is the Go port of MODEL_GROUPS for the Disha call bots.
// The Vertex grok config is a first-class member of the sales group, so
// it participates in normal health-based selection (mirrors Python,
// where it is the most stable endpoint).
var modelGroups = map[string]modelGroup{
	// Identical membership to grok-4.1-fast-sales in Python's
	// MODEL_GROUPS — both exist as separate keys so their health polls
	// and poll locks stay per-group.
	groupGrokFast: {
		Configs: []string{
			"grok_4_1_fnr_eastus",
			"grok_4_1_fnr_eastus2",
			"grok_4_1_fnr_westus",
			"grok_4_1_fnr_westus2",
			"grok_4_1_fnr_westcentralus",
			"vertex_dishaai_grok_4_1_fast_non_reasoning",
		},
		Fallback:      "vertex_dishaai_grok_4_1_fast_non_reasoning",
		FallbackGroup: groupGPT41,
	},
	groupGrokSales: {
		Configs: []string{
			"grok_4_1_fnr_eastus",
			"grok_4_1_fnr_eastus2",
			"grok_4_1_fnr_westus",
			"grok_4_1_fnr_westus2",
			"grok_4_1_fnr_westcentralus",
			"vertex_dishaai_grok_4_1_fast_non_reasoning",
		},
		Fallback:      "vertex_dishaai_grok_4_1_fast_non_reasoning",
		FallbackGroup: groupGPT41,
	},
	groupGPT41: {
		Configs: []string{
			"azure_gpt_4_1_us_east",
			"azure_gpt_4_1_us_east_2",
			"azure_gpt_4_1_us_north_central",
			"azure_gpt_4_1_us_south_central",
			"azure_gpt_4_1_us_west",
			"azure_gpt_4_1_us_west_3",
			"azure_gpt_4_1_south_india",
			"openai_gpt_4_1",
			"openai_gpt_4_1_priority",
		},
		Fallback: "openai_gpt_4_1_priority",
	},
	groupGemini31: {
		Configs: []string{
			"vertex_gemini_flash_3_1_lite",
			"google_ai_studio_gemini_flash_3_1_lite",
		},
		Fallback: "openrouter_gemini_flash_3_1_lite",
		// Python's LLMSwitchingService falls back to FALLBACK_MODEL_GROUP
		// (gpt-4.1) for any group with no available endpoints.
		FallbackGroup: groupGPT41,
	},
	groupGemini35: {
		Configs: []string{
			"vertex_gemini_flash_3_5_lite",
			"google_ai_studio_gemini_flash_3_5_lite",
		},
		Fallback:      "google_ai_studio_gemini_flash_3_5_lite",
		FallbackGroup: groupGPT41,
	},
	groupFollowUpDynamic: {
		Configs:  []string{"openrouter_gemma_4_31b_it_modelrun", "openrouter_gemma_4_31b_it_cerebras", "openrouter_gemma_4_31b_it_openinference"},
		Fallback: "openrouter_gemma_4_31b_it_modelrun",
		// No healthy gemma endpoint in any pinned provider falls back to
		// the cheap gemini-flash-3.1-lite group (decided 2026-07-17,
		// replacing the earlier gpt-4.1 target). Python mirrors this via
		// the per-group "fallback_group" override in MODEL_GROUPS —
		// LLMSwitchingService's uniform FALLBACK_MODEL_GROUP applies only
		// to groups without that key. (When the gemini group has no health
		// data either, selection returns its hardcoded fallback config;
		// the Fallback above is unreachable while the gemini group exists
		// and is kept only for shape parity with Python's group entry.)
		FallbackGroup: groupGemini31,
	},
	groupGPTOSS120Fast: {
		Configs: []string{
			"cerebras_gpt_oss_120b",
			"openrouter_gpt_oss_120b",
		},
		Fallback: "openrouter_gpt_oss_120b",
	},
}

// groupConfigsForRegion returns the configs in a group filtered by
// region, mirroring LLMSwitchingService.get_model_configs_for_group.
func groupConfigsForRegion(group, region string) ([]endpointConfig, bool) {
	g, ok := modelGroups[group]
	if !ok {
		return nil, false
	}
	out := make([]endpointConfig, 0, len(g.Configs))
	for _, key := range g.Configs {
		cfg, ok := endpointConfigs[key]
		if !ok {
			continue
		}
		if region != "" && cfg.Region != region {
			continue
		}
		out = append(out, cfg)
	}
	return out, true
}
