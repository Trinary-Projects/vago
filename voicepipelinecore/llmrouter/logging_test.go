package llmrouter

import "testing"

// Self-pinned OpenRouter configs carry the pinned provider in the logged
// deployment (OPENROUTER_MODELRUN / OPENROUTER_CEREBRAS); configs without
// an explicit provider.only pin stay plain OPENROUTER. Must match Python's
// OpenAIConfigHandler.get_deployment_name.
func TestDeploymentNamePinnedOpenRouterProvider(t *testing.T) {
	cases := []struct {
		configKey string
		want      string
	}{
		{"openrouter_gemma_4_31b_it_modelrun", "OPENROUTER_MODELRUN"},
		{"openrouter_gemma_4_31b_it_cerebras", "OPENROUTER_CEREBRAS"},
		{"openrouter_gemma_4_31b_it_openinference", "OPENROUTER_OPEN_INFERENCE"}, // hyphen normalized
		{"openrouter_gpt_oss_120b", "OPENROUTER"},                                // no ExtraBody at all
		{"openrouter_gpt_oss_120b_throughput", "OPENROUTER"},                     // sort, not a pin
		{"cerebras_gpt_oss_120b", "CEREBRAS_ENTERPRISE"},                         // non-OpenRouter untouched
	}
	for _, tc := range cases {
		cfg, ok := endpointConfigs[tc.configKey]
		if !ok {
			t.Fatalf("config %q not found", tc.configKey)
		}
		if got := deploymentName(cfg); got != tc.want {
			t.Errorf("deploymentName(%s) = %q, want %q", tc.configKey, got, tc.want)
		}
	}
}

// The provider OpenRouter actually routed to replaces the config-derived
// deployment. Must match Python's
// OpenAIConfigHandler.get_openrouter_routed_deployment.
func TestDeploymentWithRoutedProvider(t *testing.T) {
	cases := []struct {
		configKey string
		routed    string
		want      string
	}{
		{"openrouter_gpt_oss_120b", "BaseTen", "OPENROUTER_BASETEN"},
		{"openrouter_gpt_oss_120b", "Google AI Studio", "OPENROUTER_GOOGLE_AI_STUDIO"},
		{"openrouter_gemma_4_31b_it_modelrun", "ModelRun", "OPENROUTER_MODELRUN"},
		{"openrouter_gemma_4_31b_it_openinference", "Open Inference", "OPENROUTER_OPEN_INFERENCE"},
		{"openrouter_gemma_4_31b_it_modelrun", "", "OPENROUTER_MODELRUN"}, // no chunk: pinned fallback
		{"openrouter_gpt_oss_120b", " - ", "OPENROUTER"},                  // empty token: plain fallback
		{"cerebras_gpt_oss_120b", "BaseTen", "CEREBRAS_ENTERPRISE"},       // non-OpenRouter untouched
	}
	for _, tc := range cases {
		cfg, ok := endpointConfigs[tc.configKey]
		if !ok {
			t.Fatalf("config %q not found", tc.configKey)
		}
		if got := deploymentWithRoutedProvider(cfg, tc.routed); got != tc.want {
			t.Errorf("deploymentWithRoutedProvider(%s, %q) = %q, want %q", tc.configKey, tc.routed, got, tc.want)
		}
	}
}

func TestParseSSEChunkProvider(t *testing.T) {
	_, _, _, _, _, provider, _, ok := parseSSEChunk(`data: {"provider":"ModelRun","choices":[{"delta":{"content":"hi"}}]}`)
	if !ok || provider != "ModelRun" {
		t.Fatalf("content chunk provider = %q ok=%v, want ModelRun", provider, ok)
	}
	_, _, _, _, hasUsage, provider, _, ok := parseSSEChunk(`data: {"provider":"ModelRun","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`)
	if !ok || !hasUsage || provider != "ModelRun" {
		t.Fatalf("usage chunk provider = %q hasUsage=%v ok=%v, want ModelRun", provider, hasUsage, ok)
	}
}
