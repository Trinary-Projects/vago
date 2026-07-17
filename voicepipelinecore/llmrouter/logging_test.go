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
		{"openrouter_gpt_oss_120b", "OPENROUTER"},            // no ExtraBody at all
		{"openrouter_gpt_oss_120b_throughput", "OPENROUTER"}, // sort, not a pin
		{"cerebras_gpt_oss_120b", "CEREBRAS_ENTERPRISE"},     // non-OpenRouter untouched
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
