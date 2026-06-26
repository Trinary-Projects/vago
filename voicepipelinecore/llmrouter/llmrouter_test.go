package llmrouter

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

// fakeRedis is an in-memory RedisStore for tests.
type fakeRedis struct {
	mu      sync.Mutex
	data    map[string][]byte
	locks   map[string]bool
	mgetErr error
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{data: map[string][]byte{}, locks: map[string]bool{}}
}

func (f *fakeRedis) GetCache(ctx context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	return v, ok, nil
}

func (f *fakeRedis) MGetCache(ctx context.Context, keys ...string) ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mgetErr != nil {
		return nil, f.mgetErr
	}
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = f.data[k]
	}
	return out, nil
}

func (f *fakeRedis) SetCache(ctx context.Context, key string, value any, _ time.Duration) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = b
	return nil
}

func (f *fakeRedis) AcquireLock(ctx context.Context, key string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.locks[key] {
		return false, nil
	}
	f.locks[key] = true
	return true, nil
}

func (f *fakeRedis) get(key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[key]
}

func (f *fakeRedis) setHealth(configKey string, blacklisted bool, latencies ...float64) {
	h := endpointHealth{Blacklisted: blacklisted}
	for _, l := range latencies {
		lv := l
		h.PollRuns = append(h.PollRuns, pollRun{At: "t", Success: true, AdjustedLatencyMs: &lv})
	}
	b, _ := json.Marshal(h)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[healthKey(configKey)] = b
}

func ctx() context.Context { return context.Background() }

func testLLMRequest() vpc.LLMRequest {
	return vpc.LLMRequest{
		Messages: []vpc.Message{{Role: "user", Content: "hi"}},
	}
}

// --- selection ---

func TestSelectionPicksFastest(t *testing.T) {
	fr := newFakeRedis()
	fr.setHealth("grok_4_1_fnr_eastus", false, 500)
	fr.setHealth("grok_4_1_fnr_westus", false, 300)
	fr.setHealth("vertex_dishaai_grok_4_1_fast_non_reasoning", false, 400)

	sel, ok := getFastestForGroup(ctx(), fr, groupGrokSales, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if sel.ConfigKey != "grok_4_1_fnr_westus" {
		t.Errorf("fastest = %q, want grok_4_1_fnr_westus", sel.ConfigKey)
	}
	if sel.UsingFallback {
		t.Error("did not expect fallback")
	}
}

func TestSelectionSkipsBlacklisted(t *testing.T) {
	fr := newFakeRedis()
	fr.setHealth("grok_4_1_fnr_westus", true, 100) // fastest but blacklisted
	fr.setHealth("vertex_dishaai_grok_4_1_fast_non_reasoning", false, 400)

	sel, ok := getFastestForGroup(ctx(), fr, groupGrokSales, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if sel.ConfigKey != "vertex_dishaai_grok_4_1_fast_non_reasoning" {
		t.Errorf("selected %q, want the vertex config (blacklisted faster one skipped)", sel.ConfigKey)
	}
}

func TestSelectionFallsBackToGPT41Group(t *testing.T) {
	fr := newFakeRedis()
	// No grok-sales health at all; gpt-4.1 group has one healthy endpoint.
	fr.setHealth("azure_gpt_4_1_us_west", false, 250)

	sel, ok := getFastestForGroup(ctx(), fr, groupGrokSales, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if !sel.UsingFallback || sel.SelectedGroup != groupGPT41 {
		t.Errorf("expected fallback to gpt-4.1 group, got %+v", sel)
	}
	if sel.ConfigKey != "azure_gpt_4_1_us_west" {
		t.Errorf("selected %q, want azure_gpt_4_1_us_west", sel.ConfigKey)
	}
}

func TestSelectionFallbackKeyWhenNoHealth(t *testing.T) {
	fr := newFakeRedis() // empty: no health anywhere
	sel, ok := getFastestForGroup(ctx(), fr, groupGrokSales, "us")
	if !ok {
		t.Fatal("expected a last-resort selection")
	}
	if sel.ConfigKey != modelGroups[groupGPT41].Fallback {
		t.Errorf("selected %q, want gpt-4.1 fallback %q", sel.ConfigKey, modelGroups[groupGPT41].Fallback)
	}
	if !sel.UsingFallback {
		t.Error("expected using_fallback for last-resort")
	}
}

func TestSelectionGeminiGroupPicksHealthyEndpoint(t *testing.T) {
	fr := newFakeRedis()
	fr.setHealth("vertex_gemini_flash_3_1_lite", false, 300)
	fr.setHealth("openrouter_gemini_flash_3_1_lite", false, 200)

	sel, ok := getFastestForGroup(ctx(), fr, groupGemini31, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if sel.ConfigKey != "openrouter_gemini_flash_3_1_lite" || sel.UsingFallback {
		t.Fatalf("selection = %+v, want fastest gemini endpoint", sel)
	}
}

func TestSelectionGeminiGroupFallsBackToGPT41Group(t *testing.T) {
	fr := newFakeRedis()
	// No gemini health at all; gpt-4.1 group has one healthy endpoint.
	// Mirrors Python's LLMSwitchingService FALLBACK_MODEL_GROUP behavior.
	fr.setHealth("azure_gpt_4_1_us_west", false, 250)

	sel, ok := getFastestForGroup(ctx(), fr, groupGemini31, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if !sel.UsingFallback || sel.SelectedGroup != groupGPT41 || sel.ConfigKey != "azure_gpt_4_1_us_west" {
		t.Fatalf("selection = %+v, want gpt-4.1 group fallback", sel)
	}
}

func TestSelectionOneEndpointGroupUsesOwnFallback(t *testing.T) {
	fr := newFakeRedis()
	fr.setHealth("openai_gpt_4_1", false, 1)

	sel, ok := getFastestForGroup(ctx(), fr, groupFollowUpDynamic, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if sel.ConfigKey != "openrouter_gemma_4_31b_it" {
		t.Fatalf("selected %q, want dynamic follow-up endpoint", sel.ConfigKey)
	}
	if sel.UsingFallback || sel.SelectedGroup != groupFollowUpDynamic {
		t.Fatalf("selection = %+v, want own-group fallback without cross-group fallback", sel)
	}
}

func TestSelectionGuidanceGroupUsesOwnFallback(t *testing.T) {
	fr := newFakeRedis()
	fr.setHealth("openai_gpt_4_1", false, 1)

	sel, ok := getFastestForGroup(ctx(), fr, groupGPTOSS120Fast, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if sel.ConfigKey != "openrouter_gpt_oss_120b" {
		t.Fatalf("selected %q, want guidance fallback endpoint", sel.ConfigKey)
	}
	if sel.UsingFallback || sel.SelectedGroup != groupGPTOSS120Fast {
		t.Fatalf("selection = %+v, want guidance own-group fallback", sel)
	}
}

func TestSelectionRegionFilter(t *testing.T) {
	// south_india azure config must never be selected for region us even
	// if it has the best latency.
	fr := newFakeRedis()
	fr.setHealth("azure_gpt_4_1_south_india", false, 1)
	fr.setHealth("openai_gpt_4_1", false, 900)

	sel, ok := getFastestForGroup(ctx(), fr, groupGPT41, "us")
	if !ok {
		t.Fatal("expected a selection")
	}
	if sel.ConfigKey == "azure_gpt_4_1_south_india" {
		t.Error("south_india config must be filtered out for region us")
	}
}

func TestSelectionLatencyAveragesLastFive(t *testing.T) {
	// 7 runs; only the last 5 (3,4,5,6,7 -> mean 5) should count.
	h := endpointHealth{}
	for _, v := range []float64{100, 200, 3, 4, 5, 6, 7} {
		vv := v
		h.PollRuns = append(h.PollRuns, pollRun{AdjustedLatencyMs: &vv})
	}
	got, ok := h.selectionLatency()
	if !ok {
		t.Fatal("expected latency")
	}
	if got != 5 {
		t.Errorf("selectionLatency = %v, want 5 (mean of last 5)", got)
	}
}

// --- blacklist write-back ---

func TestBlacklistForLiveErrorPreservesPollRuns(t *testing.T) {
	fr := newFakeRedis()
	fr.setHealth("grok_4_1_fnr_eastus", false, 300, 320)

	if err := blacklistForLiveError(ctx(), fr, "grok_4_1_fnr_eastus", "rate_limit", 429, "too many requests"); err != nil {
		t.Fatalf("blacklist: %v", err)
	}

	h := parseHealth(fr.get(healthKey("grok_4_1_fnr_eastus")))
	if !h.Blacklisted {
		t.Error("expected blacklisted")
	}
	if h.BlacklistReason == nil || *h.BlacklistReason != blacklistReasonError {
		t.Errorf("blacklist reason = %v, want %q", h.BlacklistReason, blacklistReasonError)
	}
	if h.LastLiveCallError == nil || h.LastLiveCallError.ErrorType != "rate_limit" {
		t.Errorf("last_live_call_error = %+v, want rate_limit", h.LastLiveCallError)
	}
	if len(h.PollRuns) != 2 {
		t.Errorf("poll_runs preserved count = %d, want 2", len(h.PollRuns))
	}
}

// --- request shaping ---

func readBody(t *testing.T, req *http.Request) map[string]any {
	t.Helper()
	b, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	return m
}

func TestBuildRequestGrok(t *testing.T) {
	t.Setenv("GROK_4_1_FNR_EASTUS_API_KEY", "grok-key")
	t.Setenv("GROK_4_1_FNR_EASTUS_ENDPOINT", "https://eastus.example.com/openai/v1")

	r := &Router{cfg: Config{Group: groupGrokSales}, httpClient: &http.Client{}}
	req, err := r.buildRequest(ctx(), endpointConfigs["grok_4_1_fnr_eastus"], testLLMRequest())
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	if req.URL.String() != "https://eastus.example.com/openai/v1/chat/completions" {
		t.Errorf("url = %s", req.URL.String())
	}
	if got := req.Header.Get("Authorization"); got != "Bearer grok-key" {
		t.Errorf("auth = %q", got)
	}
	body := readBody(t, req)
	if body["model"] != "grok-4-1-fast-non-reasoning" {
		t.Errorf("model = %v", body["model"])
	}
	if body["stream"] != true {
		t.Errorf("stream = %v", body["stream"])
	}
	streamOptions, ok := body["stream_options"].(map[string]any)
	if !ok || streamOptions["include_usage"] != true {
		t.Errorf("stream_options = %v, want include_usage=true", body["stream_options"])
	}
	if body["temperature"] != float64(0) {
		t.Errorf("temperature = %v", body["temperature"])
	}
	// The Azure-hosted Grok endpoint rejects a top-level "reasoning"
	// argument, so it must NOT be sent (only OpenRouter Grok carries it).
	if _, hasReasoning := body["reasoning"]; hasReasoning {
		t.Errorf("Azure-hosted grok must not carry a reasoning field, got %v", body["reasoning"])
	}
}

func TestExtraBodyFor(t *testing.T) {
	// Azure-hosted grok + vertex grok (the sales group) carry nothing.
	if eb := extraBodyFor(endpointConfigs["grok_4_1_fnr_eastus"]); eb != nil {
		t.Errorf("azure grok extra body = %v, want nil", eb)
	}
	if eb := extraBodyFor(endpointConfigs["vertex_dishaai_grok_4_1_fast_non_reasoning"]); eb != nil {
		t.Errorf("vertex grok extra body = %v, want nil", eb)
	}
	// gpt-4.1 (azure/openai) carry nothing.
	if eb := extraBodyFor(endpointConfigs["openai_gpt_4_1"]); eb != nil {
		t.Errorf("openai gpt-4.1 extra body = %v, want nil", eb)
	}
	// OpenRouter Grok would disable reasoning (sanity check on the helper).
	openrouterGrok := endpointConfig{Provider: providerOpenRouter, Model: "x-ai/grok-4.1-fast"}
	eb := extraBodyFor(openrouterGrok)
	reasoning, ok := eb["reasoning"].(map[string]any)
	if !ok || reasoning["enabled"] != false {
		t.Errorf("openrouter grok extra body = %v, want reasoning.enabled=false", eb)
	}
}

func TestBuildRequestAzureUsesApiKeyHeaderAndDeploymentPath(t *testing.T) {
	t.Setenv("AZURE_OPENAI_US_EAST_API_KEY", "azure-key")
	t.Setenv("AZURE_OPENAI_US_EAST_ENDPOINT", "https://x.openai.azure.com")

	r := &Router{cfg: Config{Group: groupGPT41}, httpClient: &http.Client{}}
	req, err := r.buildRequest(ctx(), endpointConfigs["azure_gpt_4_1_us_east"], testLLMRequest())
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	wantURL := "https://x.openai.azure.com/openai/deployments/gpt-4.1/chat/completions?api-version=2024-02-01"
	if req.URL.String() != wantURL {
		t.Errorf("url = %s, want %s", req.URL.String(), wantURL)
	}
	if req.Header.Get("api-key") != "azure-key" {
		t.Errorf("api-key header = %q", req.Header.Get("api-key"))
	}
	if req.Header.Get("Authorization") != "" {
		t.Errorf("azure must not use Authorization header, got %q", req.Header.Get("Authorization"))
	}
	body := readBody(t, req)
	if _, hasReasoning := body["reasoning"]; hasReasoning {
		t.Error("gpt-4.1 must not carry the grok reasoning field")
	}
}

func TestBuildRequestIncludesTools(t *testing.T) {
	t.Setenv("GROK_4_1_FNR_EASTUS_API_KEY", "grok-key")
	t.Setenv("GROK_4_1_FNR_EASTUS_ENDPOINT", "https://eastus.example.com/openai/v1")

	r := &Router{cfg: Config{Group: groupGrokSales}, httpClient: &http.Client{}}
	req, err := r.buildRequest(ctx(), endpointConfigs["grok_4_1_fnr_eastus"], vpc.LLMRequest{
		Messages: []vpc.Message{{Role: "user", Content: "hi"}},
		Tools: []vpc.ToolDefinition{{
			Type: "function",
			Function: vpc.ToolFunction{
				Name:        "get_guidance",
				Description: "Fetch guidance for the next turn.",
				Parameters:  map[string]any{"type": "object"},
			},
		}},
		ToolChoice: "auto",
	})
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	body := readBody(t, req)
	if body["tool_choice"] != "auto" {
		t.Fatalf("tool_choice = %v, want auto", body["tool_choice"])
	}
	tools, ok := body["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %#v, want one tool definition", body["tools"])
	}
	tool, ok := tools[0].(map[string]any)
	if !ok || tool["type"] != "function" {
		t.Fatalf("tool = %#v, want function tool", tools[0])
	}
	fn, ok := tool["function"].(map[string]any)
	if !ok || fn["name"] != "get_guidance" {
		t.Fatalf("function = %#v, want get_guidance", tool["function"])
	}
}

func TestBuildRequestOpenRouterGemmaProviderPreferences(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "or-key")

	r := &Router{cfg: Config{}, httpClient: &http.Client{}}
	req, err := r.buildRequest(ctx(), endpointConfigs["openrouter_gemma_4_31b_it"], testLLMRequest())
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	if req.URL.String() != "https://openrouter.ai/api/v1/chat/completions" {
		t.Errorf("url = %s", req.URL.String())
	}
	if got := req.Header.Get("Authorization"); got != "Bearer or-key" {
		t.Errorf("auth = %q", got)
	}
	body := readBody(t, req)
	if body["model"] != "google/gemma-4-31b-it" {
		t.Errorf("model = %v", body["model"])
	}
	if body["temperature"] != 0.5 {
		t.Errorf("temperature = %v, want 0.5", body["temperature"])
	}
	provider, ok := body["provider"].(map[string]any)
	if !ok {
		t.Fatalf("provider = %#v, want object", body["provider"])
	}
	order, ok := provider["order"].([]any)
	if !ok || len(order) != 4 || order[0] != "modelrun/fp4" || order[1] != "deepinfra/fp8" || order[2] != "together" || order[3] != "parasail/fp8" {
		t.Fatalf("provider.order = %#v", provider["order"])
	}
	only, ok := provider["only"].([]any)
	if !ok || len(only) != 4 || only[0] != "modelrun/fp4" || only[1] != "deepinfra/fp8" || only[2] != "together" || only[3] != "parasail/fp8" {
		t.Fatalf("provider.only = %#v", provider["only"])
	}
	ignored, ok := provider["ignore"].([]any)
	if !ok || len(ignored) != 3 || ignored[0] != "google-ai-studio" {
		t.Fatalf("provider.ignore = %#v", provider["ignore"])
	}
	if provider["allow_fallbacks"] != true {
		t.Fatalf("provider.allow_fallbacks = %#v, want true", provider["allow_fallbacks"])
	}
}

func TestBuildRequestGeminiFlashLiteTemperature(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "or-key")

	r := &Router{cfg: Config{}, httpClient: &http.Client{}}
	req, err := r.buildRequest(ctx(), endpointConfigs["openrouter_gemini_flash_3_1_lite"], testLLMRequest())
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	body := readBody(t, req)
	// Python follow-up runs gemini-flash-3.1-lite at temperature 0.5.
	if body["temperature"] != 0.5 {
		t.Errorf("temperature = %v, want 0.5", body["temperature"])
	}
}

func TestBuildRequestCerebrasGPTOSSMaxTokens(t *testing.T) {
	t.Setenv("CEREBRAS_ENTERPRISE_API_KEY", "cb-key")

	r := &Router{cfg: Config{}, httpClient: &http.Client{}}
	req, err := r.buildRequest(ctx(), endpointConfigs["cerebras_gpt_oss_120b"], testLLMRequest())
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}
	if req.URL.String() != "https://api.cerebras.ai/v1/chat/completions" {
		t.Errorf("url = %s", req.URL.String())
	}
	if got := req.Header.Get("Authorization"); got != "Bearer cb-key" {
		t.Errorf("auth = %q", got)
	}
	body := readBody(t, req)
	if body["model"] != "gpt-oss-120b" {
		t.Errorf("model = %v", body["model"])
	}
	if body["max_tokens"] != float64(500) {
		t.Errorf("max_tokens = %v, want 500", body["max_tokens"])
	}
}

// --- classify ---

func TestClassifyError(t *testing.T) {
	cases := []struct {
		status int
		msg    string
		want   string
	}{
		{429, "", errorTypeRateLimit},
		{500, "internal", errorTypeOther},
		{0, "Rate limit exceeded", errorTypeRateLimit},
		{0, "too many requests", errorTypeRateLimit},
		{0, "boom", errorTypeOther},
	}
	for _, c := range cases {
		if got := classifyError(c.status, c.msg); got != c.want {
			t.Errorf("classifyError(%d,%q) = %q, want %q", c.status, c.msg, got, c.want)
		}
	}
}

// --- streaming ---

func sseServer(t *testing.T, contents []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		for _, c := range contents {
			fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", c)
			if flusher != nil {
				flusher.Flush()
			}
		}
		io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7,\"total_tokens\":18}}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		io.WriteString(w, "data: [DONE]\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}))
}

func TestParseSSEChunkToolCallFragments(t *testing.T) {
	acc := vpc.NewToolCallAccumulator()
	lines := []string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_","arguments":"{\"situation\""}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"guidance","arguments":":\"pain\"}"}}]},"finish_reason":"tool_calls"}]}`,
	}

	var finishReason string
	for _, line := range lines {
		content, deltas, fr, _, _, _, ok := parseSSEChunk(line)
		if !ok {
			t.Fatalf("parseSSEChunk(%s) returned ok=false", line)
		}
		if content != "" {
			t.Fatalf("content = %q, want no text delta", content)
		}
		if fr != "" {
			finishReason = fr
		}
		for _, delta := range deltas {
			acc.Add(delta.index, delta.call)
		}
	}
	calls := acc.List()
	if len(calls) != 1 {
		t.Fatalf("tool calls = %+v, want one call", calls)
	}
	if calls[0].ID != "call_1" || calls[0].Type != "function" {
		t.Fatalf("call metadata = %+v, want id call_1 function", calls[0])
	}
	if calls[0].Function.Name != "get_guidance" {
		t.Fatalf("function name = %q, want get_guidance", calls[0].Function.Name)
	}
	if calls[0].Function.Arguments != `{"situation":"pain"}` {
		t.Fatalf("arguments = %q, want situation pain JSON", calls[0].Function.Arguments)
	}
	if finishReason != "tool_calls" {
		t.Fatalf("finish reason = %q, want tool_calls", finishReason)
	}
}

func TestStreamSuccessAndLog(t *testing.T) {
	server := sseServer(t, []string{"foo ", "bar"})
	defer server.Close()
	t.Setenv("GROK_4_1_FNR_EASTUS_API_KEY", "k")
	t.Setenv("GROK_4_1_FNR_EASTUS_ENDPOINT", server.URL)

	fr := newFakeRedis()
	fr.setHealth("grok_4_1_fnr_eastus", false, 300)

	logCh := make(chan CallLog, 1)
	r, err := New(Config{Group: groupGrokSales, Redis: fr, LogSink: func(c CallLog) { logCh <- c }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var tokens []string
	res, err := r.Stream(ctx(), testLLMRequest(), func(tok string) {
		tokens = append(tokens, tok)
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if res.Model != "grok-4-1-fast-non-reasoning" {
		t.Errorf("model = %q", res.Model)
	}
	if strings.Join(tokens, "") != "foo bar" {
		t.Errorf("tokens = %v", tokens)
	}

	select {
	case lg := <-logCh:
		if lg.ResponseContent != "foo bar" || !lg.Completed {
			t.Errorf("log = %+v", lg)
		}
		if lg.Deployment != "GROK_4_1_FNR_EASTUS" {
			t.Errorf("deployment = %q, want GROK_4_1_FNR_EASTUS", lg.Deployment)
		}
		if lg.PromptTokens != 11 || lg.CompletionTokens != 7 {
			t.Errorf("usage tokens = prompt %d completion %d, want 11/7", lg.PromptTokens, lg.CompletionTokens)
		}
	case <-time.After(2 * time.Second):
		t.Error("expected a CallLog from the sink")
	}
}

func TestStreamErrorBlacklistsAndTriggersPoll(t *testing.T) {
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, `{"error":"boom"}`)
	}))
	defer errServer.Close()
	t.Setenv("GROK_4_1_FNR_EASTUS_API_KEY", "k")
	t.Setenv("GROK_4_1_FNR_EASTUS_ENDPOINT", errServer.URL)

	pollHit := make(chan *http.Request, 1)
	pollServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pollHit <- r
		w.WriteHeader(http.StatusOK)
	}))
	defer pollServer.Close()

	fr := newFakeRedis()
	fr.setHealth("grok_4_1_fnr_eastus", false, 300)

	t.Setenv("LLM_POLL_TRIGGER_URL", pollServer.URL)
	r, err := New(Config{Group: groupGrokSales, Redis: fr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, streamErr := r.Stream(ctx(), testLLMRequest(), func(string) {})
	if streamErr == nil {
		t.Fatal("expected a stream error on 500")
	}

	// Blacklist is written synchronously before Stream returns.
	h := parseHealth(fr.get(healthKey("grok_4_1_fnr_eastus")))
	if !h.Blacklisted {
		t.Error("expected endpoint blacklisted after live error")
	}

	select {
	case req := <-pollHit:
		if req.Method != http.MethodPost {
			t.Errorf("poll method = %q, want POST", req.Method)
		}
	case <-time.After(2 * time.Second):
		t.Error("expected a poll trigger HTTP call")
	}
}

// --- vertex token ---

func TestVertexTokenMintAndCache(t *testing.T) {
	hits := 0
	var hitsMu sync.Mutex
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsMu.Lock()
		hits++
		hitsMu.Unlock()
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if r.FormValue("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			t.Errorf("grant_type = %q", r.FormValue("grant_type"))
		}
		if parts := strings.Split(r.FormValue("assertion"), "."); len(parts) != 3 {
			t.Errorf("assertion is not a 3-part JWT: %q", r.FormValue("assertion"))
		}
		json.NewEncoder(w).Encode(map[string]any{"access_token": "tok1", "expires_in": 3600})
	}))
	defer tokenServer.Close()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

	saJSON, _ := json.Marshal(map[string]string{
		"client_email": "sa@project.iam.gserviceaccount.com",
		"private_key":  string(pemBytes),
		"token_uri":    tokenServer.URL,
	})

	var loads int
	ts := newVertexTokenSource(func(context.Context) ([]byte, error) {
		loads++
		return saJSON, nil
	}, tokenServer.Client())
	tok, err := ts.Token(ctx())
	if err != nil || tok != "tok1" {
		t.Fatalf("Token = %q, %v", tok, err)
	}
	// Second call should be served from cache (no extra exchange).
	if _, err := ts.Token(ctx()); err != nil {
		t.Fatalf("Token (cached): %v", err)
	}
	hitsMu.Lock()
	defer hitsMu.Unlock()
	if hits != 1 {
		t.Errorf("token endpoint hits = %d, want 1 (cached)", hits)
	}
	if loads != 1 {
		t.Errorf("creds loader calls = %d, want 1 (loaded once)", loads)
	}
}
