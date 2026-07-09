package llmrouter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingHandler is a controllable fake LLM endpoint: it records
// request bodies, optionally delays (respecting request-context
// cancellation), then answers an SSE stream or an error status.
type recordingHandler struct {
	delay  time.Duration
	tokens []string
	status int // 0/200 = SSE success

	mu     sync.Mutex
	bodies []map[string]any
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	h.mu.Lock()
	h.bodies = append(h.bodies, body)
	h.mu.Unlock()

	if h.delay > 0 {
		select {
		case <-time.After(h.delay):
		case <-r.Context().Done():
			return
		}
	}
	if h.status != 0 && h.status != http.StatusOK {
		w.WriteHeader(h.status)
		_, _ = io.WriteString(w, `{"error":"boom"}`)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	flusher, _ := w.(http.Flusher)
	for _, c := range h.tokens {
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", c)
		if flusher != nil {
			flusher.Flush()
		}
	}
	_, _ = io.WriteString(w, "data: [DONE]\n\n")
}

func (h *recordingHandler) hits() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.bodies)
}

func (h *recordingHandler) lastBody(t *testing.T) map[string]any {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.bodies) == 0 {
		t.Fatal("no request bodies recorded")
	}
	return h.bodies[len(h.bodies)-1]
}

func overrideEndpointBaseURL(t *testing.T, key, baseURL string) {
	t.Helper()
	orig, ok := endpointConfigs[key]
	if !ok {
		t.Fatalf("unknown endpoint config %q", key)
	}
	cfg := orig
	cfg.BaseURL = baseURL
	endpointConfigs[key] = cfg
	t.Cleanup(func() { endpointConfigs[key] = orig })
}

// newHedgedForTest points the gpt-oss120-fast-hedged pair at the two
// fake endpoints and returns the client plus the fake Redis (for
// blacklist assertions).
func newHedgedForTest(t *testing.T, primary, hedge http.Handler, cfg HedgedConfig) (*Hedged, *fakeRedis) {
	t.Helper()
	t.Setenv("CEREBRAS_ENTERPRISE_API_KEY", "cb-key")
	t.Setenv("OPENROUTER_API_KEY", "or-key")
	primaryServer := httptest.NewServer(primary)
	t.Cleanup(primaryServer.Close)
	hedgeServer := httptest.NewServer(hedge)
	t.Cleanup(hedgeServer.Close)
	overrideEndpointBaseURL(t, "cerebras_gpt_oss_120b", primaryServer.URL)
	overrideEndpointBaseURL(t, "openrouter_gpt_oss_120b_throughput", hedgeServer.URL)

	fr := newFakeRedis()
	cfg.Pair = GroupGPTOSS120FastHedged
	cfg.Redis = fr
	h, err := NewHedged(cfg)
	if err != nil {
		t.Fatalf("NewHedged: %v", err)
	}
	return h, fr
}

func isBlacklisted(t *testing.T, fr *fakeRedis, configKey string) bool {
	t.Helper()
	raw := fr.get(healthKey(configKey))
	if raw == nil {
		return false
	}
	var h endpointHealth
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatalf("decode health for %s: %v", configKey, err)
	}
	return h.Blacklisted
}

func collectText(t *testing.T, h *Hedged, ctx context.Context) (string, error) {
	t.Helper()
	var b strings.Builder
	_, err := h.Stream(ctx, testLLMRequest(), func(tok string) { b.WriteString(tok) })
	return b.String(), err
}

func TestHedgedFastPrimaryNeverFiresHedge(t *testing.T) {
	primary := &recordingHandler{tokens: []string{"pri", "mary"}}
	hedge := &recordingHandler{tokens: []string{"hedge"}}
	h, _ := newHedgedForTest(t, primary, hedge, HedgedConfig{HedgeThreshold: 2 * time.Second})

	text, err := collectText(t, h, ctx())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if text != "primary" {
		t.Fatalf("text = %q, want primary", text)
	}
	if hedge.hits() != 0 {
		t.Fatalf("hedge hits = %d, want 0", hedge.hits())
	}
}

// Threshold hit → hedge fires in parallel; the slow-but-healthy primary
// is NOT cancelled at the threshold and still wins when the hedge is
// slower. The cancelled hedge is never blacklisted and never logged.
func TestHedgedPrimaryNotCancelledAtThresholdAndWins(t *testing.T) {
	primary := &recordingHandler{delay: 300 * time.Millisecond, tokens: []string{"primary"}}
	hedge := &recordingHandler{delay: 10 * time.Second, tokens: []string{"hedge"}}

	var logMu sync.Mutex
	var logged []CallLog
	h, fr := newHedgedForTest(t, primary, hedge, HedgedConfig{
		HedgeThreshold: 50 * time.Millisecond,
		LogSink: func(c CallLog) {
			logMu.Lock()
			logged = append(logged, c)
			logMu.Unlock()
		},
	})

	text, err := collectText(t, h, ctx())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if text != "primary" {
		t.Fatalf("text = %q, want primary (not cancelled at threshold)", text)
	}
	if hedge.hits() != 1 {
		t.Fatalf("hedge hits = %d, want 1 (fired at threshold)", hedge.hits())
	}
	if isBlacklisted(t, fr, "openrouter_gpt_oss_120b_throughput") {
		t.Fatal("cancelled hedge loser must not be blacklisted")
	}
	// Only the completed primary attempt logs; give the detached sink
	// goroutine a moment.
	time.Sleep(100 * time.Millisecond)
	logMu.Lock()
	defer logMu.Unlock()
	if len(logged) != 1 || logged[0].ConfigKey != "cerebras_gpt_oss_120b" || !logged[0].Completed {
		t.Fatalf("logged = %+v, want one completed primary entry", logged)
	}
}

func TestHedgedSlowPrimaryLosesAndIsNotBlacklisted(t *testing.T) {
	primary := &recordingHandler{delay: 10 * time.Second, tokens: []string{"primary"}}
	hedge := &recordingHandler{tokens: []string{"hedge"}}
	h, fr := newHedgedForTest(t, primary, hedge, HedgedConfig{HedgeThreshold: 50 * time.Millisecond})

	text, err := collectText(t, h, ctx())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if text != "hedge" {
		t.Fatalf("text = %q, want hedge", text)
	}
	if isBlacklisted(t, fr, "cerebras_gpt_oss_120b") {
		t.Fatal("slow cancelled primary must not be blacklisted")
	}
}

// Primary error before the threshold → the hedge fires immediately and
// sequentially (a 10s threshold would time the test out if the race
// path were taken); the errored primary is blacklisted.
func TestHedgedPrimaryErrorBeforeThresholdSequentialHedge(t *testing.T) {
	primary := &recordingHandler{status: http.StatusInternalServerError}
	hedge := &recordingHandler{tokens: []string{"hedge"}}
	h, fr := newHedgedForTest(t, primary, hedge, HedgedConfig{HedgeThreshold: 10 * time.Second})

	start := time.Now()
	text, err := collectText(t, h, ctx())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if text != "hedge" {
		t.Fatalf("text = %q, want hedge", text)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("took %s — hedge did not fire immediately on primary error", elapsed)
	}
	if !isBlacklisted(t, fr, "cerebras_gpt_oss_120b") {
		t.Fatal("errored primary should be blacklisted")
	}
	if isBlacklisted(t, fr, "openrouter_gpt_oss_120b_throughput") {
		t.Fatal("successful hedge must not be blacklisted")
	}
}

// After the threshold, the first finisher failing means the other
// attempt is awaited: the hedge errors quickly, the slow primary still
// succeeds. Both attempts completed, so both log.
func TestHedgedFirstFinisherFailsAwaitsOther(t *testing.T) {
	primary := &recordingHandler{delay: 400 * time.Millisecond, tokens: []string{"primary"}}
	hedge := &recordingHandler{status: http.StatusTooManyRequests}

	var logMu sync.Mutex
	var logged []CallLog
	h, fr := newHedgedForTest(t, primary, hedge, HedgedConfig{
		HedgeThreshold: 50 * time.Millisecond,
		LogSink: func(c CallLog) {
			logMu.Lock()
			logged = append(logged, c)
			logMu.Unlock()
		},
	})

	text, err := collectText(t, h, ctx())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if text != "primary" {
		t.Fatalf("text = %q, want primary awaited after hedge failure", text)
	}
	if !isBlacklisted(t, fr, "openrouter_gpt_oss_120b_throughput") {
		t.Fatal("rate-limited hedge should be blacklisted")
	}
	time.Sleep(100 * time.Millisecond)
	logMu.Lock()
	defer logMu.Unlock()
	if len(logged) != 2 {
		t.Fatalf("logged %d entries, want 2 (both attempts completed)", len(logged))
	}
}

func TestHedgedBothFailReturnsJoinedError(t *testing.T) {
	primary := &recordingHandler{status: http.StatusInternalServerError}
	hedge := &recordingHandler{status: http.StatusInternalServerError}
	h, fr := newHedgedForTest(t, primary, hedge, HedgedConfig{HedgeThreshold: 50 * time.Millisecond})

	_, err := collectText(t, h, ctx())
	if err == nil {
		t.Fatal("want error when both attempts fail")
	}
	if !isBlacklisted(t, fr, "cerebras_gpt_oss_120b") || !isBlacklisted(t, fr, "openrouter_gpt_oss_120b_throughput") {
		t.Fatal("both completed-error attempts should be blacklisted")
	}
}

// Parent-ctx cancellation (barge-in/EndFrame) cancels both attempts, is
// returned as context.Canceled, never blacklists, and never logs.
func TestHedgedParentCancellation(t *testing.T) {
	primary := &recordingHandler{delay: 10 * time.Second, tokens: []string{"primary"}}
	hedge := &recordingHandler{delay: 10 * time.Second, tokens: []string{"hedge"}}

	var logMu sync.Mutex
	var logged []CallLog
	h, fr := newHedgedForTest(t, primary, hedge, HedgedConfig{
		HedgeThreshold: 50 * time.Millisecond,
		LogSink: func(c CallLog) {
			logMu.Lock()
			logged = append(logged, c)
			logMu.Unlock()
		},
	})

	callCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond) // past the threshold: both in flight
		cancel()
	}()
	_, err := collectText(t, h, callCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if isBlacklisted(t, fr, "cerebras_gpt_oss_120b") || isBlacklisted(t, fr, "openrouter_gpt_oss_120b_throughput") {
		t.Fatal("parent cancellation must never blacklist")
	}
	time.Sleep(100 * time.Millisecond)
	logMu.Lock()
	defer logMu.Unlock()
	if len(logged) != 0 {
		t.Fatalf("logged = %+v, want none for cancelled attempts", logged)
	}
}

// Temperature/MaxTokens overrides land in both attempts' bodies and the
// hedge leg carries OpenRouter's provider.sort=throughput.
func TestHedgedRequestShaping(t *testing.T) {
	primary := &recordingHandler{delay: 10 * time.Second, tokens: []string{"primary"}}
	hedge := &recordingHandler{tokens: []string{"hedge"}}
	h, _ := newHedgedForTest(t, primary, hedge, HedgedConfig{
		HedgeThreshold: 50 * time.Millisecond,
		Temperature:    floatPtr(0.7),
		MaxTokens:      intPtr(4000),
	})

	if _, err := collectText(t, h, ctx()); err != nil {
		t.Fatalf("Stream: %v", err)
	}

	for _, handler := range []*recordingHandler{primary, hedge} {
		body := handler.lastBody(t)
		if body["temperature"] != 0.7 {
			t.Fatalf("temperature = %v, want 0.7", body["temperature"])
		}
		if body["max_tokens"] != float64(4000) {
			t.Fatalf("max_tokens = %v, want 4000 override", body["max_tokens"])
		}
	}
	provider, _ := hedge.lastBody(t)["provider"].(map[string]any)
	if provider == nil || provider["sort"] != "throughput" {
		t.Fatalf("hedge provider = %#v, want sort=throughput", hedge.lastBody(t)["provider"])
	}
	if _, ok := primary.lastBody(t)["provider"]; ok {
		t.Fatal("primary body must not carry the OpenRouter provider field")
	}
}

func TestNewHedgedValidation(t *testing.T) {
	if _, err := NewHedged(HedgedConfig{Pair: "no-such-pair", Redis: newFakeRedis()}); err == nil {
		t.Fatal("unknown pair: want error")
	}
	if _, err := NewHedged(HedgedConfig{Pair: GroupGPTOSS120FastHedged}); err == nil {
		t.Fatal("nil redis: want error")
	}
}

// Fixed-endpoint mode on the plain Router: selection bypasses health
// entirely (no health keys exist here) and an unknown key fails at New.
func TestRouterFixedEndpointMode(t *testing.T) {
	server := sseServer(t, []string{"ok"})
	defer server.Close()
	t.Setenv("CEREBRAS_ENTERPRISE_API_KEY", "cb-key")
	overrideEndpointBaseURL(t, "cerebras_gpt_oss_120b", server.URL)

	fr := newFakeRedis()
	r, err := New(Config{FixedEndpoint: "cerebras_gpt_oss_120b", Redis: fr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var b strings.Builder
	res, err := r.Stream(ctx(), testLLMRequest(), func(tok string) { b.WriteString(tok) })
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if b.String() != "ok" || res.Model != "gpt-oss-120b" {
		t.Fatalf("text=%q model=%q", b.String(), res.Model)
	}

	if _, err := New(Config{FixedEndpoint: "no_such_config", Redis: fr}); err == nil {
		t.Fatal("unknown fixed endpoint: want error")
	}
}
