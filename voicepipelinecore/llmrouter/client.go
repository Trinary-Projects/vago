package llmrouter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	vpc "github.com/jaideep329/talk-go/voicepipelinecore"
)

// liveCallSlowThresholdMs matches LIVE_CALL_SLOW_THRESHOLD_MS: a
// completed call slower than this triggers a re-poll.
const liveCallSlowThresholdMs = 3000.0

const defaultRegion = "us"

var errNoEndpoint = errors.New("llmrouter: no endpoint available for group")

// pollTriggerURLEnv is the env var holding the public US poll-trigger
// Lambda Function URL.
const pollTriggerURLEnv = "LLM_POLL_TRIGGER_URL"

// Config configures a Router. Group is the only thing a caller really
// has to choose. Redis (the shared connection) and the optional LogSink
// are injected; infrastructure settings (the poll-trigger URL, the
// Vertex service-account key) are read from the environment by the
// router itself so callers don't have to plumb them through.
type Config struct {
	// Group is the model group to select from (e.g. "grok-4.1-fast-sales").
	Group string
	// Region filters endpoints (default "us").
	Region string
	// Redis reads endpoint health and holds the poll lock.
	Redis RedisStore
	// Logger is optional.
	Logger *log.Logger
	// HTTPClient is optional; defaults to a no-timeout client (streaming
	// relies on the per-call context for cancellation).
	HTTPClient *http.Client
	// LogSink, when set, receives a CallLog after every completion. The
	// router invokes it on a detached goroutine (best-effort), so the
	// sink must not block the call. Used to enqueue Disha's LLM logging.
	LogSink func(CallLog)
	// PromptMetadata is static per bot prompt/context and is forwarded
	// to the log sink without putting prompt concerns into voicepipelinecore.
	PromptMetadata map[string]any

	// FixedEndpoint pins the router to a single endpoint config key,
	// bypassing health-ranked selection entirely (Python's failover
	// service picks configs by fixed list position, not health). In this
	// mode the router never triggers a poll — there is nothing to
	// re-rank — but blacklist write-back on completed transient errors
	// is kept. Group is ignored when set. Used by the hedged one-shot
	// client.
	FixedEndpoint string

	// Temperature/MaxTokens override the endpoint config's values for
	// this client (Python passes them per call; deep thinking uses
	// 0.7 / 4000 where the registry pins get_guidance's 500).
	Temperature *float64
	MaxTokens   *int
}

// Router implements voicepipelinecore.LLMClient with health-based
// endpoint selection, OpenAI-format streaming, blacklisting, and
// re-poll triggering.
type Router struct {
	cfg            Config
	httpClient     *http.Client
	pollTriggerURL string
}

// New builds a Router for the given model group. The poll-trigger URL is
// read from the environment; the Vertex token source is process-wide and
// lazily loads its key from S3 on first use, so an absent/bad Vertex key
// is non-fatal (Vertex calls fail and are blacklisted, and selection
// falls back to other endpoints).
func New(cfg Config) (*Router, error) {
	if cfg.Redis == nil {
		return nil, errors.New("llmrouter: Config.Redis is required")
	}
	if cfg.FixedEndpoint != "" {
		if _, ok := endpointConfigs[cfg.FixedEndpoint]; !ok {
			return nil, fmt.Errorf("llmrouter: unknown fixed endpoint %q", cfg.FixedEndpoint)
		}
	} else {
		if cfg.Group == "" {
			return nil, errors.New("llmrouter: Config.Group is required")
		}
		if _, ok := modelGroups[cfg.Group]; !ok {
			return nil, fmt.Errorf("llmrouter: unknown model group %q", cfg.Group)
		}
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &Router{
		cfg:            cfg,
		httpClient:     httpClient,
		pollTriggerURL: strings.TrimSpace(os.Getenv(pollTriggerURLEnv)),
	}, nil
}

// selectEndpoint resolves the endpoint for one call: the pinned config
// in fixed-endpoint mode, health-ranked group selection otherwise.
func (r *Router) selectEndpoint(ctx context.Context) (selection, bool) {
	if r.cfg.FixedEndpoint != "" {
		return selection{ConfigKey: r.cfg.FixedEndpoint}, true
	}
	return getFastestForGroup(ctx, r.cfg.Redis, r.cfg.Group, r.region())
}

func (r *Router) region() string {
	if r.cfg.Region != "" {
		return r.cfg.Region
	}
	return defaultRegion
}

func (r *Router) logf(format string, args ...any) {
	if r.cfg.Logger != nil {
		r.cfg.Logger.Printf("llmrouter: "+format, args...)
	}
}

// Stream selects the fastest healthy endpoint, streams one OpenAI-format
// completion, and applies the live-call resilience policy: blacklist +
// reselect-next-turn + re-poll on error, re-poll on slow completion, and
// re-poll for fallback recovery. It returns the model that served the
// turn so the LLMProcessor can report it.
func (r *Router) Stream(ctx context.Context, llmReq vpc.LLMRequest, onToken func(string)) (res vpc.LLMResult, err error) {
	sel, ok := r.selectEndpoint(ctx)
	if !ok {
		return vpc.LLMResult{}, errNoEndpoint
	}
	cfg, ok := endpointConfigs[sel.ConfigKey]
	if !ok {
		return vpc.LLMResult{}, fmt.Errorf("llmrouter: unknown config %q", sel.ConfigKey)
	}
	r.logf("selected endpoint cfg=%s model=%s group=%s fallback=%v", cfg.Key, cfg.Model, sel.SelectedGroup, sel.UsingFallback)

	res = vpc.LLMResult{Model: cfg.Model}
	var (
		responseContent strings.Builder
		statusCode      int
		errType         string
		finishReason    string
		usage           tokenUsage
	)
	// Emit the per-call log on the way out (best-effort, off the hot path).
	defer func() {
		if r.cfg.LogSink == nil {
			return
		}
		entry := CallLog{
			Model:            cfg.Model,
			ConfigKey:        cfg.Key,
			Deployment:       deploymentName(cfg),
			Request:          llmReq,
			ResponseContent:  responseContent.String(),
			ToolCalls:        res.ToolCalls,
			PromptMetadata:   r.cfg.PromptMetadata,
			PromptTokens:     usage.PromptTokens,
			CompletionTokens: usage.CompletionTokens,
			TTFBMs:           msFromDuration(res.TTFB),
			TotalMs:          msFromDuration(res.Total),
			StatusCode:       statusCode,
			Completed:        err == nil && !res.Interrupted,
			Interrupted:      res.Interrupted,
			ErrorType:        errType,
			FinishReason:     finishReason,
			UsingFallback:    sel.UsingFallback,
			SelectedGroup:    sel.SelectedGroup,
		}
		if err != nil {
			entry.ErrorMessage = err.Error()
		}
		sink := r.cfg.LogSink
		go func() {
			defer func() {
				if p := recover(); p != nil {
					r.logf("log sink panic: %v", p)
				}
			}()
			sink(entry)
		}()
	}()

	req, buildErr := r.buildRequest(ctx, cfg, llmReq)
	if buildErr != nil {
		// A build error (missing key/creds) is treated like a live error:
		// blacklist the endpoint so the next turn skips it.
		errType = classifyError(0, buildErr.Error())
		r.handleError(cfg.Key, 0, buildErr.Error())
		return res, buildErr
	}

	start := time.Now()
	resp, doErr := r.httpClient.Do(req)
	if doErr != nil {
		res.Total = time.Since(start)
		res.Interrupted = ctx.Err() != nil
		if ctx.Err() == nil {
			errType = classifyError(0, doErr.Error())
			r.handleError(cfg.Key, 0, doErr.Error())
		}
		return res, doErr
	}
	defer resp.Body.Close()

	statusCode = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		apiErr := &apiError{statusCode: resp.StatusCode, body: strings.TrimSpace(string(body))}
		res.Total = time.Since(start)
		errType = classifyError(resp.StatusCode, apiErr.body)
		r.handleError(cfg.Key, resp.StatusCode, apiErr.body)
		return res, apiErr
	}

	firstToken := true
	sawDone := false
	toolCalls := vpc.NewToolCallAccumulator()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			res.Total = time.Since(start)
			res.Interrupted = true
			return res, ctx.Err()
		}
		content, toolDeltas, fr, chunkUsage, hasUsage, done, ok := parseSSEChunk(scanner.Text())
		if done {
			sawDone = true
			break
		}
		if !ok {
			continue
		}
		if hasUsage {
			usage = chunkUsage
		}
		if fr != "" {
			finishReason = fr
		}
		if len(toolDeltas) > 0 {
			if firstToken {
				firstToken = false
				res.TTFB = time.Since(start)
			}
			for _, delta := range toolDeltas {
				toolCalls.Add(delta.index, delta.call)
			}
		}
		if content == "" {
			continue
		}
		if firstToken {
			firstToken = false
			res.TTFB = time.Since(start)
		}
		responseContent.WriteString(content)
		onToken(content)
	}

	res.Total = time.Since(start)
	res.Interrupted = ctx.Err() != nil
	res.ToolCalls = toolCalls.List()

	// Truncation diagnostics: a clean finish is finish_reason="stop"
	// followed by [DONE]. Anything else on a non-interrupted turn means
	// the response was likely cut short (the bot speaks a half sentence).
	if !res.Interrupted {
		switch {
		case scanner.Err() != nil:
			r.logf("stream read error (likely truncated) cfg=%s chars=%d finish_reason=%q: %v", cfg.Key, responseContent.Len(), finishReason, scanner.Err())
		case finishReason != "" && finishReason != "stop":
			r.logf("stream finished reason=%q (likely truncated) cfg=%s chars=%d", finishReason, cfg.Key, responseContent.Len())
		case !sawDone && finishReason == "":
			r.logf("stream ended without [DONE]/finish_reason (possible truncation) cfg=%s chars=%d", cfg.Key, responseContent.Len())
		}
	}

	switch {
	case msFromDuration(res.Total) > liveCallSlowThresholdMs:
		r.triggerPoll("slow_live_call")
	case sel.UsingFallback:
		r.triggerPoll("fallback_recovery")
	}
	return res, nil
}

// handleError blacklists the endpoint for a live-call error and triggers
// a re-poll. There is no in-turn retry (strict Python parity): the turn
// fails and the next turn's fresh selection avoids the blacklisted
// endpoint. The blacklist write uses a detached context so it completes
// even if the turn's context was cancelled.
func (r *Router) handleError(configKey string, statusCode int, errMsg string) {
	errType := classifyError(statusCode, errMsg)
	r.logf("live error cfg=%s status=%d type=%s: %s", configKey, statusCode, errType, truncate(errMsg, 300))

	bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := blacklistForLiveError(bgCtx, r.cfg.Redis, configKey, errType, statusCode, errMsg); err != nil {
		r.logf("blacklist write failed for %s: %v", configKey, err)
	}
	r.triggerPoll("live_call_error")
}

// parseSSEChunk parses one SSE line of an OpenAI-format stream.
//   - done=true for the terminal "data: [DONE]" line.
//   - ok=false for non-data lines / unparseable chunks (skip them).
//   - content is the text delta (may be ""), finishReason the choice's
//     finish_reason when present ("stop", "length", "content_filter", …).
//   - usage is present on the stream-options usage chunk, which usually has
//     no choices and arrives just before [DONE].
type tokenUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type toolCallDelta struct {
	index int
	call  vpc.ToolCall
}

func parseSSEChunk(line string) (content string, toolDeltas []toolCallDelta, finishReason string, usage tokenUsage, hasUsage, done, ok bool) {
	if !strings.HasPrefix(line, "data:") {
		return "", nil, "", tokenUsage{}, false, false, false
	}
	data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if data == "" {
		return "", nil, "", tokenUsage{}, false, false, false
	}
	if data == "[DONE]" {
		return "", nil, "", tokenUsage{}, false, true, true
	}
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					Index    *int   `json:"index"`
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *tokenUsage `json:"usage"`
	}
	if err := json.Unmarshal([]byte(data), &chunk); err != nil {
		return "", nil, "", tokenUsage{}, false, false, false
	}
	if chunk.Usage != nil {
		usage = *chunk.Usage
		hasUsage = true
	}
	if len(chunk.Choices) == 0 {
		return "", nil, "", usage, hasUsage, false, true
	}
	if fr := chunk.Choices[0].FinishReason; fr != nil {
		finishReason = *fr
	}
	delta := chunk.Choices[0].Delta
	for _, tc := range delta.ToolCalls {
		idx := 0
		if tc.Index != nil {
			idx = *tc.Index
		}
		toolDeltas = append(toolDeltas, toolCallDelta{
			index: idx,
			call: vpc.ToolCall{
				ID:   tc.ID,
				Type: tc.Type,
				Function: vpc.ToolCallFunction{
					Name:      tc.Function.Name,
					Arguments: tc.Function.Arguments,
				},
			},
		})
	}
	return delta.Content, toolDeltas, finishReason, usage, hasUsage, false, true
}
