package disha

// OnboardingDeepThinkingManager is the Go port of Disha's
// bots/onboarding_call/deep_thinking_manager.py: it runs the deep-
// thinking LLM calls configured on the stage being ENTERED
// (StageConfig.DeepThinking), blocking ones concurrently-and-awaited
// before the stage's system prompt compiles, non-blocking ones fired in
// the background so their results can be merged into the variable store
// whenever they land. This file is a standalone unit — wiring it into
// OnboardingStageManager (the caller that would invoke RunBlocking on
// stage entry and RunNonBlocking after it, then merge results into
// ConversationState via MergeVariables and recompile/refresh the prompt)
// is a later task.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

// Python: DEEP_THINKING_LOG_PREFIX.
const deepThinkingLogPrefix = "[DEEP_THINKING]"

// thinkBlockRe strips <think>...</think> spans from DT output before
// JSON parsing, mirroring Python's re.sub(r"<think>.*?</think>", "",
// text, flags=re.DOTALL).
var thinkBlockRe = regexp.MustCompile(`(?s)<think>.*?</think>`)

// deepThinkingClientFactory builds a one-shot LLMClient for a single DT
// call. usecaseType is "deep_thinking_{stageName}"; promptMetadata
// carries the strict prompt-identity fields (system_prompt_name/version/
// variables) so the factory can attach it to the underlying router
// (e.g. via HedgedConfig.PromptMetadata) before the call is made. A nil
// return means the client could not be constructed; executeSingle
// treats that as a call error.
type deepThinkingClientFactory func(promptMetadata map[string]any, usecaseType string) voicepipelinecore.LLMClient

// OnboardingDeepThinkingManager runs configured deep-thinking LLM calls
// for a stage. It is safe for concurrent use: RunBlocking/RunNonBlocking
// may be called from any goroutine, and RunNonBlocking's per-DT
// goroutines report back independently.
type OnboardingDeepThinkingManager struct {
	docs           *DocumentStore
	callbacks      *CallEventCallbacks
	newClient      deepThinkingClientFactory
	logger         *log.Logger
	userID         string
	conversationID string
	patientInfo    string
	promptKey      string

	// Late-bound UI + Sentry hub (Python's set_infrastructure pattern,
	// mirroring the stage tracker): nil-safe, RTVI sends/hub-scoped
	// captures are skipped/fall back to global until wired.
	infraMu sync.Mutex
	ui      serverMessageEmitter
	hub     *sentry.Hub
}

func NewOnboardingDeepThinkingManager(
	docs *DocumentStore,
	callbacks *CallEventCallbacks,
	newClient deepThinkingClientFactory,
	logger *log.Logger,
	userID, conversationID, patientInfo, promptKey string,
) *OnboardingDeepThinkingManager {
	return &OnboardingDeepThinkingManager{
		docs:           docs,
		callbacks:      callbacks,
		newClient:      newClient,
		logger:         logger,
		userID:         userID,
		conversationID: conversationID,
		patientInfo:    patientInfo,
		promptKey:      promptKey,
	}
}

// SetUI injects the late-bound RTVI emitter, mirroring
// OnboardingStageTracker.SetInfrastructure's nil-safe pattern.
func (m *OnboardingDeepThinkingManager) SetUI(ui serverMessageEmitter) {
	if m == nil {
		return
	}
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	m.ui = ui
}

func (m *OnboardingDeepThinkingManager) getUI() serverMessageEmitter {
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	return m.ui
}

// SetSentryHub injects the task-scoped Sentry hub (sentry-task-hub),
// called alongside SetUI once NewPipelineTask exists. nil is safe and
// falls back to global capture, which covers any call in the window
// before this is wired.
func (m *OnboardingDeepThinkingManager) SetSentryHub(hub *sentry.Hub) {
	if m == nil {
		return
	}
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	m.hub = hub
}

func (m *OnboardingDeepThinkingManager) getHub() *sentry.Hub {
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	return m.hub
}

// RunBlocking runs every Blocking==true deep-thinking config for the
// stage being entered concurrently, waits for all of them, and merges
// their results in config-list order (later configs win key
// collisions). Each DT is error-isolated: a failed DT is Sentry-captured
// and skipped, so RunBlocking never returns an error. Matches Python:
// the two RTVI progress messages are always sent, even with zero
// blocking DTs configured.
func (m *OnboardingDeepThinkingManager) RunBlocking(ctx context.Context, stage *StageConfig, transcript, stageName string, variables map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	m.sendRTVI(fmt.Sprintf("[PROCESS] Starting blocking deep thinking for %s...", stageName))
	start := time.Now()

	var blocking []DeepThinkingConfig
	if stage != nil {
		for _, dt := range stage.DeepThinking {
			if dt.Blocking {
				blocking = append(blocking, dt)
			}
		}
	}

	results := make([]map[string]any, len(blocking))
	var wg sync.WaitGroup
	for i, dt := range blocking {
		wg.Add(1)
		go func(i int, dt DeepThinkingConfig) {
			defer wg.Done()
			defer m.recoverToSentry("deep_thinking_blocking", stageName)
			res, err := m.executeSingle(ctx, dt, transcript, stageName, variables)
			if err != nil {
				m.logf("%s BLOCKING error stage=%s prompt=%s: %v", deepThinkingLogPrefix, stageName, dt.Prompt.Name, err)
				sentryutil.Capture(sentryutil.Event{
					Hub: m.getHub(),
					Err: err,
					Tags: map[string]string{
						"component": "disha_onboarding",
						"operation": "deep_thinking_blocking",
					},
					Details: map[string]any{
						"conversation_id": m.conversationID,
						"user_id":         m.userID,
						"stage":           stageName,
						"prompt":          dt.Prompt.Name,
					},
				})
				return
			}
			results[i] = res
		}(i, dt)
	}
	wg.Wait()

	merged := map[string]any{}
	for _, res := range results {
		for k, v := range res {
			merged[k] = v
		}
	}

	elapsedMs := round2(float64(time.Since(start)) / float64(time.Millisecond))
	m.sendRTVI(fmt.Sprintf("[PROCESS] Blocking DT for %s took %.2f ms", stageName, elapsedMs))

	if len(merged) > 0 {
		m.persistDebugChunk(merged)
	}
	return merged
}

// RunNonBlocking spawns one goroutine per Blocking==false deep-thinking
// config and returns immediately; each goroutine executes its DT, and
// on success with a non-nil result persists the debug chunk then calls
// onComplete. Errors are logged, Sentry-captured, and reported via RTVI
// — except context cancellation, which is a deliberate Go-only delta:
// Go cancels in-flight DTs at call end where Python simply abandons the
// asyncio task, so a cancelled DT is not a real failure and must not
// page/alert.
func (m *OnboardingDeepThinkingManager) RunNonBlocking(ctx context.Context, stage *StageConfig, transcript, stageName string, variables map[string]any, onComplete func(map[string]any)) {
	if m == nil || stage == nil {
		return
	}
	for _, dt := range stage.DeepThinking {
		if dt.Blocking {
			continue
		}
		go m.runNonBlockingOne(ctx, dt, transcript, stageName, variables, onComplete)
	}
}

func (m *OnboardingDeepThinkingManager) runNonBlockingOne(ctx context.Context, dt DeepThinkingConfig, transcript, stageName string, variables map[string]any, onComplete func(map[string]any)) {
	defer m.recoverToSentry("deep_thinking_non_blocking", stageName)

	res, err := m.executeSingle(ctx, dt, transcript, stageName, variables)
	if err != nil {
		if isContextCancellation(ctx, err) {
			m.logf("%s NON-BLOCKING cancelled stage=%s prompt=%s", deepThinkingLogPrefix, stageName, dt.Prompt.Name)
			return
		}
		m.logf("%s NON-BLOCKING error stage=%s prompt=%s: %v", deepThinkingLogPrefix, stageName, dt.Prompt.Name, err)
		m.sendRTVI(fmt.Sprintf("Error in non-blocking DT %s: %s", stageName, runePrefix(err.Error(), 50)))
		sentryutil.Capture(sentryutil.Event{
			Hub: m.getHub(),
			Err: err,
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "deep_thinking_non_blocking",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
				"stage":           stageName,
				"prompt":          dt.Prompt.Name,
			},
		})
		return
	}
	if res == nil {
		// executeSingle already Sentry-captured the "valid JSON but not
		// an object" case; nothing to merge, nothing to complete.
		return
	}
	if len(res) > 0 {
		m.persistDebugChunk(res)
	}
	if onComplete != nil {
		onComplete(res)
	}
}

// executeSingle runs one deep-thinking LLM call and parses its output,
// mirroring Python's _execute_single_dt.
func (m *OnboardingDeepThinkingManager) executeSingle(ctx context.Context, dt DeepThinkingConfig, transcript, stageName string, variables map[string]any) (map[string]any, error) {
	if m.docs == nil {
		return nil, errors.New("disha: deep thinking document store is not configured")
	}

	docVars := DocumentVariables(variables)
	if docVars == nil {
		docVars = DocumentVariables{}
	}

	sysText, version, err := m.docs.GetDocument(ctx, dt.Prompt.Name, dt.Prompt.Version, docVars)
	if err != nil {
		return nil, fmt.Errorf("disha: render deep thinking prompt %q: %w", dt.Prompt.Name, err)
	}

	userMessage := "Transcript:\n" + transcript + "\n\nPatient Profile:\n" + m.patientInfo

	// Prompt-identity fields only (strict repo rule): no user-prompt
	// fields, since the DT call has no document-rendered user prompt —
	// the "user message" here is a raw transcript+profile composition.
	metadata := buildPromptTraceMetadata("system", dt.Prompt.Name, version, docVars)

	if m.newClient == nil {
		return nil, errors.New("disha: deep thinking client factory is not configured")
	}
	usecaseType := "deep_thinking_" + stageName
	client := m.newClient(metadata, usecaseType)
	if client == nil {
		return nil, fmt.Errorf("disha: deep thinking client unavailable for prompt %q", dt.Prompt.Name)
	}

	req := voicepipelinecore.LLMRequest{Messages: []voicepipelinecore.Message{
		{Role: "system", Content: sysText},
		{Role: "user", Content: userMessage},
	}}

	m.logf("%s Running stage=%s prompt=%s blocking=%v", deepThinkingLogPrefix, stageName, dt.Prompt.Name, dt.Blocking)

	callStart := time.Now()
	var out strings.Builder
	_, err = client.Stream(ctx, req, func(token string) { out.WriteString(token) })
	if err != nil {
		return nil, fmt.Errorf("disha: deep thinking LLM call failed for %q: %w", dt.Prompt.Name, err)
	}
	elapsedMs := round2(float64(time.Since(callStart)) / float64(time.Millisecond))
	m.sendRTVI(fmt.Sprintf("deep_thinking_%s LLM call took %.2f ms", stageName, elapsedMs))

	raw := out.String()
	m.logf("%s Completed stage=%s prompt=%s took=%.2fms response_preview=%q",
		deepThinkingLogPrefix, stageName, dt.Prompt.Name, elapsedMs, runePrefix(raw, 150))

	return parseDeepThinkingOutput(raw, dt.Prompt.Name, m.reportInvalidOutput(stageName))
}

// parseDeepThinkingOutput strips <think> blocks, trims, and classifies
// the remaining text: a JSON object merges directly; valid JSON that is
// not an object (array/scalar/null) is reported and skipped; anything
// else (including empty text) falls back to a single derived-key entry.
func parseDeepThinkingOutput(raw, promptName string, onInvalidObject func(cleaned string)) (map[string]any, error) {
	cleaned := strings.TrimSpace(thinkBlockRe.ReplaceAllString(raw, ""))

	var obj map[string]any
	if err := json.Unmarshal([]byte(cleaned), &obj); err == nil && obj != nil {
		return obj, nil
	}
	if json.Valid([]byte(cleaned)) {
		// Valid JSON but not a non-null object (array/scalar/"null").
		// Python: merged.update(non_dict) raises and is Sentry-skipped.
		if onInvalidObject != nil {
			onInvalidObject(cleaned)
		}
		return nil, nil
	}
	return map[string]any{PromptPathToVarName(promptName): cleaned}, nil
}

func (m *OnboardingDeepThinkingManager) reportInvalidOutput(stageName string) func(cleaned string) {
	return func(cleaned string) {
		sentryutil.Capture(sentryutil.Event{
			Hub:     m.getHub(),
			Message: "deep thinking output is valid JSON but not an object",
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "deep_thinking_output",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
				"stage":           stageName,
				"output":          cleaned,
			},
		})
	}
}

// persistDebugChunk stores a non-empty DT result as an is_debug_log
// chunk (Python's conversation persistence for both the blocking gather
// and each successful non-blocking DT).
func (m *OnboardingDeepThinkingManager) persistDebugChunk(result map[string]any) {
	if m.callbacks == nil || len(result) == 0 {
		return
	}
	raw, err := json.Marshal(result)
	if err != nil {
		m.logf("%s failed to marshal debug chunk: %v", deepThinkingLogPrefix, err)
		return
	}
	m.callbacks.AppendDebugLogChunk(string(raw), time.Now(), m.promptKey, nil)
}

func (m *OnboardingDeepThinkingManager) recoverToSentry(operation, stageName string) {
	p := recover()
	if p == nil {
		return
	}
	err := fmt.Errorf("disha: panic in %s: %v", operation, p)
	m.logf("%v", err)
	sentryutil.Capture(sentryutil.Event{
		Hub: m.getHub(),
		Err: err,
		Tags: map[string]string{
			"component": "disha_onboarding",
			"operation": operation,
		},
		Details: map[string]any{
			"conversation_id": m.conversationID,
			"user_id":         m.userID,
			"stage":           stageName,
		},
	})
}

func (m *OnboardingDeepThinkingManager) sendRTVI(message string) {
	if ui := m.getUI(); ui != nil {
		ui.ServerMessage(message, time.Now())
	}
}

func (m *OnboardingDeepThinkingManager) logf(format string, args ...any) {
	if m.logger != nil {
		m.logger.Printf(format+"\n", args...)
	}
}

// isContextCancellation reports whether err represents ctx being
// cancelled/timed out rather than a genuine call failure.
func isContextCancellation(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// newDeepThinkingClientFactory is the PRODUCTION deepThinkingClientFactory:
// a hedged primary/hedge race over the gpt-oss120-fast-hedged pair,
// mirroring Python's deep-thinking calls through
// generate_with_hedged_request (services/llm_failover_service.py).
// Temperature/MaxTokens (0.7/4000) match the hedged-execution deep-
// thinking decision recorded for phase 2. A construction failure (e.g.
// an unregistered pair key, which should not happen in practice) is
// logged + Sentry-captured and yields a nil client; executeSingle then
// treats the call as failed.
//
// This factory closure is built in onboarding_call.go BuildTask before
// NewPipelineTask exists, so it has no lexical path to the manager's
// late-bound Sentry hub (sentry-task-hub) even though it is invoked
// later, per-call, from executeSingle. Deliberately left on the global
// hub, the same carve-out as the plan()-time capture sites.
func newDeepThinkingClientFactory(deps Deps, logger *log.Logger, userID, conversationID string) deepThinkingClientFactory {
	temperature := 0.7
	maxTokens := 4000
	return func(promptMetadata map[string]any, usecaseType string) voicepipelinecore.LLMClient {
		client, err := llmrouter.NewHedged(llmrouter.HedgedConfig{
			Pair:           llmrouter.GroupGPTOSS120FastHedged,
			Redis:          deps.Redis,
			Logger:         logger,
			LogSink:        newLLMLogSink(deps.API, logger, usecaseType, userID, conversationID),
			PromptMetadata: promptMetadata,
			Temperature:    &temperature,
			MaxTokens:      &maxTokens,
		})
		if err != nil {
			if logger != nil {
				logger.Printf("%s failed to build hedged client for usecase=%s: %v\n", deepThinkingLogPrefix, usecaseType, err)
			}
			sentryutil.Capture(sentryutil.Event{
				Err: err,
				Tags: map[string]string{
					"component": "disha_onboarding",
					"operation": "deep_thinking_client_factory",
				},
				Details: map[string]any{
					"conversation_id": conversationID,
					"user_id":         userID,
					"usecase_type":    usecaseType,
				},
			})
			return nil
		}
		return client
	}
}
