package disha

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/voicepipelinecore"
	"github.com/jaideep329/talk-go/voicepipelinecore/llmrouter"
)

// OnboardingCarePlanManager is the Go port of Disha's
// bots/onboarding_call/careplan_manager.py: it runs the care-plan
// switcher LLM call when the stage machine enters the careplan-switcher
// stage without a selection yet, classifies the transcript into one of
// the configured care plans (or "general" as the safe fallback), and
// activates the result (persisting it to Disha and letting the caller
// store it on ConversationState).
type OnboardingCarePlanManager struct {
	config         *OnboardingConfig
	docs           *DocumentStore
	api            *APIClient
	newClient      deepThinkingClientFactory
	logger         *log.Logger
	userID         string
	conversationID string
	patientInfo    string

	// Late-bound UI (same nil-safe pattern as the tracker/stage
	// manager/DT manager): RTVI sends are skipped until wired. The
	// task-scoped Sentry hub is wired separately via the embedded
	// taskSentryHub.
	infraMu sync.Mutex
	ui      serverMessageEmitter

	taskSentryHub
}

func NewOnboardingCarePlanManager(
	config *OnboardingConfig,
	docs *DocumentStore,
	api *APIClient,
	newClient deepThinkingClientFactory,
	logger *log.Logger,
	userID, conversationID, patientInfo string,
) *OnboardingCarePlanManager {
	return &OnboardingCarePlanManager{
		config:         config,
		docs:           docs,
		api:            api,
		newClient:      newClient,
		logger:         logger,
		userID:         userID,
		conversationID: conversationID,
		patientInfo:    patientInfo,
	}
}

// SetUI injects the late-bound RTVI emitter.
func (m *OnboardingCarePlanManager) SetUI(ui serverMessageEmitter) {
	if m == nil {
		return
	}
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	m.ui = ui
}

func (m *OnboardingCarePlanManager) getUI() serverMessageEmitter {
	m.infraMu.Lock()
	defer m.infraMu.Unlock()
	return m.ui
}

// careplanSwitcherUsecaseType matches Python's care-plan-switcher LLM
// usecase tag.
const careplanSwitcherUsecaseType = "care_plan_switcher"

// Detect classifies the transcript into a configured care plan name,
// mirroring CarePlanManager.detect. It returns the resolved plan name
// (a configured plan or the "general" fallback) plus the raw detected
// value the LLM produced (kept even when it doesn't match a configured
// plan, for the detected_care_plan field on the API call). An LLM
// failure is NOT swallowed here — both hedged attempts failing
// propagates the error so callers abort the transition, matching
// Python's exception propagating out of detect() and aborting
// process_transition (the plan stays unset so a later trigger retries).
func (m *OnboardingCarePlanManager) Detect(ctx context.Context, transcript string) (string, string, error) {
	if m == nil {
		return "", "", fmt.Errorf("disha: careplan manager is not configured")
	}
	m.sendRTVI("[PROCESS] Starting care plan switcher...")

	if m.docs == nil {
		return "", "", fmt.Errorf("disha: careplan document store is not configured")
	}
	sysText, version, err := m.docs.GetDocument(ctx, m.config.CareplanSwitcherPrompt.Name, m.config.CareplanSwitcherPrompt.Version, nil)
	if err != nil {
		return "", "", fmt.Errorf("disha: render careplan switcher prompt %q: %w", m.config.CareplanSwitcherPrompt.Name, err)
	}

	// Python passes no variables to this prompt; metadata still carries
	// the prompt-identity fields (empty variables map, not omitted).
	metadata := buildPromptTraceMetadata("system", m.config.CareplanSwitcherPrompt.Name, version, DocumentVariables{})

	userMessage := "Transcript:\n" + transcript + "\n\nPatient Profile:\n" + m.patientInfo

	if m.newClient == nil {
		return "", "", fmt.Errorf("disha: careplan client factory is not configured")
	}
	client := m.newClient(metadata, careplanSwitcherUsecaseType)
	if client == nil {
		return "", "", fmt.Errorf("disha: careplan client unavailable")
	}

	req := voicepipelinecore.LLMRequest{Messages: []voicepipelinecore.Message{
		{Role: "system", Content: sysText},
		{Role: "user", Content: userMessage},
	}}

	start := time.Now()
	var out strings.Builder
	_, err = client.Stream(ctx, req, func(token string) { out.WriteString(token) })
	if err != nil {
		// Do NOT swallow: both hedged attempts failed, so the caller must
		// abort the transition (Python parity).
		return "", "", fmt.Errorf("disha: careplan switcher LLM call failed: %w", err)
	}
	elapsedMs := round2(float64(time.Since(start)) / float64(time.Millisecond))
	m.sendRTVI(fmt.Sprintf("care_plan_switcher LLM call took %.2f ms", elapsedMs))

	detected, err := parseCareplanOutput(out.String())
	if err != nil {
		sentryutil.Capture(sentryutil.Event{
			Hub: m.sentryHub(),
			Err: fmt.Errorf("disha: care plan detection parse failed: %w", err),
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "careplan_detect_parse",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
			},
		})
		m.sendRTVI(fmt.Sprintf("[ERROR] Care plan detection failed: %s", runePrefix(err.Error(), 50)))
		return "general", "unknown", nil
	}

	if m.config.FindCarePlan(detected) == nil {
		return "general", detected, nil
	}
	return detected, detected, nil
}

// parseCareplanOutput extracts the "selected_care_plan" string field from
// the LLM's raw JSON output. A parse failure, a missing key, or a
// non-string value are all reported as errors (the non-string case is a
// small Go-only hardening delta vs. Python, which would happily pass
// whatever it decoded — an int, a list, etc. — through to the API call).
func parseCareplanOutput(raw string) (string, error) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}
	value, ok := obj["selected_care_plan"]
	if !ok {
		return "", fmt.Errorf("missing selected_care_plan key")
	}
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("selected_care_plan is not a string: %#v", value)
	}
	return str, nil
}

// Activate resolves the chosen plan (falling back to "general") and
// persists the selection to Disha, mirroring CarePlanManager.activate.
// The returned plan may be nil if even "general" isn't configured — a
// deliberate Go hardening delta: Python would crash (KeyError/None
// dereference) on a config missing "general"; Go instead logs to Sentry
// and proceeds without a selection so the call doesn't crash.
func (m *OnboardingCarePlanManager) Activate(ctx context.Context, name, detected string) *CarePlanConfig {
	if m == nil {
		return nil
	}
	plan := m.config.FindCarePlan(name)
	if plan == nil {
		plan = m.config.FindCarePlan("general")
	}
	if plan == nil {
		sentryutil.Capture(sentryutil.Event{
			Hub:     m.sentryHub(),
			Message: "care plan not found, proceeding without selection",
			Tags: map[string]string{
				"component": "disha_onboarding",
				"operation": "careplan_activate",
			},
			Details: map[string]any{
				"conversation_id": m.conversationID,
				"user_id":         m.userID,
				"name":            name,
				"detected":        detected,
			},
		})
		return nil
	}

	// Python's cache_care_plan_prompts pre-warms a local Langfuse doc
	// cache here. Go's DocumentStore reads pre-rendered Redis keys on
	// demand (with its own TTL cache), so there is nothing to pre-warm.

	m.sendRTVI(fmt.Sprintf("[PROCESS] Care plan switcher complete: %s", plan.Name))

	if m.api != nil {
		err := m.api.SetUserCareplanWithFallback(ctx, SetUserCareplanRequest{
			UserID:             m.userID,
			OnboardingCarePlan: plan.Name,
			DetectedCarePlan:   &detected,
		})
		if err != nil {
			// Residual failure (API + enqueue fallback both failed) never
			// fails activation — Python parity.
			sentryutil.Capture(sentryutil.Event{
				Hub: m.sentryHub(),
				Err: fmt.Errorf("disha: set_user_careplan failed: %w", err),
				Tags: map[string]string{
					"component": "disha_onboarding",
					"operation": "careplan_set_user_careplan",
				},
				Details: map[string]any{
					"conversation_id": m.conversationID,
					"user_id":         m.userID,
					"care_plan":       plan.Name,
				},
			})
		}
	}

	return plan
}

func (m *OnboardingCarePlanManager) sendRTVI(message string) {
	if ui := m.getUI(); ui != nil {
		ui.ServerMessage(message, time.Now())
	}
}

// newCarePlanClientFactory is the PRODUCTION deepThinkingClientFactory
// for care-plan detection: the same hedged primary/hedge race over the
// gpt-oss120-fast-hedged pair used by deep thinking, mirroring Python's
// care-plan-switcher call through generate_with_hedged_request
// (services/llm_failover_service.py). This is a deliberate delta from
// Python's sequential two-attempt failover: hedging is a strict
// superset — a primary error before the hedge threshold still produces
// Python's immediate sequential retry, and hedging additionally covers
// the "primary is just slow" case with a parallel hedge after 1s.
// Temperature/MaxTokens are left nil (endpoint defaults) because Python
// uses the default temperature=0 and no max_tokens for this call.
//
// Like newDeepThinkingClientFactory, this closure is built in
// onboarding_call.go BuildTask before NewPipelineTask exists, so its
// construction-failure capture below has no lexical path to the
// manager's late-bound Sentry hub (sentry-task-hub) and deliberately
// stays on the global hub.
func newCarePlanClientFactory(deps Deps, logger *log.Logger, userID, conversationID string) deepThinkingClientFactory {
	return func(promptMetadata map[string]any, usecaseType string) voicepipelinecore.LLMClient {
		client, err := llmrouter.NewHedged(llmrouter.HedgedConfig{
			Pair:           llmrouter.GroupGPTOSS120FastHedged,
			Redis:          deps.Redis,
			Logger:         logger,
			LogSink:        newLLMLogSink(deps.API, logger, usecaseType, userID, conversationID),
			PromptMetadata: promptMetadata,
		})
		if err != nil {
			if logger != nil {
				logger.Printf("[CAREPLAN] failed to build hedged client for usecase=%s: %v\n", usecaseType, err)
			}
			sentryutil.Capture(sentryutil.Event{
				Err: err,
				Tags: map[string]string{
					"component": "disha_onboarding",
					"operation": "careplan_client_factory",
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
