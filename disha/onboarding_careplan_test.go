package disha

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	cpTestConversationID = "conv-cp-1"
	cpTestUserID         = "user-cp"
	cpTestPatientInfo    = "Kabir Mehta, age 28, wants to lose weight"
	cpSwitcherPromptName = "obtest/careplan_switcher"
)

// cpTestConfig returns a minimal OnboardingConfig with two configured
// care plans ("keto" and "general") plus the careplan-switcher prompt
// identity — everything CarePlanManager.Detect/Activate touch.
func cpTestConfig() *OnboardingConfig {
	return &OnboardingConfig{
		CareplanSwitcherPrompt:    PromptConfig{Name: cpSwitcherPromptName},
		CareplanSwitcherStageName: "careplan_switcher_stage",
		CarePlans: []CarePlanConfig{
			{Name: "keto"},
			{Name: "general"},
		},
	}
}

// cpHarness wires a real DocumentStore + UIEventSender around an
// injectable client factory, mirroring dtHarness.
type cpHarness struct {
	t           *testing.T
	redisClient RedisClient
	ui          *voicepipelinecore.UIEventSender
	manager     *OnboardingCarePlanManager
	factory     *dtFactoryRecorder
	logBuf      *syncBuffer
}

func newCPHarness(t *testing.T, cfg *OnboardingConfig, api *APIClient, byPrompt map[string]*dtStubClient) *cpHarness {
	t.Helper()
	t.Setenv("ENVIRONMENT", "staging")
	redisServer, redisClient := newRedisTestClient(t)
	seedDocument(t, redisServer, cpSwitcherPromptName, "latest", 1, "CAREPLAN SWITCHER PROMPT BODY")

	logBuf := &syncBuffer{}
	logger := log.New(logBuf, "", 0)
	docs := newDocumentStore(redisClient, logger, simpleTemplateRenderer{})
	ui := voicepipelinecore.NewUIEventSender(logger)
	factory := newDTFactoryRecorder(byPrompt)

	manager := NewOnboardingCarePlanManager(cfg, docs, api, factory.factory(), logger, cpTestUserID, cpTestConversationID, cpTestPatientInfo)
	manager.SetUI(ui)

	return &cpHarness{
		t:           t,
		redisClient: redisClient,
		ui:          ui,
		manager:     manager,
		factory:     factory,
		logBuf:      logBuf,
	}
}

func (h *cpHarness) rtviMessages() []string {
	var out []string
	for _, entry := range h.ui.Snapshot() {
		if entry.Type != "server-message" {
			continue
		}
		if s, ok := entry.Data.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (h *cpHarness) hasRTVI(substr string) bool {
	for _, msg := range h.rtviMessages() {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

// --- Detect ---

func TestCareplanDetectHappyPathReturnsConfiguredPlan(t *testing.T) {
	h := newCPHarness(t, cpTestConfig(), nil, map[string]*dtStubClient{
		cpSwitcherPromptName: {output: `{"selected_care_plan":"keto"}`},
	})

	name, detected, err := h.manager.Detect(context.Background(), "patient: I want to try keto\ndisha: got it")
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if name != "keto" || detected != "keto" {
		t.Fatalf("Detect = (%q, %q), want (keto, keto)", name, detected)
	}
	if !h.hasRTVI("[PROCESS] Starting care plan switcher...") {
		t.Fatalf("missing start RTVI: %v", h.rtviMessages())
	}
	if !h.hasRTVI("care_plan_switcher LLM call took ") {
		t.Fatalf("missing timing RTVI: %v", h.rtviMessages())
	}
}

func TestCareplanDetectUnknownPlanFallsBackToGeneral(t *testing.T) {
	h := newCPHarness(t, cpTestConfig(), nil, map[string]*dtStubClient{
		cpSwitcherPromptName: {output: `{"selected_care_plan":"totally_bogus_plan"}`},
	})

	name, detected, err := h.manager.Detect(context.Background(), "transcript")
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if name != "general" || detected != "totally_bogus_plan" {
		t.Fatalf("Detect = (%q, %q), want (general, totally_bogus_plan)", name, detected)
	}
}

func TestCareplanDetectParseFailureFallsBackAndRecordsRTVI(t *testing.T) {
	h := newCPHarness(t, cpTestConfig(), nil, map[string]*dtStubClient{
		cpSwitcherPromptName: {output: "not json at all"},
	})

	name, detected, err := h.manager.Detect(context.Background(), "transcript")
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if name != "general" || detected != "unknown" {
		t.Fatalf("Detect = (%q, %q), want (general, unknown)", name, detected)
	}
	if !h.hasRTVI("[ERROR] Care plan detection failed: ") {
		t.Fatalf("missing parse-failure RTVI: %v", h.rtviMessages())
	}
}

func TestCareplanDetectNonStringValueFallsBack(t *testing.T) {
	h := newCPHarness(t, cpTestConfig(), nil, map[string]*dtStubClient{
		cpSwitcherPromptName: {output: `{"selected_care_plan":123}`},
	})

	name, detected, err := h.manager.Detect(context.Background(), "transcript")
	if err != nil {
		t.Fatalf("Detect: %v", err)
	}
	if name != "general" || detected != "unknown" {
		t.Fatalf("Detect = (%q, %q), want (general, unknown)", name, detected)
	}
	if !h.hasRTVI("[ERROR] Care plan detection failed: ") {
		t.Fatalf("missing non-string RTVI: %v", h.rtviMessages())
	}
}

func TestCareplanDetectLLMErrorPropagates(t *testing.T) {
	h := newCPHarness(t, cpTestConfig(), nil, map[string]*dtStubClient{
		cpSwitcherPromptName: {err: errors.New("both hedged attempts failed")},
	})

	_, _, err := h.manager.Detect(context.Background(), "transcript")
	if err == nil {
		t.Fatal("Detect: want error propagated, got nil")
	}
}

// --- Activate ---

func TestCareplanActivateFallsBackToGeneral(t *testing.T) {
	h := newCPHarness(t, cpTestConfig(), nil, nil)

	plan := h.manager.Activate(context.Background(), "not_a_configured_plan", "not_a_configured_plan")
	if plan == nil || plan.Name != "general" {
		t.Fatalf("Activate = %+v, want the general plan", plan)
	}
	if !h.hasRTVI("[PROCESS] Care plan switcher complete: general") {
		t.Fatalf("missing complete RTVI: %v", h.rtviMessages())
	}
}

func TestCareplanActivateBothPlansMissingReturnsNilWithoutPanic(t *testing.T) {
	cfg := &OnboardingConfig{
		CareplanSwitcherPrompt:    PromptConfig{Name: cpSwitcherPromptName},
		CareplanSwitcherStageName: "careplan_switcher_stage",
		CarePlans:                 []CarePlanConfig{{Name: "keto"}},
	}
	h := newCPHarness(t, cfg, nil, nil)

	plan := h.manager.Activate(context.Background(), "bogus", "bogus")
	if plan != nil {
		t.Fatalf("Activate = %+v, want nil when even general is missing", plan)
	}
	if h.hasRTVI("[PROCESS] Care plan switcher complete:") {
		t.Fatal("complete RTVI sent despite no resolvable plan")
	}
}

func TestCareplanActivateSetsUserCareplanWithExpectedPayload(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()
	api := NewAPIClient(server.URL, 10*time.Second, nil)

	h := newCPHarness(t, cpTestConfig(), api, nil)
	plan := h.manager.Activate(context.Background(), "keto", "keto")
	if plan == nil || plan.Name != "keto" {
		t.Fatalf("Activate = %+v, want keto", plan)
	}

	if gotMethod != http.MethodPost || gotPath != "/bot/set_user_careplan" {
		t.Fatalf("request = %s %s, want POST /bot/set_user_careplan", gotMethod, gotPath)
	}
	if gotBody["user_id"] != cpTestUserID || gotBody["onboarding_care_plan"] != "keto" || gotBody["detected_care_plan"] != "keto" {
		t.Fatalf("request body = %+v", gotBody)
	}
}

func TestCareplanActivateSwallowsAPIFailure(t *testing.T) {
	// Every request (both the primary call and its enqueue fallback) fails,
	// so SetUserCareplanWithFallback returns a residual error — Activate
	// must still return the resolved plan without panicking or
	// propagating that error (it has no error return value at all).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	api := NewAPIClient(server.URL, 10*time.Second, nil)

	h := newCPHarness(t, cpTestConfig(), api, nil)
	plan := h.manager.Activate(context.Background(), "keto", "keto")
	if plan == nil || plan.Name != "keto" {
		t.Fatalf("Activate = %+v, want keto despite API failure", plan)
	}
}
