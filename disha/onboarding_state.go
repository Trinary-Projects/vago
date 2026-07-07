package disha

import (
	"fmt"
	"log"
	"reflect"
	"sync"
)

// ConversationState is the Go port of Disha's
// bots/onboarding_call/conversation_state.py: the onboarding call's
// mutable stage/variable state. Unlike Python (single asyncio loop), the
// Go tracker goroutine, deep-thinking callbacks, and turn persistence
// touch it concurrently, so every accessor holds the mutex.
//
// stage_threshold_reminded/alerted stay in the persisted JSON (always
// false) for shape compatibility even though the StageThresholdProcessor
// is not ported (tool-call-variant-only).
type ConversationState struct {
	mu                     sync.Mutex
	variant                string
	currentStage           *StageConfig
	selectedCarePlan       *CarePlanConfig
	variableStore          map[string]any
	stageTurnCount         int
	stageThresholdReminded bool
	stageThresholdAlerted  bool
}

// NewConversationState mirrors ConversationState.initial.
func NewConversationState(config *OnboardingConfig, variant string) *ConversationState {
	return &ConversationState{
		variant:       variant,
		currentStage:  &config.StartStage,
		variableStore: make(map[string]any),
	}
}

// ConversationStateFromResume mirrors ConversationState.from_resume:
// rebuild state from the persisted conversation_state S3 JSON. An
// unknown care plan logs and continues; an unresolved agenda falls back
// to the start stage; a missing agenda key is a malformed-state error
// (Python raises KeyError).
func ConversationStateFromResume(config *OnboardingConfig, variant string, resumeData map[string]any, logger *log.Logger) (*ConversationState, error) {
	var carePlan *CarePlanConfig
	if name, _ := resumeData["care_plan_name"].(string); name != "" {
		carePlan = config.FindCarePlan(name)
		if carePlan == nil && logger != nil {
			logger.Printf("disha: from_resume: care plan %q not found in config\n", name)
		}
	}
	agenda, ok := resumeData["agenda"].(string)
	if !ok || agenda == "" {
		return nil, fmt.Errorf("disha: resume state has no agenda")
	}
	stage, discovered := config.ResolveStageWithCarePlan(agenda, carePlan)
	if discovered != nil && carePlan == nil {
		carePlan = discovered
		if logger != nil {
			logger.Printf("disha: from_resume: discovered care plan %q from stage %q\n", carePlan.Name, agenda)
		}
	}
	if stage == nil {
		if logger != nil {
			logger.Printf("disha: from_resume: stage %q not resolved, falling back to start_stage=%s\n", agenda, config.StartStage.Name)
		}
		stage = &config.StartStage
	}
	variableStore, _ := resumeData["variable_store"].(map[string]any)
	if variableStore == nil {
		variableStore = make(map[string]any)
	}
	stageTurnCount := 0
	if v, ok := resumeData["stage_turn_count"].(float64); ok {
		stageTurnCount = int(v)
	}
	reminded, _ := resumeData["stage_threshold_reminded"].(bool)
	alerted, _ := resumeData["stage_threshold_alerted"].(bool)
	return &ConversationState{
		variant:                variant,
		currentStage:           stage,
		selectedCarePlan:       carePlan,
		variableStore:          variableStore,
		stageTurnCount:         stageTurnCount,
		stageThresholdReminded: reminded,
		stageThresholdAlerted:  alerted,
	}, nil
}

// ToPersistDict mirrors conversation_state.to_persist_dict. The
// persistence hook appends user_id/conversation_id before the S3 upload,
// matching conversation_persistence_processor._get_conversation_state_dict.
func (s *ConversationState) ToPersistDict() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var carePlanName any
	if s.selectedCarePlan != nil {
		carePlanName = s.selectedCarePlan.Name
	}
	return map[string]any{
		"variant":                  s.variant,
		"agenda":                   s.currentStage.Name,
		"stage_turn_count":         s.stageTurnCount,
		"variable_store":           cloneVariableStore(s.variableStore),
		"care_plan_name":           carePlanName,
		"stage_threshold_reminded": s.stageThresholdReminded,
		"stage_threshold_alerted":  s.stageThresholdAlerted,
	}
}

func (s *ConversationState) Variant() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.variant
}

// CurrentStage returns the active stage config. The returned pointer
// aliases the immutable loaded config; callers must not mutate it.
func (s *ConversationState) CurrentStage() *StageConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentStage
}

func (s *ConversationState) SelectedCarePlan() *CarePlanConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.selectedCarePlan
}

func (s *ConversationState) SetSelectedCarePlan(carePlan *CarePlanConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.selectedCarePlan = carePlan
}

func (s *ConversationState) StageTurnCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stageTurnCount
}

func (s *ConversationState) IncrementStageTurnCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stageTurnCount++
}

// AdvanceStage mirrors conversation_state.advance_stage: enter the new
// stage and reset the per-stage counters/flags.
func (s *ConversationState) AdvanceStage(nextStage *StageConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentStage = nextStage
	s.stageTurnCount = 0
	s.stageThresholdReminded = false
	s.stageThresholdAlerted = false
}

// MergeVariables merges deep-thinking results into the variable store
// and reports whether any value actually changed (which drives prompt
// recompilation for non-blocking deep thinking).
func (s *ConversationState) MergeVariables(newVars map[string]any) bool {
	if len(newVars) == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for k, v := range newVars {
		// Python compares variable_store.get(k) != v, so a missing key
		// counts as None: merging a nil value into a missing key is not
		// a change.
		if !changed && !reflect.DeepEqual(s.variableStore[k], v) {
			changed = true
		}
		s.variableStore[k] = v
	}
	return changed
}

// VariableStoreSnapshot returns a shallow copy of the variable store for
// prompt compilation and deep-thinking inputs.
func (s *ConversationState) VariableStoreSnapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneVariableStore(s.variableStore)
}

// GetConversationVariables mirrors get_conversation_variables (the whole
// store, handed to post-call operations).
func (s *ConversationState) GetConversationVariables() map[string]any {
	return s.VariableStoreSnapshot()
}

// GetIntensityLevels extracts diet/fitness intensity variables for the
// post-call request. Values are strings when present; nil/empty are
// skipped like Python's `not in (None, "")`.
func (s *ConversationState) GetIntensityLevels() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]string)
	if v, _ := s.variableStore["diet_intensity_level"].(string); v != "" {
		result["diet_plan_intensity_level"] = v
	}
	if v, _ := s.variableStore["fitness_intensity_level"].(string); v != "" {
		result["fitness_plan_intensity_level"] = v
	}
	return result
}

func cloneVariableStore(store map[string]any) map[string]any {
	out := make(map[string]any, len(store))
	for k, v := range store {
		out[k] = v
	}
	return out
}
