package disha

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Onboarding variant config, the Go port of Disha's
// bots/onboarding_call/config_models.py + config_manager.py. The config
// document lives in the document store as
// `OB_Call_Configs/{variant}_config` with the whole config in
// config_json (the prompt text is empty).

// PromptConfig identifies a document-store prompt, optionally pinned to
// a version. Version 0 means "latest" (Python's None), matching the
// DocumentStore convention.
type PromptConfig struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type DeepThinkingConfig struct {
	Prompt   PromptConfig `json:"prompt"`
	Blocking bool         `json:"blocking"`
}

type StageConfig struct {
	Name          string               `json:"name"`
	Prompt        PromptConfig         `json:"prompt"`
	NextStages    []string             `json:"next_stages"`
	DeepThinking  []DeepThinkingConfig `json:"deep_thinking"`
	IsEndStage    bool                 `json:"is_end_stage"`
	TurnThreshold *int                 `json:"turn_threshold"`
}

type CarePlanConfig struct {
	Name   string        `json:"name"`
	Stages []StageConfig `json:"stages"`
}

type OnboardingConfig struct {
	Model                      string           `json:"model"`
	MainSystemPrompt           PromptConfig     `json:"main_system_prompt"`
	CareplanSwitcherPrompt     PromptConfig     `json:"careplan_switcher_prompt"`
	CareplanSwitcherStageName  string           `json:"careplan_switcher_stage_name"`
	StartStage                 StageConfig      `json:"start_stage"`
	CommonStages               []StageConfig    `json:"common_stages"`
	CarePlans                  []CarePlanConfig `json:"care_plans"`
	RecordingEnabledPercentage *int             `json:"recording_enabled_percentage"`
}

var promptVarNameRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// PromptPathToVarName mirrors config_models.prompt_path_to_var_name:
// non-blocking deep-thinking results whose output is not JSON are merged
// into the variable store under this derived key.
func PromptPathToVarName(promptPath string) string {
	return promptVarNameRe.ReplaceAllString(strings.ReplaceAll(promptPath, "/", "__"), "_")
}

// LoadOnboardingConfig fetches and parses the variant config document. A
// missing variant is a startup error by decision (Python's legacy
// "student" forcing lives in bot_session_manager and is not ported).
func LoadOnboardingConfig(ctx context.Context, docs *DocumentStore, variant string) (*OnboardingConfig, error) {
	variant = strings.TrimSpace(variant)
	if variant == "" {
		return nil, fmt.Errorf("disha: onboarding_call_variant is required")
	}
	if docs == nil {
		return nil, fmt.Errorf("disha: document store is required for onboarding config")
	}
	name := fmt.Sprintf("OB_Call_Configs/%s_config", variant)
	_, _, configJSON, err := docs.GetDocumentWithConfig(ctx, name, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("disha: load onboarding config %q: %w", name, err)
	}
	cfg, err := ParseOnboardingConfig(configJSON)
	if err != nil {
		return nil, fmt.Errorf("disha: parse onboarding config %q: %w", name, err)
	}
	return cfg, nil
}

// ParseOnboardingConfig decodes and validates a variant config's
// config_json, enforcing the same required fields pydantic's Config
// model does.
func ParseOnboardingConfig(configJSON map[string]any) (*OnboardingConfig, error) {
	if len(configJSON) == 0 {
		return nil, fmt.Errorf("config_json is empty")
	}
	raw, err := json.Marshal(configJSON)
	if err != nil {
		return nil, fmt.Errorf("marshal config_json: %w", err)
	}
	var cfg OnboardingConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("decode config_json: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *OnboardingConfig) validate() error {
	if strings.TrimSpace(c.Model) == "" {
		return fmt.Errorf("config field %q is required", "model")
	}
	if strings.TrimSpace(c.CareplanSwitcherStageName) == "" {
		return fmt.Errorf("config field %q is required", "careplan_switcher_stage_name")
	}
	if err := c.MainSystemPrompt.validate("main_system_prompt"); err != nil {
		return err
	}
	if err := c.CareplanSwitcherPrompt.validate("careplan_switcher_prompt"); err != nil {
		return err
	}
	if err := c.StartStage.validate("start_stage"); err != nil {
		return err
	}
	for i := range c.CommonStages {
		if err := c.CommonStages[i].validate(fmt.Sprintf("common_stages[%d]", i)); err != nil {
			return err
		}
	}
	for i := range c.CarePlans {
		cp := &c.CarePlans[i]
		if strings.TrimSpace(cp.Name) == "" {
			return fmt.Errorf("care_plans[%d]: field %q is required", i, "name")
		}
		for j := range cp.Stages {
			if err := cp.Stages[j].validate(fmt.Sprintf("care_plans[%d].stages[%d]", i, j)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p PromptConfig) validate(path string) error {
	if strings.TrimSpace(p.Name) == "" {
		return fmt.Errorf("%s: field %q is required", path, "name")
	}
	return nil
}

func (s *StageConfig) validate(path string) error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("%s: field %q is required", path, "name")
	}
	return s.Prompt.validate(path + ".prompt")
}

// ResolveStage looks a stage up by name in Python's search order:
// start stage, common stages, the selected care plan, then every care
// plan (config_manager.ConfigManager.resolve_stage).
func (c *OnboardingConfig) ResolveStage(stageName string, selectedCarePlan *CarePlanConfig) *StageConfig {
	stage, _ := c.ResolveStageWithCarePlan(stageName, selectedCarePlan)
	return stage
}

// ResolveStageWithCarePlan is ResolveStage plus care-plan discovery:
// when the stage is only found inside an unselected care plan, that plan
// is returned so resume can adopt it
// (config_manager.resolve_stage_with_care_plan).
func (c *OnboardingConfig) ResolveStageWithCarePlan(stageName string, selectedCarePlan *CarePlanConfig) (*StageConfig, *CarePlanConfig) {
	if c.StartStage.Name == stageName {
		return &c.StartStage, selectedCarePlan
	}
	for i := range c.CommonStages {
		if c.CommonStages[i].Name == stageName {
			return &c.CommonStages[i], selectedCarePlan
		}
	}
	if selectedCarePlan != nil {
		for i := range selectedCarePlan.Stages {
			if selectedCarePlan.Stages[i].Name == stageName {
				return &selectedCarePlan.Stages[i], selectedCarePlan
			}
		}
	}
	for i := range c.CarePlans {
		cp := &c.CarePlans[i]
		for j := range cp.Stages {
			if cp.Stages[j].Name == stageName {
				return &cp.Stages[j], cp
			}
		}
	}
	return nil, selectedCarePlan
}

func (c *OnboardingConfig) FindCarePlan(carePlanName string) *CarePlanConfig {
	for i := range c.CarePlans {
		if c.CarePlans[i].Name == carePlanName {
			return &c.CarePlans[i]
		}
	}
	return nil
}

// CollectPromptConfigs gathers every prompt the config references (main
// system prompt, careplan switcher, stage prompts, deep-thinking
// prompts), optionally including care-plan stages. Used to pre-warm the
// document cache (config_manager.collect_prompt_configs).
func (c *OnboardingConfig) CollectPromptConfigs(includeCarePlans bool) []PromptConfig {
	prompts := []PromptConfig{
		c.MainSystemPrompt,
		c.CareplanSwitcherPrompt,
		c.StartStage.Prompt,
	}
	// Python collect_prompt_configs skips start_stage.deep_thinking;
	// match it exactly.
	for _, stage := range c.CommonStages {
		prompts = append(prompts, stage.Prompt)
		prompts = appendStagePrompts(prompts, stage.DeepThinking)
	}
	if includeCarePlans {
		for i := range c.CarePlans {
			prompts = append(prompts, c.CollectCarePlanPromptConfigs(&c.CarePlans[i])...)
		}
	}
	return prompts
}

func (c *OnboardingConfig) CollectCarePlanPromptConfigs(carePlan *CarePlanConfig) []PromptConfig {
	var prompts []PromptConfig
	for _, stage := range carePlan.Stages {
		prompts = append(prompts, stage.Prompt)
		prompts = appendStagePrompts(prompts, stage.DeepThinking)
	}
	return prompts
}

func appendStagePrompts(prompts []PromptConfig, deepThinking []DeepThinkingConfig) []PromptConfig {
	for _, dt := range deepThinking {
		prompts = append(prompts, dt.Prompt)
	}
	return prompts
}
