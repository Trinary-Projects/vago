package disha

// Hand-port of Disha's Python onboarding stage-transition fuzzy matcher
// (bots/onboarding_call/stage_transition_fuzzy_matcher.py). rapidfuzz's
// fuzz.token_set_ratio / fuzz.partial_ratio are based on normalized Indel
// similarity (Levenshtein distance restricted to insertions/deletions,
// equivalent to 2*LCS(a,b)/(len(a)+len(b))*100), NOT difflib's
// SequenceMatcher — existing Go fuzzywuzzy ports use SequenceMatcher and
// score-diverge from rapidfuzz, which is why this is a hand-port instead
// of a dependency. The scoring functions below mirror rapidfuzz's pure
// Python fallback implementation (rapidfuzz/fuzz_py.py, rapidfuzz/
// distance/Indel_py.py in the pinned rapidfuzz==3.14.5), which was
// verified byte-for-byte against the compiled C++ engine across the full
// Python parity test corpus (test_stage_transition_fuzzy_matcher.py) plus
// randomized long-string fuzzing before this port was written.
//
// If changing fuzzy decision logic, keep onboarding_stage_fuzzy_matcher_test.go
// (the Go port of the Python parity corpus) in sync.

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const (
	fuzzyYesScoreThreshold                    = 90.0
	fuzzyYesCoverageThreshold                 = 85.0
	fuzzyYesPartialScoreThreshold             = 85.0
	fuzzyMaybeScoreThreshold                  = 70.0
	fuzzyMaybeCoverageThreshold               = 40.0
	fuzzyMaybePartialScoreThreshold           = 60.0
	fuzzyMaybeStrongScoreThreshold            = 85.0
	stageTransitionTriggerLengthInfoThreshold = 140
	stageTransitionTriggerPreviewLength       = 240
)

// StageTransitionDecision mirrors Python's `StageTransitionDecision =
// Literal["yes", "maybe", "no"]`.
type StageTransitionDecision string

const (
	StageTransitionDecisionYes   StageTransitionDecision = "yes"
	StageTransitionDecisionMaybe StageTransitionDecision = "maybe"
	StageTransitionDecisionNo    StageTransitionDecision = "no"
)

// StageTransitionConfigError is the typed error raised for a malformed
// next_stages config (mirrors Python's StageTransitionConfigError(ValueError))
// and for a structurally valid config that has no usable trigger statements.
type StageTransitionConfigError struct {
	Msg string
}

func (e *StageTransitionConfigError) Error() string { return e.Msg }

// StageTransition mirrors Python's `StageTransition` dataclass: one
// parsed/allowed next-stage entry from the stage prompt's config_json.
type StageTransition struct {
	ToStage           string
	TriggerStatements []string
}

// FuzzyStageTransitionCandidate mirrors Python's
// `FuzzyStageTransitionCandidate` dataclass: one (stage, trigger) scoring
// result, using the best-scoring trigger variant (see triggerVariants).
type FuzzyStageTransitionCandidate struct {
	ToStage           string
	Trigger           string
	NormalizedTrigger string
	Score             float64
	TokenSetScore     float64
	PartialScore      float64
	Coverage          float64
}

// StageTransitionFuzzyResult mirrors Python's `StageTransitionFuzzyResult`
// dataclass, the full output of evaluating one stage prompt's configured
// next_stages against the latest assistant response.
type StageTransitionFuzzyResult struct {
	Decision                   StageTransitionDecision
	Output                     string
	Score                      float64
	TokenSetScore              float64
	PartialScore               float64
	Coverage                   float64
	MatchedTrigger             *string
	CandidateStage             *string
	LatencyMs                  float64
	Candidates                 []FuzzyStageTransitionCandidate
	ConfiguredStageTransitions []StageTransition
	LLMStageTransitions        []StageTransition
	TransitionCondition        string
	LLMNextStageNames          []string
}

// ToPayload mirrors Python's `StageTransitionFuzzyResult.to_payload()`:
// the rounded (2 decimal) scalar summary suitable for logging.
func (r *StageTransitionFuzzyResult) ToPayload() map[string]any {
	var matchedTrigger, candidateStage any
	if r.MatchedTrigger != nil {
		matchedTrigger = *r.MatchedTrigger
	}
	if r.CandidateStage != nil {
		candidateStage = *r.CandidateStage
	}
	return map[string]any{
		"decision":        string(r.Decision),
		"output":          r.Output,
		"score":           round2(r.Score),
		"token_set_score": round2(r.TokenSetScore),
		"partial_score":   round2(r.PartialScore),
		"coverage":        round2(r.Coverage),
		"matched_trigger": matchedTrigger,
		"candidate_stage": candidateStage,
		"latency_ms":      round2(r.LatencyMs),
	}
}

// LLMStageTransitionsPayload mirrors Python's
// `StageTransitionFuzzyResult.llm_stage_transitions_payload()`.
func (r *StageTransitionFuzzyResult) LLMStageTransitionsPayload() []map[string]any {
	out := make([]map[string]any, 0, len(r.LLMStageTransitions))
	for _, st := range r.LLMStageTransitions {
		triggers := make([]string, len(st.TriggerStatements))
		copy(triggers, st.TriggerStatements)
		out = append(out, map[string]any{
			"to_stage":           st.ToStage,
			"trigger_statements": triggers,
		})
	}
	return out
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// StageTransitionEvalConfig is the input to EvaluateStageTransitionFromConfig,
// mirroring Python's evaluate_stage_transition_from_config keyword args.
// DocumentVersion is `any` (not int) rather than the literal signature
// suggested in the port brief because Python falls back to the stage
// prompt config's own "version" field when the caller-supplied value is
// nil/None, and that config value can be any JSON scalar (string, number,
// etc.); a plain int parameter cannot represent "unset" distinctly from 0.
type StageTransitionEvalConfig struct {
	StagePromptConfig       map[string]any
	AllowedNextStages       []string
	LatestAssistantResponse string
	PatientInfo             string
	DocumentName            string
	DocumentVersion         any
}

// EvaluateStageTransitionFromConfig mirrors Python's
// evaluate_stage_transition_from_config.
func EvaluateStageTransitionFromConfig(cfg StageTransitionEvalConfig) (*StageTransitionFuzzyResult, error) {
	start := time.Now()

	stageTransitions, err := parseStageTransitions(cfg.StagePromptConfig, cfg.AllowedNextStages)
	if err != nil {
		return nil, err
	}

	documentVersion := cfg.DocumentVersion
	if documentVersion == nil {
		documentVersion = cfg.StagePromptConfig["version"]
	}
	configID := cfg.StagePromptConfig["id"]

	reportLongStageTransitionTriggers(stageTransitions, cfg.DocumentName, documentVersion, configID)

	if len(stageTransitions) == 0 {
		return nil, &StageTransitionConfigError{Msg: "next_stages config has no valid trigger statements"}
	}

	var candidates []FuzzyStageTransitionCandidate
	for _, st := range stageTransitions {
		for _, trigger := range st.TriggerStatements {
			candidates = append(candidates, scoreTrigger(cfg.LatestAssistantResponse, st, trigger, cfg.PatientInfo))
		}
	}

	ranked := make([]FuzzyStageTransitionCandidate, len(candidates))
	copy(ranked, candidates)
	sort.SliceStable(ranked, func(i, j int) bool {
		return compareCandidateTuple(ranked[i], ranked[j]) > 0
	})

	var best *FuzzyStageTransitionCandidate
	if len(ranked) > 0 {
		best = &ranked[0]
	}

	bestScore, bestPartialScore, bestCoverage := 0.0, 0.0, 0.0
	if best != nil {
		bestScore = best.TokenSetScore
		bestPartialScore = best.PartialScore
		bestCoverage = best.Coverage
	}

	var decision StageTransitionDecision
	var output string

	switch {
	case bestScore < fuzzyMaybeScoreThreshold ||
		bestCoverage < fuzzyMaybeCoverageThreshold ||
		(bestPartialScore < fuzzyMaybePartialScoreThreshold && bestScore < fuzzyMaybeStrongScoreThreshold):
		decision = StageTransitionDecisionNo
		output = "no"
	case best != nil &&
		bestScore >= fuzzyYesScoreThreshold &&
		bestCoverage >= fuzzyYesCoverageThreshold &&
		bestPartialScore >= fuzzyYesPartialScoreThreshold:
		decision = StageTransitionDecisionYes
		output = best.ToStage
	default:
		decision = StageTransitionDecisionMaybe
		output = "no"
	}

	llmStageTransitions := filterStageTransitionsForMaybe(stageTransitions, ranked)

	var matchedTrigger, candidateStage *string
	if best != nil {
		mt := best.Trigger
		matchedTrigger = &mt
		cs := best.ToStage
		candidateStage = &cs
	}

	llmNextStageNames := make([]string, 0, len(llmStageTransitions))
	for _, st := range llmStageTransitions {
		llmNextStageNames = append(llmNextStageNames, st.ToStage)
	}

	return &StageTransitionFuzzyResult{
		Decision:                   decision,
		Output:                     output,
		Score:                      bestScore,
		TokenSetScore:              bestScore,
		PartialScore:               bestPartialScore,
		Coverage:                   bestCoverage,
		MatchedTrigger:             matchedTrigger,
		CandidateStage:             candidateStage,
		LatencyMs:                  float64(time.Since(start)) / float64(time.Millisecond),
		Candidates:                 ranked,
		ConfiguredStageTransitions: stageTransitions,
		LLMStageTransitions:        llmStageTransitions,
		TransitionCondition:        formatTransitionCondition(llmStageTransitions),
		LLMNextStageNames:          llmNextStageNames,
	}, nil
}

// parseStageTransitions mirrors Python's `_parse_stage_transitions`,
// including the pydantic-shaped validation semantics: any structurally
// invalid next_stages entry fails the whole parse (StageTransitionConfigError),
// while stage_name filtering against allowedNextStages and empty-trigger
// stages happen only after the whole list validates successfully.
func parseStageTransitions(config map[string]any, allowedNextStages []string) ([]StageTransition, error) {
	rawNextStages, ok := config["next_stages"]
	if !ok || rawNextStages == nil {
		return nil, nil
	}

	items, ok := rawNextStages.([]any)
	if !ok {
		return nil, &StageTransitionConfigError{Msg: "next_stages must be a list"}
	}

	type parsedEntry struct {
		stageName         string
		triggerStatements []string
	}

	parsedEntries := make([]parsedEntry, 0, len(items))
	for _, rawItem := range items {
		item, ok := rawItem.(map[string]any)
		if !ok {
			return nil, &StageTransitionConfigError{Msg: "next_stages entries must be objects"}
		}

		stageNameRaw, ok := item["stage_name"]
		if !ok {
			return nil, &StageTransitionConfigError{Msg: "stage_name is required"}
		}
		stageNameStr, ok := stageNameRaw.(string)
		if !ok {
			return nil, &StageTransitionConfigError{Msg: "stage_name must be a string"}
		}
		stageName := strings.TrimSpace(stageNameStr)
		if stageName == "" {
			return nil, &StageTransitionConfigError{Msg: "stage_name cannot be empty"}
		}

		var triggerStatements []string
		if rawTriggers, ok := item["trigger_statements"]; ok && rawTriggers != nil {
			triggerItems, ok := rawTriggers.([]any)
			if !ok {
				return nil, &StageTransitionConfigError{Msg: "trigger_statements must be a list of strings"}
			}
			seen := map[string]bool{}
			for _, rawTrigger := range triggerItems {
				// Python parity: pydantic validates `list[str]` before the
				// "after"-mode field validator runs, so any non-string (or
				// None) item fails the whole parse with ValidationError →
				// StageTransitionConfigError; the validator body's isinstance
				// filter is dead code. Only the trim/empty/dedupe filtering
				// below is reachable in after mode.
				s, ok := rawTrigger.(string)
				if !ok {
					return nil, &StageTransitionConfigError{Msg: "trigger_statements must be a list of strings"}
				}
				trimmed := strings.TrimSpace(s)
				if trimmed == "" || seen[trimmed] {
					continue
				}
				seen[trimmed] = true
				triggerStatements = append(triggerStatements, trimmed)
			}
		}

		parsedEntries = append(parsedEntries, parsedEntry{stageName: stageName, triggerStatements: triggerStatements})
	}

	allowed := stringSet(allowedNextStages)

	stageTransitions := make([]StageTransition, 0, len(parsedEntries))
	for _, entry := range parsedEntries {
		if !allowed[entry.stageName] {
			continue
		}
		if len(entry.triggerStatements) == 0 {
			continue
		}
		stageTransitions = append(stageTransitions, StageTransition{
			ToStage:           entry.stageName,
			TriggerStatements: entry.triggerStatements,
		})
	}

	return stageTransitions, nil
}

// longTriggerKey is the process-wide dedupe key, mirroring Python's
// `_REPORTED_LONG_STAGE_TRANSITION_TRIGGERS: set[tuple[str, Any, str, str]]`.
type longTriggerKey struct {
	documentName    string
	documentVersion any
	toStage         string
	trigger         string
}

var (
	reportedLongTriggersMu sync.Mutex
	reportedLongTriggers   = map[longTriggerKey]bool{}
)

// reportLongStageTransitionTriggers mirrors Python's
// `_report_long_stage_transition_triggers`: best-effort, must never break
// evaluation.
func reportLongStageTransitionTriggers(stageTransitions []StageTransition, documentName string, documentVersion any, configID any) {
	defer func() {
		// Reporting failures (including a panic from a pathological
		// documentVersion type) must never break evaluation.
		_ = recover()
	}()
	captureLongStageTransitionTriggers(stageTransitions, documentName, documentVersion, configID)
}

func captureLongStageTransitionTriggers(stageTransitions []StageTransition, documentName string, documentVersion any, configID any) {
	if documentName == "" {
		return
	}

	for _, st := range stageTransitions {
		for _, trigger := range st.TriggerStatements {
			if utf8.RuneCountInString(trigger) <= stageTransitionTriggerLengthInfoThreshold {
				continue
			}

			key := longTriggerKey{
				documentName:    documentName,
				documentVersion: documentVersion,
				toStage:         st.ToStage,
				trigger:         trigger,
			}

			reportedLongTriggersMu.Lock()
			if reportedLongTriggers[key] {
				reportedLongTriggersMu.Unlock()
				continue
			}
			reportedLongTriggers[key] = true
			reportedLongTriggersMu.Unlock()

			details := map[string]any{
				"trigger_length":    utf8.RuneCountInString(trigger),
				"trigger_preview":   runePrefix(trigger, stageTransitionTriggerPreviewLength),
				"trigger_statement": trigger,
			}
			if configID != nil {
				details["config_id"] = configID
			}

			sentryutil.Capture(sentryutil.Event{
				Message: "Long onboarding stage transition trigger statement",
				Level:   sentry.LevelInfo,
				Tags: map[string]string{
					"document_name":    documentName,
					"document_version": fmt.Sprintf("%v", documentVersion),
					"target_stage":     st.ToStage,
					"threshold":        fmt.Sprintf("%d", stageTransitionTriggerLengthInfoThreshold),
				},
				Details: details,
			})
		}
	}
}

func runePrefix(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// filterStageTransitionsForMaybe mirrors Python's
// `_filter_stage_transitions_for_maybe`.
func filterStageTransitionsForMaybe(stageTransitions []StageTransition, ranked []FuzzyStageTransitionCandidate) []StageTransition {
	candidateStages := map[string]bool{}
	for _, c := range ranked {
		if passesMaybeGate(c) {
			candidateStages[c.ToStage] = true
		}
	}

	filtered := make([]StageTransition, 0, len(stageTransitions))
	for _, st := range stageTransitions {
		if candidateStages[st.ToStage] {
			filtered = append(filtered, st)
		}
	}
	if len(filtered) == 0 {
		return stageTransitions
	}
	return filtered
}

// passesMaybeGate mirrors Python's `_passes_maybe_gate`.
func passesMaybeGate(c FuzzyStageTransitionCandidate) bool {
	if c.TokenSetScore < fuzzyMaybeScoreThreshold {
		return false
	}
	if c.Coverage < fuzzyMaybeCoverageThreshold {
		return false
	}
	return c.PartialScore >= fuzzyMaybePartialScoreThreshold || c.TokenSetScore >= fuzzyMaybeStrongScoreThreshold
}

// formatTransitionCondition mirrors Python's `_format_transition_condition`.
func formatTransitionCondition(stageTransitions []StageTransition) string {
	var lines []string
	for _, st := range stageTransitions {
		lines = append(lines, fmt.Sprintf("Target stage: %s", st.ToStage))
		for _, trigger := range st.TriggerStatements {
			lines = append(lines, fmt.Sprintf("- Trigger: %s", trigger))
		}
		lines = append(lines, "")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// scoreTrigger mirrors Python's `_score_trigger`.
func scoreTrigger(assistantText string, stageTransition StageTransition, trigger string, patientInfo string) FuzzyStageTransitionCandidate {
	normalizedText := normalizeForFuzzy(assistantText)

	bestTokenSet, bestPartial, bestCoverage := 0.0, 0.0, 0.0
	bestNormalizedTrigger := ""

	for _, variant := range triggerVariants(trigger, patientInfo) {
		normalizedTrigger := normalizeForFuzzy(variant)

		var tokenSetScore, partialScore, coverage float64
		if normalizedTrigger != "" && normalizedText != "" {
			tokenSetScore = tokenSetRatio(normalizedText, normalizedTrigger)
			partialScore = partialRatio(normalizedText, normalizedTrigger)
			coverage = triggerTokenCoverage(normalizedText, normalizedTrigger)
		}

		if tupleGreater(tokenSetScore, partialScore, coverage, bestTokenSet, bestPartial, bestCoverage) {
			bestTokenSet, bestPartial, bestCoverage = tokenSetScore, partialScore, coverage
			bestNormalizedTrigger = normalizedTrigger
		}
	}

	return FuzzyStageTransitionCandidate{
		ToStage:           stageTransition.ToStage,
		Trigger:           trigger,
		NormalizedTrigger: bestNormalizedTrigger,
		Score:             bestTokenSet,
		TokenSetScore:     bestTokenSet,
		PartialScore:      bestPartial,
		Coverage:          bestCoverage,
	}
}

func tupleGreater(a1, a2, a3, b1, b2, b3 float64) bool {
	if a1 != b1 {
		return a1 > b1
	}
	if a2 != b2 {
		return a2 > b2
	}
	return a3 > b3
}

// compareCandidateTuple returns >0 if a ranks above b, <0 if below, 0 if
// equal, using the (token_set_score, partial_score, coverage) ranking key
// from Python's `sorted(candidates, key=..., reverse=True)`.
func compareCandidateTuple(a, b FuzzyStageTransitionCandidate) int {
	if a.TokenSetScore != b.TokenSetScore {
		if a.TokenSetScore > b.TokenSetScore {
			return 1
		}
		return -1
	}
	if a.PartialScore != b.PartialScore {
		if a.PartialScore > b.PartialScore {
			return 1
		}
		return -1
	}
	if a.Coverage != b.Coverage {
		if a.Coverage > b.Coverage {
			return 1
		}
		return -1
	}
	return 0
}

// triggerTokenCoverage mirrors Python's `_trigger_token_coverage`.
func triggerTokenCoverage(normalizedText, normalizedTrigger string) float64 {
	triggerTokens := strings.Fields(normalizedTrigger)
	if len(triggerTokens) == 0 {
		return 0
	}
	textTokens := strings.Fields(normalizedText)
	m := len(textTokens)
	if len(triggerTokens) < m {
		m = len(triggerTokens)
	}
	return float64(m) / float64(len(triggerTokens)) * 100
}

// triggerVariants mirrors Python's `_trigger_variants`.
func triggerVariants(trigger string, patientInfo string) []string {
	names := extractPatientNames(patientInfo)
	var variants []string

	hasNamePlaceholder := strings.Contains(trigger, "[Name]") || strings.Contains(trigger, "<Name>")
	if hasNamePlaceholder {
		for _, name := range names {
			v := strings.ReplaceAll(trigger, "[Name]", name)
			v = strings.ReplaceAll(v, "<Name>", name)
			variants = append(variants, v)
		}
		v := strings.ReplaceAll(trigger, "[Name]", "")
		v = strings.ReplaceAll(v, "<Name>", "")
		variants = append(variants, v)
	}
	variants = append(variants, trigger)

	return dedupePreserveOrder(variants)
}

// extractPatientNames mirrors Python's `_extract_patient_names`, including
// its quirks: a JSON dict's Name/name/patient_name key wins over line
// scanning, and the line scan checks `line.strip().lower().startswith("name:")`
// while extracting from the *original* (unstripped) line, so a line like
// "* Name: Jai" deliberately does not match (leading "* ").
func extractPatientNames(patientInfo string) []string {
	if patientInfo == "" {
		return nil
	}

	fullName := ""

	var parsed map[string]any
	if err := json.Unmarshal([]byte(patientInfo), &parsed); err == nil && parsed != nil {
		var value any
		for _, key := range []string{"Name", "name", "patient_name"} {
			if v, ok := parsed[key]; ok && isTruthy(v) {
				value = v
				break
			}
		}
		if s, ok := value.(string); ok {
			fullName = strings.TrimSpace(s)
		}
	}

	if fullName == "" {
		for _, line := range splitLines(patientInfo) {
			trimmedLower := strings.ToLower(strings.TrimSpace(line))
			if strings.HasPrefix(trimmedLower, "name:") {
				if idx := strings.Index(line, ":"); idx >= 0 {
					fullName = strings.TrimSpace(line[idx+1:])
				}
				break
			}
		}
	}

	if fullName == "" {
		return nil
	}

	fields := strings.Fields(fullName)
	firstName := ""
	if len(fields) > 0 {
		firstName = fields[0]
	}

	return dedupePreserveOrder(nonEmptyStrings(fullName, firstName))
}

func isTruthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case float64:
		return t != 0
	case string:
		return t != ""
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	default:
		return true
	}
}

var lineSplitRE = regexp.MustCompile(`\r\n|\r|\n`)

func splitLines(s string) []string {
	return lineSplitRE.Split(s, -1)
}

func nonEmptyStrings(items ...string) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		if it != "" {
			out = append(out, it)
		}
	}
	return out
}

func dedupePreserveOrder(items []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(items))
	for _, it := range items {
		if seen[it] {
			continue
		}
		seen[it] = true
		out = append(out, it)
	}
	return out
}

func stringSet(items []string) map[string]bool {
	set := make(map[string]bool, len(items))
	for _, s := range items {
		set[s] = true
	}
	return set
}

// --- Normalization (mirrors Python's `_normalize_for_fuzzy` family) ---

var (
	zwspRE  = regexp.MustCompile(`[\x{200b}\x{200c}\x{200d}\x{feff}]`)
	punctRE = regexp.MustCompile(`[।.,!?;:"'“”‘’()\[\]{}\-—–…]+`)
	spaceRE = regexp.MustCompile(`\s+`)
)

// leadingFillers mirrors Python's `_LEADING_FILLERS`, verbatim.
var leadingFillers = []string{
	"अच्छा",
	"ठीक है",
	"समझ गई",
	"समझ गया",
	"समझ सकती हूँ",
	"समझ सकता हूँ",
	"okay",
	"ok",
	"got it",
	"great",
	"nice",
	"noted",
}

func normalizeForFuzzy(text string) string {
	text = zwspRE.ReplaceAllString(text, "")
	text = strings.ToLower(text)
	text = punctRE.ReplaceAllString(text, " ")
	text = strings.TrimSpace(spaceRE.ReplaceAllString(text, " "))
	return stripLeadingFillers(text)
}

func normalizeForFuzzyWithoutFillers(text string) string {
	text = zwspRE.ReplaceAllString(text, "")
	text = strings.ToLower(text)
	text = punctRE.ReplaceAllString(text, " ")
	return strings.TrimSpace(spaceRE.ReplaceAllString(text, " "))
}

// stripLeadingFillers mirrors Python's `_strip_leading_fillers`: repeatedly
// strip a leading "{filler} " prefix (or return "" on an exact filler
// match), restarting the filler scan after each strip, until no change.
func stripLeadingFillers(text string) string {
	normalizedFillers := make([]string, 0, len(leadingFillers))
	for _, f := range leadingFillers {
		normalizedFillers = append(normalizedFillers, normalizeForFuzzyWithoutFillers(f))
	}

	stripped := text
	previous := ""
	hasPrevious := false

	for stripped != "" && (!hasPrevious || stripped != previous) {
		previous = stripped
		hasPrevious = true

		for _, filler := range normalizedFillers {
			if filler == "" {
				continue
			}
			if stripped == filler {
				return ""
			}
			if strings.HasPrefix(stripped, filler+" ") {
				stripped = strings.TrimSpace(stripped[len(filler):])
				break
			}
		}
	}

	return stripped
}

// --- rapidfuzz-parity scorers (pure-Go rune-level ports of
// rapidfuzz/fuzz_py.py + rapidfuzz/distance/Indel_py.py) ---

// lcsLength returns the longest common subsequence length between two
// rune slices via an O(n*m) time / O(min(n,m)) space DP.
func lcsLength(a, b []rune) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	prev := make([]int, len(a)+1)
	curr := make([]int, len(a)+1)
	for j := 1; j <= len(b); j++ {
		for i := 1; i <= len(a); i++ {
			if a[i-1] == b[j-1] {
				curr[i] = prev[i-1] + 1
			} else if prev[i] >= curr[i-1] {
				curr[i] = prev[i]
			} else {
				curr[i] = curr[i-1]
			}
		}
		prev, curr = curr, prev
	}
	return prev[len(a)]
}

// indelRatio mirrors rapidfuzz's `fuzz.ratio` (normalized Indel similarity):
// 2*LCS(a,b) / (len(a)+len(b)) * 100.
func indelRatio(a, b []rune) float64 {
	lensum := len(a) + len(b)
	if lensum == 0 {
		return 100
	}
	lcs := lcsLength(a, b)
	return float64(2*lcs) / float64(lensum) * 100
}

func indelRatioStr(a, b string) float64 {
	return indelRatio([]rune(a), []rune(b))
}

// indelDistanceStr mirrors rapidfuzz's `Indel.distance`.
func indelDistanceStr(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	return len(ra) + len(rb) - 2*lcsLength(ra, rb)
}

func normDistance(dist, lensum int) float64 {
	if lensum == 0 {
		return 100
	}
	score := 100 - 100*float64(dist)/float64(lensum)
	if score < 0 {
		score = 0
	}
	return score
}

func max3(a, b, c float64) float64 {
	m := a
	if b > m {
		m = b
	}
	if c > m {
		m = c
	}
	return m
}

// partialRatio mirrors rapidfuzz's `fuzz.partial_ratio`: the best
// indelRatio of the shorter string against every optimally-aligned
// (including edge-overhanging) window of the longer string. This always
// performs the exhaustive/guaranteed-optimal search rapidfuzz's own
// pure-Python fallback uses for every length (rather than the C++ engine's
// >64-char approximate "long needle" fast path), which was verified to
// produce identical scores across the full parity corpus plus randomized
// fuzzing of long strings (see the package doc comment above).
func partialRatio(s1, s2 string) float64 {
	r1 := []rune(s1)
	r2 := []rune(s2)
	if len(r1) == 0 && len(r2) == 0 {
		return 100
	}

	var shorter, longer []rune
	if len(r1) <= len(r2) {
		shorter, longer = r1, r2
	} else {
		shorter, longer = r2, r1
	}

	len1 := len(shorter)
	len2 := len(longer)
	if len1 == 0 {
		return 0
	}

	best := 0.0
	for start := -(len1 - 1); start <= len2-1; start++ {
		wStart := start
		if wStart < 0 {
			wStart = 0
		}
		wEnd := start + len1
		if wEnd > len2 {
			wEnd = len2
		}
		if wEnd <= wStart {
			continue
		}
		window := longer[wStart:wEnd]
		if r := indelRatio(shorter, window); r > best {
			best = r
			if best >= 100 {
				break
			}
		}
	}
	return best
}

// tokenSetRatio mirrors rapidfuzz's `fuzz.token_set_ratio`.
func tokenSetRatio(s1, s2 string) float64 {
	tokensA := stringSet(strings.Fields(s1))
	tokensB := stringSet(strings.Fields(s2))
	if len(tokensA) == 0 || len(tokensB) == 0 {
		return 0
	}

	intersect := map[string]bool{}
	diffAB := map[string]bool{}
	diffBA := map[string]bool{}
	for k := range tokensA {
		if tokensB[k] {
			intersect[k] = true
		} else {
			diffAB[k] = true
		}
	}
	for k := range tokensB {
		if !tokensA[k] {
			diffBA[k] = true
		}
	}

	// One token set is a subset of the other (rapidfuzz convention).
	if len(intersect) > 0 && (len(diffAB) == 0 || len(diffBA) == 0) {
		return 100
	}

	diffABJoined := strings.Join(sortedKeys(diffAB), " ")
	diffBAJoined := strings.Join(sortedKeys(diffBA), " ")
	sectJoined := strings.Join(sortedKeys(intersect), " ")

	abLen := utf8.RuneCountInString(diffABJoined)
	baLen := utf8.RuneCountInString(diffBAJoined)
	sectLen := utf8.RuneCountInString(sectJoined)

	sectFlag := 0
	if sectLen != 0 {
		sectFlag = 1
	}

	sectABLen := sectLen + sectFlag + abLen
	sectBALen := sectLen + sectFlag + baLen

	dist := indelDistanceStr(diffABJoined, diffBAJoined)
	result := normDistance(dist, sectABLen+sectBALen)

	if sectLen == 0 {
		return result
	}

	sectABDist := sectFlag + abLen
	sectABRatio := normDistance(sectABDist, sectLen+sectABLen)

	sectBADist := sectFlag + baLen
	sectBARatio := normDistance(sectBADist, sectLen+sectBALen)

	return max3(result, sectABRatio, sectBARatio)
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
