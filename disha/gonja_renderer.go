package disha

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/noirbizarre/gonja"
	gonjatokens "github.com/noirbizarre/gonja/tokens"
)

// The shared renderer contract (DocumentVariables, TemplateRenderRequest,
// TemplateRenderResult, TemplateRenderer) lives in template_renderer.go.

// GonjaJinjaRenderer is the pure-Go, in-process document-store renderer built on
// gonja. It renders Langfuse-backed prompts and reports unresolved variables. It
// needs no subprocess. The protocol path (gonjaProtocolRenderer,
// protocol_gonja_renderer.go) shares this file's render core and variable
// preflight but keeps its own missing-only reporting contract.
//
// Precision notes vs. Python Jinja2 (PythonJinjaRenderer, kept only as a test
// oracle):
//   - gonja has no jinja2.meta.find_undeclared_variables, so referenced names
//     are recovered from a lexer preflight (see gonjaTemplateVariableNames).
//   - gonja has no StrictUndefined mode (a missing name silently resolves to
//     nil at render time), so detection is static and top-level-name granular:
//     `{{ user.name }}` inspects `user`. It answers "which input variable has no
//     usable value", branch-agnostically — not Python's runtime, per-render-site
//     attribution.
//
// Known OUTPUT divergences from Python Jinja2 (accepted; verified logically
// identical on the live corpus):
//
//   - nil value in `{{ x }}`: gonja renders "" (empty); Python renders "None".
//     Conditionals agree (nil is falsey in both).
//   - integral float in `{{ x }}`: gonja renders "499.0"; Python renders "499"
//     (encoding/json decodes JSON numbers to float64).
//   - `{{ x | default('y') }}` with x==nil: gonja replaces nil and renders "y";
//     Python's default() only fires for *undefined* names, not None.
type GonjaJinjaRenderer struct {
	logger *log.Logger
}

// NewGonjaJinjaRenderer constructs the in-process gonja renderer.
func NewGonjaJinjaRenderer(logger *log.Logger) *GonjaJinjaRenderer {
	return &GonjaJinjaRenderer{logger: logger}
}

// Render satisfies TemplateRenderer for the document store. It renders the
// template and reports every referenced variable with no usable value (missing
// OR nil) in UnresolvedVariables. Compile/render failures return an error.
func (r *GonjaJinjaRenderer) Render(ctx context.Context, req TemplateRenderRequest) (TemplateRenderResult, error) {
	if r == nil {
		return TemplateRenderResult{}, errors.New("disha: gonja jinja renderer is nil")
	}
	output, undefined, nilVars, err := gonjaRenderText(ctx, req.DocumentName, req.DocumentVersion, req.Text, req.Variables)
	if err != nil {
		return TemplateRenderResult{}, err
	}
	unresolved := mergeSortedNames(undefined, nilVars)
	if r.logger != nil && len(unresolved) > 0 {
		r.logger.Printf("disha: gonja document %q version=%d unresolved variables=%v\n",
			req.DocumentName, req.DocumentVersion, unresolved)
	}
	return TemplateRenderResult{Output: output, UnresolvedVariables: unresolved}, nil
}

// Close is a no-op: gonja renders in-process, so there is no subprocess to tear
// down. It exists so this renderer satisfies TemplateRenderer.
func (r *GonjaJinjaRenderer) Close() error { return nil }

// gonjaRenderText compiles and renders text with stock gonja and statically
// classifies referenced variables into undefined (key absent) and nilVars (key
// present, value nil). It is the shared core behind GonjaJinjaRenderer.Render
// (document store, which merges both) and gonjaProtocolRenderer.RenderTemplate
// (protocols, which report undefined only). Rendering itself uses gonja's native
// contract: a missing name renders empty and evaluates false.
func gonjaRenderText(ctx context.Context, label string, version int, text string, variables DocumentVariables) (output string, undefined, nilVars []string, err error) {
	if err := ctx.Err(); err != nil {
		return "", nil, nil, err
	}
	if variables == nil {
		variables = DocumentVariables{}
	}

	referenced, err := gonjaTemplateVariableRefs(text)
	if err != nil {
		return "", nil, nil, fmt.Errorf("disha: inspect template %q version=%d: %w", label, version, err)
	}

	names := make([]string, 0, len(referenced))
	for name := range referenced {
		names = append(names, name)
	}
	sort.Strings(names) // keep undefined/nilVars sorted for mergeSortedNames

	for _, name := range names {
		value, ok := variables[name]
		switch {
		case !ok:
			undefined = append(undefined, name)
		case value == nil && referenced[name].IsOutput:
			// A nil matters only where it renders directly ({{ x }}); a nil used
			// solely in a block ({% if x is not none %}) is a legitimate input.
			nilVars = append(nilVars, name)
		}
	}

	template, err := gonja.FromString(text)
	if err != nil {
		return "", undefined, nilVars, fmt.Errorf("disha: compile template %q version=%d with gonja: %w", label, version, err)
	}
	// Deliberately pass the original values to gonja: its native behavior is the
	// contract (a missing name renders empty and evaluates false).
	output, err = template.Execute(gonja.Context(variables))
	if err != nil {
		return "", undefined, nilVars, fmt.Errorf("disha: render template %q version=%d with gonja: %w", label, version, err)
	}
	if err := ctx.Err(); err != nil {
		return "", undefined, nilVars, err
	}
	return output, undefined, nilVars, nil
}

// mergeSortedNames unions two already-sorted, disjoint name lists into one
// sorted list. undefined and nilVars are disjoint by construction (a name is
// either absent or present-nil, never both).
func mergeSortedNames(undefined, nilVars []string) []string {
	if len(undefined) == 0 {
		return nilVars
	}
	if len(nilVars) == 0 {
		return undefined
	}
	merged := make([]string, 0, len(undefined)+len(nilVars))
	merged = append(merged, undefined...)
	merged = append(merged, nilVars...)
	sort.Strings(merged)
	return merged
}

// TemplateVarRef holds preflight facts about a referenced variable. It is a
// struct so more properties can be added later without changing the map shape.
type TemplateVarRef struct {
	IsOutput bool // referenced at an output site ({{ ... }})
}

// gonjaTemplateVariableNames returns the sorted names referenced by the template,
// derived from gonjaTemplateVariableRefs.
func gonjaTemplateVariableNames(text string) ([]string, error) {
	refs, err := gonjaTemplateVariableRefs(text)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// gonjaTemplateVariableRefs is a reporting-only preflight over gonja's lexer.
// It returns the top-level context names referenced by the template that must be
// SUPPLIED EXTERNALLY, each mapped to its TemplateVarRef. It does not affect
// rendering.
// gonja has no jinja2.meta.find_undeclared_variables equivalent, so the token
// stream is scanned directly: names inside {{ ... }} / {% ... %} that are not
// keywords, filter/test names, attribute lookups, or keyword-argument names.
//
// Template-local names — which are defined by the template itself and can never
// be passed in — are collected and subtracted, so they are never reported as
// unresolved (which would fire a redundant missing-variable Sentry on every
// render). These are: `for` loop targets (`{% for item in items %}` → `item`),
// `set` assignment targets (`{% set x = ... %}` → `x`), the special `loop`
// object, and test names after `is` / `is not` (`{{ y is not defined %}` →
// `defined`). This matches Python's find_undeclared_variables for these forms.
//
// It deliberately still OVER-reports the branch-agnostic case: a variable used
// only inside an untaken `{% if %}` branch is reported, because the scan cannot
// know which branch runs and every branch's inputs must be supplied.
//
// Known remaining over-reports vs find_undeclared_variables, all for advanced
// tags that are unlikely in prompt/protocol templates and where over-reporting
// is the SAFE direction (an extra Sentry, never a hidden missing var):
//   - `{% with foo = bar %}` reports the target `foo` (bar is correct).
//   - `{% macro m(a) %}` reports the macro name `m` and args `a`.
//   - `{% filter fname %}` reports the filter name `fname`.
//   - `{% block name %}` reports the block name.
//   - `{% for x in xs recursive %}` reports `recursive` (a contextual word Jinja
//     also allows as an identifier, so it is not blanket-excluded).
//
// Note: gonja does NOT support Jinja's inline conditional expression
// `{{ a if cond else b }}` at all — it is a hard COMPILE error, so such a
// template fails to render entirely rather than mis-reporting a variable.
func gonjaTemplateVariableRefs(text string) (map[string]TemplateVarRef, error) {
	lexer := gonjatokens.NewLexer(text)
	go lexer.Run()
	tokens := make([]*gonjatokens.Token, 0)
	for token := range lexer.Tokens {
		tokens = append(tokens, token)
	}

	variables := make(map[string]struct{})
	outputVars := make(map[string]struct{}) // subset referenced inside {{ ... }}
	locals := make(map[string]struct{})     // template-defined names (for/set targets)
	inExpression := false
	inOutputSite := false // true inside {{ ... }}, false inside {% ... %}
	expectBlockName := false
	inForTargets := false   // between `for` and `in`: names are loop locals
	inSetTargets := false   // between `set` and `=`: names are assignment targets
	expectTestName := false // after `is` / `is not`: the next name is a test name
	var previous *gonjatokens.Token

	for index, token := range tokens {
		switch token.Type {
		case gonjatokens.Error:
			return nil, errors.New(token.Val)
		case gonjatokens.VariableBegin:
			inExpression = true
			inOutputSite = true
			expectBlockName = false
			inForTargets = false
			inSetTargets = false
			expectTestName = false
			previous = nil
			continue
		case gonjatokens.BlockBegin:
			inExpression = true
			inOutputSite = false
			expectBlockName = true
			inForTargets = false
			inSetTargets = false
			expectTestName = false
			previous = nil
			continue
		case gonjatokens.VariableEnd, gonjatokens.BlockEnd:
			inExpression = false
			expectBlockName = false
			inForTargets = false
			inSetTargets = false
			expectTestName = false
			previous = nil
			continue
		case gonjatokens.Whitespace:
			continue
		}
		if !inExpression {
			continue
		}

		// The first Name after {% is the tag name (if/for/set/...), not a var.
		if token.Type == gonjatokens.Name && expectBlockName {
			expectBlockName = false
			switch strings.ToLower(token.Val) {
			case "for":
				inForTargets = true
			case "set":
				inSetTargets = true
			}
			previous = token
			continue
		}

		// `for` header: loop targets up to `in` are locals, not external vars.
		// `in` closes the target list; names after it (the iterable) are real.
		if inForTargets {
			if token.Type == gonjatokens.Name {
				if strings.EqualFold(token.Val, "in") {
					inForTargets = false
				} else {
					locals[token.Val] = struct{}{}
				}
			}
			previous = token
			continue
		}

		// `set` header: assignment targets up to `=` are locals. The `=` closes
		// the target list; the RHS names after it are real variables.
		if inSetTargets {
			if token.Type == gonjatokens.Assign {
				inSetTargets = false
			} else if token.Type == gonjatokens.Name {
				locals[token.Val] = struct{}{}
			}
			previous = token
			continue
		}

		if token.Type != gonjatokens.Name {
			previous = token
			continue
		}

		name := token.Val
		lower := strings.ToLower(name)

		// `is` / `is not` introduce a test name (e.g. `defined`, `divisibleby`),
		// which is not a context var. `not` here is part of `is not`, not the
		// unary operator, so keep waiting for the test name.
		if expectTestName {
			if lower != "not" {
				expectTestName = false
			}
			previous = token
			continue
		}
		if lower == "is" {
			expectTestName = true
			previous = token
			continue
		}
		if isGonjaExpressionKeyword(lower) {
			previous = token
			continue
		}
		// Jinja's special `loop` object exists only inside a for-body and can
		// never be passed in externally.
		if lower == "loop" {
			previous = token
			continue
		}
		// Attribute (`.name`) or filter/test (`| name`) — not a context var.
		if previous != nil && (previous.Type == gonjatokens.Dot || previous.Type == gonjatokens.Pipe) {
			previous = token
			continue
		}
		// `name=` is a filter/test keyword argument, not a context var.
		if index+1 < len(tokens) && tokens[index+1].Type == gonjatokens.Assign {
			previous = token
			continue
		}
		variables[name] = struct{}{}
		if inOutputSite {
			outputVars[name] = struct{}{}
		}
		previous = token
	}

	refs := make(map[string]TemplateVarRef, len(variables))
	for name := range variables {
		// Body uses of a for/set target (`{{ item }}`, `{{ x }}`) are locals too.
		if _, isLocal := locals[name]; isLocal {
			continue
		}
		_, isOutput := outputVars[name]
		refs[name] = TemplateVarRef{IsOutput: isOutput}
	}
	return refs, nil
}

// isGonjaExpressionKeyword reports whether a bare name is a Jinja hard keyword
// that can never be a context variable. It lists the expression operators/
// literals plus if/elif/else, which appear inline in for-loop conditions
// (`{% for x in xs if cond %}`) and as tag names. Jinja forbids all of these as
// variable identifiers, so excluding them cannot hide a genuinely-missing var.
// Contextual words that Jinja DOES allow as identifiers (e.g. `recursive`,
// `block`, `with`, `filter`) are deliberately NOT here — excluding them could
// cause a false negative; they are handled positionally or accepted as
// over-reports (see gonjaTemplateVariableNames).
func isGonjaExpressionKeyword(name string) bool {
	switch name {
	case "and", "elif", "else", "false", "if", "in", "is", "none", "not", "or", "true":
		return true
	default:
		return false
	}
}
