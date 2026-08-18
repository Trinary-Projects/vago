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

	referenced, err := gonjaTemplateVariableNames(text)
	if err != nil {
		return "", nil, nil, fmt.Errorf("disha: inspect template %q version=%d: %w", label, version, err)
	}

	// referenced is sorted, so both sublists stay sorted without extra work.
	for _, name := range referenced {
		value, ok := variables[name]
		switch {
		case !ok:
			undefined = append(undefined, name)
		case value == nil:
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

// gonjaTemplateVariableNames is a reporting-only preflight over gonja's lexer.
// It returns the sorted set of top-level context names referenced by the
// template. It does not affect rendering. gonja has no
// jinja2.meta.find_undeclared_variables equivalent, so the token stream is
// scanned directly: names inside {{ ... }} / {% ... %} that are not keywords,
// filter/test names, attribute lookups, or keyword-argument names.
func gonjaTemplateVariableNames(text string) ([]string, error) {
	lexer := gonjatokens.NewLexer(text)
	go lexer.Run()
	tokens := make([]*gonjatokens.Token, 0)
	for token := range lexer.Tokens {
		tokens = append(tokens, token)
	}

	variables := make(map[string]struct{})
	inExpression := false
	expectBlockName := false
	var previous *gonjatokens.Token

	for index, token := range tokens {
		switch token.Type {
		case gonjatokens.Error:
			return nil, errors.New(token.Val)
		case gonjatokens.VariableBegin:
			inExpression = true
			previous = nil
			continue
		case gonjatokens.BlockBegin:
			inExpression = true
			expectBlockName = true
			previous = nil
			continue
		case gonjatokens.VariableEnd, gonjatokens.BlockEnd:
			inExpression = false
			expectBlockName = false
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
			previous = token
			continue
		}
		if token.Type != gonjatokens.Name {
			previous = token
			continue
		}

		name := token.Val
		if isGonjaExpressionKeyword(strings.ToLower(name)) {
			previous = token
			continue
		}
		// Attribute (`.name`) or filter/test (`| name`) — not a context var.
		if previous != nil && (previous.Type == gonjatokens.Dot || previous.Type == gonjatokens.Pipe) {
			previous = token
			continue
		}
		// Test name after `is` — not a context var.
		if previous != nil && previous.Type == gonjatokens.Name && strings.EqualFold(previous.Val, "is") {
			previous = token
			continue
		}
		// `name=` is a filter/test keyword argument, not a context var.
		if index+1 < len(tokens) && tokens[index+1].Type == gonjatokens.Assign {
			previous = token
			continue
		}
		variables[name] = struct{}{}
		previous = token
	}

	names := make([]string, 0, len(variables))
	for name := range variables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func isGonjaExpressionKeyword(name string) bool {
	switch name {
	case "and", "false", "in", "is", "none", "not", "or", "true":
		return true
	default:
		return false
	}
}
