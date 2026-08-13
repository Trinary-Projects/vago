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

// GonjaRenderRequest is the input to GonjaJinjaRenderer. It is self-contained
// and does not share types with jinja_renderer.go.
type GonjaRenderRequest struct {
	DocumentName    string
	DocumentVersion int
	Text            string
	Variables       map[string]any
}

// GonjaRenderResult is the output of GonjaJinjaRenderer. Variable problems are
// reported as two lists, undefined first then nil:
//
//   - Undefined: names the template references that were NOT supplied.
//   - NilVars:   referenced names that WERE supplied but whose value is nil
//     (e.g. JSON null).
type GonjaRenderResult struct {
	Output    string
	Undefined []string
	NilVars   []string
}

// GonjaJinjaRenderer is a pure-Go, fully self-contained template renderer built
// on gonja. It is a parallel implementation to PythonJinjaRenderer
// (jinja_renderer.go), which is left untouched for reference. Unlike the Python
// renderer it needs no subprocess: gonja renders in-process.
//
// Precision notes vs. Python Jinja2:
//   - gonja has no jinja2.meta.find_undeclared_variables, so referenced names
//     are recovered from a lexer preflight (see gonjaTemplateVariableNames).
//   - gonja has no StrictUndefined mode (a missing name silently resolves to
//     nil at render time), so "undefined" is detected structurally from the
//     referenced-name set rather than from a strict render.
//   - Detection is at top-level-name granularity: `{{ user.name }}` inspects
//     `user`. This answers "which input variable is missing/nil", not Python's
//     per-expression render-site attribution.
//
// Known OUTPUT divergences from Python Jinja2 (gonja is a different engine, so
// byte-identical parity is not guaranteed — these are the cases observed so
// far, and new ones can surface as templates use more features):
//
//   - nil value in `{{ x }}`: gonja renders "" (empty); Python Jinja2 renders
//     the literal "None". Conditionals agree (nil is falsey in both).
//   - integral float in `{{ x }}`: gonja renders "499.0"; Python renders "499".
//     Root cause is JSON parsing, not the engine: encoding/json decodes JSON
//     numbers to float64 (499 -> 499.0), while Python's json decodes to int.
//   - `{{ x | default('y') }}` with x==nil: gonja replaces nil and renders "y";
//     Python's default() only fires for *undefined* names, not None, so it
//     renders "None" (use default('y', true) in Python to also replace None).
//
// These are unresolved w.r.t. the "same document store feeds both runtimes"
// consistency goal; see the open renderer decision. If exact parity is
// required, render through PythonJinjaRenderer (jinja_renderer.go) instead.
type GonjaJinjaRenderer struct {
	logger *log.Logger
}

// NewGonjaJinjaRenderer constructs the in-process gonja renderer.
func NewGonjaJinjaRenderer(logger *log.Logger) *GonjaJinjaRenderer {
	return &GonjaJinjaRenderer{logger: logger}
}

// Render compiles and renders req.Text with req.Variables, reporting undefined
// and nil variables. Compile/render failures return an error; missing/nil
// variables are reported in the result, not as errors (gonja renders them as
// empty/false, matching its native contract).
func (r *GonjaJinjaRenderer) Render(ctx context.Context, req GonjaRenderRequest) (GonjaRenderResult, error) {
	if r == nil {
		return GonjaRenderResult{}, errors.New("disha: gonja jinja renderer is nil")
	}
	if err := ctx.Err(); err != nil {
		return GonjaRenderResult{}, err
	}
	variables := req.Variables
	if variables == nil {
		variables = map[string]any{}
	}

	referenced, err := gonjaTemplateVariableNames(req.Text)
	if err != nil {
		return GonjaRenderResult{}, fmt.Errorf("disha: inspect document %q version=%d: %w", req.DocumentName, req.DocumentVersion, err)
	}

	// referenced is sorted, so both sublists stay sorted without extra work.
	var undefined, nilVars []string
	for _, name := range referenced {
		value, ok := variables[name]
		switch {
		case !ok:
			undefined = append(undefined, name)
		case value == nil:
			nilVars = append(nilVars, name)
		}
	}

	result := GonjaRenderResult{Undefined: undefined, NilVars: nilVars}
	if r.logger != nil && (len(undefined) > 0 || len(nilVars) > 0) {
		r.logger.Printf("disha: gonja document %q version=%d undefined=%v nil=%v\n",
			req.DocumentName, req.DocumentVersion, undefined, nilVars)
	}

	template, err := gonja.FromString(req.Text)
	if err != nil {
		return GonjaRenderResult{}, fmt.Errorf("disha: compile document %q version=%d with gonja: %w", req.DocumentName, req.DocumentVersion, err)
	}
	// Deliberately pass the original values to gonja: its native behavior is the
	// contract (a missing name renders empty and evaluates false).
	output, err := template.Execute(gonja.Context(variables))
	if err != nil {
		return GonjaRenderResult{}, fmt.Errorf("disha: render document %q version=%d with gonja: %w", req.DocumentName, req.DocumentVersion, err)
	}
	if err := ctx.Err(); err != nil {
		return GonjaRenderResult{}, err
	}
	result.Output = output
	return result, nil
}

// Close is a no-op: gonja renders in-process, so there is no subprocess to tear
// down. It exists so this renderer mirrors PythonJinjaRenderer's lifecycle.
func (r *GonjaJinjaRenderer) Close() error { return nil }

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
