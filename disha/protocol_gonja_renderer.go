package disha

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/noirbizarre/gonja"
	gonjatokens "github.com/noirbizarre/gonja/tokens"
)

// gonjaProtocolRenderer renders only protocol instruction text fetched from
// Weaviate. DocumentStore deliberately continues to use Python Jinja2 for all
// Langfuse-backed prompts.
type gonjaProtocolRenderer struct{}

func newGonjaProtocolRenderer() *gonjaProtocolRenderer {
	return &gonjaProtocolRenderer{}
}

func (r *gonjaProtocolRenderer) RenderTemplate(
	ctx context.Context,
	label, text string,
	variables DocumentVariables,
) (protocolTemplateRenderResult, error) {
	if r == nil {
		return protocolTemplateRenderResult{}, errors.New("disha: gonja protocol renderer is nil")
	}
	if err := ctx.Err(); err != nil {
		return protocolTemplateRenderResult{}, err
	}

	template, err := gonja.FromString(text)
	if err != nil {
		return protocolTemplateRenderResult{}, fmt.Errorf("disha: compile protocol template %q with gonja: %w", label, err)
	}
	variableNames, err := protocolTemplateVariableNames(text)
	if err != nil {
		return protocolTemplateRenderResult{}, fmt.Errorf("disha: inspect protocol template %q: %w", label, err)
	}
	var missing []string
	for _, name := range variableNames {
		if _, ok := variables[name]; !ok {
			missing = append(missing, name)
		}
	}

	// Deliberately pass the original values to Gonja. Its native behavior is
	// the contract here: a missing name renders empty and evaluates false.
	output, err := template.Execute(gonja.Context(variables))
	if err != nil {
		return protocolTemplateRenderResult{}, fmt.Errorf("disha: render protocol template %q with gonja: %w", label, err)
	}
	if err := ctx.Err(); err != nil {
		return protocolTemplateRenderResult{}, err
	}
	return protocolTemplateRenderResult{Text: output, MissingVariables: missing}, nil
}

// protocolTemplateVariableNames is a reporting-only preflight over Gonja's
// lexer. It does not modify rendering: missing names are returned to the
// caller for Sentry capture, then Gonja keeps its native empty/false behavior.
func protocolTemplateVariableNames(text string) ([]string, error) {
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
		lowerName := strings.ToLower(name)
		if isProtocolExpressionKeyword(lowerName) {
			previous = token
			continue
		}
		if previous != nil && (previous.Type == gonjatokens.Dot || previous.Type == gonjatokens.Pipe) {
			previous = token
			continue
		}
		if previous != nil && previous.Type == gonjatokens.Name && strings.EqualFold(previous.Val, "is") {
			previous = token
			continue
		}
		if index+1 < len(tokens) && tokens[index+1].Type == gonjatokens.Assign {
			// A filter/test keyword argument, not a context variable.
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

func isProtocolExpressionKeyword(name string) bool {
	switch name {
	case "and", "false", "in", "is", "none", "not", "or", "true":
		return true
	default:
		return false
	}
}
