package disha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/noirbizarre/gonja"
	gonjaexec "github.com/noirbizarre/gonja/exec"
	gonjatokens "github.com/noirbizarre/gonja/tokens"
)

var protocolBareOutputPattern = regexp.MustCompile(
	`\{\{(-?)\s*([A-Za-z_][A-Za-z0-9_]*)\s*(-?)\}\}`,
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
) (string, error) {
	if r == nil {
		return "", errors.New("disha: gonja protocol renderer is nil")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	variableNames, err := protocolTemplateVariableNames(text)
	if err != nil {
		return "", fmt.Errorf("disha: inspect protocol template %q: %w", label, err)
	}
	gonjaVariables, explicitNulls, err := normalizeProtocolTemplateVariables(variables)
	if err != nil {
		return "", fmt.Errorf("disha: normalize protocol template %q variables: %w", label, err)
	}
	for _, name := range variableNames {
		if _, ok := gonjaVariables[name]; !ok {
			// Gonja normally treats a missing name as nil. Supplying an error Value
			// recreates Jinja StrictUndefined lazily: an evaluated missing value
			// fails, while a missing value in an untaken branch is never touched.
			gonjaVariables[name] = gonjaexec.ValueError(fmt.Errorf("%q is undefined", name))
		}
	}

	// Jinja renders an explicitly supplied null as "None". Gonja renders nil
	// as an empty string. Rewriting only bare output expressions preserves the
	// current protocol corpus byte-for-byte without changing nil truthiness in
	// an if condition.
	text = renderProtocolNullOutputs(text, explicitNulls)
	template, err := gonja.FromString(text)
	if err != nil {
		return "", fmt.Errorf("disha: compile protocol template %q with gonja: %w", label, err)
	}
	output, err := template.Execute(gonjaVariables)
	if err != nil {
		return "", fmt.Errorf("disha: render protocol template %q with gonja: %w", label, err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return output, nil
}

// protocolTemplateVariableNames extracts root variables from the expression
// forms protocols use. Protocol templates intentionally support output
// expressions plus if/elif/else/endif; other statement families are rejected
// so a future author cannot silently rely on Python-only scope semantics.
func protocolTemplateVariableNames(text string) ([]string, error) {
	lexer := gonjatokens.NewLexer(text)
	go lexer.Run()
	tokens := make([]*gonjatokens.Token, 0)
	for token := range lexer.Tokens {
		tokens = append(tokens, token)
	}

	variables := make(map[string]struct{})
	inExpression := false
	inBlock := false
	expectBlockName := false
	var previous *gonjatokens.Token

	for index, token := range tokens {
		switch token.Type {
		case gonjatokens.Error:
			return nil, errors.New(token.Val)
		case gonjatokens.VariableBegin:
			inExpression = true
			inBlock = false
			previous = nil
			continue
		case gonjatokens.BlockBegin:
			inExpression = true
			inBlock = true
			expectBlockName = true
			previous = nil
			continue
		case gonjatokens.VariableEnd, gonjatokens.BlockEnd:
			inExpression = false
			inBlock = false
			expectBlockName = false
			previous = nil
			continue
		case gonjatokens.Whitespace:
			continue
		}
		if !inExpression {
			continue
		}

		if token.Type == gonjatokens.Name && inBlock && expectBlockName {
			expectBlockName = false
			switch strings.ToLower(token.Val) {
			case "if", "elif", "else", "endif":
				previous = token
				continue
			default:
				return nil, fmt.Errorf("unsupported protocol template statement %q", token.Val)
			}
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

// normalizeProtocolTemplateVariables recreates the Go -> JSON -> Python value
// boundary used by the old renderer. This dereferences pointers, preserves
// explicit nulls, converts structs through their JSON representation, and
// keeps integral JSON numbers integral before Gonja sees them.
func normalizeProtocolTemplateVariables(
	variables DocumentVariables,
) (gonja.Context, map[string]bool, error) {
	wire, err := json.Marshal(variables)
	if err != nil {
		return nil, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, nil, err
	}
	if decoded == nil {
		decoded = map[string]any{}
	}

	normalized := make(gonja.Context, len(decoded))
	explicitNulls := make(map[string]bool)
	for name, value := range decoded {
		normalized[name] = normalizeProtocolJSONValue(value)
		if value == nil {
			explicitNulls[name] = true
		}
	}
	return normalized, explicitNulls, nil
}

func normalizeProtocolJSONValue(value any) any {
	switch value := value.(type) {
	case json.Number:
		text := value.String()
		if !strings.ContainsAny(text, ".eE") {
			if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
				return integer
			}
			if integer, err := strconv.ParseUint(text, 10, 64); err == nil {
				return integer
			}
		}
		if number, err := strconv.ParseFloat(text, 64); err == nil {
			return number
		}
		return text
	case []any:
		for index, item := range value {
			value[index] = normalizeProtocolJSONValue(item)
		}
		return value
	case map[string]any:
		for key, item := range value {
			value[key] = normalizeProtocolJSONValue(item)
		}
		return value
	default:
		return value
	}
}

func renderProtocolNullOutputs(text string, explicitNulls map[string]bool) string {
	if len(explicitNulls) == 0 {
		return text
	}
	return protocolBareOutputPattern.ReplaceAllStringFunc(text, func(output string) string {
		match := protocolBareOutputPattern.FindStringSubmatch(output)
		if len(match) != 4 || !explicitNulls[match[2]] {
			return output
		}
		return "{{" + match[1] + ` "None" ` + match[3] + "}}"
	})
}
