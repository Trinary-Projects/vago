package weaviate

import (
	"encoding/json"
	"strings"
)

// Filter builders for GraphQL `where` clauses. They exist so callers never
// hand-concatenate GraphQL: every path segment and value goes through
// json.Marshal, which quotes and escapes it, so a value containing a quote or
// backslash cannot break out of its literal.
//
// Paths may cross a reference, matching Weaviate's syntax:
//
//	EqualBool([]string{"answeredBy", "ProtocolInstruction", "isStaging"}, true)

// EqualBool matches a boolean property.
func EqualBool(path []string, value bool) string {
	return equal(path, "valueBoolean", value)
}

// EqualString matches a string/text property.
func EqualString(path []string, value string) string {
	return equal(path, "valueString", value)
}

// EqualInt matches an int property.
func EqualInt(path []string, value int) string {
	return equal(path, "valueInt", value)
}

func equal(path []string, valueKey string, value any) string {
	return "{ path: " + encode(path) + " operator: Equal " + valueKey + ": " + encode(value) + " }"
}

// And combines operands with operator: And. Empty operands are dropped; a
// single surviving operand is returned unwrapped, and zero operands return ""
// (which callers treat as "no filter").
func And(operands ...string) string { return combine("And", operands) }

// Or combines operands with operator: Or, with the same empty/single-operand
// handling as And.
func Or(operands ...string) string { return combine("Or", operands) }

func combine(operator string, operands []string) string {
	kept := make([]string, 0, len(operands))
	for _, operand := range operands {
		if strings.TrimSpace(operand) != "" {
			kept = append(kept, operand)
		}
	}
	switch len(kept) {
	case 0:
		return ""
	case 1:
		return kept[0]
	default:
		return "{ operator: " + operator + " operands: [ " + strings.Join(kept, " ") + " ] }"
	}
}

// encode renders a Go value as a GraphQL literal. GraphQL's literal syntax for
// strings, numbers, booleans and lists thereof is JSON-compatible, so
// json.Marshal is both correct and injection-safe here.
func encode(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		// Unreachable for the string/bool/int/[]string inputs these helpers
		// accept; an empty literal fails the query loudly rather than
		// silently widening the filter.
		return `""`
	}
	return string(raw)
}
