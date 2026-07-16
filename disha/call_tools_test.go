package disha

import (
	"encoding/json"
	"strings"
	"testing"
)

// A tool with an empty "required" list (e.g. the dynamic-checkin end_call
// config) must marshal as "required":[] — a nil slice becomes JSON null,
// which OpenAI/Azure reject with "None is not of type 'array'". The bug
// stayed hidden while gemma calls only hit OpenRouter, and broke every
// gpt-4.1 fallback turn in prod on 2026-07-16.
func TestToolDefinitionEmptyRequiredMarshalsAsArray(t *testing.T) {
	def, err := toolDefinitionFromConfig(map[string]any{
		"name":        "end_call",
		"required":    []any{},
		"properties":  map[string]any{},
		"description": "End the call.",
	})
	if err != nil {
		t.Fatalf("toolDefinitionFromConfig: %v", err)
	}
	encoded, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), `"required":null`) {
		t.Fatalf("tool definition marshals required as null: %s", encoded)
	}
	if !strings.Contains(string(encoded), `"required":[]`) {
		t.Fatalf("tool definition missing empty required array: %s", encoded)
	}
}

// Same guarantee when the config omits "required" entirely.
func TestToolDefinitionMissingRequiredMarshalsAsArray(t *testing.T) {
	def, err := toolDefinitionFromConfig(map[string]any{
		"name":        "end_call",
		"description": "End the call.",
	})
	if err != nil {
		t.Fatalf("toolDefinitionFromConfig: %v", err)
	}
	encoded, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), `"required":null`) {
		t.Fatalf("tool definition marshals required as null: %s", encoded)
	}
}
