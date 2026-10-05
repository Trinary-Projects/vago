package disha

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jaideep329/talk-go/voicepipelinecore"
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

func gatedEndCallDefinition(t *testing.T) voicepipelinecore.ToolDefinition {
	t.Helper()
	def, err := toolDefinitionFromConfig(map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        "end_call",
			"description": "Disconnect the phone call.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"reason":          map[string]any{"type": "string"},
					"should_end_call": map[string]any{"type": "string", "enum": []any{"yes", "no"}},
				},
				"required": []any{"reason", "should_end_call"},
			},
		},
	})
	if err != nil {
		t.Fatalf("toolDefinitionFromConfig: %v", err)
	}
	return def
}

func runEndCallHandler(t *testing.T, def voicepipelinecore.ToolDefinition, args map[string]any) (bool, voicepipelinecore.ToolCallResponse) {
	t.Helper()
	ended := false
	handler := newEndCallHandler(def, func() { ended = true }, nil)
	resp, err := handler(context.Background(), voicepipelinecore.ToolCallRequest{FunctionName: endCallToolName, Arguments: args})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	return ended, resp
}

func TestEndCallLegacySchemaAlwaysEnds(t *testing.T) {
	def := onboardingEndCallTool()
	for _, args := range []map[string]any{{}, {"should_end_call": "no"}} {
		ended, resp := runEndCallHandler(t, def, args)
		if !ended {
			t.Fatalf("legacy end_call with args %v did not end the call", args)
		}
		if resp.Result.(map[string]any)["status"] != "call_ending" || resp.RunLLM {
			t.Fatalf("legacy response = %+v", resp)
		}
	}
}

func TestEndCallGatedSchemaEndsOnlyOnYes(t *testing.T) {
	def := gatedEndCallDefinition(t)
	cases := []struct {
		args    map[string]any
		wantEnd bool
	}{
		{map[string]any{"reason": "User said bye.", "should_end_call": "yes"}, true},
		{map[string]any{"should_end_call": " YES "}, true},
		{map[string]any{"should_end_call": true}, true},
		{map[string]any{"reason": "User did not confirm.", "should_end_call": "no"}, false},
		{map[string]any{"should_end_call": false}, false},
		{map[string]any{"reason": "User did not confirm."}, false},
		{map[string]any{}, false},
	}
	for _, tc := range cases {
		ended, resp := runEndCallHandler(t, def, tc.args)
		if ended != tc.wantEnd {
			t.Fatalf("args %v: ended = %v, want %v", tc.args, ended, tc.wantEnd)
		}
		wantStatus := "call_continues"
		if tc.wantEnd {
			wantStatus = "call_ending"
		}
		if resp.Result.(map[string]any)["status"] != wantStatus || resp.RunLLM {
			t.Fatalf("args %v: response = %+v, want status %s and RunLLM=false", tc.args, resp, wantStatus)
		}
	}
}
