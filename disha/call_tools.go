package disha

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const (
	endCallToolName           = "end_call"
	shouldEndCallArg          = "should_end_call"
	endCallRejectedPromptText = "Hello?"
)

func buildCallToolDefinitionsFromConfig(promptConfig map[string]any) ([]voicepipelinecore.ToolDefinition, error) {
	rawTools, ok := promptConfig["tools"]
	if !ok || rawTools == nil {
		return nil, nil
	}
	items, ok := rawTools.([]any)
	if !ok {
		return nil, fmt.Errorf("disha: prompt config tools must be a list, got %T", rawTools)
	}
	if len(items) == 0 {
		return nil, nil
	}

	tools := make([]voicepipelinecore.ToolDefinition, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("disha: invalid tool config: %T", item)
		}
		def, err := toolDefinitionFromConfig(m)
		if err != nil {
			return nil, err
		}
		tools = append(tools, def)
	}
	return tools, nil
}

func toolDefinitionFromConfig(config map[string]any) (voicepipelinecore.ToolDefinition, error) {
	functionConfig := config
	if nested, ok := config["function"].(map[string]any); ok {
		functionConfig = nested
	}
	name, _ := functionConfig["name"].(string)
	name = strings.TrimSpace(name)
	if name == "" {
		return voicepipelinecore.ToolDefinition{}, errors.New("disha: tool config missing function name")
	}
	description, _ := functionConfig["description"].(string)
	parameters, _ := functionConfig["parameters"].(map[string]any)
	if parameters == nil {
		properties, _ := functionConfig["properties"].(map[string]any)
		required, _ := functionConfig["required"].([]any)
		parameters = map[string]any{
			"type":       "object",
			"properties": propertiesOrEmpty(properties),
			"required":   stringSliceFromAny(required),
		}
	}
	var strict *bool
	if v, ok := functionConfig["strict"].(bool); ok {
		strict = &v
	}
	return voicepipelinecore.ToolDefinition{
		Type: "function",
		Function: voicepipelinecore.ToolFunction{
			Name:        name,
			Description: description,
			Parameters:  parameters,
			Strict:      strict,
		},
	}, nil
}

func propertiesOrEmpty(properties map[string]any) map[string]any {
	if properties == nil {
		return map[string]any{}
	}
	return properties
}

// stringSliceFromAny always returns a non-nil slice: it feeds the tool
// schema's "required" field, and a nil slice marshals to JSON null,
// which OpenAI/Azure reject with "None is not of type 'array'"
// (OpenRouter tolerates it, so the bug only surfaced on gpt-4.1
// fallback turns).
func stringSliceFromAny(values []any) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
			out = append(out, s)
		}
	}
	return out
}

func registerEndCallTool(llm *voicepipelinecore.LLMProcessor, task *voicepipelinecore.PipelineTask, def voicepipelinecore.ToolDefinition) {
	var logger *log.Logger
	if task != nil && task.TaskCtx != nil {
		logger = task.TaskCtx.Logger
	}
	endCall := func() {
		if task != nil {
			task.End(voicepipelinecore.EndReasonUnspecified)
		}
	}
	llm.RegisterTool(def, newEndCallHandler(def, endCall, logger), voicepipelinecore.ToolOptions{CancelOnInterruption: false, Timeout: 5 * time.Second})
}

// newEndCallHandler ends the call on every invocation, unless the tool
// definition declares should_end_call. Then it ends the call only when
// the model sets should_end_call to "yes", because the model sometimes
// calls end_call and still decides that the call should continue. A
// rejected call is dropped from the model context, and if the model said
// nothing in that turn, the user hears the idle prompt instead of silence.
func newEndCallHandler(def voicepipelinecore.ToolDefinition, endCall func(), logger *log.Logger) voicepipelinecore.ToolHandler {
	gated := declaresShouldEndCall(def)
	return func(_ context.Context, req voicepipelinecore.ToolCallRequest) (voicepipelinecore.ToolCallResponse, error) {
		if gated && !shouldEndCallFromArgs(req.Arguments) {
			emptyReply := strings.TrimSpace(req.AssistantText) == ""
			if logger != nil {
				logger.Printf("disha: end_call ignored should_end_call=%v empty_reply=%v reason=%q\n", req.Arguments[shouldEndCallArg], emptyReply, req.Arguments["reason"])
			}
			resp := voicepipelinecore.ToolCallResponse{Result: map[string]any{"status": "call_continues"}, RunLLM: false, DropFromContext: true}
			if emptyReply {
				resp.SpeakText = endCallRejectedPromptText
			}
			return resp, nil
		}
		if gated && logger != nil {
			logger.Printf("disha: end_call accepted should_end_call=%v reason=%q\n", req.Arguments[shouldEndCallArg], req.Arguments["reason"])
		}
		endCall()
		return voicepipelinecore.ToolCallResponse{Result: map[string]any{"status": "call_ending"}, RunLLM: false}, nil
	}
}

func declaresShouldEndCall(def voicepipelinecore.ToolDefinition) bool {
	properties, _ := def.Function.Parameters["properties"].(map[string]any)
	_, ok := properties[shouldEndCallArg]
	return ok
}

func shouldEndCallFromArgs(args map[string]any) bool {
	switch v := args[shouldEndCallArg].(type) {
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "yes")
	case bool:
		return v
	default:
		return false
	}
}
