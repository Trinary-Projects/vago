package disha

func buildPromptTraceMetadata(promptType, name string, version int, variables DocumentVariables) map[string]any {
	return map[string]any{
		promptType + "_prompt_name":      name,
		promptType + "_prompt_version":   version,
		promptType + "_prompt_variables": cloneDocumentVariables(variables),
	}
}

func cloneDocumentVariables(variables DocumentVariables) DocumentVariables {
	copied := DocumentVariables{}
	for key, value := range variables {
		copied[key] = clonePromptMetadataValue(value)
	}
	return copied
}

func clonePromptMetadataValue(value any) any {
	switch v := value.(type) {
	case DocumentVariables:
		return cloneDocumentVariables(v)
	case map[string]any:
		copied := make(map[string]any, len(v))
		for key, item := range v {
			copied[key] = clonePromptMetadataValue(item)
		}
		return copied
	case []any:
		copied := make([]any, len(v))
		for i, item := range v {
			copied[i] = clonePromptMetadataValue(item)
		}
		return copied
	default:
		return value
	}
}
