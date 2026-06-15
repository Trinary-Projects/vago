package voicepipelinecore

func messagesFromInitial(initial []Message) []Message {
	out := make([]Message, 0, len(initial))
	for _, msg := range initial {
		if msg.Role == "" {
			continue
		}
		if msg.Content == "" && len(msg.ToolCalls) == 0 && msg.ToolCallID == "" {
			continue
		}
		out = append(out, msg)
	}
	return out
}

func cloneMessages(messages []Message) []Message {
	out := make([]Message, len(messages))
	for i, msg := range messages {
		out[i] = msg
		if len(msg.ToolCalls) > 0 {
			out[i].ToolCalls = append([]ToolCall(nil), msg.ToolCalls...)
		}
	}
	return out
}
