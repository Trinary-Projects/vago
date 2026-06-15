package voicepipelinecore

type ContextAggregatorPair struct {
	user      *UserContextAggregator
	assistant *AssistantContextAggregator
}

func NewContextAggregatorPair(taskCtx *TaskContext, initialMessages []Message, mainAgentSystemPromptLangfuseKey string) *ContextAggregatorPair {
	state := newAggregatorSharedState(taskCtx, initialMessages, mainAgentSystemPromptLangfuseKey)
	return &ContextAggregatorPair{
		user:      newUserContextAggregatorWithState(taskCtx, state),
		assistant: newAssistantContextAggregatorWithState(taskCtx, state),
	}
}

func (p *ContextAggregatorPair) User() *UserContextAggregator {
	if p == nil {
		return nil
	}
	return p.user
}

func (p *ContextAggregatorPair) Assistant() *AssistantContextAggregator {
	if p == nil {
		return nil
	}
	return p.assistant
}
