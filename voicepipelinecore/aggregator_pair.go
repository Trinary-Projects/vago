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

// ReplaceSystemMessage swaps the shared conversation history's system
// message mid-call (mutex-guarded on the shared-state lock). The next
// LLM run picks up the new prompt; an in-flight run keeps the snapshot
// it already took, matching Python where a transition lands between
// turns.
func (p *ContextAggregatorPair) ReplaceSystemMessage(text string) {
	p.user.state.replaceSystemMessage(text)
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
