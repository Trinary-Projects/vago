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

// MessagesSnapshot returns a read-only, deep-copied snapshot of the
// shared conversation history (mutex-guarded on the shared-state lock,
// same as ReplaceSystemMessage). It is a business-side read surface for
// consumers such as the Disha onboarding stage tracker, which builds
// LLM-classifier transcripts from it, mirroring Python's reads of
// main_context.messages. Mutating the returned slice or its messages
// (including ToolCalls) cannot affect shared conversation state; core
// does no transcript formatting (speaker labels, windowing) on it.
func (p *ContextAggregatorPair) MessagesSnapshot() []Message {
	return p.user.state.snapshot()
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
