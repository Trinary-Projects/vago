package voicepipelinecore

import (
	"errors"
	"sync"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const minBargeInWords = 3

var errEmptyInitialMessages = errors.New("voicepipelinecore: aggregator pair requires non-empty initial messages")

type aggregatorSharedState struct {
	mu                               sync.Mutex
	messages                         []Message
	liveUserMessageIndex             int
	mainAgentSystemPromptLangfuseKey string
}

func newAggregatorSharedState(taskCtx *TaskContext, initialMessages []Message, mainAgentSystemPromptLangfuseKey string) *aggregatorSharedState {
	messages := messagesFromInitial(initialMessages)
	if len(messages) == 0 {
		if taskCtx != nil && taskCtx.Logger != nil {
			taskCtx.Logger.Println(errEmptyInitialMessages)
		}
		sentryutil.Capture(sentryutil.Event{
			Hub: taskCtx.SentryHub(),
			Err: errEmptyInitialMessages,
			Tags: map[string]string{
				"component": "aggregator_pair",
				"operation": "initial_messages",
			},
			Details: map[string]any{
				"initial_messages_len": len(initialMessages),
				"prompt_key":           mainAgentSystemPromptLangfuseKey,
			},
		})
		panic(errEmptyInitialMessages)
	}
	return &aggregatorSharedState{
		messages:                         messages,
		liveUserMessageIndex:             -1,
		mainAgentSystemPromptLangfuseKey: mainAgentSystemPromptLangfuseKey,
	}
}

// replaceSystemMessage swaps the conversation's system message in place,
// mirroring Python's ConversationContextManager._set_system_prompt: if
// the first message is a system message its content is replaced,
// otherwise a system message is inserted at the front. Used by the
// onboarding stage machine on every stage transition / deep-thinking
// recompile.
func (s *aggregatorSharedState) replaceSystemMessage(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.messages) > 0 && s.messages[0].Role == "system" {
		s.messages[0].Content = text
		return
	}
	s.messages = append([]Message{{Role: "system", Content: text}}, s.messages...)
}

// snapshot returns a deep copy of the shared conversation history,
// mutex-guarded the same as replaceSystemMessage. Callers own the
// returned slice/messages outright; mutating them (including a
// message's ToolCalls) cannot affect shared state.
func (s *aggregatorSharedState) snapshot() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneMessages(s.messages)
}

func (s *aggregatorSharedState) messagesForTest() []Message {
	return s.snapshot()
}
