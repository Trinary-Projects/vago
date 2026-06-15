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
	mainAgentSystemPromptLangfuseKey string
}

func newAggregatorSharedState(taskCtx *TaskContext, initialMessages []Message, mainAgentSystemPromptLangfuseKey string) *aggregatorSharedState {
	messages := messagesFromInitial(initialMessages)
	if len(messages) == 0 {
		if taskCtx != nil && taskCtx.Logger != nil {
			taskCtx.Logger.Println(errEmptyInitialMessages)
		}
		sentryutil.Capture(sentryutil.Event{
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
		mainAgentSystemPromptLangfuseKey: mainAgentSystemPromptLangfuseKey,
	}
}

func (s *aggregatorSharedState) messagesForTest() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneMessages(s.messages)
}
