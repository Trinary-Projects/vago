package voicepipelinecore

import (
	"context"
	"sync"
	"time"
)

type AssistantContextAggregator struct {
	*BaseProcessor
	mu          sync.Mutex
	taskCtx     *TaskContext
	state       *aggregatorSharedState
	playedWords []string
}

func newAssistantContextAggregatorWithState(taskCtx *TaskContext, state *aggregatorSharedState) *AssistantContextAggregator {
	a := &AssistantContextAggregator{
		taskCtx: taskCtx,
		state:   state,
	}
	a.BaseProcessor = NewBaseProcessor("AssistantContextAggregator", a, taskCtx)
	return a
}

func (a *AssistantContextAggregator) messagesForTest() []Message {
	return a.state.messagesForTest()
}

func (a *AssistantContextAggregator) appendPlayedAssistantWords(words []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, w := range words {
		if len(a.playedWords) > 0 && len(w) > 0 && w[0] != '.' && w[0] != ',' && w[0] != '!' && w[0] != '?' && w[0] != ';' && w[0] != ':' {
			a.playedWords = append(a.playedWords, " "+w)
		} else {
			a.playedWords = append(a.playedWords, w)
		}
	}
}

func (a *AssistantContextAggregator) playedTextLocked() string {
	var spoken string
	for _, w := range a.playedWords {
		spoken += w
	}
	a.playedWords = nil
	return spoken
}

func (a *AssistantContextAggregator) commitPlayedAssistantText(interrupted bool) {
	a.mu.Lock()
	spoken := a.playedTextLocked()
	a.mu.Unlock()

	var promptKey string
	if spoken != "" {
		a.state.mu.Lock()
		a.state.messages = append(a.state.messages, Message{Role: "assistant", Content: spoken})
		promptKey = a.state.mainAgentSystemPromptLangfuseKey
		a.state.mu.Unlock()
	}

	if spoken != "" {
		a.taskCtx.Logger.Printf("Committing to history (interrupted=%v): %s\n", interrupted, spoken)
		metrics := TurnMetrics{}
		if a.taskCtx.metrics != nil {
			metrics = a.taskCtx.metrics.snapshotAndReset()
		}
		if a.taskCtx.callEvents != nil {
			a.taskCtx.callEvents.fireAssistantTurnCommitted(spoken, time.Now(), metrics, promptKey)
		}
		if interrupted {
			a.taskCtx.UIEvents.BotStoppedSpeaking(time.Now())
		}
	} else if interrupted {
		a.taskCtx.Logger.Println("Barge-in interrupted bot before any assistant words were committed")
		a.taskCtx.UIEvents.BotStoppedSpeaking(time.Now())
	}
}

func (a *AssistantContextAggregator) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case WordTimestampFrame:
		// Downstream from PlaybackSink after the audio frame for these words
		// has actually been played.
		a.appendPlayedAssistantWords(f.Words)
		a.PushFrame(f, dir)
	case BotStoppedSpeakingFrame:
		a.commitPlayedAssistantText(false)
		a.PushFrame(f, dir)
	case InterruptFrame:
		a.commitPlayedAssistantText(true)
		a.PushFrame(f, dir)
	case EndFrame:
		a.taskCtx.Logger.Printf("EndFrame at AssistantContextAggregator: reason=%q\n", f.Reason)
		a.commitPlayedAssistantText(false)
		a.PushFrame(f, dir)
	default:
		a.PushFrame(frame, dir)
	}
}
