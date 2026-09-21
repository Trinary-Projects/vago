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

func (a *AssistantContextAggregator) commitPlayedAssistantText(turn AssistantTurnCompletion) {
	a.mu.Lock()
	spoken := a.playedTextLocked()
	a.mu.Unlock()

	var promptKey string
	if spoken != "" {
		a.state.mu.Lock()
		a.state.messages = append(a.state.messages, Message{Role: "assistant", Content: spoken, ResponseID: turn.ResponseID})
		promptKey = a.state.mainAgentSystemPromptLangfuseKey
		if turn.Reason == AssistantTurnPlaybackCompleted && turn.ResponseID != 0 &&
			turn.ResponseID == a.state.responseID && !a.state.ending {
			a.state.completedResponseID = turn.ResponseID
		}
		a.state.mu.Unlock()
	}

	interrupted := turn.Reason == AssistantTurnInterrupted
	metrics := TurnMetrics{}
	if spoken != "" {
		a.taskCtx.Logger.Printf("Committing to history (interrupted=%v): %s\n", interrupted, spoken)
		if a.taskCtx.metrics != nil {
			metrics = a.taskCtx.metrics.snapshotAndReset()
		}
		if interrupted {
			a.taskCtx.UIEvents.BotStoppedSpeaking(time.Now())
		}
	} else if interrupted {
		a.taskCtx.Logger.Println("Barge-in interrupted bot before any assistant words were committed")
		a.taskCtx.UIEvents.BotStoppedSpeaking(time.Now())
	}
	// Empty interruptions/end flushes still invalidate pending continuations;
	// integrations must not persist them as empty conversation chunks.
	if a.taskCtx.callEvents != nil {
		a.taskCtx.callEvents.fireAssistantTurnCommitted(spoken, time.Now(), metrics, promptKey, turn)
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
		a.commitPlayedAssistantText(AssistantTurnCompletion{ResponseID: f.ResponseID, Reason: AssistantTurnPlaybackCompleted})
		a.PushFrame(f, dir)
	case InterruptFrame:
		a.commitPlayedAssistantText(AssistantTurnCompletion{Reason: AssistantTurnInterrupted})
		a.PushFrame(f, dir)
	case EndFrame:
		a.taskCtx.Logger.Printf("EndFrame at AssistantContextAggregator: reason=%q\n", f.Reason)
		a.commitPlayedAssistantText(AssistantTurnCompletion{Reason: AssistantTurnEnding})
		a.PushFrame(f, dir)
	default:
		a.PushFrame(frame, dir)
	}
}
