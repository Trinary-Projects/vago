package disha

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

const continuationTestStatement = "ab hum agle mudde par baat karenge"

type stageContinuationSink struct {
	*voicepipelinecore.BaseProcessor
	requests chan voicepipelinecore.LLMMessagesFrame
}

func (s *stageContinuationSink) ProcessFrame(_ context.Context, frame voicepipelinecore.Frame, _ voicepipelinecore.Direction) {
	if request, ok := frame.(voicepipelinecore.LLMMessagesFrame); ok {
		s.requests <- request
	}
}

func newStageContinuationHarness(t *testing.T) (*stageMachineHarness, *stageContinuationSink, voicepipelinecore.LLMMessagesFrame) {
	t.Helper()
	h := newStageMachineHarness(t, &stubStageClassifier{output: stageTestNextStage})
	stage := &StageConfig{Name: "continuation_intro", Prompt: PromptConfig{Name: "obtest/continuation", Version: 1}, NextStages: []string{stageTestNextStage}}
	seedStageDocWithConfig(t, h.redisServer, stage.Prompt.Name, 1, map[string]any{
		"next_stages": []any{map[string]any{"stage_name": stageTestNextStage, "trigger_statements": []any{continuationTestStatement}}},
	})
	h.state.AdvanceStage(stage)
	cleanedUp := make(chan struct{})
	task, err := voicepipelinecore.NewPipelineTask(context.Background(), voicepipelinecore.TaskConfig{
		Logger: h.manager.logger, OnCleanup: func() { close(cleanedUp) },
	})
	if err != nil {
		t.Fatal(err)
	}
	taskCtx := task.TaskCtx
	ctx := taskCtx.Ctx
	h.ui = taskCtx.UIEvents
	pair := voicepipelinecore.NewContextAggregatorPair(taskCtx, h.pair.MessagesSnapshot(), h.promptKey)
	h.pair = pair
	h.manager.SetInfrastructure(pair, h.routerMeta, h.ui)
	h.tracker.SetInfrastructure(ctx, pair, h.ui)
	sink := &stageContinuationSink{requests: make(chan voicepipelinecore.LLMMessagesFrame, 10)}
	sink.BaseProcessor = voicepipelinecore.NewBaseProcessor("continuation-test-sink", sink, taskCtx)
	task.SetPipeline(nil, voicepipelinecore.NewPipeline([]voicepipelinecore.Processor{pair.User(), sink}))
	task.Start()
	t.Cleanup(func() {
		task.CompleteEnd(voicepipelinecore.NewEndFrame("test cleanup"))
		select {
		case <-cleanedUp:
		case <-time.After(2 * time.Second):
			t.Error("continuation test pipeline did not stop")
		}
	})
	pair.User().QueueFrame(voicepipelinecore.NewLLMMessagesAppendFrame(nil, true), voicepipelinecore.Downstream)
	return h, sink, awaitContinuationRequest(t, sink)
}

func awaitContinuationRequest(t *testing.T, sink *stageContinuationSink) voicepipelinecore.LLMMessagesFrame {
	t.Helper()
	select {
	case request := <-sink.requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("no LLM continuation request")
		return voicepipelinecore.LLMMessagesFrame{}
	}
}

func noContinuationRequest(t *testing.T, sink *stageContinuationSink) {
	t.Helper()
	select {
	case request := <-sink.requests:
		t.Fatalf("unexpected LLM continuation: %+v", request.Messages)
	case <-time.After(50 * time.Millisecond):
	}
}

func finishContinuationPlayback(h *stageMachineHarness, id int64) {
	// Played text deliberately differs from the generated trigger. Matching
	// stays on generated text while the next request contains what was heard.
	h.pair.Assistant().ProcessFrame(context.Background(), voicepipelinecore.NewWordTimestampFrame([]string{"played", "trigger"}), voicepipelinecore.Downstream)
	stopped := voicepipelinecore.NewBotStoppedSpeakingFrame()
	stopped.ResponseID = id
	h.pair.Assistant().ProcessFrame(context.Background(), stopped, voicepipelinecore.Downstream)
	h.tracker.OnAssistantTurnCommitted(voicepipelinecore.AssistantTurnCompletion{ResponseID: id, Reason: voicepipelinecore.AssistantTurnPlaybackCompleted})
}

func TestStageContinuationBothCompletionOrders(t *testing.T) {
	for _, playbackFirst := range []bool{false, true} {
		name := "transition before playback"
		if playbackFirst {
			name = "playback before generation callback"
		}
		t.Run(name, func(t *testing.T) {
			h, sink, request := newStageContinuationHarness(t)
			id := request.ID()
			if playbackFirst {
				finishContinuationPlayback(h, id)
				noContinuationRequest(t, sink)
			}
			h.tracker.OnLLMCallCompleted(voicepipelinecore.LLMCallCompletion{ResponseID: id, Text: continuationTestStatement})
			if !playbackFirst {
				waitForCondition(t, time.Second, "pending transition", func() bool {
					h.tracker.continuationMu.Lock()
					defer h.tracker.continuationMu.Unlock()
					return h.tracker.pendingResponseID == id
				})
				noContinuationRequest(t, sink)
				finishContinuationPlayback(h, id)
			}
			next := awaitContinuationRequest(t, sink)
			messages := next.Messages
			if !strings.Contains(messages[0].Content, "<problem_discovery_and_exploration>") {
				t.Fatal("continuation used the old stage prompt")
			}
			last := messages[len(messages)-1]
			if last.Role != "user" || last.Content != stageContinuationInstruction || !last.Synthetic {
				t.Fatalf("continuation instruction = %+v", last)
			}
			if messages[len(messages)-2].Content != "played trigger" {
				t.Fatal("continuation missing played assistant history")
			}
			if next.ID() == id {
				t.Fatal("continuation reused the old response identity")
			}
			if strings.Contains(onboardingTranscript(messages, transcriptAllTurns), stageContinuationInstruction) {
				t.Fatal("synthetic instruction entered patient transcript")
			}
			// A duplicate completion event cannot run the same continuation twice.
			h.tracker.OnAssistantTurnCommitted(voicepipelinecore.AssistantTurnCompletion{ResponseID: id, Reason: voicepipelinecore.AssistantTurnPlaybackCompleted})
			noContinuationRequest(t, sink)
		})
	}
}

func TestStageContinuationQuestionsAndToolsDoNotAutoRun(t *testing.T) {
	for _, tc := range []struct {
		name, suffix string
		tools        bool
	}{
		{"question", "?", false}, {"question earlier in response", "? yes", false},
		{"full width question", "？", false}, {"Arabic question", "؟", false},
		{"tool call", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, sink, request := newStageContinuationHarness(t)
			finishContinuationPlayback(h, request.ID())
			h.tracker.OnLLMCallCompleted(voicepipelinecore.LLMCallCompletion{ResponseID: request.ID(), Text: continuationTestStatement + tc.suffix, HasToolCalls: tc.tools})
			h.waitForRTVI("Transition complete continuation_intro => " + stageTestNextStage)
			noContinuationRequest(t, sink)
		})
	}
}

func TestStageContinuationInterruptedAndEmptyGeneration(t *testing.T) {
	for _, completion := range []voicepipelinecore.LLMCallCompletion{
		{Text: continuationTestStatement, Interrupted: true}, {Text: "   "},
	} {
		h, sink, request := newStageContinuationHarness(t)
		finishContinuationPlayback(h, request.ID())
		completion.ResponseID = request.ID()
		h.tracker.OnLLMCallCompleted(completion)
		noContinuationRequest(t, sink)
		if h.state.CurrentStage().Name != "continuation_intro" {
			t.Fatal("empty or interrupted generation advanced the stage")
		}
	}
}

func TestStageContinuationInterruptionAndShutdownWhileTransitionPending(t *testing.T) {
	for _, reason := range []voicepipelinecore.AssistantTurnEndReason{voicepipelinecore.AssistantTurnInterrupted, voicepipelinecore.AssistantTurnEnding} {
		t.Run(string(reason), func(t *testing.T) {
			h, sink, request := newStageContinuationHarness(t)
			id := request.ID()
			h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", id)
			var frame voicepipelinecore.Frame = voicepipelinecore.NewInterruptFrame()
			if reason == voicepipelinecore.AssistantTurnEnding {
				frame = voicepipelinecore.NewEndFrame("done")
			}
			h.pair.User().ProcessFrame(context.Background(), frame, voicepipelinecore.Downstream)
			h.pair.Assistant().ProcessFrame(context.Background(), frame, voicepipelinecore.Downstream)
			h.tracker.OnAssistantTurnCommitted(voicepipelinecore.AssistantTurnCompletion{Reason: reason})
			// Even a late success callback cannot resurrect the invalidated turn.
			finishContinuationPlayback(h, id)
			h.tracker.armContinuation(id)
			noContinuationRequest(t, sink)
		})
	}
}

func TestStageContinuationFailedCompileAfterAdvanceDoesNotRun(t *testing.T) {
	h, sink, request := newStageContinuationHarness(t)
	finishContinuationPlayback(h, request.ID())
	target := h.config.ResolveStage(stageTestNextStage, nil)
	target.Prompt = PromptConfig{Name: "missing/continuation-test-document"}
	h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", request.ID())
	if h.state.CurrentStage().Name != stageTestNextStage {
		t.Fatal("test did not reach compile-after-advance failure")
	}
	if !h.hasRTVI("[ERROR] System prompt update failed") {
		t.Fatal("expected prompt compilation failure")
	}
	noContinuationRequest(t, sink)
}

func TestStageContinuationStaleResultDoesNotRun(t *testing.T) {
	h, sink, request := newStageContinuationHarness(t)
	finishContinuationPlayback(h, request.ID())
	h.state.AdvanceStage(h.config.ResolveStage(stageTestNextStage, nil))
	h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", request.ID())
	if !h.hasRTVI("Stale output ignored") {
		t.Fatal("expected stale result rejection")
	}
	noContinuationRequest(t, sink)
}

func TestStageContinuationDelayedClassifierCannotSupersedeUserInput(t *testing.T) {
	h, sink, request := newStageContinuationHarness(t)
	installMaybeStage(t, h)
	entered, release := make(chan struct{}), make(chan struct{})
	classifier := &stubStageClassifier{output: stageTestNextStage, onCall: func() {
		close(entered)
		<-release
	}}
	h.tracker.classifier = classifier
	finishContinuationPlayback(h, request.ID())
	h.tracker.OnLLMCallCompleted(voicepipelinecore.LLMCallCompletion{ResponseID: request.ID(), Text: "alpha beta gamma"})
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("classifier did not start")
	}
	// A new utterance invalidates continuation even before a final transcript.
	h.pair.User().ProcessFrame(context.Background(), voicepipelinecore.TranscriptFrame{Text: "actually", ResponseID: 1}, voicepipelinecore.Downstream)
	close(release)
	h.waitForRTVI("Transition complete intro_maybe => " + stageTestNextStage)
	noContinuationRequest(t, sink)
}

func TestStageContinuationCanContinueConsecutiveStages(t *testing.T) {
	h, sink, request := newStageContinuationHarness(t)
	finishContinuationPlayback(h, request.ID())
	h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", request.ID())
	next := awaitContinuationRequest(t, sink)
	finishContinuationPlayback(h, next.ID())
	h.tracker.processOutput(context.Background(), "introduction", stageTestNextStage, []string{"introduction"}, "transcript", next.ID())
	third := awaitContinuationRequest(t, sink)
	if third.ID() <= next.ID() || !strings.Contains(third.Messages[0].Content, "<introduction_and_call_overview>") {
		t.Fatal("consecutive stage continuation did not use the latest prompt and response identity")
	}
}

func TestStageContinuationCannotUseIdleNudgeCompletion(t *testing.T) {
	h, sink, request := newStageContinuationHarness(t)
	h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", request.ID())
	finishContinuationPlayback(h, 0)
	noContinuationRequest(t, sink)
	finishContinuationPlayback(h, request.ID())
	awaitContinuationRequest(t, sink)
}

func TestOnboardingTranscriptSkipsSyntheticInstructionsBeforeWindowing(t *testing.T) {
	messages := []voicepipelinecore.Message{
		{Role: "system", Content: "prompt"}, {Role: "user", Content: "real answer"},
		{Role: "assistant", Content: "trigger"}, {Role: "user", Content: stageContinuationInstruction, Synthetic: true},
	}
	if got := onboardingTranscript(messages, 2); got != "patient: real answer\ndisha: trigger" {
		t.Fatalf("transcript = %q", got)
	}
}
