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
	requests chan voicepipelinecore.LLMContextFrame
}

func (s *stageContinuationSink) ProcessFrame(_ context.Context, frame voicepipelinecore.Frame, _ voicepipelinecore.Direction) {
	if request, ok := frame.(voicepipelinecore.LLMContextFrame); ok {
		s.requests <- request
	}
}

func newStageContinuationHarness(t *testing.T) (*stageMachineHarness, *stageContinuationSink, voicepipelinecore.LLMContextFrame) {
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
	sink := &stageContinuationSink{requests: make(chan voicepipelinecore.LLMContextFrame, 10)}
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

func awaitContinuationRequest(t *testing.T, sink *stageContinuationSink) voicepipelinecore.LLMContextFrame {
	t.Helper()
	select {
	case request := <-sink.requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("no LLM continuation request")
		return voicepipelinecore.LLMContextFrame{}
	}
}

func noContinuationRequest(t *testing.T, sink *stageContinuationSink) {
	t.Helper()
	select {
	case request := <-sink.requests:
		t.Fatalf("unexpected LLM continuation: %+v", request.Context.GetMessages())
	case <-time.After(50 * time.Millisecond):
	}
}

func finishContinuationPlayback(h *stageMachineHarness) {
	h.pair.Assistant().ProcessFrame(context.Background(), voicepipelinecore.NewWordTimestampFrame([]string{"played", "trigger"}), voicepipelinecore.Downstream)
	h.pair.Assistant().ProcessFrame(context.Background(), voicepipelinecore.NewLLMResponseEndFrame(), voicepipelinecore.Downstream)
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
				finishContinuationPlayback(h)
				noContinuationRequest(t, sink)
			}
			h.tracker.OnLLMCallCompleted(voicepipelinecore.LLMCallCompletion{Text: continuationTestStatement})
			next := awaitContinuationRequest(t, sink)
			messages := next.Context.GetMessages()
			if !strings.Contains(messages[0].Content, "<problem_discovery_and_exploration>") {
				t.Fatal("continuation used the old stage prompt")
			}
			last := messages[len(messages)-1]
			if last.Role != "user" || last.Content != stageContinuationInstruction {
				t.Fatalf("continuation instruction = %+v", last)
			}
			if playbackFirst && messages[len(messages)-2].Content != "played trigger" {
				t.Fatalf("played text missing: %+v", messages)
			}
			for _, m := range messages {
				if m.Role == "assistant" && m.Content == continuationTestStatement {
					t.Fatal("unplayed generated text entered context")
				}
			}
			if next.ID() == id {
				t.Fatal("continuation reused the old response identity")
			}
			if !strings.Contains(onboardingTranscript(messages, transcriptAllTurns), "patient: "+stageContinuationInstruction) {
				t.Fatal("continuation instruction missing from patient transcript")
			}
			// There is only one successful transition callback here.

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
			h, sink, _ := newStageContinuationHarness(t)
			finishContinuationPlayback(h)
			h.tracker.OnLLMCallCompleted(voicepipelinecore.LLMCallCompletion{Text: continuationTestStatement + tc.suffix, HasToolCalls: tc.tools})
			h.waitForRTVI("Transition complete continuation_intro => " + stageTestNextStage)
			noContinuationRequest(t, sink)
		})
	}
}

func TestStageContinuationInterruptedAndEmptyGeneration(t *testing.T) {
	for _, completion := range []voicepipelinecore.LLMCallCompletion{
		{Text: continuationTestStatement, Interrupted: true}, {Text: "   "},
	} {
		h, sink, _ := newStageContinuationHarness(t)
		finishContinuationPlayback(h)
		h.tracker.OnLLMCallCompleted(completion)
		noContinuationRequest(t, sink)
		if h.state.CurrentStage().Name != "continuation_intro" {
			t.Fatal("empty or interrupted generation advanced the stage")
		}
	}
}

func TestStageContinuationFailedCompileAfterAdvanceDoesNotRun(t *testing.T) {
	h, sink, _ := newStageContinuationHarness(t)
	finishContinuationPlayback(h)
	target := h.config.ResolveStage(stageTestNextStage, nil)
	target.Prompt = PromptConfig{Name: "missing/continuation-test-document"}
	h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", true)
	if h.state.CurrentStage().Name != stageTestNextStage {
		t.Fatal("test did not reach compile-after-advance failure")
	}
	if !h.hasRTVI("[ERROR] System prompt update failed") {
		t.Fatal("expected prompt compilation failure")
	}
	noContinuationRequest(t, sink)
}

func TestStageContinuationStaleResultDoesNotRun(t *testing.T) {
	h, sink, _ := newStageContinuationHarness(t)
	finishContinuationPlayback(h)
	h.state.AdvanceStage(h.config.ResolveStage(stageTestNextStage, nil))
	h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", true)
	if !h.hasRTVI("Stale output ignored") {
		t.Fatal("expected stale result rejection")
	}
	noContinuationRequest(t, sink)
}

func TestStageContinuationDelayedClassifierUsesNativeAppend(t *testing.T) {
	h, sink, _ := newStageContinuationHarness(t)
	installMaybeStage(t, h)
	entered, release := make(chan struct{}), make(chan struct{})
	classifier := &stubStageClassifier{output: stageTestNextStage, onCall: func() {
		close(entered)
		<-release
	}}
	h.tracker.classifier = classifier
	finishContinuationPlayback(h)
	h.tracker.OnLLMCallCompleted(voicepipelinecore.LLMCallCompletion{Text: "alpha beta gamma"})
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("classifier did not start")
	}
	// Native append has no originating-response guard: a late classifier can still request inference.
	h.pair.User().ProcessFrame(context.Background(), voicepipelinecore.TranscriptFrame{Text: "actually", ResponseID: 1}, voicepipelinecore.Downstream)
	close(release)
	h.waitForRTVI("Transition complete intro_maybe => " + stageTestNextStage)
	awaitContinuationRequest(t, sink)
}

func TestStageContinuationCanContinueConsecutiveStages(t *testing.T) {
	h, sink, _ := newStageContinuationHarness(t)
	finishContinuationPlayback(h)
	h.tracker.processOutput(context.Background(), stageTestNextStage, "continuation_intro", []string{stageTestNextStage}, "transcript", true)
	next := awaitContinuationRequest(t, sink)
	finishContinuationPlayback(h)
	h.tracker.processOutput(context.Background(), "introduction", stageTestNextStage, []string{"introduction"}, "transcript", true)
	third := awaitContinuationRequest(t, sink)
	if third.ID() <= next.ID() || !strings.Contains(third.Context.GetMessages()[0].Content, "<introduction_and_call_overview>") {
		t.Fatal("consecutive stage continuation did not use the latest prompt and response identity")
	}
}

func TestOnboardingTranscriptIncludesContinuationInstructionInWindow(t *testing.T) {
	messages := []voicepipelinecore.Message{
		{Role: "system", Content: "prompt"}, {Role: "user", Content: "real answer"},
		{Role: "assistant", Content: "trigger"}, {Role: "user", Content: stageContinuationInstruction},
	}
	if got := onboardingTranscript(messages, 2); got != "disha: trigger\npatient: "+stageContinuationInstruction {
		t.Fatalf("transcript = %q", got)
	}
}
