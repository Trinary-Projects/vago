package voicepipelinecore

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func continuationPair(t *testing.T) (*testFixture, *ContextAggregatorPair, int64) {
	t.Helper()
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, []Message{
		{Role: "system", Content: "old stage"},
		{Role: "user", Content: "hello"},
	}, "")
	pair.User().ProcessFrame(fix.RootCtx, NewLLMMessagesAppendFrame(nil, true), Downstream)
	id := pair.user.state.responseID
	if id == 0 {
		t.Fatal("LLM request has no response ID")
	}
	return fix, pair, id
}

func commitContinuationResponse(fix *testFixture, pair *ContextAggregatorPair, id int64) {
	pair.Assistant().ProcessFrame(fix.RootCtx, NewWordTimestampFrame([]string{"spoken", "trigger"}), Downstream)
	stopped := NewBotStoppedSpeakingFrame()
	stopped.ResponseID = id
	pair.Assistant().ProcessFrame(fix.RootCtx, stopped, Downstream)
}

func continuationFrame(id int64) LLMMessagesAppendFrame {
	f := NewLLMMessagesAppendFrame([]Message{{Role: "user", Content: "continue with the new stage", Synthetic: true}}, true)
	f.AfterResponseID = id
	return f
}

func TestResponseContinuationUsesPlayedHistoryAndNewPromptOnce(t *testing.T) {
	fix, pair, id := continuationPair(t)
	// Downstream completion can reach the tracker before the upstream
	// BotStoppedSpeaking copy reaches the user aggregator.
	pair.User().ProcessFrame(fix.RootCtx, NewBotStartedSpeakingFrame(), Upstream)
	commitContinuationResponse(fix, pair, id)
	pair.ReplaceSystemMessage("new stage")
	pair.User().ProcessFrame(fix.RootCtx, continuationFrame(id), Downstream)
	pair.User().ProcessFrame(fix.RootCtx, continuationFrame(id), Downstream)
	messages := pair.MessagesSnapshot()
	if len(messages) != 4 || messages[0].Content != "new stage" || messages[2].Content != "spoken trigger" || !messages[3].Synthetic {
		t.Fatalf("continuation context = %+v", messages)
	}
	if pair.user.state.responseID == id || pair.user.state.responseID == 0 {
		t.Fatal("continuation did not start a distinct response")
	}
	// Instruction metadata is internal, not part of the provider payload.
	raw, err := json.Marshal(messages)
	if err != nil || strings.Contains(string(raw), "Synthetic") || strings.Contains(string(raw), "ResponseID") {
		t.Fatalf("internal fields leaked into provider JSON: %s, %v", raw, err)
	}
}

func TestResponseContinuationRejectsInvalidatedRequest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		invalidate func(*testFixture, *ContextAggregatorPair, int64)
	}{
		{"interruption", func(f *testFixture, p *ContextAggregatorPair, _ int64) {
			p.User().ProcessFrame(f.RootCtx, NewInterruptFrame(), Downstream)
		}},
		{"partial new user speech", func(f *testFixture, p *ContextAggregatorPair, _ int64) {
			p.User().ProcessFrame(f.RootCtx, TranscriptFrame{Text: "actually", ResponseID: 1}, Downstream)
		}},
		{"new user turn and response", func(f *testFixture, p *ContextAggregatorPair, _ int64) {
			p.User().ProcessFrame(f.RootCtx, TranscriptFrame{Text: "new user turn", IsFinal: true}, Downstream)
			p.User().ProcessFrame(f.RootCtx, TranscriptFrame{Text: "<end>", IsFinal: true}, Downstream)
			commitContinuationResponse(f, p, p.user.state.responseID)
		}},
		{"idle nudge", func(f *testFixture, p *ContextAggregatorPair, _ int64) {
			p.User().ProcessFrame(f.RootCtx, NewTTSSpeakFrame("Hello?"), Downstream)
			commitContinuationResponse(f, p, 0)
		}},
		{"end before context cancellation", func(f *testFixture, p *ContextAggregatorPair, _ int64) {
			p.User().ProcessFrame(f.RootCtx, NewEndFrame("done"), Downstream)
			if f.RootCtx.Err() != nil {
				t.Fatal("test must cover the live-context shutdown window")
			}
		}},
		{"cancelled task", func(f *testFixture, _ *ContextAggregatorPair, _ int64) { f.RootCancel() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fix, pair, id := continuationPair(t)
			commitContinuationResponse(fix, pair, id)
			tc.invalidate(fix, pair, id)
			before := len(pair.MessagesSnapshot())
			pair.User().ProcessFrame(fix.RootCtx, continuationFrame(id), Downstream)
			if got := len(pair.MessagesSnapshot()); got != before {
				t.Fatalf("invalidated continuation appended a message: %d -> %d", before, got)
			}
		})
	}
}

func TestResponseContinuationRequiresMatchingNonemptyPlayback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		words  bool
		offset int64
	}{
		{"empty", false, 0}, {"different response", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fix, pair, id := continuationPair(t)
			if tc.words {
				pair.Assistant().ProcessFrame(fix.RootCtx, NewWordTimestampFrame([]string{"other"}), Downstream)
			}
			stopped := NewBotStoppedSpeakingFrame()
			stopped.ResponseID = id + tc.offset
			pair.Assistant().ProcessFrame(fix.RootCtx, stopped, Downstream)
			before := len(pair.MessagesSnapshot())
			pair.User().ProcessFrame(fix.RootCtx, continuationFrame(id), Downstream)
			if len(pair.MessagesSnapshot()) != before {
				t.Fatal("unplayed response continued")
			}
		})
	}
}

func TestResponseContinuationSurvivingInputQueueIsRevalidated(t *testing.T) {
	fix, pair, id := continuationPair(t)
	commitContinuationResponse(fix, pair, id)
	// Reproduce the gap in the base processor: interruption purges procCh,
	// but this request is still in inputDataCh and will be processed later.
	pair.User().QueueFrame(continuationFrame(id), Downstream)
	pair.User().ProcessFrame(fix.RootCtx, NewInterruptFrame(), Downstream)
	env := <-pair.User().inputDataCh
	pair.User().ProcessFrame(fix.RootCtx, env.Frame, env.Direction)
	if len(pair.MessagesSnapshot()) != 3 {
		t.Fatal("queued stale continuation survived interruption")
	}
}

func TestResponseContinuationDoesNotMergeIntoRealUserSpeech(t *testing.T) {
	fix, pair, id := continuationPair(t)
	commitContinuationResponse(fix, pair, id)
	pair.User().ProcessFrame(fix.RootCtx, continuationFrame(id), Downstream)
	pair.User().ProcessFrame(fix.RootCtx, TranscriptFrame{Text: "my actual answer", IsFinal: true}, Downstream)
	pair.User().ProcessFrame(fix.RootCtx, TranscriptFrame{Text: "<end>", IsFinal: true}, Downstream)
	msgs := pair.MessagesSnapshot()
	if len(msgs) != 5 || msgs[4].Content != "my actual answer" || msgs[4].Synthetic {
		t.Fatalf("instruction merged into real user speech: %+v", msgs)
	}
	var transcripts []string
	for _, event := range fix.TaskCtx.UIEvents.Snapshot() {
		if event.Type == "user-transcription" {
			transcripts = append(transcripts, event.Data.(map[string]any)["text"].(string))
		}
	}
	if len(transcripts) != 1 || transcripts[0] != "my actual answer" {
		t.Fatalf("transcriptions = %v", transcripts)
	}
}

func TestAssistantTurnCompletionReportsReasonEvenWithoutWords(t *testing.T) {
	for _, tc := range []struct {
		frame  Frame
		reason AssistantTurnEndReason
	}{
		{NewBotStoppedSpeakingFrame(), AssistantTurnPlaybackCompleted},
		{NewInterruptFrame(), AssistantTurnInterrupted},
		{NewEndFrame("done"), AssistantTurnEnding},
	} {
		t.Run(string(tc.reason), func(t *testing.T) {
			fix := newTestFixture(t)
			var events []AssistantTurnCompletion
			fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
				OnAssistantTurnCommitted: func(text string, _ time.Time, _ TurnMetrics, _ string, turn AssistantTurnCompletion) {
					if text != "" {
						t.Errorf("unexpected committed text %q", text)
					}
					events = append(events, turn)
				},
			})
			pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
			pair.Assistant().ProcessFrame(fix.RootCtx, tc.frame, Downstream)
			fix.TaskCtx.callEvents.stopAndDrain()
			if len(events) != 1 || events[0].Reason != tc.reason {
				t.Fatalf("events = %+v", events)
			}
		})
	}
}

func TestLLMResponseIdentitySurvivesGenerationAndEnrichment(t *testing.T) {
	fix := newTestFixture(t)
	request := NewLLMMessagesFrame([]Message{{Role: "user", Content: "hi"}})
	var completed LLMCallCompletion
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnLLMCallCompleted: func(c LLMCallCompletion) { completed = c },
	})
	p := NewLLMProcessorWithClient(fix.TaskCtx, &stubLLMClient{tokens: []string{"trigger"}, model: "test"})
	down, _ := runProcessorTest(t, fix, runConfig{processor: p, framesToSend: []Frame{request}, settleDelay: 100 * time.Millisecond, sendEndFrame: true})
	fix.TaskCtx.callEvents.stopAndDrain()
	start, ok := findFrame[LLMResponseStartFrame](down)
	if !ok || start.ResponseID != request.ID() || completed.ResponseID != request.ID() {
		t.Fatalf("request=%d start=%+v completion=%+v", request.ID(), start, completed)
	}

	enricher := NewContextEnricherProcessor(fix.TaskCtx, func(_ context.Context, messages []Message) []Message {
		return append(messages, Message{Role: "system", Content: "extra"})
	})
	enriched := enricher.enrichFrame(context.Background(), request).(LLMMessagesFrame)
	if enriched.ID() != request.ID() {
		t.Fatal("enrichment changed response identity")
	}
}

func TestPlaybackCompletionPreservesResponseIDAndClearsItForNudges(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.Room = &testOutputRoom{outputSampleRate: defaultOutputSampleRate}
	p := NewPlaybackSinkProcessor(fix.TaskCtx)
	sink := newQueueProcessor(fix.TaskCtx, "capture", Downstream)
	p.Link(sink)
	sink.Start(fix.RootCtx)
	start := NewLLMResponseStartFrame(time.Now())
	start.ResponseID = 123
	p.handleQueueFrame(start)
	p.handleQueueFrame(NewTTSDoneFrame())
	p.tick()
	p.handleQueueFrame(NewTTSSpeakFrame("Hello?"))
	p.handleQueueFrame(NewTTSDoneFrame())
	p.tick()
	deadline := time.Now().Add(time.Second)
	for countFrames[BotStoppedSpeakingFrame](sink.Captured()) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stopProcessorsAndWait(t, fix, time.Second, sink)
	var ids []int64
	for _, f := range sink.Captured() {
		if stopped, ok := f.(BotStoppedSpeakingFrame); ok {
			ids = append(ids, stopped.ResponseID)
		}
	}
	if len(ids) != 2 || ids[0] != 123 || ids[1] != 0 {
		t.Fatalf("playback response IDs = %v", ids)
	}
}
