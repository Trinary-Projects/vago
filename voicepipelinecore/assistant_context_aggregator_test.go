package voicepipelinecore

import (
	"testing"
	"time"
)

// TestAssistantContextAggregator_BotStoppedCommitsAssistantMessage verifies
// the Pipecat shape: played words flow downstream out of playback, and
// BotStoppedSpeakingFrame flushes them into shared assistant history.
func TestAssistantContextAggregator_BotStoppedCommitsAssistantMessage(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	user := pair.User()
	assistant := pair.Assistant()

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	mid := newQueueProcessor(fix.TaskCtx, "test-mid", Downstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(user)
	user.Link(mid)
	mid.Link(assistant)
	assistant.Link(sink)
	source.Start(fix.RootCtx)
	user.Start(fix.RootCtx)
	mid.Start(fix.RootCtx)
	assistant.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(TranscriptFrame{Text: "hello", IsFinal: true}, Downstream)
	source.QueueFrame(TranscriptFrame{Text: "<end>", IsFinal: true}, Downstream)
	time.Sleep(20 * time.Millisecond)

	mid.QueueFrame(NewWordTimestampFrame([]string{"hi"}), Downstream)
	mid.QueueFrame(NewWordTimestampFrame([]string{"there"}), Downstream)
	mid.QueueFrame(NewBotStoppedSpeakingFrame(), Downstream)
	time.Sleep(20 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, user, mid, assistant, sink)

	var sawAssistant bool
	for _, m := range assistant.messagesForTest() {
		if m.Role == "assistant" {
			sawAssistant = true
			if m.Content != "hi there" {
				t.Errorf("assistant content: got %q, want 'hi there'", m.Content)
			}
		}
	}
	if !sawAssistant {
		t.Error("expected an assistant message after BotStoppedSpeakingFrame")
	}
}

func TestAssistantContextAggregator_CommitsBeforeEndFrameReachesSink(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, []Message{{Role: "system", Content: "prompt"}}, "")
	assistant := pair.Assistant()
	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(assistant)
	assistant.Link(sink)
	source.Start(fix.RootCtx)
	assistant.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewWordTimestampFrame([]string{"goodbye"}), Downstream)
	source.QueueFrame(NewWordTimestampFrame([]string{"."}), Downstream)
	source.QueueFrame(NewEndFrame(string(EndReasonUnspecified)), Downstream)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if countFrames[EndFrame](sink.Captured()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if countFrames[EndFrame](sink.Captured()) == 0 {
		t.Fatal("EndFrame did not reach sink")
	}
	stopProcessorsAndWait(t, fix, 3*time.Second, source, assistant, sink)

	messages := assistant.messagesForTest()
	last := messages[len(messages)-1]
	if last.Role != "assistant" || last.Content != "goodbye." {
		t.Fatalf("last committed message = %+v, want assistant goodbye.", last)
	}
}

func TestAssistantContextAggregator_InterruptCommitsPlayedAssistantText(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, []Message{{Role: "system", Content: "prompt"}}, "")
	assistant := pair.Assistant()
	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(assistant)
	assistant.Link(sink)
	source.Start(fix.RootCtx)
	assistant.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewWordTimestampFrame([]string{"partial"}), Downstream)
	source.QueueFrame(NewInterruptFrame(), Downstream)
	time.Sleep(20 * time.Millisecond)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, assistant, sink)

	if c := countFrames[InterruptFrame](sink.Captured()); c != 1 {
		t.Fatalf("expected InterruptFrame forwarded, got %d in %s", c, describeFrameTypes(sink.Captured()))
	}
	messages := assistant.messagesForTest()
	last := messages[len(messages)-1]
	if last.Role != "assistant" || last.Content != "partial" {
		t.Fatalf("last committed message = %+v, want assistant partial", last)
	}
}

func TestAssistantContextAggregator_EmitsCommittedTurnCallEventsWithMetrics(t *testing.T) {
	fix := newTestFixture(t)
	var assistants []string
	var metrics []TurnMetrics
	var assistantPromptKeys []string
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnAssistantTurnCommitted: func(text string, at time.Time, m TurnMetrics, promptKey string) {
			assistants = append(assistants, text)
			metrics = append(metrics, m)
			assistantPromptKeys = append(assistantPromptKeys, promptKey)
		},
	})
	defer fix.TaskCtx.callEvents.stopAndDrain()
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "sales_call/main_sys-3day_v2_v17")
	assistant := pair.Assistant()

	fix.TaskCtx.metrics.absorb(NewMetricsFrame([]MetricsData{
		{Processor: "llm", Label: MetricTTFB, ValueMs: 12},
		{Processor: "tts", Label: MetricTTFB, ValueMs: 34},
	}))
	assistant.appendPlayedAssistantWords([]string{"hi", "there"})
	assistant.commitPlayedAssistantText(false)
	fix.TaskCtx.callEvents.stopAndDrain()

	if len(assistants) != 1 || assistants[0] != "hi there" {
		t.Fatalf("assistant turn events = %v, want [hi there]", assistants)
	}
	if len(metrics) != 1 || metrics[0].LLMTTFBMs != 12 || metrics[0].TTSTTFBMs != 34 {
		t.Fatalf("assistant metrics = %+v, want llm=12 tts=34", metrics)
	}
	if len(assistantPromptKeys) != 1 || assistantPromptKeys[0] != "sales_call/main_sys-3day_v2_v17" {
		t.Fatalf("assistant prompt keys = %v, want sales prompt key", assistantPromptKeys)
	}
}
