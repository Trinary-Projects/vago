package voicepipelinecore

import (
	"sync"
	"testing"
)

func TestReplaceSystemMessageSwapsContent(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, []Message{
		{Role: "system", Content: "stage one prompt"},
		{Role: "user", Content: "hello?"},
	}, "")
	defer fix.RootCancel()

	pair.ReplaceSystemMessage("stage two prompt")

	messages := pair.User().messagesForTest()
	if messages[0].Role != "system" || messages[0].Content != "stage two prompt" {
		t.Fatalf("messages[0] = %+v, want replaced system message", messages[0])
	}
	// Non-system messages are untouched.
	if len(messages) < 2 || messages[1].Role == "system" {
		t.Fatalf("messages = %+v, want original tail preserved", messages)
	}
}

func TestReplaceSystemMessageInsertsWhenNoSystemMessage(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, []Message{{Role: "user", Content: "hello?"}}, "")
	defer fix.RootCancel()

	pair.ReplaceSystemMessage("fresh system prompt")

	messages := pair.User().messagesForTest()
	if len(messages) != 2 {
		t.Fatalf("messages = %+v, want system inserted at front", messages)
	}
	if messages[0].Role != "system" || messages[0].Content != "fresh system prompt" {
		t.Fatalf("messages[0] = %+v", messages[0])
	}
	if messages[1].Role != "user" || messages[1].Content != "hello?" {
		t.Fatalf("messages[1] = %+v", messages[1])
	}
}

// The stage tracker replaces the system message from a background
// goroutine while the user aggregator is running LLM turns. This drives
// real transcript → LLMMessagesFrame traffic through the aggregator
// while hammering ReplaceSystemMessage; the race detector fails the
// build if the shared-state lock does not cover both sides.
func TestReplaceSystemMessageConcurrentWithLLMRuns(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")

	frames := make([]Frame, 0, 40)
	for i := 0; i < 20; i++ {
		frames = append(frames,
			TranscriptFrame{Text: "hello", IsFinal: true},
			TranscriptFrame{Text: "<end>", IsFinal: true},
		)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			pair.ReplaceSystemMessage("prompt revision")
			_ = pair.User().messagesForTest()
		}
	}()

	down, _ := runProcessorTest(t, fix, runConfig{
		processor:    pair.User(),
		framesToSend: frames,
		sendEndFrame: true,
	})
	close(stop)
	wg.Wait()

	if _, ok := findFrame[LLMMessagesFrame](down); !ok {
		t.Fatalf("expected LLMMessagesFrame, got %s", describeFrameTypes(down))
	}
	messages := pair.User().messagesForTest()
	if messages[0].Role != "system" || messages[0].Content != "prompt revision" {
		t.Fatalf("messages[0] = %+v, want final replaced prompt", messages[0])
	}
}
