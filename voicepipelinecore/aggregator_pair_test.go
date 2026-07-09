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

func TestMessagesSnapshotMutationDoesNotAffectSharedState(t *testing.T) {
	fix := newTestFixture(t)
	pair := NewContextAggregatorPair(fix.TaskCtx, []Message{
		{Role: "system", Content: "stage one prompt"},
		{Role: "user", Content: "hello?"},
		{
			Role: "assistant",
			ToolCalls: []ToolCall{
				{ID: "call_1", Type: "function", Function: ToolCallFunction{Name: "end_call", Arguments: `{"reason":"done"}`}},
			},
		},
	}, "")
	defer fix.RootCancel()

	snapshot := pair.MessagesSnapshot()
	if len(snapshot) != 3 {
		t.Fatalf("snapshot len = %d, want 3", len(snapshot))
	}

	// Mutate the returned slice and its contents, including a nested
	// ToolCalls entry, then verify a fresh snapshot is unaffected.
	snapshot[0].Content = "mutated system prompt"
	snapshot[1].Content = "mutated user message"
	snapshot[2].ToolCalls[0].ID = "mutated_id"
	snapshot[2].ToolCalls[0].Function.Arguments = `{"reason":"mutated"}`
	snapshot = append(snapshot, Message{Role: "user", Content: "injected"})

	fresh := pair.MessagesSnapshot()
	if len(fresh) != 3 {
		t.Fatalf("fresh snapshot len = %d, want 3 (mutation leaked length)", len(fresh))
	}
	if fresh[0].Content != "stage one prompt" {
		t.Fatalf("fresh[0].Content = %q, want unmutated system prompt", fresh[0].Content)
	}
	if fresh[1].Content != "hello?" {
		t.Fatalf("fresh[1].Content = %q, want unmutated user message", fresh[1].Content)
	}
	if fresh[2].ToolCalls[0].ID != "call_1" {
		t.Fatalf("fresh[2].ToolCalls[0].ID = %q, want unmutated call_1", fresh[2].ToolCalls[0].ID)
	}
	if fresh[2].ToolCalls[0].Function.Arguments != `{"reason":"done"}` {
		t.Fatalf("fresh[2].ToolCalls[0].Function.Arguments = %q, want unmutated arguments", fresh[2].ToolCalls[0].Function.Arguments)
	}

	// Sanity check the local mutation actually happened, so this test
	// would fail if snapshot() ever stopped deep-copying.
	if snapshot[0].Content != "mutated system prompt" || len(snapshot) != 4 {
		t.Fatalf("local snapshot mutation did not take effect: %+v", snapshot)
	}
}

// The stage tracker reads MessagesSnapshot from a background goroutine
// while ReplaceSystemMessage and live LLM-run traffic mutate the shared
// state concurrently. The race detector fails the build if snapshot()
// does not share the same lock as replaceSystemMessage.
func TestMessagesSnapshotConcurrentWithReplaceSystemMessage(t *testing.T) {
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
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			pair.ReplaceSystemMessage("prompt revision")
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snapshot := pair.MessagesSnapshot()
			if len(snapshot) > 0 {
				// Mutate the caller-owned copy to prove reads and
				// ReplaceSystemMessage writes never alias memory.
				snapshot[0].Content = "reader-local scratch"
			}
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
	messages := pair.MessagesSnapshot()
	if messages[0].Role != "system" || messages[0].Content != "prompt revision" {
		t.Fatalf("messages[0] = %+v, want final replaced prompt", messages[0])
	}
}
