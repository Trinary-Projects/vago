package voicepipelinecore

import (
	"testing"
	"time"
)

func testInitialMessages() []Message {
	return []Message{{Role: "system", Content: "test prompt"}}
}

// TestUserContextAggregator_FinalTranscriptEmitsLLMMessages verifies a
// final transcript ending with <end> produces an LLMContextFrame
// downstream.
func TestUserContextAggregator_FinalTranscriptEmitsLLMMessages(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			TranscriptFrame{Text: "hello", IsFinal: true},
			TranscriptFrame{Text: "<end>", IsFinal: true},
		},
		sendEndFrame: true,
	})

	llmMsg, ok := findFrame[LLMContextFrame](down)
	if !ok {
		t.Fatalf("expected LLMContextFrame, got %s", describeFrameTypes(down))
	}
	if len(llmMsg.Context.GetMessages()) == 0 {
		t.Fatal("LLMContextFrame should contain messages")
	}
	// First message is the system prompt; second is the user message.
	if llmMsg.Context.GetMessages()[0].Role != "system" {
		t.Errorf("first message should be system, got %q", llmMsg.Context.GetMessages()[0].Role)
	}
	last := llmMsg.Context.GetMessages()[len(llmMsg.Context.GetMessages())-1]
	if last.Role != "user" {
		t.Errorf("last message should be user, got %q", last.Role)
	}
	if last.Content != "hello" {
		t.Errorf("user content: got %q, want 'hello'", last.Content)
	}
}

func TestUserContextAggregator_SuppressesInterimUserTranscriptionEvents(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			TranscriptFrame{Text: "hel", IsFinal: false, ResponseID: 1},
			TranscriptFrame{Text: "lo", IsFinal: false, ResponseID: 1},
		},
		sendEndFrame: true,
	})

	for _, entry := range fix.TaskCtx.UIEvents.Snapshot() {
		if entry.Type == "user-transcription" {
			t.Fatalf("interim transcript emitted RTVI user-transcription event: %+v", entry)
		}
	}
}

func TestUserContextAggregator_FinalUserTranscriptionStillEmitsRTVI(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			TranscriptFrame{Text: "hello", IsFinal: true, ResponseID: 1},
			TranscriptFrame{Text: "<end>", IsFinal: true, ResponseID: 1},
		},
		sendEndFrame: true,
	})

	var found bool
	for _, entry := range fix.TaskCtx.UIEvents.Snapshot() {
		if entry.Type != "user-transcription" {
			continue
		}
		data, ok := entry.Data.(map[string]any)
		if !ok {
			t.Fatalf("user-transcription data = %#v", entry.Data)
		}
		if data["text"] != "hello" || data["final"] != true {
			t.Fatalf("user-transcription data = %+v, want final hello", data)
		}
		found = true
	}
	if !found {
		t.Fatal("final transcript did not emit RTVI user-transcription event")
	}
}

func TestUserContextAggregator_InitialMessagesSeedLLMContext(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, []Message{
		{Role: "system", Content: "seed context"},
		{Role: "assistant", Content: "hello seed"},
	}, "")

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			TranscriptFrame{Text: "new user", IsFinal: true},
			TranscriptFrame{Text: "<end>", IsFinal: true},
		},
		sendEndFrame: true,
	})

	llmMsg, ok := findFrame[LLMContextFrame](down)
	if !ok {
		t.Fatalf("expected LLMContextFrame, got %s", describeFrameTypes(down))
	}
	if len(llmMsg.Context.GetMessages()) != 3 {
		t.Fatalf("message count = %d, want 3", len(llmMsg.Context.GetMessages()))
	}
	if llmMsg.Context.GetMessages()[0].Content != "seed context" {
		t.Fatalf("first message content = %q, want seed context", llmMsg.Context.GetMessages()[0].Content)
	}
	last := llmMsg.Context.GetMessages()[len(llmMsg.Context.GetMessages())-1]
	if last.Role != "user" || last.Content != "new user" {
		t.Fatalf("last message = %+v, want user new user", last)
	}
}

func TestUserContextAggregator_AppendRunLLMEmitsFirstTurnFromInitialContext(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, []Message{
		{Role: "system", Content: "sales prompt"},
		{Role: "user", Content: "hello?"},
	}, "")

	// Greet-first: append no messages, just run the LLM on the initial
	// context (what DailyRoom pushes on user join).
	down, _ := runProcessorTest(t, fix, runConfig{
		processor:    a,
		framesToSend: []Frame{LLMMessagesAppendFrame{RunLLM: true}},
		settleDelay:  100 * time.Millisecond,
		sendEndFrame: true,
	})

	llmMsg, ok := findFrame[LLMContextFrame](down)
	if !ok {
		t.Fatalf("expected LLMContextFrame, got %s", describeFrameTypes(down))
	}
	if len(llmMsg.Context.GetMessages()) != 2 {
		t.Fatalf("message count = %d, want 2 (the initial context)", len(llmMsg.Context.GetMessages()))
	}
	if llmMsg.Context.GetMessages()[0].Content != "sales prompt" {
		t.Fatalf("first message = %q, want sales prompt", llmMsg.Context.GetMessages()[0].Content)
	}
	// The append frame itself must be consumed, not forwarded.
	if _, forwarded := findFrame[LLMMessagesAppendFrame](down); forwarded {
		t.Fatal("LLMMessagesAppendFrame should be consumed by UserContextAggregator, not forwarded")
	}
}

func TestUserContextAggregator_AppendAddsMessages(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, []Message{
		{Role: "system", Content: "sales prompt"},
	}, "")

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			LLMMessagesAppendFrame{Messages: []Message{{Role: "system", Content: "injected nudge"}}, RunLLM: true},
		},
		settleDelay:  100 * time.Millisecond,
		sendEndFrame: true,
	})

	llmMsg, ok := findFrame[LLMContextFrame](down)
	if !ok {
		t.Fatalf("expected LLMContextFrame, got %s", describeFrameTypes(down))
	}
	if len(llmMsg.Context.GetMessages()) != 2 {
		t.Fatalf("message count = %d, want 2 (prompt + appended)", len(llmMsg.Context.GetMessages()))
	}
	if llmMsg.Context.GetMessages()[1].Content != "injected nudge" {
		t.Fatalf("appended message = %q, want injected nudge", llmMsg.Context.GetMessages()[1].Content)
	}
}

func TestUserContextAggregator_EmptyInitialMessagesPanics(t *testing.T) {
	fix := newTestFixture(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected empty initial messages to panic")
		}
	}()
	NewUserContextAggregator(fix.TaskCtx, []Message{}, "")
}

func TestUserContextAggregator_NilInitialMessagesPanics(t *testing.T) {
	fix := newTestFixture(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected nil initial messages to panic")
		}
	}()
	NewUserContextAggregator(fix.TaskCtx, nil, "")
}

func TestUserContextAggregator_InvalidInitialMessagesPanics(t *testing.T) {
	fix := newTestFixture(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected invalid initial messages to panic")
		}
	}()
	NewUserContextAggregator(fix.TaskCtx, []Message{{Role: "system"}}, "")
}

// TestUserContextAggregator_BargeInEmitsInterrupt verifies that >= 3 interim
// words during bot speech triggers an InterruptFrame downstream.
func TestUserContextAggregator_BargeInEmitsInterrupt(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			BotStartedSpeakingFrame{}, // arrives upstream from PlaybackSink; UserContextAggregator sets botSpeaking
			TranscriptFrame{Text: "one two three", IsFinal: false, ResponseID: 1}, // 3 words → barge-in
		},
		settleDelay:  30 * time.Millisecond,
		sendEndFrame: true,
	})

	if c := countFrames[InterruptFrame](down); c != 1 {
		t.Errorf("expected 1 InterruptFrame downstream, got %d: %s", c, describeFrameTypes(down))
	}
}

// TestUserContextAggregator_NoBargeInBelowThreshold verifies that fewer
// than 3 interim words does NOT trigger an InterruptFrame.
func TestUserContextAggregator_NoBargeInBelowThreshold(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			BotStartedSpeakingFrame{},
			TranscriptFrame{Text: "hi", IsFinal: false, ResponseID: 1}, // 1 word
		},
		settleDelay:  30 * time.Millisecond,
		sendEndFrame: true,
	})

	if c := countFrames[InterruptFrame](down); c != 0 {
		t.Errorf("expected no InterruptFrame for below-threshold transcript, got %d", c)
	}
}

// TestUserContextAggregator_BackchannelDiscardedWhenBotFinishesFirst verifies
// the race-case bug fix: when the user speaks below the barge-in
// threshold WHILE the bot is talking, AND the bot stops speaking BEFORE
// Soniox emits the <end> for that speech, the lagging
// <end> must NOT submit those words as a user turn. Mirrors Pipecat's
// reset_aggregation behavior at the bot-turn boundary.
func TestUserContextAggregator_BackchannelDiscardedWhenBotFinishesFirst(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	// Bot starts speaking.
	sink.QueueFrame(BotStartedSpeakingFrame{}, Upstream)
	time.Sleep(20 * time.Millisecond)

	// User says "okay" (1 word, below 3-word threshold). Interim then
	// final, but NO <end> yet.
	source.QueueFrame(TranscriptFrame{Text: "okay", IsFinal: false, ResponseID: 1}, Downstream)
	source.QueueFrame(TranscriptFrame{Text: "okay", IsFinal: true, ResponseID: 1}, Downstream)
	time.Sleep(20 * time.Millisecond)

	// Bot stops naturally — BotStoppedSpeakingFrame arrives BEFORE Soniox's <end>.
	sink.QueueFrame(NewBotStoppedSpeakingFrame(), Upstream)
	time.Sleep(20 * time.Millisecond)

	// Now Soniox finally emits <end> for the backchannel. The aggregator
	// must NOT submit "okay" as a user turn.
	source.QueueFrame(TranscriptFrame{Text: "<end>", IsFinal: true, ResponseID: 1}, Downstream)
	time.Sleep(20 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)

	if c := countFrames[LLMContextFrame](sink.Captured()); c != 0 {
		t.Errorf("expected NO LLMContextFrame for back-channel speech, got %d in %s", c, describeFrameTypes(sink.Captured()))
	}
	if c := countFrames[InterruptFrame](sink.Captured()); c != 0 {
		t.Errorf("did not expect InterruptFrame for sub-threshold speech, got %d", c)
	}
	// And no user-role message should have been appended.
	for _, m := range a.messagesForTest() {
		if m.Role == "user" {
			t.Errorf("aggregator should not have a user message; got %q", m.Content)
		}
	}
}

// TestUserContextAggregator_BackchannelDiscardedWhileBotStillSpeaking
// verifies the existing in-progress discard branch still works: when
// <end> arrives WHILE botSpeaking is still true, the below-threshold
// transcript is discarded synchronously.
func TestUserContextAggregator_BackchannelDiscardedWhileBotStillSpeaking(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	// Bot starts and remains speaking; user mumbles "yeah" (1 word).
	sink.QueueFrame(BotStartedSpeakingFrame{}, Upstream)
	time.Sleep(20 * time.Millisecond)
	source.QueueFrame(TranscriptFrame{Text: "yeah", IsFinal: false, ResponseID: 1}, Downstream)
	source.QueueFrame(TranscriptFrame{Text: "yeah", IsFinal: true, ResponseID: 1}, Downstream)
	source.QueueFrame(TranscriptFrame{Text: "<end>", IsFinal: true, ResponseID: 1}, Downstream)
	time.Sleep(30 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)

	if c := countFrames[LLMContextFrame](sink.Captured()); c != 0 {
		t.Errorf("expected NO LLMContextFrame; in-progress discard should fire, got %d", c)
	}
	for _, m := range a.messagesForTest() {
		if m.Role == "user" {
			t.Errorf("aggregator should not have a user message; got %q", m.Content)
		}
	}
}

// TestUserContextAggregator_BargeInPreservesUserTranscript verifies the
// regression boundary: when the user DOES cross the barge-in threshold,
// their accumulated speech is NOT reset by the TTSDone path (since
// barge-in fires before TTSDone in our flow, and interruptSent gates
// the reset).
func TestUserContextAggregator_BargeInPreservesUserTranscript(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	sink.QueueFrame(BotStartedSpeakingFrame{}, Upstream)
	time.Sleep(20 * time.Millisecond)

	// User says "I have a question" — 4 words → barge-in fires.
	source.QueueFrame(TranscriptFrame{Text: "I have a question", IsFinal: false, ResponseID: 1}, Downstream)
	time.Sleep(20 * time.Millisecond)
	sink.QueueFrame(BotStoppedSpeakingFrame{}, Upstream)
	time.Sleep(10 * time.Millisecond)
	// Now final tokens + <end> arrive after barge-in.
	source.QueueFrame(TranscriptFrame{Text: "I have a question", IsFinal: true, ResponseID: 1}, Downstream)
	source.QueueFrame(TranscriptFrame{Text: "<end>", IsFinal: true, ResponseID: 1}, Downstream)
	time.Sleep(30 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)

	if c := countFrames[InterruptFrame](sink.Captured()); c != 1 {
		t.Errorf("expected 1 InterruptFrame, got %d", c)
	}
	llmMsg, ok := findFrame[LLMContextFrame](sink.Captured())
	if !ok {
		t.Fatalf("expected LLMContextFrame after barge-in, got %s", describeFrameTypes(sink.Captured()))
	}
	last := llmMsg.Context.GetMessages()[len(llmMsg.Context.GetMessages())-1]
	if last.Role != "user" || last.Content != "I have a question" {
		t.Errorf("user message: got %q=%q, want user='I have a question'", last.Role, last.Content)
	}
}

func TestUserContextAggregator_EmitsUserCommittedTurnCallEvent(t *testing.T) {
	fix := newTestFixture(t)
	var users []string
	var userPromptKeys []string
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnUserTurnCommitted: func(text string, at time.Time, promptKey string) {
			users = append(users, text)
			userPromptKeys = append(userPromptKeys, promptKey)
		},
	})
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "sales_call/main_sys-3day_v2_v17")

	a.addUserMessage("hello")
	fix.TaskCtx.callEvents.stopAndDrain()

	if len(users) != 1 || users[0] != "hello" {
		t.Fatalf("user turn events = %v, want [hello]", users)
	}
	if len(userPromptKeys) != 1 || userPromptKeys[0] != "sales_call/main_sys-3day_v2_v17" {
		t.Fatalf("user prompt keys = %v, want sales prompt key", userPromptKeys)
	}
}

func TestUserContextAggregator_MergedUserTurnCallEventMatchesLLMContext(t *testing.T) {
	fix := newTestFixture(t)
	var users []string
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnUserTurnCommitted: func(text string, at time.Time, promptKey string) {
			users = append(users, text)
		},
	})
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	sink := newQueueProcessor(fix.TaskCtx, "output", Downstream)
	processors := []Processor{pair.User(), pair.Assistant(), sink}
	NewPipeline(processors).Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, processors...)
	for i, text := range []string{"first", "second"} {
		pair.User().QueueFrame(NewTranscriptFrame(text, true, i+1, false), Downstream)
		pair.User().QueueFrame(NewTranscriptFrame("<end>", true, i+1, false), Downstream)
		awaitSpeechCondition(t, "user request", func() bool {
			return countFrames[LLMContextFrame](sink.Captured()) == i+1
		})
	}
	down := sink.Captured()
	fix.TaskCtx.callEvents.stopAndDrain()

	if len(users) != 2 || users[0] != "first" || users[1] != "first second" {
		t.Fatalf("user turn events = %v, want [\"first\" \"first second\"]", users)
	}

	var llmFrames []LLMContextFrame
	for _, frame := range down {
		if llmFrame, ok := frame.(LLMContextFrame); ok {
			llmFrames = append(llmFrames, llmFrame)
		}
	}
	if len(llmFrames) != 2 {
		t.Fatalf("LLMContextFrame count = %d, want 2 in %s", len(llmFrames), describeFrameTypes(down))
	}
	messages := llmFrames[len(llmFrames)-1].Context.GetMessages()
	if len(messages) == 0 {
		t.Fatal("last LLMContextFrame has no messages")
	}
	userMessages := 0
	for _, message := range messages {
		if message.Role == "user" {
			userMessages++
		}
	}
	last := messages[len(messages)-1]
	if userMessages != 1 || last.Role != "user" || last.Content != "first second" {
		t.Fatalf("last LLM messages = %+v, want one user message ending with content %q", messages, "first second")
	}
}

func TestUserContextAggregator_UserFirstSpeechLifecycleFiresOnce(t *testing.T) {
	fix := newTestFixture(t)
	calls := make(chan time.Time, 2)
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnUserFirstSpeech: func(at time.Time) { calls <- at },
	})
	defer fix.TaskCtx.callEvents.stopAndDrain()
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			TranscriptFrame{Text: "first", IsFinal: true},
			TranscriptFrame{Text: "<end>", IsFinal: true},
			TranscriptFrame{Text: "second", IsFinal: true},
			TranscriptFrame{Text: "<end>", IsFinal: true},
		},
		sendEndFrame: true,
	})
	fix.TaskCtx.callEvents.stopAndDrain()

	select {
	case <-calls:
	default:
		t.Fatal("OnUserFirstSpeech was not called")
	}
	select {
	case <-calls:
		t.Fatal("OnUserFirstSpeech should only fire once")
	default:
	}
}

// serverMessageStrings returns the string-payload server-message RTVI
// entries from a snapshot, in order.
func serverMessageStrings(entries []RTVIDebugLogEntry) []string {
	var out []string
	for _, e := range entries {
		if e.Type != "server-message" {
			continue
		}
		if s, ok := e.Data.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// TestUserContextAggregator_BargeInEmitsUserTurnStartedRTVI verifies the
// Python-parity "User turn started" RTVI line
// (base_pipeline_manager.py:425-431, MinWordsUserTurnStartStrategy
// crossing the 3-word bot-speaking threshold) fires at Go's barge-in
// trigger point.
func TestUserContextAggregator_BargeInEmitsUserTurnStartedRTVI(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			BotStartedSpeakingFrame{},
			TranscriptFrame{Text: "one two three", IsFinal: false, ResponseID: 1},
		},
		settleDelay:  30 * time.Millisecond,
		sendEndFrame: true,
	})

	msgs := serverMessageStrings(fix.TaskCtx.UIEvents.Snapshot())
	if !containsString(msgs, "User turn started") {
		t.Errorf("expected 'User turn started' RTVI line, got %v", msgs)
	}
}

// TestUserContextAggregator_NormalTurnEmitsUserTurnStartedRTVI verifies
// the same RTVI line fires for a normal (bot-silent) user turn — Go's
// equivalent of Python's min_words=1-when-bot-silent threshold crossing.
func TestUserContextAggregator_NormalTurnEmitsUserTurnStartedRTVI(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			TranscriptFrame{Text: "hello", IsFinal: true},
			TranscriptFrame{Text: "<end>", IsFinal: true},
		},
		sendEndFrame: true,
	})

	msgs := serverMessageStrings(fix.TaskCtx.UIEvents.Snapshot())
	if !containsString(msgs, "User turn started") {
		t.Errorf("expected 'User turn started' RTVI line, got %v", msgs)
	}
}

// TestUserContextAggregator_BargeInContinuationDoesNotDuplicateUserTurnStartedRTVI
// verifies a barge-in turn's eventual final-transcript submission does NOT
// emit a second "User turn started" line — Python fires the event exactly
// once per turn (at threshold crossing), not again when the turn commits.
func TestUserContextAggregator_BargeInContinuationDoesNotDuplicateUserTurnStartedRTVI(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, testInitialMessages(), "")

	source := newQueueProcessor(fix.TaskCtx, "test-source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	source.Link(a)
	a.Link(sink)
	source.Start(fix.RootCtx)
	a.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	sink.QueueFrame(BotStartedSpeakingFrame{}, Upstream)
	time.Sleep(20 * time.Millisecond)

	// Cross the barge-in threshold (fires InterruptFrame + "User turn started").
	source.QueueFrame(TranscriptFrame{Text: "one two three", IsFinal: false, ResponseID: 1}, Downstream)
	time.Sleep(20 * time.Millisecond)

	// The same turn's final transcript commits.
	source.QueueFrame(TranscriptFrame{Text: "one two three", IsFinal: true, ResponseID: 1}, Downstream)
	source.QueueFrame(TranscriptFrame{Text: "<end>", IsFinal: true, ResponseID: 1}, Downstream)
	time.Sleep(20 * time.Millisecond)

	source.QueueFrame(EndFrame{}, Downstream)
	stopProcessorsAndWait(t, fix, 3*time.Second, source, a, sink)

	msgs := serverMessageStrings(fix.TaskCtx.UIEvents.Snapshot())
	count := 0
	for _, m := range msgs {
		if m == "User turn started" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 'User turn started' line for the whole barge-in turn, got %d: %v", count, msgs)
	}
}
