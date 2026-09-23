package voicepipelinecore

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"strings"
	"testing"
	"time"
)

func awaitSpeechCondition(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPlaybackIngressPreservesPendingAudioWordAndEndOrder(t *testing.T) {
	fix := newTestFixture(t)
	p := NewPlaybackSinkProcessor(fix.TaskCtx)
	// Backlog before starting workers reproduces the former priority inversion.
	want := []Frame{NewAudioFrame(make([]byte, framePCMBytes)), NewWordTimestampFrame([]string{"hello"}), NewTTSDoneFrame()}
	for _, f := range want {
		p.QueueFrame(f, Downstream)
	}
	p.BaseProcessor.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, p)
	for _, f := range want {
		select {
		case got := <-p.queueCh:
			if reflect.TypeOf(got.frame) != reflect.TypeOf(f) {
				t.Fatalf("got %T, want %T", got.frame, f)
			}
		case <-time.After(time.Second):
			t.Fatal("playback ingress stalled")
		}
	}
}

func TestInterruptionDoesNotAddAnInputQueueEpoch(t *testing.T) {
	fix := newTestFixture(t)
	p := newPassThroughProcessor(fix.TaskCtx, "ingress")
	// Pipecat resets the process queue, not its priority input queue.
	p.QueueFrame(NewTextFrame("still in input queue"), Downstream)
	p.QueueFrame(NewInterruptFrame(), Downstream)
	p.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, p)
	awaitSpeechCondition(t, "input processed after interruption", func() bool { return len(p.Received()) == 2 })
	got := p.Received()
	if _, ok := got[0].frame.(InterruptFrame); !ok {
		t.Fatalf("system frame lost priority: %T", got[0].frame)
	}
	if f, ok := got[1].frame.(TextFrame); !ok || f.Text != "still in input queue" {
		t.Fatalf("input frame was invalidated: %+v", got[1])
	}
}

func sendSpeechAudio(t *testing.T, fc *fakeCartesiaConn, id string, value byte, frames int) {
	t.Helper()
	err := fc.conn.WriteJSON(map[string]any{"type": "chunk", "context_id": id, "data": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{value}, frames*framePCMBytes))})
	if err != nil {
		t.Fatal(err)
	}
}

func sendSpeechWords(t *testing.T, fc *fakeCartesiaConn, id string, words []string, starts []float64) {
	t.Helper()
	if err := fc.conn.WriteJSON(map[string]any{"type": "timestamps", "context_id": id, "word_timestamps": map[string]any{"words": words, "start": starts}}); err != nil {
		t.Fatal(err)
	}
}

// Both requests must reach Cartesia before it returns audio for either one.
// This distinguishes independent speech contexts from playback deferral.
func collectSpeechRequests(t *testing.T, fc *fakeCartesiaConn, count int) map[string]string {
	t.Helper()
	ids := map[string]string{}
	closed := 0
	deadline := time.After(2 * time.Second)
	for closed < count {
		select {
		case msg := <-fc.inbound:
			if text, _ := msg["transcript"].(string); text != "" {
				ids[text], _ = msg["context_id"].(string)
			}
			if cont, ok := msg["continue"].(bool); ok && !cont {
				closed++
			}
		case <-deadline:
			t.Fatal("speech requests waited for earlier synthesis/playback")
		}
	}
	return ids
}

func TestSpeechContextsSynthesizeIndependentlyAndEmitInOrder(t *testing.T) {
	for _, direct := range []bool{false, true} {
		name := "two LLM responses"
		if direct {
			name = "direct speech during LLM response"
		}
		t.Run(name, func(t *testing.T) {
			fix := newTestFixture(t)
			fs := newFakeCartesiaServer(t)
			withTTSDialURL(t, fs.URL)
			tts := NewTTSProcessor(fix.TaskCtx, nil, "")
			sink := newQueueProcessor(fix.TaskCtx, "output", Downstream)
			tts.Link(sink)
			sink.Start(fix.RootCtx)
			tts.Start(fix.RootCtx)
			defer stopProcessorsAndWait(t, fix, time.Second, tts, sink)
			a := NewLLMResponseStartFrame(time.Now())
			b := NewLLMResponseStartFrame(time.Now())
			tts.QueueFrame(a, Downstream)
			tts.QueueFrame(NewTextFrame("alpha."), Downstream)
			if direct {
				tts.QueueFrame(NewTTSSpeakFrame("beta."), Downstream)
			}
			tts.QueueFrame(NewLLMResponseEndFrame(), Downstream)
			if !direct {
				tts.QueueFrame(b, Downstream)
				tts.QueueFrame(NewTextFrame("beta."), Downstream)
				tts.QueueFrame(NewLLMResponseEndFrame(), Downstream)
			}
			fc := fs.conn(t, 0)
			ids := collectSpeechRequests(t, fc, 2)
			if ids["alpha."] == "" || ids["beta."] == "" || ids["alpha."] == ids["beta."] {
				t.Fatalf("context IDs: %v", ids)
			}
			tts.QueueFrame(NewLLMMessagesFrame([]Message{{Role: "user", Content: "ordered marker"}}), Downstream)
			tts.QueueFrame(NewEndFrame(""), Downstream)
			// B finishes first; it must remain behind A in the output stream.
			sendSpeechWords(t, fc, ids["beta."], []string{"beta"}, []float64{0})
			sendSpeechAudio(t, fc, ids["beta."], 2, 1)
			fc.sendDone(ids["beta."])
			sendSpeechWords(t, fc, ids["alpha."], []string{"alpha"}, []float64{0})
			sendSpeechAudio(t, fc, ids["alpha."], 1, 1)
			fc.sendDone(ids["alpha."])
			awaitSpeechCondition(t, "graceful end after both utterances", func() bool { return countFrames[EndFrame](sink.Captured()) == 1 })
			var order []string
			for _, f := range sink.Captured() {
				switch f := f.(type) {
				case AudioFrame:
					if f.Data[0] == 1 {
						order = append(order, "audio A")
					} else {
						order = append(order, "audio B")
					}
				case WordTimestampFrame:
					order = append(order, strings.Join(f.Words, " "))
				case TTSDoneFrame:
					order = append(order, "done")
				case EndFrame:
					order = append(order, "end")
				case LLMMessagesFrame:
					order = append(order, "marker")
				}
			}
			want := []string{"audio A", "alpha", "done", "audio B", "beta", "done", "marker", "end"}
			if !reflect.DeepEqual(order, want) {
				t.Fatalf("output = %v, want %v", order, want)
			}
		})
	}
}

func TestSpeechInterruptionCancelsAllContextsAndRejectsLateAudio(t *testing.T) {
	fix := newTestFixture(t)
	fs := newFakeCartesiaServer(t)
	withTTSDialURL(t, fs.URL)
	tts := NewTTSProcessor(fix.TaskCtx, nil, "")
	sink := newQueueProcessor(fix.TaskCtx, "output", Downstream)
	tts.Link(sink)
	sink.Start(fix.RootCtx)
	tts.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, tts, sink)
	tts.QueueFrame(NewTTSSpeakFrame("alpha."), Downstream)
	tts.QueueFrame(NewTTSSpeakFrame("beta."), Downstream)
	fc := fs.conn(t, 0)
	ids := collectSpeechRequests(t, fc, 2)
	tts.QueueFrame(NewInterruptFrame(), Downstream)
	cancelled := map[string]bool{}
	for len(cancelled) < 2 {
		select {
		case msg := <-fc.inbound:
			if cancel, _ := msg["cancel"].(bool); cancel {
				cancelled[msg["context_id"].(string)] = true
			}
		case <-time.After(time.Second):
			t.Fatal("not all pending contexts were cancelled")
		}
	}
	if !cancelled[ids["alpha."]] || !cancelled[ids["beta."]] {
		t.Fatalf("cancelled %v, want %v", cancelled, ids)
	}
	tts.QueueFrame(NewTTSSpeakFrame("gamma."), Downstream)
	newID := collectSpeechRequests(t, fc, 1)["gamma."]
	for _, id := range ids {
		sendSpeechAudio(t, fc, id, 1, 1)
		fc.sendDone(id)
	}
	sendSpeechAudio(t, fc, newID, 3, 1)
	fc.sendDone(newID)
	awaitSpeechCondition(t, "new speech completed", func() bool { return countFrames[TTSDoneFrame](sink.Captured()) == 1 })
	for _, f := range sink.Captured() {
		if audio, ok := f.(AudioFrame); ok && audio.Data[0] != 3 {
			t.Fatal("cancelled speech returned after interruption")
		}
	}
}

func TestImmediateAppendGeneratesAheadWithoutSpeculativeHistory(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.Room = &testOutputRoom{outputSampleRate: defaultOutputSampleRate}
	fs := newFakeCartesiaServer(t)
	withTTSDialURL(t, fs.URL)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	client := &stubLLMClient{responses: []stubLLMResponse{
		{tokens: []string{"alpha unheard."}}, {tokens: []string{"queued beta."}}, {tokens: []string{"new answer."}},
	}}
	llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
	tts := NewTTSProcessor(fix.TaskCtx, nil, "")
	playback := NewPlaybackSinkProcessor(fix.TaskCtx)
	processors := []Processor{pair.User(), llm, tts, playback, pair.Assistant()}
	NewPipeline(processors).Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, 2*time.Second, processors...)
	pair.User().QueueFrame(NewLLMMessagesAppendFrame(nil, true), Downstream)
	fc := fs.conn(t, 0)
	aID := collectSpeechRequests(t, fc, 1)["alpha unheard."]
	pair.User().QueueFrame(NewLLMMessagesAppendFrame([]Message{{Role: "user", Content: "<system_message>continue</system_message>"}}, true), Downstream)
	awaitSpeechCondition(t, "B generation before any A audio", func() bool { return len(client.Requests()) == 2 })
	bID := collectSpeechRequests(t, fc, 1)["queued beta."]
	second := client.Requests()[1].Messages
	for _, m := range second {
		if m.Role == "assistant" {
			t.Fatalf("B saw unplayed generated text: %+v", second)
		}
	}
	// Queue B first at the provider, then start long A. Only A may play.
	sendSpeechAudio(t, fc, bID, 2, 1)
	sendSpeechWords(t, fc, bID, []string{"queued", "beta"}, []float64{0, 0.01})
	fc.sendDone(bID)
	sendSpeechWords(t, fc, aID, []string{"alpha", "unheard"}, []float64{0, 1})
	sendSpeechAudio(t, fc, aID, 1, 100)
	fc.sendDone(aID)
	awaitSpeechCondition(t, "A's first played word", func() bool {
		pair.assistant.mu.Lock()
		defer pair.assistant.mu.Unlock()
		return len(pair.assistant.playedWords) > 0
	})
	pair.User().QueueFrame(NewTranscriptFrame("change the topic", false, 1, false), Downstream)
	pair.User().QueueFrame(NewTranscriptFrame("change the topic", true, 1, false), Downstream)
	pair.User().QueueFrame(NewTranscriptFrame("<end>", true, 1, false), Downstream)
	awaitSpeechCondition(t, "replacement user turn", func() bool { return len(client.Requests()) == 3 })
	third := client.Requests()[2].Messages
	var assistants []string
	var keptInstruction bool
	for _, m := range third {
		if m.Role == "assistant" {
			assistants = append(assistants, m.Content)
		}
		if strings.Contains(m.Content, "<system_message>continue</system_message>") {
			keptInstruction = true
		}
	}
	// Pipecat does not wait for assistant history before replacement inference.
	for _, text := range assistants {
		if text != "alpha" {
			t.Fatalf("unheard speech in inference: %+v", third)
		}
	}
	awaitSpeechCondition(t, "played history committed after interruption", func() bool {
		for _, m := range pair.MessagesSnapshot() {
			if m.Role == "assistant" {
				return m.Content == "alpha"
			}
		}
		return false
	})
	if !keptInstruction || !strings.Contains(third[len(third)-1].Content, "change the topic") {
		t.Fatalf("user messages were lost: %+v", third)
	}
}

func TestSpeechQueuedAppendRunsAfterAssistantCommit(t *testing.T) {
	for _, playbackFirst := range []bool{false, true} {
		name := "append before playback"
		if playbackFirst {
			name = "append after playback"
		}
		t.Run(name, func(t *testing.T) {
			fix := newTestFixture(t)
			fix.TaskCtx.Room = &testOutputRoom{outputSampleRate: defaultOutputSampleRate}
			fs := newFakeCartesiaServer(t)
			withTTSDialURL(t, fs.URL)
			pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
			client := &stubLLMClient{responses: []stubLLMResponse{
				{tokens: []string{"previous statement."}}, {tokens: []string{"next question?"}},
			}}
			llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
			tts := NewTTSProcessor(fix.TaskCtx, nil, "")
			playback := NewPlaybackSinkProcessor(fix.TaskCtx)
			processors := []Processor{pair.User(), llm, tts, playback, pair.Assistant()}
			NewPipeline(processors).Start(fix.RootCtx)
			defer stopProcessorsAndWait(t, fix, 2*time.Second, processors...)
			pair.User().QueueFrame(NewLLMMessagesAppendFrame(nil, true), Downstream)
			fc := fs.conn(t, 0)
			id := collectSpeechRequests(t, fc, 1)["previous statement."]
			instruction := Message{Role: "user", Content: "<system_message>continue</system_message>"}
			appendContinuation := func() {
				// Bypass the user handler so the ordinary frame reaches the
				// assistant handler after serialized TTS and paced playback.
				pair.User().PushFrame(NewLLMMessagesAppendFrame([]Message{instruction}, true), Downstream)
			}
			if !playbackFirst {
				appendContinuation()
			}
			sendSpeechWords(t, fc, id, []string{"previous", "statement."}, []float64{0, 0.02})
			sendSpeechAudio(t, fc, id, 1, 10)
			fc.sendDone(id)
			if playbackFirst {
				awaitSpeechCondition(t, "statement committed before append", func() bool {
					messages := pair.MessagesSnapshot()
					return messages[len(messages)-1].Content == "previous statement."
				})
				appendContinuation()
			}
			awaitSpeechCondition(t, "continuation inference", func() bool { return len(client.Requests()) == 2 })
			want := append(testInitialMessages(), Message{Role: "assistant", Content: "previous statement."}, instruction)
			if got := client.Requests()[1].Messages; !reflect.DeepEqual(got, want) {
				t.Fatalf("continuation history = %+v, want %+v", got, want)
			}
			if got := pair.MessagesSnapshot(); !reflect.DeepEqual(got, want) {
				t.Fatalf("shared history = %+v, want %+v", got, want)
			}
		})
	}
}

func TestSpeechQueuedAppendIsDroppedOnInterruption(t *testing.T) {
	for _, duringPlayback := range []bool{false, true} {
		name := "during synthesis"
		if duringPlayback {
			name = "during playback"
		}
		t.Run(name, func(t *testing.T) {
			fix := newTestFixture(t)
			fix.TaskCtx.Room = &testOutputRoom{outputSampleRate: defaultOutputSampleRate}
			fs := newFakeCartesiaServer(t)
			withTTSDialURL(t, fs.URL)
			pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
			client := &stubLLMClient{responses: []stubLLMResponse{
				{tokens: []string{"alpha unheard."}}, {tokens: []string{"new answer."}},
			}}
			llm := NewLLMProcessorWithClient(fix.TaskCtx, client)
			tts := NewTTSProcessor(fix.TaskCtx, nil, "")
			output := newQueueProcessor(fix.TaskCtx, "tts-output", Downstream)
			playback := NewPlaybackSinkProcessor(fix.TaskCtx)
			processors := []Processor{pair.User(), llm, tts, output, playback, pair.Assistant()}
			NewPipeline(processors).Start(fix.RootCtx)
			defer stopProcessorsAndWait(t, fix, 2*time.Second, processors...)
			pair.User().QueueFrame(NewLLMMessagesAppendFrame(nil, true), Downstream)
			fc := fs.conn(t, 0)
			id := collectSpeechRequests(t, fc, 1)["alpha unheard."]
			pair.User().PushFrame(NewLLMMessagesAppendFrame([]Message{{Role: "user", Content: "queued continuation"}}, true), Downstream)
			// This subsequent speak request reaching the provider proves TTS
			// has consumed the preceding append into its serialization queue.
			pair.User().PushFrame(NewTTSSpeakFrame("queue marker."), Downstream)
			collectSpeechRequests(t, fc, 1)
			if duringPlayback {
				sendSpeechWords(t, fc, id, []string{"alpha", "unheard"}, []float64{0, 1.5})
				sendSpeechAudio(t, fc, id, 1, 100)
				fc.sendDone(id)
				awaitSpeechCondition(t, "continuation released to playback", func() bool {
					return countFrames[LLMMessagesAppendFrame](output.Captured()) == 1
				})
				awaitSpeechCondition(t, "first word played", func() bool {
					pair.assistant.mu.Lock()
					defer pair.assistant.mu.Unlock()
					return len(pair.assistant.playedWords) > 0
				})
			}
			if len(client.Requests()) != 1 {
				t.Fatal("continuation ran before speech finished")
			}
			pair.User().QueueFrame(NewInterruptFrame(), Downstream)
			awaitSpeechCondition(t, "interruption handled downstream", func() bool {
				return countFrames[InterruptFrame](output.Captured()) == 1
			})
			// A fresh turn still works; it must not pick up the discarded append.
			pair.User().QueueFrame(NewTranscriptFrame("change the topic", true, 1, false), Downstream)
			pair.User().QueueFrame(NewTranscriptFrame("<end>", true, 1, false), Downstream)
			awaitSpeechCondition(t, "replacement user turn", func() bool { return len(client.Requests()) == 2 })
			collectSpeechRequests(t, fc, 1)
			for _, m := range client.Requests()[1].Messages {
				if m.Content == "queued continuation" || (m.Role == "assistant" && m.Content != "alpha") {
					t.Fatalf("unplayed continuation entered replacement context: %+v", m)
				}
			}
			var keptUserInput bool
			for _, m := range pair.MessagesSnapshot() {
				keptUserInput = keptUserInput || (m.Role == "user" && m.Content == "change the topic")
				if m.Content == "queued continuation" {
					t.Fatal("discarded continuation entered shared history")
				}
			}
			if !keptUserInput {
				t.Fatal("new patient input lost")
			}
			if len(client.Requests()) != 2 {
				t.Fatal("discarded continuation triggered an extra LLM run")
			}
		})
	}
}

func TestLLMRequestsQueueWithoutCancellingEarlierGeneration(t *testing.T) {
	fix := newTestFixture(t)
	release := make(chan struct{})
	client := &stubLLMClient{responses: []stubLLMResponse{{tokens: []string{"A"}, blockUntil: release}, {tokens: []string{"B"}}}}
	p := NewLLMProcessorWithClient(fix.TaskCtx, client)
	sink := newQueueProcessor(fix.TaskCtx, "output", Downstream)
	p.Link(sink)
	sink.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, p, sink)
	p.QueueFrame(NewLLMMessagesFrame(testInitialMessages()), Downstream)
	awaitSpeechCondition(t, "A started", func() bool { return len(client.Requests()) == 1 })
	p.QueueFrame(NewLLMMessagesFrame(testInitialMessages()), Downstream)
	close(release)
	awaitSpeechCondition(t, "both generations complete", func() bool { return countFrames[LLMResponseEndFrame](sink.Captured()) == 2 })
	var text string
	for _, f := range sink.Captured() {
		if token, ok := f.(TextFrame); ok {
			text += token.Text
		}
	}
	if text != "AB" {
		t.Fatalf("generations were replaced/interleaved: %q", text)
	}
}

// Processor measurements are best-effort at chunk commit, not response-correlated.
func TestProcessorMetricsUseLatestMeasurement(t *testing.T) {
	m := &perTurnMetrics{}
	for _, value := range []float64{10, 20} {
		m.absorb(NewMetricsFrame([]MetricsData{{Processor: "tts", Label: MetricTTFB, ValueMs: value}}))
	}
	if got := m.snapshotAndReset(); got.TTSTTFBMs != 20 {
		t.Fatalf("latest metrics=%+v", got)
	}
	if got := m.snapshotAndReset(); got != (TurnMetrics{}) {
		t.Fatalf("metrics not reset: %+v", got)
	}
}

func TestStandaloneSpeechFlushesThroughNativeAggregationFrame(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.Room = &testOutputRoom{outputSampleRate: defaultOutputSampleRate}
	server := newFakeCartesiaServer(t)
	withTTSDialURL(t, server.URL)
	pair := NewContextAggregatorPair(fix.TaskCtx, testInitialMessages(), "")
	tts := NewTTSProcessor(fix.TaskCtx, nil, "")
	playback := NewPlaybackSinkProcessor(fix.TaskCtx)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	processors := []Processor{tts, playback, pair.Assistant(), sink}
	NewPipeline(processors).Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, processors...)
	tts.QueueFrame(NewTTSSpeakFrame("Hello?"), Downstream)
	conn := server.conn(t, 0)
	id := collectSpeechRequests(t, conn, 1)["Hello?"]
	sendSpeechWords(t, conn, id, []string{"Hello?"}, []float64{0})
	sendSpeechAudio(t, conn, id, 1, 1)
	conn.sendDone(id)
	awaitSpeechCondition(t, "standalone aggregation frame after speech", func() bool {
		return countFrames[LLMAssistantPushAggregationFrame](sink.Captured()) == 1
	})
	messages := pair.MessagesSnapshot()
	if messages[len(messages)-1].Role != "assistant" || messages[len(messages)-1].Content != "Hello?" {
		t.Fatalf("standalone speech context=%+v", messages)
	}
	if countFrames[LLMResponseStartFrame](sink.Captured()) != 0 {
		t.Fatal("direct speech invented an LLM response")
	}
}
