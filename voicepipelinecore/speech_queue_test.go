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

func TestInterruptDropsOldIngressAndPreservesNewFrames(t *testing.T) {
	fix := newTestFixture(t)
	p := newPassThroughProcessor(fix.TaskCtx, "ingress")
	// Both input queues are backlogged before any worker starts.
	p.QueueFrame(NewTextFrame("old"), Downstream)
	p.QueueFrame(NewInterruptFrame(), Downstream)
	p.QueueFrame(NewTextFrame("new"), Downstream)
	p.QueueFrame(NewEndFrame(""), Downstream)
	p.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, p)
	awaitSpeechCondition(t, "new frames after interrupt", func() bool { return len(p.Received()) == 3 })
	got := p.Received()
	if _, ok := got[0].frame.(InterruptFrame); !ok {
		t.Fatalf("new processing started before interruption: %T", got[0].frame)
	}
	if f, ok := got[1].frame.(TextFrame); !ok || f.Text != "new" || !got[1].ctxAlive {
		t.Fatalf("new frame lost or assigned an old context: %+v", got[1])
	}
}

func TestConcurrentInterruptEnqueueDoesNotStrandNewData(t *testing.T) {
	fix := newTestFixture(t)
	p := newPassThroughProcessor(fix.TaskCtx, "ingress")
	// Model sender 1 pausing after assigning its epoch while sender 2 enqueues.
	p.epoch.Store(2)
	p.inputSysCh <- Envelope{Frame: NewInterruptFrame(), epoch: 2}
	p.inputSysCh <- Envelope{Frame: NewInterruptFrame(), epoch: 1}
	p.QueueFrame(NewTextFrame("new"), Downstream)
	p.Start(fix.RootCtx)
	defer stopProcessorsAndWait(t, fix, time.Second, p)
	awaitSpeechCondition(t, "new data after concurrent interruptions", func() bool { return len(p.Received()) == 2 })
	got := p.Received()[1]
	if f, ok := got.frame.(TextFrame); !ok || f.Text != "new" || !got.ctxAlive {
		t.Fatalf("latest input was lost: %+v", got)
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
			a.ResponseID = 10
			b := NewLLMResponseStartFrame(time.Now())
			b.ResponseID = 20
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

func TestContinuationGeneratesAheadAndInterruptionReconcilesHistory(t *testing.T) {
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
	if second[len(second)-2].Content != "alpha unheard." {
		t.Fatalf("B did not see A's generated context: %+v", second)
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
	if !reflect.DeepEqual(assistants, []string{"alpha"}) {
		t.Fatalf("unheard speech remained in history: %+v", third)
	}
	if !keptInstruction || !strings.Contains(third[len(third)-1].Content, "change the topic") {
		t.Fatalf("user messages were lost: %+v", third)
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

func TestMetricsStayWithTheirResponseDuringQueuedPlayback(t *testing.T) {
	m := &perTurnMetrics{}
	for id := int64(1); id <= 2; id++ {
		f := NewMetricsFrame([]MetricsData{{Processor: "tts", Label: MetricTTFB, ValueMs: float64(id * 10)}})
		f.ResponseID = id
		m.absorb(f)
	}
	if a, b := m.snapshotAndReset(1), m.snapshotAndReset(2); a.TTSTTFBMs != 10 || b.TTSTTFBMs != 20 {
		t.Fatalf("metrics crossed responses: A=%+v B=%+v", a, b)
	}
}
