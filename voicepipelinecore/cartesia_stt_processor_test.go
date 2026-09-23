package voicepipelinecore

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// All wire tests stop and join the processor goroutines before restoring the
// package-var seams. Do not use t.Parallel with these endpoint overrides.
func configureCartesiaSTTTest(t *testing.T, serverURL string, interval, idle time.Duration) {
	t.Helper()
	oldURL, oldModel := cartesiaSTTDialURL, cartesiaSTTModel
	oldDelays := cartesiaSTTConnectRetryDelays
	oldInterval, oldIdle := cartesiaSTTKeepaliveInterval, cartesiaSTTKeepaliveIdleTimeout
	cartesiaSTTDialURL = "ws" + strings.TrimPrefix(serverURL, "http")
	cartesiaSTTConnectRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	cartesiaSTTKeepaliveInterval, cartesiaSTTKeepaliveIdleTimeout = interval, idle
	t.Setenv("CARTESIA_API_KEY", "cartesia-unit-test-credential")
	t.Cleanup(func() {
		cartesiaSTTDialURL, cartesiaSTTModel = oldURL, oldModel
		cartesiaSTTConnectRetryDelays = oldDelays
		cartesiaSTTKeepaliveInterval, cartesiaSTTKeepaliveIdleTimeout = oldInterval, oldIdle
	})
}

func startCartesiaSTTTest(t *testing.T, fix *testFixture) (*CartesiaSTTProcessor, *QueueProcessor, *QueueProcessor) {
	t.Helper()
	p := NewCartesiaSTTProcessor(fix.TaskCtx)
	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)
	t.Cleanup(func() { stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink) })
	return p, source, sink
}

// Reuse Soniox's wire-message type and wait helpers; the Auto endpoint has no
// initial JSON config frame to consume. Returning false closes the server socket.
func cartesiaSTTServer(t *testing.T, onConnect func(*websocket.Conn, *http.Request, int32) bool) (string, <-chan int32, <-chan sttWireMessage) {
	t.Helper()
	connected := make(chan int32, 16)
	messages := make(chan sttWireMessage, 512)
	var connections atomic.Int32
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		n := connections.Add(1)
		connected <- n
		if onConnect != nil && !onConnect(conn, r, n) {
			return
		}
		for {
			mt, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			messages <- sttWireMessage{connection: n, messageType: mt, payload: string(data)}
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, connected, messages
}

func waitForCartesiaFrameCount(t *testing.T, capture func() []Frame, count int) []Frame {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if frames := capture(); len(frames) >= count {
			return frames
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d frames; got %s", count, describeFrameTypes(capture()))
	return nil
}

func TestCartesiaSTTRouting(t *testing.T) {
	fix := newTestFixture(t)
	p, source, sink := startCartesiaSTTTest(t, fix)
	downText := NewTextFrame("downstream")
	upText := NewTextFrame("upstream")
	p.QueueFrame(downText, Downstream)
	p.QueueFrame(upText, Upstream)
	p.QueueFrame(NewAudioFrame([]byte{0, 0, 1, 0}), Downstream)
	end := NewEndFrame("routing-test")
	p.QueueFrame(end, Downstream)
	gotEnd := waitForFrameType[EndFrame](t, sink.Captured, time.Second)
	waitForFrameType[TextFrame](t, source.Captured, time.Second)
	stopProcessorsAndWait(t, fix, time.Second, source, p, sink)
	if gotEnd != end || p.ctx.Err() == nil {
		t.Fatal("EndFrame must be preserved and stop the processor")
	}
	if got := sink.Captured(); !reflect.DeepEqual(got, []Frame{downText, end}) {
		t.Fatalf("downstream frames = %s, want TextFrame + EndFrame (audio consumed)", describeFrameTypes(got))
	}
	if got := source.Captured(); !reflect.DeepEqual(got, []Frame{upText}) {
		t.Fatalf("upstream frames = %s, want unchanged TextFrame", describeFrameTypes(got))
	}
}

func TestCartesiaSTTEndBeforeActivation(t *testing.T) {
	fix := newTestFixture(t)
	p, _, sink := startCartesiaSTTTest(t, fix)
	p.Start(fix.RootCtx) // Repeated Start must still leave exactly one writer.
	p.QueueFrame(NewEndFrame("never-joined"), Downstream)
	waitForFrameType[EndFrame](t, sink.Captured, time.Second)
	if p.currentWebsocketConn() != nil {
		t.Fatal("EndFrame must not activate the websocket")
	}
}

func TestCartesiaSTTActivationLogs(t *testing.T) {
	fix := newTestFixture(t)
	var logs bytes.Buffer
	fix.TaskCtx.Logger = log.New(&logs, "", 0)
	p := NewCartesiaSTTProcessor(fix.TaskCtx)
	at := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	p.noteAudioBeforeConnect(at)
	p.activate("first_audio_fallback", at)
	p.logInitialConnectLatency(at.Add(250 * time.Millisecond))
	p.logInitialConnectLatency(at.Add(time.Second))
	for _, want := range []string{
		"STT websocket activation requested provider=cartesia reason=first_audio_fallback activation_at=2026-09-15T00:00:00Z",
		"STT lazy connect latency provider=cartesia activation_reason=first_audio_fallback",
		"activation_to_connected_ms=250.0", "first_audio_to_connected_ms=250.0",
		"preconnect_audio_observed=true", "queued_audio_frames_before_connect=1",
	} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing log field/prefix %q", want)
		}
	}
	if strings.Count(logs.String(), "STT lazy connect latency") != 1 {
		t.Fatal("initial connection latency must log only once")
	}
}

func TestCartesiaSTTAggregatorContract(t *testing.T) {
	fix := newTestFixture(t)
	a := NewUserContextAggregator(fix.TaskCtx, []Message{{Role: "system", Content: "test"}}, "")
	process := func(f Frame) { a.ProcessFrame(context.Background(), f, Downstream) }
	process(NewBotStartedSpeakingFrame())
	process(NewTranscriptFrame("yes please", false, 1, false))
	process(NewTranscriptFrame("yes okay", false, 2, false))
	if a.interruptSent || a.interimTranscript != "yes okay" {
		t.Fatal("fresh IDs must replace interims without accumulating a false barge-in")
	}
	process(NewTranscriptFrame("please help me", false, 3, false))
	if !a.interruptSent {
		t.Fatal("a three-word interim must trigger barge-in")
	}
	process(NewTranscriptFrame("<end>", true, 4, false))
	if a.interimTranscript != "" || a.currentTranscript != "" || len(a.messagesForTest()) != 1 {
		t.Fatal("an empty end must clear transcript buffers without submitting a turn")
	}
	process(NewTranscriptFrame(" Hindi 21 चाहिए। ", true, 5, false))
	process(NewTranscriptFrame("<end>", true, 5, false))
	if got := a.messagesForTest(); len(got) != 2 || got[1].Content != " Hindi 21 चाहिए। " {
		t.Fatal("final text must concatenate raw and submit exactly once")
	}
}

func TestCartesiaSTTLazyConnectAndHandshake(t *testing.T) {
	for _, reason := range []string{"user_joined", "first_audio_fallback"} {
		t.Run(reason, func(t *testing.T) {
			handshake := make(chan bool, 1)
			wantQuery := url.Values{
				"model": {"ink-preview"}, "encoding": {"pcm_s16le"},
				"sample_rate": {"16000"}, "cartesia_version": {"2026-08-14"},
				"turn_start_threshold": {"0.8"}, "turn_eager_end_threshold": {"0.4"},
				"turn_end_threshold": {"0.2"}, "turn_end_timeout_ms": {"5600"},
			}
			serverURL, connected, messages := cartesiaSTTServer(t, func(_ *websocket.Conn, r *http.Request, _ int32) bool {
				// Never print headers or their values, including on failure.
				handshake <- r.Header.Get("X-API-Key") == "cartesia-unit-test-credential" && reflect.DeepEqual(r.URL.Query(), wantQuery)
				return true
			})
			configureCartesiaSTTTest(t, serverURL, time.Hour, time.Hour)
			if !reflect.DeepEqual(cartesiaSTTQueryParams(), wantQuery) {
				t.Fatal("query must contain exactly model, encoding, sample_rate, cartesia_version")
			}
			fix := newTestFixture(t)
			p, _, _ := startCartesiaSTTTest(t, fix)
			select {
			case <-connected:
				t.Fatal("dialed before activation")
			case <-time.After(30 * time.Millisecond):
			}
			audio := bytes.Repeat([]byte{1, 2}, 320) // One 20ms frame, unchanged on the wire.
			if reason == "user_joined" {
				p.QueueFrame(NewSTTConnectFrame(reason, time.Now()), Downstream)
			} else {
				p.QueueFrame(NewAudioFrame(audio), Downstream)
			}
			waitForSTTConnection(t, connected, 1)
			select {
			case valid := <-handshake:
				if !valid {
					t.Fatal("unexpected handshake query or missing/incorrect X-API-Key header")
				}
			case <-time.After(time.Second):
				t.Fatal("handshake not observed")
			}
			if reason == "user_joined" {
				p.QueueFrame(NewAudioFrame(audio), Downstream)
			}
			got := waitForSTTWireMessage(t, messages)
			if got.messageType != websocket.BinaryMessage || got.payload != string(audio) {
				t.Fatal("audio was modified or not sent as binary")
			}
			p.timingMu.Lock()
			gotReason, at := p.activationReason, p.activatedAt
			p.timingMu.Unlock()
			if gotReason != reason || at.IsZero() {
				t.Fatalf("activation reason = %q, want %q with timestamp", gotReason, reason)
			}
		})
	}
}

func TestCartesiaSTTTurnMapping(t *testing.T) {
	events := []string{
		`{"type":"connected"}`, `{"type":"turn.start"}`,
		`{"type":"turn.update","transcript":"  "}`,
		`{"type":"turn.update","transcript":"मुझे plan"}`,
		`{"type":"turn.update","transcript":"मुझे plan 21 चाहिए। "}`,
		`{"type":"turn.eager_end","transcript":"ignored"}`,
		`{"type":"turn.resume"}`,
		`{"type":"turn.end","transcript":"मुझे plan 21 चाहिए। "}`,
		`{"type":"turn.update","transcript":"unfinished"}`,
		`{"type":"turn.end","transcript":""}`,
		`{"type":"turn.end","transcript":" \t "}`,
	}
	serverURL, _, _ := cartesiaSTTServer(t, func(conn *websocket.Conn, _ *http.Request, _ int32) bool {
		for _, event := range events {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
				return false
			}
		}
		return true
	})
	configureCartesiaSTTTest(t, serverURL, time.Hour, time.Hour)
	fix := newTestFixture(t)
	p, source, sink := startCartesiaSTTTest(t, fix)
	p.QueueFrame(NewSTTConnectFrame("user_joined", time.Now()), Downstream)
	waitForCartesiaFrameCount(t, sink.Captured, 7)
	stopProcessorsAndWait(t, fix, time.Second, source, p, sink)
	frames := sink.Captured()
	wantText := []string{"मुझे plan", "मुझे plan 21 चाहिए। ", "मुझे plan 21 चाहिए। ", "<end>", "unfinished", "<end>", "<end>"}
	wantIDs := []int{1, 2, 3, 3, 4, 5, 6}
	if len(frames) != len(wantText) || len(source.Captured()) != 0 {
		t.Fatalf("unexpected frames: down=%s up=%s", describeFrameTypes(frames), describeFrameTypes(source.Captured()))
	}
	a := NewUserContextAggregator(fix.TaskCtx, []Message{{Role: "system", Content: "test"}}, "")
	for i, frame := range frames {
		f, ok := frame.(TranscriptFrame)
		if !ok || f.Text != wantText[i] || f.ResponseID != wantIDs[i] || f.Finished || f.IsFinal != (i != 0 && i != 1 && i != 4) {
			t.Fatalf("frame[%d] has incorrect transcript mapping", i)
		}
		a.ProcessFrame(context.Background(), frame, Downstream)
		if i == 1 && a.interimTranscript != wantText[i] {
			t.Fatal("cumulative update appended instead of replacing interim snapshot")
		}
	}
	if a.interimTranscript != "" || a.currentTranscript != "" {
		t.Fatal("empty turn.end did not clear transcripts")
	}
	if got := a.messagesForTest(); len(got) != 2 || got[1].Content != wantText[2] {
		t.Fatal("final was normalized, duplicated, or empty turn.end submitted a user turn")
	}
}

func TestCartesiaSTTProviderError(t *testing.T) {
	serverURL, _, _ := cartesiaSTTServer(t, func(conn *websocket.Conn, _ *http.Request, _ int32) bool {
		return conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status_code":429,"error_code":"rate_limit_exceeded","title":"Too many requests","message":"Capacity exceeded"}`)) == nil
	})
	configureCartesiaSTTTest(t, serverURL, time.Hour, time.Hour)
	fix := newTestFixture(t)
	transport := attachMockSentryHub(t, fix)
	fix.TaskCtx.sentryHub.Scope().SetTag("task-test", "cartesia-call")
	p, source, sink := startCartesiaSTTTest(t, fix)
	p.QueueFrame(NewSTTConnectFrame("user_joined", time.Now()), Downstream)
	f := waitForFrameType[ErrorFrame](t, source.Captured, time.Second)
	stopProcessorsAndWait(t, fix, time.Second, source, p, sink)
	if f.Fatal || f.Err != "Error: rate_limit_exceeded (_receive_messages) - Capacity exceeded" {
		t.Fatalf("unexpected ErrorFrame: %#v", f)
	}
	if len(source.Captured()) != 1 || len(sink.Captured()) != 0 {
		t.Fatal("expected exactly one upstream error and no downstream frames")
	}
	events := transport.Events()
	if len(events) != 1 || events[0].Tags["provider"] != "cartesia" || events[0].Tags["task-test"] != "cartesia-call" {
		t.Fatal("provider error did not use the task-scoped Sentry hub")
	}
}

func TestCartesiaSTTProviderErrorLimitAndRedaction(t *testing.T) {
	t.Setenv("CARTESIA_API_KEY", "fake-credential-for-redaction")
	fix := newTestFixture(t)
	transport := attachMockSentryHub(t, fix)
	p := NewCartesiaSTTProcessor(fix.TaskCtx)
	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	source.Link(p)
	source.Start(fix.RootCtx)
	t.Cleanup(func() { stopProcessorsAndWait(t, fix, time.Second, source, p) })
	for i := 0; i < 102; i++ {
		p.reportProviderError(cartesiaSTTMessage{StatusCode: 400, Message: "reflected fake-credential-for-redaction"})
	}
	if len(transport.Events()) != 100 {
		t.Fatal("expected exactly 100 reports for the same error")
	}
	frames := waitForCartesiaFrameCount(t, source.Captured, 100)
	if len(frames) != 100 {
		t.Fatal("provider ErrorFrames must share Sentry's rate limit")
	}
	for _, frame := range frames {
		f, ok := frame.(ErrorFrame)
		if !ok || f.Fatal || f.Err != "Error: 400 (_receive_messages) - reflected [REDACTED]" {
			t.Fatal("provider ErrorFrame must be nonfatal, use status fallback, and redact credentials")
		}
	}
	encoded, err := json.Marshal(transport.Events())
	if err != nil || bytes.Contains(encoded, []byte("fake-credential-for-redaction")) || !bytes.Contains(encoded, []byte("[REDACTED]")) {
		t.Fatal("Sentry event did not redact a reflected credential")
	}
	now := time.Now()
	for i := 0; i < 100; i++ {
		if !p.shouldReportProviderError("window-test", now) {
			t.Fatal("rate limited before 100 errors")
		}
	}
	if p.shouldReportProviderError("window-test", now.Add(time.Minute-time.Nanosecond)) || !p.shouldReportProviderError("distinct", now) || !p.shouldReportProviderError("window-test", now.Add(time.Minute)) {
		t.Fatal("rate limit must be per distinct error and reset after one minute")
	}
}

func TestCartesiaSTTIdleSilenceAndAudioReset(t *testing.T) {
	serverURL, connected, messages := cartesiaSTTServer(t, nil)
	configureCartesiaSTTTest(t, serverURL, 10*time.Millisecond, 200*time.Millisecond)
	fix := newTestFixture(t)
	p, _, sink := startCartesiaSTTTest(t, fix)
	p.QueueFrame(NewSTTConnectFrame("user_joined", time.Now()), Downstream)
	waitForSTTConnection(t, connected, 1)
	first := waitForSTTWireMessage(t, messages)
	if first.messageType != websocket.BinaryMessage || first.payload != string(make([]byte, 3200)) {
		t.Fatal("idle keepalive must be 3200 zero PCM bytes, sent as binary")
	}
	// Silence itself resets the idle clock.
	select {
	case <-messages:
		t.Fatal("silence did not reset the idle clock")
	case <-time.After(80 * time.Millisecond):
	}
	audio := []byte{1, 0, 2, 0}
	p.QueueFrame(NewAudioFrame(audio), Downstream)
	if got := waitForSTTWireMessage(t, messages); got.payload != string(audio) || got.messageType != websocket.BinaryMessage {
		t.Fatal("expected the real audio frame")
	}
	// Without the audio reset the next silence would arrive in ~120ms.
	select {
	case <-messages:
		t.Fatal("audio did not reset the idle clock")
	case <-time.After(150 * time.Millisecond):
	}
	if got := waitForSTTWireMessage(t, messages); got.messageType != websocket.BinaryMessage || got.payload != string(make([]byte, 3200)) {
		t.Fatal("expected another silence frame after audio idles")
	}
	p.QueueFrame(NewEndFrame("done"), Downstream)
	waitForFrameType[EndFrame](t, sink.Captured, time.Second)
	if got := waitForSTTWireMessage(t, messages); got.messageType != websocket.TextMessage || got.payload != cartesiaSTTCloseMessage {
		t.Fatal("EndFrame must send the Auto close command")
	}
	if p.currentWebsocketConn() != nil {
		t.Fatal("EndFrame forwarded before closing websocket")
	}
	select {
	case <-messages:
		t.Fatal("writer sent audio/silence after close")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestCartesiaSTTReconnect(t *testing.T) {
	for _, code := range []int{websocket.CloseGoingAway, websocket.CloseInternalServerErr, 0} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			serverURL, _, messages := cartesiaSTTServer(t, func(conn *websocket.Conn, _ *http.Request, n int32) bool {
				if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"turn.update","transcript":"snapshot"}`)); err != nil {
					return false
				}
				if n == 1 {
					if code != 0 {
						_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, "Idle timeout"), time.Now().Add(time.Second))
					}
					return false
				}
				return true
			})
			configureCartesiaSTTTest(t, serverURL, 10*time.Millisecond, 5*time.Millisecond)
			fix := newTestFixture(t)
			transport := attachMockSentryHub(t, fix)
			p, source, sink := startCartesiaSTTTest(t, fix)
			p.QueueFrame(NewSTTConnectFrame("user_joined", time.Now()), Downstream)
			frames := waitForCartesiaFrameCount(t, sink.Captured, 2)
			if frames[0].(TranscriptFrame).ResponseID == frames[1].(TranscriptFrame).ResponseID {
				t.Fatal("response IDs must remain unique after reconnect")
			}
			for {
				if got := waitForSTTWireMessage(t, messages); got.connection == 2 {
					if got.messageType != websocket.BinaryMessage || len(got.payload) != 3200 {
						t.Fatal("writer did not keep the replacement connection alive")
					}
					break
				}
			}
			stopProcessorsAndWait(t, fix, time.Second, source, p, sink)
			if len(transport.Events()) != 0 || len(source.Captured()) != 0 {
				t.Fatal("recovered transport close must not report to Sentry or emit ErrorFrame")
			}
		})
	}
}

func TestCartesiaSTTConnectRetriesExhausted(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)
	configureCartesiaSTTTest(t, server.URL, time.Hour, time.Hour)
	fix := newTestFixture(t)
	transport := attachMockSentryHub(t, fix)
	p, source, sink := startCartesiaSTTTest(t, fix)
	p.QueueFrame(NewSTTConnectFrame("user_joined", time.Now()), Downstream)
	f := waitForFrameType[ErrorFrame](t, source.Captured, time.Second)
	if !f.Fatal || !strings.Contains(f.Err, "Cartesia connection failed after 3 attempts") || attempts.Load() != 3 {
		t.Fatal("expected fatal error after exactly three dial attempts")
	}
	p.QueueFrame(NewEndFrame("error"), Downstream)
	waitForFrameType[EndFrame](t, sink.Captured, time.Second)
	stopProcessorsAndWait(t, fix, time.Second, source, p, sink)
	if len(source.Captured()) != 1 || len(transport.Events()) != 1 {
		t.Fatal("dial exhaustion must report exactly once")
	}
}

func TestCartesiaSTTParentCancellationClosesSocket(t *testing.T) {
	serverURL, connected, _ := cartesiaSTTServer(t, nil)
	configureCartesiaSTTTest(t, serverURL, time.Hour, time.Hour)
	fix := newTestFixture(t)
	p, _, _ := startCartesiaSTTTest(t, fix)
	p.QueueFrame(NewSTTConnectFrame("user_joined", time.Now()), Downstream)
	waitForSTTConnection(t, connected, 1)
	fix.RootCancel()
	if err := waitForWG(fix.WG, time.Second); err != nil {
		t.Fatal("parent cancellation left reader blocked")
	}
	if p.currentWebsocketConn() != nil {
		t.Fatal("parent cancellation left socket open")
	}
}
