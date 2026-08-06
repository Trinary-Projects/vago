package voicepipelinecore

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// STT/TTS unit tests focus on ProcessFrame routing behaviour. The
// websocket connect goroutines run in the background but fail to dial
// (the test_setup_test.go init redirects sttDialURL/ttsDialURL to an
// unreachable URL). They exit cleanly when the test fixture cancels
// taskCtx.Ctx.
//
// Tests that need to exercise actual STT transcription or TTS
// synthesis flow are integration tests (out of scope here).

type sttWireMessage struct {
	connection  int32
	messageType int
	payload     string
}

// TestSTT_ForwardsEndFrame verifies EndFrame propagates downstream and
// stops the STT processor.
func TestSTT_ForwardsEndFrame(t *testing.T) {
	fix := newTestFixture(t)
	p := NewSTTProcessor(fix.TaskCtx)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor:    p,
		framesToSend: []Frame{},
		sendEndFrame: true,
		timeout:      3 * time.Second,
	})

	// EndFrame is stripped from down by the helper; we just need the
	// test to complete without timing out.
	if len(down) != 0 {
		t.Errorf("expected no downstream frames besides EndFrame, got %s", describeFrameTypes(down))
	}
}

// TestSTT_QueuesAudioFrameToWriter verifies AudioFrames are forwarded
// to the internal writer channel (which would write them to the
// Soniox websocket if connected).
func TestSTT_QueuesAudioFrameToWriter(t *testing.T) {
	fix := newTestFixture(t)
	p := NewSTTProcessor(fix.TaskCtx)

	// Send an AudioFrame; STT should consume it (not forward downstream).
	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			AudioFrame{Data: []byte{0, 0, 0, 0}},
		},
		settleDelay:  50 * time.Millisecond,
		sendEndFrame: true,
	})

	// AudioFrame should NOT appear downstream — STT consumes it.
	if c := countFrames[AudioFrame](down); c != 0 {
		t.Errorf("AudioFrame should be consumed by STT, not forwarded; got %d downstream", c)
	}
}

// TestSTT_PassesThroughOtherFrames verifies frames STT doesn't
// specifically handle pass through.
func TestSTT_PassesThroughOtherFrames(t *testing.T) {
	fix := newTestFixture(t)
	p := NewSTTProcessor(fix.TaskCtx)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			TextFrame{Text: "passthrough"},
		},
		sendEndFrame: true,
	})

	if c := countFrames[TextFrame](down); c != 1 {
		t.Errorf("expected TextFrame to pass through, got %d in %s", c, describeFrameTypes(down))
	}
}

func TestSTT_ProviderErrorPreservesTokensAndReportsOnce(t *testing.T) {
	errorMessage := "Organization balance exhausted. Top up your account to continue using Soniox."
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var config map[string]any
		if err := conn.ReadJSON(&config); err != nil {
			return
		}
		response := map[string]any{
			"tokens": []map[string]any{{
				"text":     "hello",
				"is_final": true,
			}},
			"finished":      true,
			"error_code":    402,
			"error_message": errorMessage,
		}
		for i := 0; i < sttProviderErrorReportLimit+2; i++ {
			if err := conn.WriteJSON(response); err != nil {
				return
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)

	originalURL := sttDialURL
	sttDialURL = "ws" + strings.TrimPrefix(server.URL, "http")
	t.Cleanup(func() { sttDialURL = originalURL })

	fix := newTestFixture(t)
	transport := attachMockSentryHub(t, fix)
	p := NewSTTProcessor(fix.TaskCtx)
	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(NewSTTConnectFrame("test", time.Now()), Downstream)
	transcript := waitForFrameType[TranscriptFrame](t, sink.Captured, 2*time.Second)
	errorFrame := waitForFrameType[ErrorFrame](t, source.Captured, 2*time.Second)

	wantError := "Error: 402 (_receive_messages) - " + errorMessage
	if transcript.Text != "hello" || !transcript.IsFinal {
		t.Fatalf("transcript = %#v, want final hello token", transcript)
	}
	if errorFrame.Err != wantError || errorFrame.Fatal || errorFrame.Processor != "STT" {
		t.Fatalf("ErrorFrame = %#v, want non-fatal STT error %q", errorFrame, wantError)
	}

	deadline := time.Now().Add(2 * time.Second)
	for countFrames[TranscriptFrame](sink.Captured()) < sttProviderErrorReportLimit+2 ||
		countFrames[ErrorFrame](source.Captured()) < sttProviderErrorReportLimit {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for repeated Soniox responses")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := countFrames[ErrorFrame](source.Captured()); got != sttProviderErrorReportLimit {
		t.Fatalf("ErrorFrame count = %d, want rate-limited count %d", got, sttProviderErrorReportLimit)
	}
	events := transport.Events()
	if len(events) != sttProviderErrorReportLimit {
		t.Fatalf("Sentry event count = %d, want rate-limited count %d", len(events), sttProviderErrorReportLimit)
	}
	if events[0].Message != "STT error: "+wantError {
		t.Errorf("Sentry message = %q, want %q", events[0].Message, "STT error: "+wantError)
	}
	if events[0].Tags["provider"] != "soniox" || events[0].Tags["error_code"] != "402" {
		t.Errorf("Sentry tags = %v, want Soniox 402 tags", events[0].Tags)
	}

	stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink)
}

func TestSTT_ProviderErrorRateLimitResetsAfterWindow(t *testing.T) {
	p := NewSTTProcessor(newTestFixture(t).TaskCtx)
	startedAt := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	for i := 0; i < sttProviderErrorReportLimit; i++ {
		if !p.shouldReportProviderError("same-error", startedAt) {
			t.Fatalf("occurrence %d unexpectedly suppressed", i+1)
		}
	}
	if p.shouldReportProviderError("same-error", startedAt.Add(sttProviderErrorReportWindow-time.Nanosecond)) {
		t.Fatal("occurrence above the fixed-window limit was reported")
	}
	if !p.shouldReportProviderError("same-error", startedAt.Add(sttProviderErrorReportWindow)) {
		t.Fatal("first occurrence in the next fixed window was suppressed")
	}
}

func TestSTT_SendsSonioxKeepaliveWhenAudioIsIdle(t *testing.T) {
	messages := make(chan sttWireMessage, 10)
	connected := make(chan int32, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var config map[string]any
		if err := conn.ReadJSON(&config); err != nil {
			return
		}
		connected <- 1
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			messages <- sttWireMessage{connection: 1, messageType: messageType, payload: string(payload)}
		}
	}))
	t.Cleanup(server.Close)
	configureSTTWireTest(t, server.URL, 10*time.Millisecond, 5*time.Millisecond)

	fix := newTestFixture(t)
	p := NewSTTProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)
	p.QueueFrame(NewSTTConnectFrame("test", time.Now()), Downstream)
	waitForSTTConnection(t, connected, 1)

	message := waitForSTTWireMessage(t, messages)
	if message.messageType != websocket.TextMessage || message.payload != sttKeepaliveMessage {
		t.Fatalf("idle STT message = %#v, want text keepalive %q", message, sttKeepaliveMessage)
	}

	stopSTTWireTest(t, fix, p)
}

func TestSTT_AudioResetsSonioxKeepaliveIdleTimer(t *testing.T) {
	messages := make(chan sttWireMessage, 20)
	connected := make(chan int32, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var config map[string]any
		if err := conn.ReadJSON(&config); err != nil {
			return
		}
		connected <- 1
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			messages <- sttWireMessage{connection: 1, messageType: messageType, payload: string(payload)}
		}
	}))
	t.Cleanup(server.Close)
	configureSTTWireTest(t, server.URL, 5*time.Millisecond, 25*time.Millisecond)

	fix := newTestFixture(t)
	p := NewSTTProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)
	p.QueueFrame(NewSTTConnectFrame("test", time.Now()), Downstream)
	waitForSTTConnection(t, connected, 1)

	for i := 0; i < 5; i++ {
		p.QueueFrame(NewAudioFrame([]byte{byte(i), 0}), Downstream)
		message := waitForSTTWireMessage(t, messages)
		if message.messageType != websocket.BinaryMessage {
			t.Fatalf("message while audio is active = %#v, want binary audio", message)
		}
		time.Sleep(5 * time.Millisecond)
	}

	message := waitForSTTWireMessage(t, messages)
	if message.messageType != websocket.TextMessage || message.payload != sttKeepaliveMessage {
		t.Fatalf("first message after audio became idle = %#v, want text keepalive %q", message, sttKeepaliveMessage)
	}

	stopSTTWireTest(t, fix, p)
}

func TestSTT_SonioxKeepaliveContinuesAfterReconnect(t *testing.T) {
	messages := make(chan sttWireMessage, 20)
	connected := make(chan int32, 2)
	var connectionCount atomic.Int32
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connection := connectionCount.Add(1)
		var config map[string]any
		if err := conn.ReadJSON(&config); err != nil {
			return
		}
		connected <- connection
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			messages <- sttWireMessage{connection: connection, messageType: messageType, payload: string(payload)}
			if connection == 1 && messageType == websocket.TextMessage && string(payload) == sttKeepaliveMessage {
				_ = conn.WriteJSON(map[string]any{
					"tokens":        []any{},
					"error_code":    408,
					"error_message": "Request timeout.",
				})
				_ = conn.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
					time.Now().Add(time.Second),
				)
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	configureSTTWireTest(t, server.URL, 10*time.Millisecond, 5*time.Millisecond)

	fix := newTestFixture(t)
	p := NewSTTProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)
	p.QueueFrame(NewSTTConnectFrame("test", time.Now()), Downstream)
	waitForSTTConnection(t, connected, 1)

	first := waitForSTTWireMessage(t, messages)
	if first.connection != 1 || first.messageType != websocket.TextMessage || first.payload != sttKeepaliveMessage {
		t.Fatalf("first connection message = %#v, want Soniox keepalive", first)
	}
	waitForSTTConnection(t, connected, 2)
	second := waitForSTTWireMessage(t, messages)
	if second.connection != 2 || second.messageType != websocket.TextMessage || second.payload != sttKeepaliveMessage {
		t.Fatalf("reconnected message = %#v, want Soniox keepalive", second)
	}

	stopSTTWireTest(t, fix, p)
}

func TestSTT_StopCancelsSonioxKeepalive(t *testing.T) {
	messages := make(chan sttWireMessage, 10)
	connected := make(chan int32, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var config map[string]any
		if err := conn.ReadJSON(&config); err != nil {
			return
		}
		connected <- 1
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			messages <- sttWireMessage{connection: 1, messageType: messageType, payload: string(payload)}
		}
	}))
	t.Cleanup(server.Close)
	configureSTTWireTest(t, server.URL, 10*time.Millisecond, time.Second)

	fix := newTestFixture(t)
	p := NewSTTProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)
	p.QueueFrame(NewSTTConnectFrame("test", time.Now()), Downstream)
	waitForSTTConnection(t, connected, 1)
	stopSTTWireTest(t, fix, p)

	select {
	case message := <-messages:
		t.Fatalf("STT wrote after Stop: %#v", message)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSTTConnectCapsRetries(t *testing.T) {
	fix := newTestFixture(t)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)

	oldURL := sttDialURL
	oldDelays := sttConnectRetryDelays
	sttDialURL = "ws" + strings.TrimPrefix(server.URL, "http")
	sttConnectRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() {
		sttDialURL = oldURL
		sttConnectRetryDelays = oldDelays
	})

	p := NewSTTProcessor(fix.TaskCtx)
	err := p.connect()
	if err == nil {
		t.Fatal("connect returned nil, want exhausted retry error")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("connect attempts = %d, want 3", got)
	}
}

func TestSTTLazyConnectWaitsForActivation(t *testing.T) {
	fix := newTestFixture(t)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)

	oldURL := sttDialURL
	sttDialURL = "ws" + strings.TrimPrefix(server.URL, "http")
	t.Cleanup(func() { sttDialURL = oldURL })

	p := NewSTTProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)
	time.Sleep(50 * time.Millisecond)

	if got := attempts.Load(); got != 0 {
		t.Fatalf("connect attempts before activation = %d, want 0", got)
	}
	p.Stop()
	if err := waitForWG(fix.WG, time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}

func TestSTTLazyConnectActivatesOnSTTConnectFrame(t *testing.T) {
	fix := newTestFixture(t)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)

	oldURL := sttDialURL
	oldDelays := sttConnectRetryDelays
	sttDialURL = "ws" + strings.TrimPrefix(server.URL, "http")
	sttConnectRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() {
		sttDialURL = oldURL
		sttConnectRetryDelays = oldDelays
	})

	p := NewSTTProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)
	p.QueueFrame(NewSTTConnectFrame("user_joined", time.Now()), Downstream)

	if !waitForAttempts(&attempts, 500*time.Millisecond) {
		t.Fatal("STT did not attempt to connect after STTConnectFrame")
	}
	p.Stop()
	if err := waitForWG(fix.WG, time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}

func TestSTTLazyConnectFallsBackOnFirstAudio(t *testing.T) {
	fix := newTestFixture(t)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)

	oldURL := sttDialURL
	oldDelays := sttConnectRetryDelays
	sttDialURL = "ws" + strings.TrimPrefix(server.URL, "http")
	sttConnectRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	t.Cleanup(func() {
		sttDialURL = oldURL
		sttConnectRetryDelays = oldDelays
	})

	p := NewSTTProcessor(fix.TaskCtx)
	p.Start(fix.RootCtx)
	p.QueueFrame(NewAudioFrame([]byte{0, 0, 1, 0}), Downstream)

	if !waitForAttempts(&attempts, 500*time.Millisecond) {
		t.Fatal("STT did not attempt to connect after first audio fallback")
	}
	p.Stop()
	if err := waitForWG(fix.WG, time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}

func TestRoomUserJoinedQueuesSTTConnectFrame(t *testing.T) {
	fix := newTestFixture(t)
	dailyAudio := NewAudioSourceProcessor(fix.TaskCtx)

	daily := &DailyRoom{
		roomName:    "daily-room",
		taskCtx:     fix.TaskCtx,
		audioSource: dailyAudio,
	}
	daily.markUserJoined("daily-user")
	assertQueuedSTTConnectFrame(t, dailyAudio, "user_joined")

	liveKitAudio := NewAudioSourceProcessor(fix.TaskCtx)
	liveKit := &LiveKitRoom{
		roomName:    "livekit-room",
		taskCtx:     fix.TaskCtx,
		audioSource: liveKitAudio,
	}
	liveKit.markUserJoined("livekit-user")
	assertQueuedSTTConnectFrame(t, liveKitAudio, "user_joined")
}

func assertQueuedSTTConnectFrame(t *testing.T, audioSource *AudioSourceProcessor, wantReason string) {
	t.Helper()
	select {
	case env := <-audioSource.inputSysCh:
		if env.Direction != Downstream {
			t.Fatalf("STTConnectFrame direction = %v, want Downstream", env.Direction)
		}
		frame, ok := env.Frame.(STTConnectFrame)
		if !ok {
			t.Fatalf("queued system frame = %T, want STTConnectFrame", env.Frame)
		}
		if frame.Reason != wantReason {
			t.Fatalf("STTConnectFrame reason = %q, want %q", frame.Reason, wantReason)
		}
		if frame.At.IsZero() {
			t.Fatal("STTConnectFrame At was zero")
		}
	case <-time.After(time.Second):
		t.Fatal("room user join did not queue STTConnectFrame")
	}
}

func waitForAttempts(attempts *atomic.Int32, timeout time.Duration) bool {
	deadline := time.After(timeout)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if attempts.Load() > 0 {
			return true
		}
		select {
		case <-deadline:
			return attempts.Load() > 0
		case <-ticker.C:
		}
	}
}

func configureSTTWireTest(t *testing.T, serverURL string, interval, idleTimeout time.Duration) {
	t.Helper()
	oldURL := sttDialURL
	oldInterval := sttKeepaliveInterval
	oldIdleTimeout := sttKeepaliveIdleTimeout
	sttDialURL = "ws" + strings.TrimPrefix(serverURL, "http")
	sttKeepaliveInterval = interval
	sttKeepaliveIdleTimeout = idleTimeout
	t.Cleanup(func() {
		sttDialURL = oldURL
		sttKeepaliveInterval = oldInterval
		sttKeepaliveIdleTimeout = oldIdleTimeout
	})
}

func waitForSTTConnection(t *testing.T, connected <-chan int32, want int32) {
	t.Helper()
	select {
	case got := <-connected:
		if got != want {
			t.Fatalf("STT websocket connection = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for STT websocket connection %d", want)
	}
}

func waitForSTTWireMessage(t *testing.T, messages <-chan sttWireMessage) sttWireMessage {
	t.Helper()
	select {
	case message := <-messages:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for STT websocket message")
		return sttWireMessage{}
	}
}

func stopSTTWireTest(t *testing.T, fix *testFixture, p *STTProcessor) {
	t.Helper()
	p.Stop()
	if err := waitForWG(fix.WG, time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}
