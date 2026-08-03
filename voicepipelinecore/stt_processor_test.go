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
