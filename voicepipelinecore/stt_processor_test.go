package voicepipelinecore

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
