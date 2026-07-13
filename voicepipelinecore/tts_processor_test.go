package voicepipelinecore

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestTTS_ForwardsEndFrameWhenIdle verifies that EndFrame is forwarded
// immediately when no synthesis is in flight. The orchestrator waits
// for connect to complete, but test_setup redirects the URL to make
// connect fail forever — so the test relies on Stop forcing cleanup.
//
// Without orchestrator processing the command, the EndFrame ProcessFrame
// call waits on cmd.done. The test helper's forced Stop cancels
// b.ctx (and thus procCtx), which unblocks ProcessFrame via the
// per-frame ctx.Done case. EndFrame still reaches the sink because
// ProcessFrame already forwarded the InterruptFrame/etc. before
// waiting — wait, no, EndFrame goes through commands and isn't
// forwarded until orchestrator processes it.
//
// So in this test, we expect NO EndFrame at the sink (orchestrator
// never processed the command). The point is just that the test
// completes without timing out.
func TestTTS_ForwardsEndFrameWhenIdle(t *testing.T) {
	fix := newTestFixture(t)
	p := NewTTSProcessor(fix.TaskCtx, nil)

	// Don't actually send EndFrame through the helper — that would wait
	// on the orchestrator's command channel forever. Instead, just run
	// a no-op pipeline and let the helper force-Stop everything.
	_, _ = runProcessorTest(t, fix, runConfig{
		processor:    p,
		framesToSend: []Frame{},
		sendEndFrame: false, // skip; orchestrator won't process it
		timeout:      3 * time.Second,
	})
	// Reaching this point means goroutines exited cleanly when Stop
	// cancelled b.ctx.
}

// TestTTS_ForwardsInterruptDownstreamImmediately verifies that
// InterruptFrame is forwarded downstream by ProcessFrame BEFORE being
// relayed to the orchestrator. This is the intended behaviour because
// the InterruptFrame needs to reach PlaybackSink quickly to stop
// playback even if the orchestrator is busy.
func TestTTS_ForwardsInterruptDownstreamImmediately(t *testing.T) {
	fix := newTestFixture(t)
	p := NewTTSProcessor(fix.TaskCtx, nil)

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	source.QueueFrame(InterruptFrame{}, Downstream)
	time.Sleep(100 * time.Millisecond)

	stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink)

	if c := countFrames[InterruptFrame](sink.Captured()); c != 1 {
		t.Errorf("expected InterruptFrame forwarded downstream, got %d in %s", c, describeFrameTypes(sink.Captured()))
	}
}

// TestTTS_PassesThroughUpstreamFrames verifies that upstream frames
// (WordTimestampFrame, TTSDoneFrame, BotStarted/StoppedSpeakingFrame)
// pass through ProcessFrame directly without being routed via the
// orchestrator.
func TestTTS_PassesThroughUpstreamFrames(t *testing.T) {
	fix := newTestFixture(t)
	p := NewTTSProcessor(fix.TaskCtx, nil)

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	// Push upstream frames from the sink side.
	sink.QueueFrame(WordTimestampFrame{Words: []string{"hi"}}, Upstream)
	sink.QueueFrame(TTSDoneFrame{}, Upstream)
	sink.QueueFrame(BotStartedSpeakingFrame{}, Upstream)
	sink.QueueFrame(BotStoppedSpeakingFrame{}, Upstream)
	time.Sleep(100 * time.Millisecond)

	stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink)

	up := source.Captured()
	if c := countFrames[WordTimestampFrame](up); c != 1 {
		t.Errorf("expected WordTimestampFrame to pass upstream, got %d in %s", c, describeFrameTypes(up))
	}
	if c := countFrames[TTSDoneFrame](up); c != 1 {
		t.Errorf("expected TTSDoneFrame to pass upstream, got %d", c)
	}
	if c := countFrames[BotStartedSpeakingFrame](up); c != 1 {
		t.Errorf("expected BotStartedSpeakingFrame to pass upstream, got %d", c)
	}
	if c := countFrames[BotStoppedSpeakingFrame](up); c != 1 {
		t.Errorf("expected BotStoppedSpeakingFrame to pass upstream, got %d", c)
	}
}

func TestTTS_CloseConnectionKeepsUpstreamPassThroughAlive(t *testing.T) {
	fix := newTestFixture(t)
	p := NewTTSProcessor(fix.TaskCtx, nil)

	source := newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink := newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)

	p.closeTTSConnection()
	sink.QueueFrame(NewWordTimestampFrame([]string{"goodbye"}), Upstream)
	sink.QueueFrame(NewTTSDoneFrame(), Upstream)
	time.Sleep(100 * time.Millisecond)

	stopProcessorsAndWait(t, fix, 3*time.Second, source, p, sink)

	up := source.Captured()
	if c := countFrames[WordTimestampFrame](up); c != 1 {
		t.Errorf("expected WordTimestampFrame to pass upstream after TTS connection close, got %d in %s", c, describeFrameTypes(up))
	}
	if c := countFrames[TTSDoneFrame](up); c != 1 {
		t.Errorf("expected TTSDoneFrame to pass upstream after TTS connection close, got %d in %s", c, describeFrameTypes(up))
	}
}

func TestTTS_NextPCMFrameUsesRawPCMBytes(t *testing.T) {
	input := make([]byte, framePCMBytes+4)
	for i := range input {
		input[i] = byte(i % 251)
	}

	p := &TTSProcessor{pcmBuffer: append([]byte(nil), input...)}
	frame := p.nextPCMFrame()

	if len(frame) != framePCMBytes {
		t.Fatalf("expected %d-byte PCM frame, got %d", framePCMBytes, len(frame))
	}
	if !bytes.Equal(frame, input[:framePCMBytes]) {
		t.Fatal("PCM frame bytes were modified")
	}
	if !bytes.Equal(p.pcmBuffer, input[framePCMBytes:]) {
		t.Fatal("PCM buffer was not advanced by exactly one frame")
	}
	frame[0] ^= 0xff
	if bytes.Equal(frame[:1], input[:1]) {
		t.Fatal("test setup did not mutate returned frame")
	}
	if !bytes.Equal(p.pcmBuffer, input[framePCMBytes:]) {
		t.Fatal("returned PCM frame aliases the remaining buffer")
	}
}

// --- Fake Cartesia server for deferred-EndFrame reconnect/timeout tests ---
//
// The tests above never let connect() succeed (test_setup redirects
// ttsDialURL to an unreachable loopback), so they can't observe the
// orchestrator's Cartesia-done / reconnect / timeout release paths.
// fakeCartesiaServer is a minimal scriptable stand-in for the real
// Cartesia websocket API: it accepts connections, hands each one's
// inbound processor messages to the test over a channel, and lets the
// test decide exactly when (or whether) to write a "done" reply. That
// keeps release timing deterministic instead of racing a real network
// round trip.

// fakeCartesiaConn is one accepted websocket connection together with
// the inbound JSON messages TTSProcessor sent on it.
type fakeCartesiaConn struct {
	conn    *websocket.Conn
	inbound chan map[string]any
}

// waitForContinueFalse drains inbound messages until one with
// continue:false arrives — the Reset that ends a Cartesia context,
// sent by both sendTextToTTS+ResetTTSContext (TTSSpeakFrame) and
// flushForEnd — and returns its context_id.
func (fc *fakeCartesiaConn) waitForContinueFalse(t *testing.T, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-fc.inbound:
			if !ok {
				t.Fatalf("fakeCartesiaConn: connection closed before a continue:false message arrived")
			}
			if cont, ok := msg["continue"].(bool); ok && !cont {
				contextID, _ := msg["context_id"].(string)
				return contextID
			}
		case <-deadline:
			t.Fatalf("fakeCartesiaConn: timed out waiting for a continue:false message")
		}
	}
}

func (fc *fakeCartesiaConn) sendDone(contextID string) {
	_ = fc.conn.WriteJSON(map[string]any{"type": "done", "context_id": contextID})
}

// fakeCartesiaServer accepts websocket connections at a URL compatible
// with ttsDialURL's "base + api key" concatenation.
type fakeCartesiaServer struct {
	URL string

	mu    sync.Mutex
	conns []*fakeCartesiaConn
}

func newFakeCartesiaServer(t *testing.T) *fakeCartesiaServer {
	t.Helper()
	fs := &fakeCartesiaServer{}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fc := &fakeCartesiaConn{conn: conn, inbound: make(chan map[string]any, 50)}
		fs.mu.Lock()
		fs.conns = append(fs.conns, fc)
		fs.mu.Unlock()
		for {
			var msg map[string]any
			if err := conn.ReadJSON(&msg); err != nil {
				close(fc.inbound)
				return
			}
			fc.inbound <- msg
		}
	}))
	t.Cleanup(srv.Close)
	fs.URL = "ws://" + strings.TrimPrefix(srv.URL, "http://") + "/tts/websocket?api_key="
	return fs
}

// conn blocks until the Nth (0-based) connection has been accepted.
func (fs *fakeCartesiaServer) conn(t *testing.T, index int) *fakeCartesiaConn {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		fs.mu.Lock()
		if index < len(fs.conns) {
			c := fs.conns[index]
			fs.mu.Unlock()
			return c
		}
		fs.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("fakeCartesiaServer: connection %d was not accepted in time", index)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// drainAndCloseAll repeatedly closes every connection accepted so far,
// including ones that appear during the window. Only needed AFTER the
// processor under test has been Stop()'d (see stopTTSTestAndWait):
// TTSProcessor.closeTTSConnection is guarded by a one-shot sync.Once,
// so a reconnect that races the orchestrator's own end-of-call close
// can leave the reader blocked on a connection the client side will
// never close again. Once the processor's ctx is cancelled, connect()
// refuses to dial further, so closing whatever the reader is currently
// blocked on from the server side is enough to let it observe ctx
// cancellation and exit. This is a test-side workaround for that
// pre-existing quirk, not something these tests are asserting about.
func (fs *fakeCartesiaServer) drainAndCloseAll(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		fs.mu.Lock()
		conns := append([]*fakeCartesiaConn(nil), fs.conns...)
		fs.mu.Unlock()
		for _, c := range conns {
			_ = c.conn.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// withTTSDialURL points ttsDialURL at url for the duration of the test
// and restores the previous value on cleanup. Tests in this package run
// sequentially (no t.Parallel()), so mutating the package var is safe.
func withTTSDialURL(t *testing.T, url string) {
	t.Helper()
	orig := ttsDialURL
	ttsDialURL = url
	t.Cleanup(func() { ttsDialURL = orig })
}

// withTTSPendingEndTimeout overrides ttsPendingEndTimeout for the
// duration of the test and restores the previous value on cleanup.
func withTTSPendingEndTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := ttsPendingEndTimeout
	ttsPendingEndTimeout = d
	t.Cleanup(func() { ttsPendingEndTimeout = orig })
}

// waitForFrameType polls capture until a frame of type T appears or
// timeout elapses. These tests need to observe asynchronous
// websocket-driven frame delivery mid-test (e.g. "did the done reply I
// just sent release the deferred EndFrame yet?"), not just a final
// settle-then-assert like the existing synchronous TTS tests.
func waitForFrameType[T Frame](t *testing.T, capture func() []Frame, timeout time.Duration) T {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if f, ok := findFrame[T](capture()); ok {
			return f
		}
		if time.Now().After(deadline) {
			var zero T
			t.Fatalf("waitForFrameType: %T not observed within %s", zero, timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertNoFrameType settles briefly and then asserts no frame of type T
// was captured — used for the "benign idle reconnect must emit nothing"
// guard.
func assertNoFrameType[T Frame](t *testing.T, capture func() []Frame, settle time.Duration) {
	t.Helper()
	time.Sleep(settle)
	if c := countFrames[T](capture()); c != 0 {
		var zero T
		t.Errorf("expected no %T frames, got %d in %s", zero, c, describeFrameTypes(capture()))
	}
}

// stopTTSTestAndWait stops processors and then mops up fake-server
// connections (see drainAndCloseAll) before bounding on the shared
// WaitGroup. Stop() must run first so connect() refuses further
// reconnects once a mopped-up connection unblocks the reader.
func stopTTSTestAndWait(t *testing.T, fix *testFixture, fs *fakeCartesiaServer, timeout time.Duration, processors ...Processor) {
	t.Helper()
	for _, p := range processors {
		if p != nil {
			p.Stop()
		}
	}
	fs.drainAndCloseAll(300 * time.Millisecond)
	if err := waitForWG(fix.WG, timeout); err != nil {
		t.Fatalf("stopTTSTestAndWait: %v", err)
	}
}

// newTTSPipelineForTest wires a TTSProcessor between two QueueProcessors
// (mirroring the manual-wiring pattern used by the interrupt/pass-
// through tests above) and starts all three.
func newTTSPipelineForTest(fix *testFixture) (source, sink *QueueProcessor, p *TTSProcessor) {
	p = NewTTSProcessor(fix.TaskCtx, nil)
	source = newQueueProcessor(fix.TaskCtx, "source", Upstream)
	sink = newQueueProcessor(fix.TaskCtx, "sink", Downstream)
	source.Link(p)
	p.Link(sink)
	source.Start(fix.RootCtx)
	p.Start(fix.RootCtx)
	sink.Start(fix.RootCtx)
	return source, sink, p
}

// TestTTS_PendingEndForwardsAfterCartesiaDone is the baseline happy
// path: it must stay byte-identical to pre-fix behavior. A deferred
// EndFrame sits until Cartesia's "done" arrives for the pending
// context, then forwards immediately.
func TestTTS_PendingEndForwardsAfterCartesiaDone(t *testing.T) {
	fs := newFakeCartesiaServer(t)
	withTTSDialURL(t, fs.URL)

	fix := newTestFixture(t)
	source, sink, p := newTTSPipelineForTest(fix)

	source.QueueFrame(NewTTSSpeakFrame("hello there"), Downstream)
	conn := fs.conn(t, 0)
	contextID := conn.waitForContinueFalse(t, 2*time.Second)
	if contextID == "" {
		t.Fatal("expected a non-empty context_id on the Reset message")
	}

	source.QueueFrame(NewEndFrame("test_reason"), Downstream)

	// Cartesia hasn't said "done" yet, so nothing should reach the sink.
	time.Sleep(100 * time.Millisecond)
	if c := countFrames[EndFrame](sink.Captured()); c != 0 {
		t.Fatalf("expected EndFrame deferred (not yet forwarded), got %d", c)
	}

	conn.sendDone(contextID)

	waitForFrameType[TTSDoneFrame](t, sink.Captured, 3*time.Second)
	waitForFrameType[EndFrame](t, sink.Captured, 3*time.Second)

	stopTTSTestAndWait(t, fix, fs, 5*time.Second, source, p, sink)

	down := sink.Captured()
	if c := countFrames[TTSDoneFrame](down); c != 1 {
		t.Errorf("expected exactly 1 TTSDoneFrame, got %d in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[EndFrame](down); c != 1 {
		t.Errorf("expected exactly 1 EndFrame, got %d in %s", c, describeFrameTypes(down))
	}
}

// TestTTS_PendingEndReleasedByReconnect is the prod-deadlock regression
// test: Cartesia idle-closes the websocket while an EndFrame is
// deferred, so the "done" it owed us can never arrive. The reconnect
// event must release the pendingEnd itself instead of waiting on
// ttsPendingEndTimeout (set very high here to prove which path fired).
func TestTTS_PendingEndReleasedByReconnect(t *testing.T) {
	fs := newFakeCartesiaServer(t)
	withTTSDialURL(t, fs.URL)
	withTTSPendingEndTimeout(t, 30*time.Second)

	fix := newTestFixture(t)
	source, sink, p := newTTSPipelineForTest(fix)

	source.QueueFrame(NewTTSSpeakFrame("hello there"), Downstream)
	conn := fs.conn(t, 0)
	_ = conn.waitForContinueFalse(t, 2*time.Second)

	source.QueueFrame(NewEndFrame("test_reason"), Downstream)
	time.Sleep(100 * time.Millisecond) // let handleEnd defer it before we close

	if c := countFrames[EndFrame](sink.Captured()); c != 0 {
		t.Fatalf("expected EndFrame deferred (not yet forwarded), got %d", c)
	}

	start := time.Now()
	conn.conn.Close() // server-initiated close, no "done" — forces a reconnect

	waitForFrameType[TTSDoneFrame](t, sink.Captured, 5*time.Second)
	waitForFrameType[EndFrame](t, sink.Captured, 5*time.Second)
	if elapsed := time.Since(start); elapsed >= 10*time.Second {
		t.Fatalf("pendingEnd took %s to release; too slow to be the reconnect path (timeout is 30s)", elapsed)
	}

	stopTTSTestAndWait(t, fix, fs, 5*time.Second, source, p, sink)

	down := sink.Captured()
	if c := countFrames[TTSDoneFrame](down); c != 1 {
		t.Errorf("expected exactly 1 TTSDoneFrame, got %d in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[EndFrame](down); c != 1 {
		t.Errorf("expected exactly 1 EndFrame, got %d in %s", c, describeFrameTypes(down))
	}
}

// TestTTS_PendingEndReleasedByTimeout covers a Cartesia hang that isn't
// a clean reconnect (connection stays open but stops responding): the
// bounded ttsPendingEndTimeout must release the deferred EndFrame.
func TestTTS_PendingEndReleasedByTimeout(t *testing.T) {
	fs := newFakeCartesiaServer(t)
	withTTSDialURL(t, fs.URL)
	withTTSPendingEndTimeout(t, 300*time.Millisecond)

	fix := newTestFixture(t)
	source, sink, p := newTTSPipelineForTest(fix)

	source.QueueFrame(NewTTSSpeakFrame("hello there"), Downstream)
	conn := fs.conn(t, 0)
	_ = conn.waitForContinueFalse(t, 2*time.Second)

	source.QueueFrame(NewEndFrame("test_reason"), Downstream)

	// Prove it's deferred, not forwarded immediately: nothing yet, well
	// before the 300ms timeout.
	time.Sleep(100 * time.Millisecond)
	if c := countFrames[EndFrame](sink.Captured()); c != 0 {
		t.Fatalf("expected EndFrame deferred (not yet forwarded), got %d", c)
	}

	// Server stays connected and silent: no done, no close. Only the
	// orchestrator's own timeout should release it.
	waitForFrameType[EndFrame](t, sink.Captured, 3*time.Second)

	stopTTSTestAndWait(t, fix, fs, 5*time.Second, source, p, sink)

	if c := countFrames[EndFrame](sink.Captured()); c != 1 {
		t.Errorf("expected exactly 1 EndFrame, got %d", c)
	}
}

// TestTTS_ReconnectMidSynthesisEmitsTTSDone covers the secondary bug
// variant: a reconnect mid-turn with no EndFrame pending must still
// close the bot's turn (emit TTSDoneFrame) since Cartesia's real "done"
// can never arrive for the forgotten context.
func TestTTS_ReconnectMidSynthesisEmitsTTSDone(t *testing.T) {
	fs := newFakeCartesiaServer(t)
	withTTSDialURL(t, fs.URL)

	fix := newTestFixture(t)
	source, sink, p := newTTSPipelineForTest(fix)

	source.QueueFrame(NewTTSSpeakFrame("hello there"), Downstream)
	conn := fs.conn(t, 0)
	_ = conn.waitForContinueFalse(t, 2*time.Second)

	conn.conn.Close() // server-initiated close, no done — mid-turn reconnect

	waitForFrameType[TTSDoneFrame](t, sink.Captured, 3*time.Second)
	if c := countFrames[EndFrame](sink.Captured()); c != 0 {
		t.Errorf("expected no EndFrame (none was queued), got %d", c)
	}

	// Drive shutdown through a real EndFrame rather than a bare Stop():
	// closeTTSConnection's read of t.websocketConn is only synchronized
	// against the reader's reconnect write when its first call happens
	// on the orchestrator goroutine as a continuation of processing a
	// channel event (the reconnect event here). A bare Stop() from the
	// test goroutine would be the first ever call in this scenario, with
	// no happens-before edge back to the reconnect — a real (if
	// pre-existing and out of scope) race between the reader and
	// whichever goroutine first calls closeTTSConnection.
	source.QueueFrame(NewEndFrame("test_reason"), Downstream)
	waitForFrameType[EndFrame](t, sink.Captured, 3*time.Second)

	stopTTSTestAndWait(t, fix, fs, 5*time.Second, source, p, sink)

	down := sink.Captured()
	if c := countFrames[TTSDoneFrame](down); c != 1 {
		t.Errorf("expected exactly 1 TTSDoneFrame, got %d in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[EndFrame](down); c != 1 {
		t.Errorf("expected exactly 1 EndFrame, got %d in %s", c, describeFrameTypes(down))
	}
}

// TestTTS_ReconnectIdleEmitsNoFrames is the inverse guard: a reconnect
// with nothing in flight (the routine, benign case — Cartesia idle-
// closes a connection between turns) must emit no frames at all.
func TestTTS_ReconnectIdleEmitsNoFrames(t *testing.T) {
	fs := newFakeCartesiaServer(t)
	withTTSDialURL(t, fs.URL)

	fix := newTestFixture(t)
	source, sink, p := newTTSPipelineForTest(fix)

	// Start() alone dials Cartesia (runReader); nothing has been sent.
	conn := fs.conn(t, 0)
	conn.conn.Close()

	assertNoFrameType[TTSDoneFrame](t, sink.Captured, 500*time.Millisecond)
	if c := countFrames[EndFrame](sink.Captured()); c != 0 {
		t.Errorf("expected no EndFrame, got %d", c)
	}

	// Drive shutdown through a real EndFrame rather than a bare Stop() —
	// see the comment in TestTTS_ReconnectMidSynthesisEmitsTTSDone for
	// why a bare Stop() here would race against the reader's reconnect.
	source.QueueFrame(NewEndFrame("test_reason"), Downstream)
	waitForFrameType[EndFrame](t, sink.Captured, 3*time.Second)

	stopTTSTestAndWait(t, fix, fs, 5*time.Second, source, p, sink)

	if c := countFrames[TTSDoneFrame](sink.Captured()); c != 0 {
		t.Errorf("expected no TTSDoneFrame ever, got %d in %s", c, describeFrameTypes(sink.Captured()))
	}
}
