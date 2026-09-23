package voicepipelinecore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// Auto turns protocol, matching cmd/ink-stt-probe. Ink preview auto-detects
// language; endpointing uses Cartesia's balanced defaults.
var (
	cartesiaSTTDialURL              = "wss://api.cartesia.ai/stt/turns/websocket"
	cartesiaSTTModel                = "ink-preview"
	cartesiaSTTConnectRetryDelays   = []time.Duration{time.Second, 2 * time.Second}
	cartesiaSTTKeepaliveInterval    = 5 * time.Second
	cartesiaSTTKeepaliveIdleTimeout = time.Second
)

const cartesiaSTTCloseMessage = `{"type":"close"}`

type cartesiaSTTMessage struct {
	Type       string `json:"type"`
	Transcript string `json:"transcript"`
	ErrorCode  string `json:"error_code"`
	StatusCode int    `json:"status_code"`
	Title      string `json:"title"`
	Message    string `json:"message"`
}

// CartesiaSTTProcessor adapts Auto turn events to the existing Soniox frame
// contract. The reader owns dial/publication; the sole writer owns audio,
// silence keepalive, and the best-effort close command on EndFrame.
type CartesiaSTTProcessor struct {
	*BaseProcessor
	taskCtx          *TaskContext
	websocketMu      sync.RWMutex
	websocketConn    *websocket.Conn
	audioFrames      chan AudioFrame
	connected        chan struct{} // closed when the websocket is established
	activateCh       chan struct{}
	endOnce          sync.Once
	endCh            chan struct{}
	writerDone       chan struct{}
	startOnce        sync.Once
	activateOnce     sync.Once
	connectLogOnce   sync.Once
	timingMu         sync.Mutex
	activatedAt      time.Time
	activationReason string
	firstAudioAt     time.Time
	queuedBeforeConn int

	providerErrorWindows map[string]sttProviderErrorWindow
}

func NewCartesiaSTTProcessor(taskCtx *TaskContext) *CartesiaSTTProcessor {
	p := &CartesiaSTTProcessor{
		taskCtx:              taskCtx,
		audioFrames:          make(chan AudioFrame, 100),
		connected:            make(chan struct{}),
		activateCh:           make(chan struct{}),
		endCh:                make(chan struct{}),
		writerDone:           make(chan struct{}),
		providerErrorWindows: make(map[string]sttProviderErrorWindow),
	}
	p.BaseProcessor = NewBaseProcessor("CartesiaSTT", p, taskCtx)
	return p
}

// activate unblocks the Cartesia websocket dial. It is safe to call before
// Start; the reader goroutine will observe the already-closed channel when
// the pipeline starts.
func (s *CartesiaSTTProcessor) activate(reason string, at time.Time) {
	if s == nil {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "unknown"
	}
	s.activateOnce.Do(func() {
		s.timingMu.Lock()
		s.activatedAt = at
		s.activationReason = reason
		s.timingMu.Unlock()
		s.logf("STT websocket activation requested provider=cartesia reason=%s activation_at=%s", reason, at.UTC().Format(time.RFC3339Nano))
		close(s.activateCh)
	})
}

// Cancellation precedes socket close so the reader cannot reconnect during
// shutdown. Close is safe alongside a writer and unblocks both network I/O loops.
func (s *CartesiaSTTProcessor) Stop() {
	s.BaseProcessor.Stop()
	s.closeWebsocket()
}

func (s *CartesiaSTTProcessor) closeWebsocket() {
	s.websocketMu.Lock()
	conn := s.websocketConn
	s.websocketConn = nil
	s.websocketMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (s *CartesiaSTTProcessor) ending() bool {
	select {
	case <-s.endCh:
		return true
	default:
		return s.ctx.Err() != nil
	}
}

func (s *CartesiaSTTProcessor) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		s.BaseProcessor.Start(ctx)
		s.Go(s.runReader)
		s.Go(s.runWriter)
	})
}

func (s *CartesiaSTTProcessor) waitForActivation() bool {
	select {
	case <-s.activateCh:
		return true
	case <-s.ctx.Done():
		return false
	}
}

// Turn-detection settings for the Auto endpoint, set to Cartesia's documented
// "balanced" defaults. These are the full set of turn parameters the endpoint
// accepts; tune the values here.
const (
	// Confidence needed to open a turn (turn.start). Range 0.5-0.9.
	cartesiaSTTTurnStartThreshold = "0.8"
	// Confidence for the speculative turn.eager_end event. Range 0.3-0.6.
	// We ignore eager_end, so this has no effect on our turn-taking today.
	cartesiaSTTTurnEagerEndThreshold = "0.4"
	// Confidence needed to close a turn (turn.end). Range 0.05-0.5.
	cartesiaSTTTurnEndThreshold = "0.2"
	// Silence after which a turn is force-closed. Range 640-11200 ms.
	cartesiaSTTTurnEndTimeoutMs = "5600"
)

func cartesiaSTTQueryParams() url.Values {
	return url.Values{
		"model":                    {cartesiaSTTModel},
		"encoding":                 {"pcm_s16le"},
		"sample_rate":              {"16000"},
		"cartesia_version":         {"2026-08-14"},
		"turn_start_threshold":     {cartesiaSTTTurnStartThreshold},
		"turn_eager_end_threshold": {cartesiaSTTTurnEagerEndThreshold},
		"turn_end_threshold":       {cartesiaSTTTurnEndThreshold},
		"turn_end_timeout_ms":      {cartesiaSTTTurnEndTimeoutMs},
	}
}

// Exactly the probe's Auto handshake: configuration in the query, auth only
// in X-API-Key. Never log headers, handshake bodies, or redirects.
func (s *CartesiaSTTProcessor) connect() error {
	maxAttempts := len(cartesiaSTTConnectRetryDelays) + 1
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if s.ending() {
			return context.Canceled
		}
		headers := http.Header{"X-Api-Key": {os.Getenv("CARTESIA_API_KEY")}}
		conn, response, err := dialer.DialContext(s.ctx, cartesiaSTTDialURL+"?"+cartesiaSTTQueryParams().Encode(), headers)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if err == nil {
			s.websocketMu.Lock()
			if s.ending() {
				s.websocketMu.Unlock()
				_ = conn.Close()
				return context.Canceled
			}
			s.websocketConn = conn
			s.websocketMu.Unlock()
			s.logf("STT websocket connected provider=cartesia")
			s.logInitialConnectLatency(time.Now())
			return nil
		}
		// Sanitise before any log, ErrorFrame, or Sentry path.
		err = fmt.Errorf("%s", s.safeError(err.Error()))
		if attempt == maxAttempts {
			return err
		}
		delay := cartesiaSTTConnectRetryDelays[attempt-1]
		s.logf("STT connect failed provider=cartesia attempt=%d/%d: %v, retrying in %s...", attempt, maxAttempts, err, delay)
		select {
		case <-time.After(delay):
		case <-s.endCh:
			return context.Canceled
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	return nil
}

func (s *CartesiaSTTProcessor) runReader() {
	if !s.waitForActivation() {
		return
	}
	if err := s.connect(); err != nil {
		s.handleConnectExhausted(err)
		return
	}
	close(s.connected)
	s.read()
}

func (s *CartesiaSTTProcessor) logInitialConnectLatency(connectedAt time.Time) {
	s.connectLogOnce.Do(func() {
		s.timingMu.Lock()
		activatedAt := s.activatedAt
		reason := s.activationReason
		firstAudioAt := s.firstAudioAt
		queuedBeforeConn := s.queuedBeforeConn
		s.timingMu.Unlock()

		var activationToConnectedMs float64
		if !activatedAt.IsZero() && connectedAt.After(activatedAt) {
			activationToConnectedMs = float64(connectedAt.Sub(activatedAt).Microseconds()) / 1000.0
		}
		var firstAudioToConnectedMs float64
		if !firstAudioAt.IsZero() && connectedAt.After(firstAudioAt) {
			firstAudioToConnectedMs = float64(connectedAt.Sub(firstAudioAt).Microseconds()) / 1000.0
		}
		s.logf(
			"STT lazy connect latency provider=cartesia activation_reason=%s activation_to_connected_ms=%.1f first_audio_to_connected_ms=%.1f preconnect_audio_observed=%t queued_audio_frames_before_connect=%d",
			reason,
			activationToConnectedMs,
			firstAudioToConnectedMs,
			!firstAudioAt.IsZero(),
			queuedBeforeConn,
		)
	})
}

func (s *CartesiaSTTProcessor) noteAudioBeforeConnect(at time.Time) {
	select {
	case <-s.connected:
		return
	default:
	}
	if at.IsZero() {
		at = time.Now()
	}
	s.timingMu.Lock()
	if s.firstAudioAt.IsZero() {
		s.firstAudioAt = at
	}
	s.queuedBeforeConn++
	s.timingMu.Unlock()
}

func (s *CartesiaSTTProcessor) read() {
	responseID := 0 // Reader-owned, monotonic across reconnects.
	for !s.ending() {
		conn := s.currentWebsocketConn()
		if conn == nil {
			return
		}
		_, msg, err := conn.ReadMessage()
		if s.ending() {
			return
		}
		if err != nil {
			// Retire the old connection before publishing its replacement.
			s.closeWebsocket()
			if websocket.IsCloseError(err, websocket.CloseGoingAway) {
				s.logf("STT websocket idle close provider=cartesia code=1001, reconnecting")
			} else {
				s.logf("STT read error, reconnecting provider=cartesia: %s", s.safeError(err.Error()))
			}
			if err := s.connect(); err != nil {
				s.handleConnectExhausted(err)
				return
			}
			continue
		}
		var resp cartesiaSTTMessage
		if err := json.Unmarshal(msg, &resp); err != nil {
			// Do not echo malformed provider payloads.
			s.logf("STT json unmarshal error provider=cartesia")
			continue
		}
		switch resp.Type {
		case "turn.update":
			if strings.TrimSpace(resp.Transcript) != "" {
				responseID++
				s.PushFrame(NewTranscriptFrame(resp.Transcript, false, responseID, false), Downstream)
			}
		case "turn.end":
			responseID++
			if strings.TrimSpace(resp.Transcript) != "" {
				s.PushFrame(NewTranscriptFrame(resp.Transcript, true, responseID, false), Downstream)
			}
			// Even an empty final must clear the aggregator's interim snapshot.
			s.PushFrame(NewTranscriptFrame("<end>", true, responseID, false), Downstream)
		case "turn.start":
			s.logf("STT turn.start provider=cartesia")
		case "turn.eager_end", "turn.resume", "connected":
			// Logged for visibility only; turn.end is the sole commit point.
			s.logf("STT %s provider=cartesia", resp.Type)
		case "error":
			s.reportProviderError(resp)
		}
	}
}

func (s *CartesiaSTTProcessor) reportProviderError(resp cartesiaSTTMessage) {
	// Preserve the Soniox ErrorFrame shape. Auto supplies a symbolic error_code
	// and an HTTP-like status_code; prefer the symbolic code when present.
	errorCode := resp.ErrorCode
	if errorCode == "" && resp.StatusCode != 0 {
		errorCode = strconv.Itoa(resp.StatusCode)
	}
	if errorCode == "" {
		errorCode = "None"
	}
	errorMessage := resp.Message
	if errorMessage == "" {
		errorMessage = resp.Title
	}
	if errorMessage == "" {
		errorMessage = "Unknown error"
	}
	errorCode = s.safeError(errorCode)
	errorMessage = s.safeError(errorMessage)
	message := fmt.Sprintf("Error: %s (_receive_messages) - %s", errorCode, errorMessage)
	if !s.shouldReportProviderError(message, time.Now()) {
		return
	}
	sentryutil.Capture(sentryutil.Event{
		Hub:     s.taskCtx.SentryHub(),
		Message: "STT error: " + message,
		Tags:    map[string]string{"component": "stt", "provider": "cartesia", "operation": "provider_error", "error_code": errorCode},
		Details: map[string]any{"error_code": errorCode, "status_code": resp.StatusCode, "error_message": errorMessage},
	})
	s.PushError(message, false)
}

// Provider errors can reflect credentials; redact before reporting them.
func (s *CartesiaSTTProcessor) safeError(message string) string {
	if key := os.Getenv("CARTESIA_API_KEY"); key != "" {
		for _, value := range []string{key, url.QueryEscape(key), url.PathEscape(key)} {
			message = strings.ReplaceAll(message, value, "[REDACTED]")
		}
	}
	return message
}

func (s *CartesiaSTTProcessor) shouldReportProviderError(key string, now time.Time) bool {
	window := s.providerErrorWindows[key]
	if window.startedAt.IsZero() || now.Before(window.startedAt) || now.Sub(window.startedAt) >= sttProviderErrorReportWindow {
		s.providerErrorWindows[key] = sttProviderErrorWindow{startedAt: now, count: 1}
		return true
	}
	if window.count >= sttProviderErrorReportLimit {
		return false
	}
	window.count++
	s.providerErrorWindows[key] = window
	return true
}

func (s *CartesiaSTTProcessor) handleConnectExhausted(err error) {
	if err == nil || s.ending() {
		return
	}
	wrapped := fmt.Errorf("Cartesia connection failed after %d attempts: %w", len(cartesiaSTTConnectRetryDelays)+1, err)
	sentryutil.Capture(sentryutil.Event{
		Hub:  s.taskCtx.SentryHub(),
		Err:  wrapped,
		Tags: map[string]string{"component": "stt", "provider": "cartesia"},
	})
	s.taskCtx.Logger.Println(wrapped)
	s.PushError(wrapped.Error(), true)
}

func (s *CartesiaSTTProcessor) runWriter() {
	defer close(s.writerDone)
	defer func() {
		// EndFrame signals this writer, waits for it, then forwards the frame.
		// Neither ProcessFrame nor the reader writes the close command.
		if s.ctx.Err() == nil && s.ending() {
			if conn := s.currentWebsocketConn(); conn != nil {
				_ = conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
				_ = conn.WriteMessage(websocket.TextMessage, []byte(cartesiaSTTCloseMessage))
			}
		}
		s.closeWebsocket() // Also unblocks ReadMessage on parent cancellation.
	}()
	select {
	case <-s.connected:
	case <-s.endCh:
		return
	case <-s.ctx.Done():
		return
	}
	ticker := time.NewTicker(cartesiaSTTKeepaliveInterval)
	defer ticker.Stop()
	lastAudioAt := time.Now()
	// Pipecat STTService sends 100ms of silent s16le mono audio. Auto has no
	// keepalive command; 16,000 samples/s * 2 bytes/sample * 0.1s = 3,200 bytes.
	silence := make([]byte, 3200)
	for !s.ending() {
		var data []byte
		select {
		case <-s.ctx.Done():
			return
		case <-s.endCh:
			return
		case audio := <-s.audioFrames:
			data = audio.Data
		case now := <-ticker.C:
			if now.Sub(lastAudioAt) < cartesiaSTTKeepaliveIdleTimeout {
				continue
			}
			data = silence
		}
		if s.ending() {
			return
		}
		conn := s.currentWebsocketConn()
		if conn == nil {
			continue
		}
		// Bound an in-flight audio write so EndFrame cannot wait indefinitely.
		_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
		if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
			if s.ending() {
				return
			}
			s.logf("STT write error, skipping frame provider=cartesia: %s", s.safeError(err.Error()))
			_ = conn.Close() // Unblock the reader to reconnect after write failure.
			continue
		}
		lastAudioAt = time.Now()
	}
}

func (s *CartesiaSTTProcessor) currentWebsocketConn() *websocket.Conn {
	s.websocketMu.RLock()
	defer s.websocketMu.RUnlock()
	return s.websocketConn
}

func (s *CartesiaSTTProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case STTConnectFrame:
		s.activate(f.Reason, f.At)
	case AudioFrame:
		at := time.Now()
		s.noteAudioBeforeConnect(at)
		s.activate("first_audio_fallback", at)
		select {
		case <-ctx.Done():
		case <-s.ctx.Done():
		case s.audioFrames <- f:
		}
	case EndFrame:
		s.taskCtx.Logger.Printf("EndFrame at CartesiaSTTProcessor: reason=%q\n", f.Reason)
		s.endOnce.Do(func() { close(s.endCh) })
		if s.started.Load() {
			select {
			case <-s.writerDone:
			case <-s.ctx.Done():
			}
		}
		s.Stop()
		s.PushFrame(f, dir)
	default:
		s.PushFrame(frame, dir)
	}
}

func (s *CartesiaSTTProcessor) logf(format string, args ...any) {
	if s != nil && s.taskCtx != nil && s.taskCtx.Logger != nil {
		s.taskCtx.Logger.Printf(format, args...)
	}
}
