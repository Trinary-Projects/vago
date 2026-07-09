package voicepipelinecore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

type SonioxToken struct {
	Text    string `json:"text"`
	IsFinal bool   `json:"is_final"`
}

type SonioxResponseMessage struct {
	Tokens   []SonioxToken `json:"tokens"`
	Finished bool          `json:"finished"`
}

// sttDialURL is the Soniox websocket endpoint. Exposed as a package
// variable so tests can override it to point at an unreachable URL.
var sttDialURL = "wss://stt-rt.soniox.com/transcribe-websocket"

// Python's CustomSonioxSTTService caps initial connect retries at three
// attempts with short exponential backoff. Keep these as vars so tests
// can shorten the retry window without sleeping for seconds.
var sttConnectRetryDelays = []time.Duration{time.Second, 2 * time.Second}

// STTProcessor uses a single cancellation signal — the embedded
// BaseProcessor.ctx (referred to via s.ctx). Three things shut it down:
//
//   - s.Stop() called by ProcessFrame(EndFrame) when EndFrame propagates
//     through this processor.
//   - s.Stop() called by Pipeline.Stop() from PipelineTask.completeEnd.
//   - taskCtx.Ctx cancellation (transitively cancels s.ctx, since s.ctx
//     was derived from taskCtx.Ctx in NewBaseProcessor).
//
// Stop closes the websocket — which is what unblocks the reader's
// blocking ReadMessage — and cancels b.ctx, which unblocks the writer's
// select. Idempotent via closeOnce + BaseProcessor's atomic cancelling
// flag.
type STTProcessor struct {
	*BaseProcessor
	taskCtx          *TaskContext
	websocketConn    *websocket.Conn
	audioFrames      chan AudioFrame
	connected        chan struct{} // closed when the websocket is established
	activateCh       chan struct{}
	closeOnce        sync.Once
	activateOnce     sync.Once
	connectLogOnce   sync.Once
	timingMu         sync.Mutex
	activatedAt      time.Time
	activationReason string
	firstAudioAt     time.Time
	queuedBeforeConn int
}

func NewSTTProcessor(taskCtx *TaskContext) *STTProcessor {
	p := &STTProcessor{
		taskCtx:     taskCtx,
		audioFrames: make(chan AudioFrame, 100),
		connected:   make(chan struct{}),
		activateCh:  make(chan struct{}),
	}
	p.BaseProcessor = NewBaseProcessor("STT", p, taskCtx)
	return p
}

// activate unblocks the Soniox websocket dial. It is safe to call before
// Start; the reader goroutine will observe the already-closed channel when
// the pipeline starts.
func (s *STTProcessor) activate(reason string, at time.Time) {
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
		s.logf("STT websocket activation requested reason=%s activation_at=%s", reason, at.UTC().Format(time.RFC3339Nano))
		close(s.activateCh)
	})
}

// Stop cancels b.ctx first (so the reader's post-ReadMessage ctx check
// fires before it tries to reconnect) and then closes the websocket
// (so the blocked ReadMessage returns). Order matters.
func (s *STTProcessor) Stop() {
	s.BaseProcessor.Stop()
	s.closeOnce.Do(func() {
		if s.websocketConn != nil {
			s.websocketConn.Close()
		}
	})
}

func (s *STTProcessor) Start(ctx context.Context) {
	s.BaseProcessor.Start(ctx)
	s.Go(s.runReader)
	s.Go(s.runWriter)
}

func (s *STTProcessor) waitForActivation() bool {
	select {
	case <-s.activateCh:
		return true
	case <-s.ctx.Done():
		return false
	}
}

func sttConfigPayload() map[string]interface{} {
	return map[string]interface{}{
		"api_key":                   os.Getenv("SONIOX_API_KEY"),
		"model":                     "stt-rt-v4",
		"audio_format":              "s16le",
		"sample_rate":               16000,
		"num_channels":              1,
		"language_hints":            []string{"hi"},
		"enable_endpoint_detection": true,
	}
}

// connect dials Soniox in an interruptible retry loop. It caps retries
// so a bad key or broken endpoint cannot hold a worker slot forever.
func (s *STTProcessor) connect() error {
	maxAttempts := len(sttConnectRetryDelays) + 1
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if s.ctx.Err() != nil {
			return s.ctx.Err()
		}
		conn, _, err := websocket.DefaultDialer.Dial(sttDialURL, nil)
		if err == nil {
			err = conn.WriteJSON(sttConfigPayload())
			if err == nil {
				s.websocketConn = conn
				s.taskCtx.Logger.Println("STT websocket connected")
				s.logInitialConnectLatency(time.Now())
				return nil
			}
			conn.Close()
		}
		if attempt == maxAttempts {
			return err
		}
		delay := sttConnectRetryDelays[attempt-1]
		s.taskCtx.Logger.Printf("STT connect failed attempt=%d/%d: %v, retrying in %s...", attempt, maxAttempts, err, delay)
		select {
		case <-time.After(delay):
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
	return nil
}

func (s *STTProcessor) runReader() {
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

func (s *STTProcessor) logInitialConnectLatency(connectedAt time.Time) {
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
			"STT lazy connect latency activation_reason=%s activation_to_connected_ms=%.1f first_audio_to_connected_ms=%.1f preconnect_audio_observed=%t queued_audio_frames_before_connect=%d",
			reason,
			activationToConnectedMs,
			firstAudioToConnectedMs,
			!firstAudioAt.IsZero(),
			queuedBeforeConn,
		)
	})
}

func (s *STTProcessor) noteAudioBeforeConnect(at time.Time) {
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

func (s *STTProcessor) read() {
	responseID := 0
	for {
		if s.ctx.Err() != nil {
			s.taskCtx.Logger.Println("STT reader exiting")
			return
		}
		_, msg, err := s.websocketConn.ReadMessage()
		if err != nil {
			if s.ctx.Err() != nil {
				s.taskCtx.Logger.Println("STT reader exiting")
				return
			}
			s.taskCtx.Logger.Println("STT read error, reconnecting:", err)
			if err := s.connect(); err != nil {
				s.handleConnectExhausted(err)
				return
			}
			continue
		}
		var resp SonioxResponseMessage
		if err := json.Unmarshal(msg, &resp); err != nil {
			s.taskCtx.Logger.Println("STT json unmarshal error:", err)
			continue
		}
		responseID++
		//s.taskCtx.Logger.Printf("STT response received: response_id=%d finished=%v tokens=%d\n", responseID, resp.Finished, len(resp.Tokens))
		for _, tok := range resp.Tokens {
			//s.taskCtx.Logger.Printf("STT token received: response_id=%d finished=%v is_final=%v text=%q\n", responseID, resp.Finished, tok.IsFinal, tok.Text)
			s.PushFrame(NewTranscriptFrame(tok.Text, tok.IsFinal, responseID, resp.Finished), Downstream)
		}
	}
}

func (s *STTProcessor) handleConnectExhausted(err error) {
	if err == nil || s.ctx.Err() != nil {
		return
	}
	wrapped := fmt.Errorf("Soniox connection failed after %d attempts: %w", len(sttConnectRetryDelays)+1, err)
	sentryutil.Capture(sentryutil.Event{
		Err:  wrapped,
		Tags: map[string]string{"component": "stt", "provider": "soniox"},
	})
	s.taskCtx.Logger.Println(wrapped)
	s.PushError(wrapped.Error(), true)
}

func (s *STTProcessor) runWriter() {
	select {
	case <-s.connected:
	case <-s.ctx.Done():
		return
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case audio := <-s.audioFrames:
			if err := s.websocketConn.WriteMessage(websocket.BinaryMessage, audio.Data); err != nil {
				if s.ctx.Err() != nil {
					return
				}
				s.taskCtx.Logger.Println("STT write error, skipping frame:", err)
			}
		}
	}
}

func (s *STTProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case STTConnectFrame:
		s.activate(f.Reason, f.At)
	case AudioFrame:
		at := time.Now()
		s.noteAudioBeforeConnect(at)
		s.activate("first_audio_fallback", at)
		select {
		case <-s.ctx.Done():
		case s.audioFrames <- f:
		}
	case EndFrame:
		s.taskCtx.Logger.Printf("EndFrame at STTProcessor: reason=%q\n", f.Reason)
		s.PushFrame(f, dir)
		s.Stop()
	default:
		s.PushFrame(frame, dir)
	}
}

func (s *STTProcessor) logf(format string, args ...any) {
	if s != nil && s.taskCtx != nil && s.taskCtx.Logger != nil {
		s.taskCtx.Logger.Printf(format, args...)
	}
}
