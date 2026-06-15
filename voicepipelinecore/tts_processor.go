package voicepipelinecore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

func endsWithPunctuation(s string) bool {
	if s == "" {
		return false
	}
	lastRune, _ := utf8.DecodeLastRuneInString(s)
	return unicode.IsPunct(lastRune)
}

type pendingWord struct {
	word  string
	start float64 // seconds from context start
}

type ttsEventType int

const (
	ttsEventAudioChunk ttsEventType = iota
	ttsEventWordTimestamps
	ttsEventDone
)

type ttsEvent struct {
	eventType ttsEventType
	contextID string
	audioData []byte
	words     []pendingWord
}

// ttsCommand is the relay envelope used by ProcessFrame to hand frames
// off to the orchestrator goroutine. done (if non-nil) is closed by the
// orchestrator after the frame has been fully processed and (for
// EndFrame) forwarded downstream.
type ttsCommand struct {
	frame Frame
	dir   Direction
	done  chan struct{}
}

// ttsDialURL is the Cartesia websocket endpoint base. The API key is
// appended at dial time. Exposed as a package variable so tests can
// override it.
var ttsDialURL = "wss://api.cartesia.ai/tts/websocket?cartesia_version=2025-04-16&api_key="

// TTSProcessor wraps Cartesia. After migration its shape is:
//
//   - readTTSConnectionData goroutine: reads from the websocket, parses
//     messages, and pushes typed events to ttsEvents.
//   - orchestrator goroutine: owns ALL TTS state (aggregation, synthesis,
//     shutdown). It drives Cartesia (send text, reset, cancel) and
//     emits downstream frames (AudioFrame, WordTimestampFrame,
//     TTSDoneFrame, deferred EndFrame). After forwarding EndFrame it
//     closes the Cartesia connection and exits, but leaves the base
//     processor queues alive until PipelineTask cleanup stops them.
//   - ProcessFrame: thin relay. For EndFrame it blocks until the
//     orchestrator confirms the EndFrame has been forwarded; for
//     InterruptFrame it forwards downstream immediately and signals the
//     orchestrator to clean up; for other downstream frames it queues a
//     command; for upstream pass-through frames it forwards directly.
//
// State ownership: only the orchestrator mutates state, so there is no
// concurrent access to currentAggregation, pcmBuffer, etc.
type TTSProcessor struct {
	*BaseProcessor
	taskCtx  *TaskContext
	metrics  *ProcessorMetrics
	phonetic *phoneticFilter

	outputSampleRate int

	// Aggregation state (orchestrator only)
	currentAggregation string
	currentContextId   string
	firstTextReceived  bool
	firstSentenceSent  bool

	// Synthesis state (orchestrator only — modified inside handleTTSEvent
	// which is called only from orchestrator goroutine)
	pcmBuffer          []byte
	pendingWords       []pendingWord
	audioTimePushed    float64
	firstAudioReceived bool

	// Shared (atomic) — read by reader goroutine, written by orchestrator.
	activeContextId atomic.Value // string

	// Channels
	commands  chan ttsCommand
	ttsEvents chan ttsEvent
	connected chan struct{} // closed when websocketConn is established

	websocketConn *websocket.Conn
	closeOnce     sync.Once // idempotent websocket close
}

type CartesiaTTSMessage struct {
	Type  string `json:"type"`
	Error string `json:"error"`
}

type CartesiaTTSAudioChunkMessage struct {
	Type      string `json:"type"`
	Data      string `json:"data"`
	ContextId string `json:"context_id"`
	Done      bool   `json:"done"`
	Error     string `json:"error"`
}

type CartesiaTTSWordTimestampMessage struct {
	Type           string `json:"type"`
	StatusCode     int    `json:"status_code"`
	ContextId      string `json:"context_id"`
	Done           bool   `json:"done"`
	Error          string `json:"error"`
	WordTimestamps struct {
		Words []string  `json:"words"`
		Start []float64 `json:"start"`
		End   []float64 `json:"end"`
	} `json:"word_timestamps"`
}

type CartesiaTTSDoneMessage struct {
	Type       string `json:"type"`
	StatusCode int    `json:"status_code"`
	Done       bool   `json:"done"`
	ContextId  string `json:"context_id"`
	Error      string `json:"error"`
}

// NewTTSProcessor builds the Cartesia TTS stage. phoneticDict is the
// only thing the caller supplies for pronunciation rewriting — the
// filter itself is built and owned here, so the dictionary never has to
// live on the shared TaskContext. A nil/empty dict means no filtering.
func NewTTSProcessor(taskCtx *TaskContext, phoneticDict map[string]string) *TTSProcessor {
	t := &TTSProcessor{
		taskCtx:          taskCtx,
		metrics:          NewProcessorMetrics("tts"),
		phonetic:         newPhoneticFilter(phoneticDict),
		outputSampleRate: outputSampleRateFromRoom(taskCtx),
		commands:         make(chan ttsCommand, 100),
		ttsEvents:        make(chan ttsEvent, 100),
		connected:        make(chan struct{}),
	}
	t.BaseProcessor = NewBaseProcessor("TTS", t, taskCtx)
	return t
}

func (t *TTSProcessor) Start(ctx context.Context) {
	t.BaseProcessor.Start(ctx)
	t.Go(t.runReader)
	t.Go(t.orchestrator)
}

// Stop cancels b.ctx (unblocking the orchestrator/writer selects) and
// closes the websocket (unblocking the reader's ReadMessage). Order
// matters: cancel ctx before closing the ws so the reader exits
// cleanly on its ctx check rather than racing into a reconnect attempt.
// Idempotent via BaseProcessor's cancelling flag + closeOnce.
func (t *TTSProcessor) Stop() {
	t.BaseProcessor.Stop()
	t.closeTTSConnection()
}

func (t *TTSProcessor) closeTTSConnection() {
	t.activeContextId.Store("")
	t.closeOnce.Do(func() {
		if t.websocketConn != nil {
			t.websocketConn.Close()
		}
	})
}

// runReader connects to Cartesia, signals readiness, and then reads
// messages until shutdown. Spawned in a Go-tracked goroutine.
func (t *TTSProcessor) runReader() {
	if !t.connect() {
		return
	}
	close(t.connected)
	t.readTTSConnectionData()
}

// ProcessFrame relays frames to the orchestrator. EndFrame blocks until
// the orchestrator has fully processed it (waited for Cartesia done +
// forwarded the EndFrame downstream); InterruptFrame is forwarded
// downstream immediately, then the orchestrator is signalled to clean
// up; other frames are queued non-blocking.
func (t *TTSProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch frame.(type) {
	case EndFrame:
		done := make(chan struct{})
		select {
		case t.commands <- ttsCommand{frame: frame, dir: dir, done: done}:
		case <-t.ctx.Done():
			return
		}
		select {
		case <-done:
		case <-ctx.Done():
		}
	case InterruptFrame:
		// Forward downstream right away so PlaybackSink/playback goroutine
		// can stop in parallel with orchestrator cleanup.
		t.PushFrame(frame, dir)
		select {
		case t.commands <- ttsCommand{frame: frame, dir: dir}:
		case <-t.ctx.Done():
		}
	case TTSDoneFrame, BotStartedSpeakingFrame, BotStoppedSpeakingFrame, WordTimestampFrame:
		// Upstream pass-through; orchestrator doesn't react to these.
		t.PushFrame(frame, dir)
	default:
		// All other downstream frames (LLMResponseStart/End, TextFrame,
		// TTSSpeakFrame, LLMMessagesFrame, etc.) go through the orchestrator.
		select {
		case t.commands <- ttsCommand{frame: frame, dir: dir}:
		case <-t.ctx.Done():
		}
	}
}

// orchestrator owns all TTS state and drives Cartesia. It mirrors the
// original Process loop's switch structure, but reads commands from a
// dedicated channel (relayed by ProcessFrame) instead of from
// ProcessorChannels.
//
// The orchestrator waits for runReader to establish the Cartesia
// connection before processing any command. This avoids racing on
// t.websocketConn (the reader goroutine writes it; orchestrator
// methods like sendTextToTTS read it). Commands queued during the
// pre-connect window sit in t.commands (capacity 100). If shutdown is
// requested before connect succeeds, orchestrator exits via ctx.Done
// and any ProcessFrame caller blocked on a command's done channel
// unblocks via its own per-frame ctx (procCtx, derived from b.ctx).
func (t *TTSProcessor) orchestrator() {
	select {
	case <-t.connected:
	case <-t.ctx.Done():
		return
	}

	var pendingEnd *EndFrame
	var pendingEndDir Direction
	var pendingEndDone chan struct{}

	// cartesiaTextSent: have we sent text to Cartesia in the current
	// context that hasn't been resolved by a "done" event yet? When
	// true, Cartesia owes us a "done" once we send Reset (continue:false).
	// Set on sendTextToTTS success, cleared on done/interrupt/clear.
	//
	// Replaces the previous 3-state ttsSynth enum. We tolerate the rare
	// duplicate Reset that can fire if EndFrame arrives between
	// LLMResponseEndFrame's Reset and Cartesia's done — Cartesia
	// responds with "Invalid context ID" which the reader already filters.
	cartesiaTextSent := false

	clearTTSState := func() {
		t.activeContextId.Store("")
		t.currentAggregation = ""
		t.currentContextId = ""
		t.pcmBuffer = nil
		t.pendingWords = nil
		t.audioTimePushed = 0
		t.firstTextReceived = false
		t.firstSentenceSent = false
		t.firstAudioReceived = false
		cartesiaTextSent = false
	}

	forwardPendingEnd := func(reason string) bool {
		if pendingEnd == nil {
			return false
		}
		t.taskCtx.Logger.Printf("TTS forwarding pending EndFrame %s: reason=%q\n", reason, pendingEnd.Reason)
		t.PushFrame(*pendingEnd, pendingEndDir)
		if pendingEndDone != nil {
			close(pendingEndDone)
			pendingEndDone = nil
		}
		pendingEnd = nil
		t.closeTTSConnection()
		return true
	}

	flushForEnd := func() {
		if strings.TrimSpace(t.currentAggregation) != "" {
			if t.sendTextToTTS(t.currentAggregation) {
				cartesiaTextSent = true
			}
			t.currentAggregation = ""
		}
		if cartesiaTextSent {
			if !t.ResetTTSContext() {
				cartesiaTextSent = false
			}
		}
	}

	handleEnd := func(frame EndFrame, dir Direction, doneCh chan struct{}) bool {
		if pendingEnd != nil {
			t.taskCtx.Logger.Printf("EndFrame at TTSProcessor ignored: shutdown already pending: reason=%q\n", frame.Reason)
			if doneCh != nil {
				close(doneCh)
			}
			return false
		}
		flushForEnd()
		if cartesiaTextSent {
			pending := frame
			pendingEnd = &pending
			pendingEndDir = dir
			pendingEndDone = doneCh
			t.taskCtx.Logger.Printf("EndFrame at TTSProcessor deferred until TTS done: reason=%q\n", frame.Reason)
			return false
		}
		t.taskCtx.Logger.Printf("EndFrame at TTSProcessor forwarding immediately: reason=%q\n", frame.Reason)
		t.PushFrame(frame, dir)
		if doneCh != nil {
			close(doneCh)
		}
		t.closeTTSConnection()
		return true
	}

	handleCommand := func(cmd ttsCommand) (exit bool) {
		switch f := cmd.frame.(type) {
		case InterruptFrame:
			if pendingEnd != nil {
				t.taskCtx.Logger.Println("TTS shutdown is pending, dropping interrupt")
				return false
			}
			t.CancelTTSContext()
			clearTTSState()
			drainTTSEvents(t.ttsEvents)
			t.metrics.Reset()
			t.taskCtx.Logger.Println("TTS interrupted, cleared state")
			// (ProcessFrame already forwarded the InterruptFrame downstream)
			return false

		case EndFrame:
			return handleEnd(f, cmd.dir, cmd.done)

		case LLMResponseStartFrame:
			if pendingEnd != nil {
				return false
			}
			clearTTSState()
			drainTTSEvents(t.ttsEvents)
			t.metrics.Reset()
			t.currentContextId = fmt.Sprintf("ctx-%d", rand.IntN(9000000))
			t.activeContextId.Store(t.currentContextId)
			t.taskCtx.Logger.Println("LLM response started, resetting TTS aggregation")
			t.PushFrame(f, cmd.dir)
			return false

		case TextFrame:
			if pendingEnd != nil {
				return false
			}
			if !t.firstTextReceived {
				t.firstTextReceived = true
				t.metrics.Start(MetricTextAggregation)
			}
			t.currentAggregation += f.Text
			if endsWithPunctuation(t.currentAggregation) {
				if !t.firstSentenceSent {
					t.firstSentenceSent = true
					if mf := t.metrics.Stop(MetricTextAggregation); mf != nil {
						t.PushFrame(*mf, Downstream)
					}
					t.metrics.Start(MetricTTFB)
				}
				if t.sendTextToTTS(t.currentAggregation) {
					cartesiaTextSent = true
				}
				t.currentAggregation = ""
			}
			return false

		case LLMResponseEndFrame:
			if pendingEnd != nil {
				return false
			}
			if strings.TrimSpace(t.currentAggregation) != "" {
				if t.sendTextToTTS(t.currentAggregation) {
					cartesiaTextSent = true
				}
			}
			if cartesiaTextSent {
				if !t.ResetTTSContext() {
					cartesiaTextSent = false
				}
			} else {
				// Empty turn — no text was sent to Cartesia, so no "done"
				// event will arrive. Emit TTSDone directly so the turn
				// still closes (PlaybackSink broadcasts BotStopped and
				// UserIdle arms). Covers a failed LLM turn and a rare
				// zero-token response.
				t.PushFrame(NewTTSDoneFrame(), Downstream)
			}
			t.currentAggregation = ""
			t.pcmBuffer = nil
			t.taskCtx.Logger.Println("LLM response ended, flushing TTS context")
			t.PushFrame(f, cmd.dir)
			return false

		case TTSSpeakFrame:
			if pendingEnd != nil {
				return false
			}
			clearTTSState()
			drainTTSEvents(t.ttsEvents)
			t.firstSentenceSent = true // skip aggregation metrics for idle prompts
			t.metrics.Reset()
			t.currentContextId = fmt.Sprintf("ctx-%d", rand.IntN(9000000))
			t.activeContextId.Store(t.currentContextId)
			t.metrics.Start(MetricTTFB)
			if t.sendTextToTTS(f.Text) {
				cartesiaTextSent = true
				if !t.ResetTTSContext() {
					cartesiaTextSent = false
				}
			}
			t.PushFrame(f, cmd.dir)
			return false

		default:
			if pendingEnd != nil {
				t.taskCtx.Logger.Printf("TTS shutdown pending, dropping frame: %T\n", f)
				return false
			}
			t.PushFrame(cmd.frame, cmd.dir)
			return false
		}
	}

	for {
		select {
		case <-t.ctx.Done():
			return
		case cmd := <-t.commands:
			if handleCommand(cmd) {
				return
			}
		case event := <-t.ttsEvents:
			if t.handleTTSEvent(event) {
				cartesiaTextSent = false
				if forwardPendingEnd("after TTS done") {
					return
				}
			}
		}
	}
}

// --- Cartesia interactions (called only from orchestrator goroutine) ---

func (t *TTSProcessor) sendTextToTTS(text string) bool {
	speakable := text
	if t.phonetic != nil {
		speakable = t.phonetic.apply(text)
		if strings.TrimSpace(speakable) == "" {
			t.taskCtx.Logger.Printf("TTS filter dropped non-speakable fragment: %q\n", text)
			return false
		}
	}
	payload := map[string]interface{}{
		"model_id":       "sonic-3",
		"transcript":     speakable,
		"voice":          map[string]interface{}{"mode": "id", "id": "95d51f79-c397-46f9-b49a-23763d3eaa2d"},
		"output_format":  map[string]interface{}{"container": "raw", "encoding": "pcm_s16le", "sample_rate": t.outputRate()},
		"language":       "hi",
		"context_id":     t.currentContextId,
		"continue":       true,
		"add_timestamps": true,
	}
	if err := t.websocketConn.WriteJSON(payload); err != nil {
		t.taskCtx.Logger.Println("failed to send TTS payload:", err)
		return false
	}
	// Surface the unfiltered text to the UI/debug log — phonetics are
	// a Cartesia-pronunciation artifact, not user-visible content.
	t.taskCtx.UIEvents.BotTranscription(text, time.Now())
	return true
}

func (t *TTSProcessor) ResetTTSContext() bool {
	if t.currentContextId == "" {
		return false
	}
	payload := map[string]interface{}{
		"model_id":      "sonic-3",
		"transcript":    "",
		"voice":         map[string]interface{}{"mode": "id", "id": "95d51f79-c397-46f9-b49a-23763d3eaa2d"},
		"output_format": map[string]interface{}{"container": "raw", "encoding": "pcm_s16le", "sample_rate": t.outputRate()},
		"context_id":    t.currentContextId,
		"continue":      false,
	}
	if err := t.websocketConn.WriteJSON(payload); err != nil {
		t.taskCtx.Logger.Println("failed to reset TTS context:", err)
		return false
	}
	return true
}

func (t *TTSProcessor) CancelTTSContext() bool {
	if t.currentContextId == "" {
		return false
	}
	payload := map[string]interface{}{
		"context_id": t.currentContextId,
		"cancel":     true,
	}
	if err := t.websocketConn.WriteJSON(payload); err != nil {
		t.taskCtx.Logger.Println("failed to cancel TTS context:", err)
		return false
	}
	return true
}

func (t *TTSProcessor) isActiveContext(contextId string) bool {
	active, _ := t.activeContextId.Load().(string)
	return active != "" && active == contextId
}

func (t *TTSProcessor) isCurrentContext(contextID string) bool {
	return t.currentContextId != "" && t.currentContextId == contextID
}

// handleTTSEvent processes a Cartesia event. Called only from the
// orchestrator goroutine. Returns true if a done event was processed
// (which the orchestrator uses to clear cartesiaTextSent and potentially
// forward a pendingEnd).
func (t *TTSProcessor) handleTTSEvent(event ttsEvent) bool {
	if !t.isCurrentContext(event.contextID) {
		if event.eventType == ttsEventDone {
			t.taskCtx.Logger.Printf("TTS ignoring stale done: context_id=%s\n", event.contextID)
		}
		return false
	}

	switch event.eventType {
	case ttsEventAudioChunk:
		t.handleAudioChunkData(event.audioData)
	case ttsEventWordTimestamps:
		t.pendingWords = append(t.pendingWords, event.words...)
		t.emitPendingWords()
	case ttsEventDone:
		t.taskCtx.Logger.Printf("TTS synthesis done: context_id=%s\n", event.contextID)
		t.pushRemainingAudioFrames()
		t.PushFrame(NewTTSDoneFrame(), Downstream)
		return true
	}
	return false
}

func (t *TTSProcessor) handleAudioChunkData(audioData []byte) {
	if t.currentContextId == "" {
		return
	}
	if !t.firstAudioReceived {
		t.firstAudioReceived = true
		if mf := t.metrics.Stop(MetricTTFB); mf != nil {
			t.PushFrame(*mf, Downstream)
		}
	}
	t.pcmBuffer = append(t.pcmBuffer, audioData...)

	frameBytes := t.frameBytes()
	for len(t.pcmBuffer) >= frameBytes {
		pcmFrame := t.nextPCMFrame()
		if pcmFrame == nil {
			break
		}
		t.PushFrame(NewAudioFrame(pcmFrame), Downstream)
		t.audioTimePushed += 0.02 // 20ms per PCM frame
		t.emitPendingWords()
	}
}

// emitPendingWords sends WordTimestampFrames for words whose start time
// has been reached by the audio pushed so far.
func (t *TTSProcessor) emitPendingWords() {
	for len(t.pendingWords) > 0 && t.pendingWords[0].start <= t.audioTimePushed {
		w := t.pendingWords[0]
		t.pendingWords = t.pendingWords[1:]
		t.PushFrame(NewWordTimestampFrame([]string{w.word}), Downstream)
	}
}

func (t *TTSProcessor) pushRemainingAudioFrames() {
	if t.currentContextId == "" {
		return
	}
	if len(t.pcmBuffer) > 0 {
		frameBytes := t.frameBytes()
		for len(t.pcmBuffer) < frameBytes {
			t.pcmBuffer = append(t.pcmBuffer, 0)
		}
		pcmFrame := t.nextPCMFrame()
		if pcmFrame != nil {
			t.PushFrame(NewAudioFrame(pcmFrame), Downstream)
			t.audioTimePushed += 0.02
		}
	}
	for _, w := range t.pendingWords {
		t.PushFrame(NewWordTimestampFrame([]string{w.word}), Downstream)
	}
	t.pendingWords = nil
}

func (t *TTSProcessor) nextPCMFrame() []byte {
	frameBytes := t.frameBytes()
	if len(t.pcmBuffer) < frameBytes {
		return nil
	}
	timingEnabled := t.audioTimingEnabled()
	var start time.Time
	if timingEnabled {
		start = time.Now()
	}
	frame := make([]byte, frameBytes)
	copy(frame, t.pcmBuffer[:frameBytes])
	t.pcmBuffer = t.pcmBuffer[frameBytes:]
	if timingEnabled {
		t.recordAudioTiming("go_tts_pcm_frame_copy", time.Since(start))
	}
	return frame
}

func (t *TTSProcessor) outputRate() int {
	if t == nil || t.outputSampleRate <= 0 {
		return defaultOutputSampleRate
	}
	return t.outputSampleRate
}

func (t *TTSProcessor) frameBytes() int {
	return pcmFrameBytesForRate(t.outputRate())
}

// --- Cartesia websocket reader ---

func (t *TTSProcessor) connect() bool {
	for {
		if t.ctx.Err() != nil {
			return false
		}
		conn, _, err := websocket.DefaultDialer.Dial(ttsDialURL+os.Getenv("CARTESIA_API_KEY"), nil)
		if err == nil {
			t.websocketConn = conn
			t.taskCtx.Logger.Println("TTS websocket connected")
			return true
		}
		t.taskCtx.Logger.Printf("TTS connect failed: %v, retrying in 1s...", err)
		select {
		case <-time.After(time.Second):
		case <-t.ctx.Done():
			return false
		}
	}
}

func (t *TTSProcessor) pushTTSEvent(event ttsEvent) bool {
	select {
	case <-t.ctx.Done():
		return false
	case t.ttsEvents <- event:
		return true
	}
}

func (t *TTSProcessor) readTTSConnectionData() {
	for {
		if t.ctx.Err() != nil {
			t.taskCtx.Logger.Println("TTS reader exiting")
			return
		}
		_, msg, err := t.websocketConn.ReadMessage()
		if err != nil {
			if t.ctx.Err() != nil {
				t.taskCtx.Logger.Println("TTS reader exiting")
				return
			}
			t.taskCtx.Logger.Println("TTS read error, reconnecting:", err)
			if !t.connect() {
				return
			}
			continue
		}
		var resp CartesiaTTSMessage
		timingEnabled := t.audioTimingEnabled()
		var start time.Time
		if timingEnabled {
			start = time.Now()
		}
		if err := json.Unmarshal(msg, &resp); err != nil {
			if timingEnabled {
				t.recordAudioTiming("go_tts_json_unmarshal", time.Since(start))
			}
			t.taskCtx.Logger.Println("TTS json unmarshal error:", err)
			continue
		}
		if timingEnabled {
			t.recordAudioTiming("go_tts_json_unmarshal", time.Since(start))
		}

		if resp.Error != "" {
			if !strings.Contains(resp.Error, "Invalid context ID") {
				t.taskCtx.Logger.Println("TTS error message received:", resp.Error)
			}
			continue
		}
		switch resp.Type {
		case "chunk":
			var audioMsg CartesiaTTSAudioChunkMessage
			if timingEnabled {
				start = time.Now()
			}
			if err := json.Unmarshal(msg, &audioMsg); err != nil {
				if timingEnabled {
					t.recordAudioTiming("go_tts_audio_json_unmarshal", time.Since(start))
				}
				t.taskCtx.Logger.Println("TTS audio chunk unmarshal error:", err)
				continue
			}
			if timingEnabled {
				t.recordAudioTiming("go_tts_audio_json_unmarshal", time.Since(start))
			}
			if !t.isActiveContext(audioMsg.ContextId) {
				continue
			}
			if timingEnabled {
				start = time.Now()
			}
			audioData, err := base64.StdEncoding.DecodeString(audioMsg.Data)
			if timingEnabled {
				t.recordAudioTiming("go_tts_audio_base64_decode", time.Since(start))
			}
			if err != nil {
				t.taskCtx.Logger.Println("base64 decode error:", err)
				continue
			}
			if !t.pushTTSEvent(ttsEvent{
				eventType: ttsEventAudioChunk,
				contextID: audioMsg.ContextId,
				audioData: audioData,
			}) {
				return
			}
		case "timestamps":
			var tsMsg CartesiaTTSWordTimestampMessage
			if err := json.Unmarshal(msg, &tsMsg); err != nil {
				t.taskCtx.Logger.Println("TTS word timestamp unmarshal error:", err)
				continue
			}
			if !t.isActiveContext(tsMsg.ContextId) {
				continue
			}
			words := make([]pendingWord, 0, len(tsMsg.WordTimestamps.Words))
			for i, w := range tsMsg.WordTimestamps.Words {
				if i < len(tsMsg.WordTimestamps.Start) {
					words = append(words, pendingWord{word: w, start: tsMsg.WordTimestamps.Start[i]})
				}
			}
			if len(words) > 0 {
				if !t.pushTTSEvent(ttsEvent{
					eventType: ttsEventWordTimestamps,
					contextID: tsMsg.ContextId,
					words:     words,
				}) {
					return
				}
			}
		case "done":
			var doneMsg CartesiaTTSDoneMessage
			if err := json.Unmarshal(msg, &doneMsg); err != nil {
				t.taskCtx.Logger.Println("TTS done message unmarshal error:", err)
				continue
			}
			if !t.isActiveContext(doneMsg.ContextId) {
				t.taskCtx.Logger.Printf("TTS ignoring stale done: context_id=%s\n", doneMsg.ContextId)
				continue
			}
			if !t.pushTTSEvent(ttsEvent{
				eventType: ttsEventDone,
				contextID: doneMsg.ContextId,
			}) {
				return
			}
		}
	}
}

func (t *TTSProcessor) recordAudioTiming(name string, elapsed time.Duration) {
	if t == nil || t.taskCtx == nil || t.taskCtx.Room == nil {
		return
	}
	t.taskCtx.Room.recordAudioTiming(name, elapsed)
}

func (t *TTSProcessor) audioTimingEnabled() bool {
	return t != nil && t.taskCtx != nil && t.taskCtx.Room != nil && t.taskCtx.Room.perfDiagnosticsEnabled()
}

func drainTTSEvents(ch chan ttsEvent) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
