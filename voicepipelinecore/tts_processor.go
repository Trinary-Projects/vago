package voicepipelinecore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jaideep329/talk-go/internal/sentryutil"
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
	// ttsEventReconnected signals that the reader re-established the
	// Cartesia websocket after a read error. It carries no contextID —
	// Cartesia has no idea what context we were in — so the orchestrator
	// must finish all contexts from the lost connection.
	ttsEventReconnected
	// ttsEventProviderError carries a Cartesia error response to the
	// orchestrator, which owns the active synthesis state.
	ttsEventProviderError
)

type ttsEvent struct {
	eventType ttsEventType
	contextID string
	audioData []byte
	words     []pendingWord
	error     string // Python-parity ErrorFrame text, including the raw payload
	errorKey  string // stable across context IDs for once-per-call reporting
	summary   string // stable Sentry message; raw payload remains in Details
}

// ttsCommand is the relay envelope used by ProcessFrame to hand frames
// off to the orchestrator goroutine. done (if non-nil) is closed by the
// orchestrator after the frame has been fully processed and (for
// EndFrame) forwarded downstream.
type ttsCommand struct {
	ctx   context.Context
	frame Frame
	dir   Direction
	done  chan struct{}
}

// ttsDialURL is the Cartesia websocket endpoint base. The API key is
// appended at dial time. Exposed as a package variable so tests can
// override it.
var ttsDialURL = "wss://api.cartesia.ai/tts/websocket?cartesia_version=2025-04-16&api_key="

// defaultCartesiaModelID is used when the caller passes an empty modelID,
// matching the pre-A/B-test Cartesia model.
const defaultCartesiaModelID = "sonic-3"

// ttsPendingEndTimeout bounds how long the orchestrator will hold a
// deferred EndFrame waiting for Cartesia's "done" event before giving
// up and forwarding it anyway. Exposed as a package variable so tests
// can override it.
var ttsPendingEndTimeout = 10 * time.Second

const (
	ttsProviderErrorReportLimit  = 100
	ttsProviderErrorReportWindow = time.Minute
)

type ttsProviderErrorWindow struct {
	startedAt time.Time
	count     int
}

// ttsContext keeps synthesis state separate for each utterance. Provider
// responses may arrive out of order; output is released in creation order.
type ttsContext struct {
	id              string
	aggregation     string
	firstText       bool
	firstSentence   bool
	firstAudio      bool
	pcmBuffer       []byte
	pendingWords    []pendingWord
	audioTimePushed float64
	sent            bool
	closed          bool
	done            bool
	output          []Frame
}

// ttsOutputItem mirrors Pipecat's serialization queue: either a context to
// drain completely or an ordinary frame that follows that context.
type ttsOutputItem struct {
	context *ttsContext
	frame   Frame
}

// TTSProcessor has one websocket reader and one state-owning orchestrator.
// Like Pipecat's audio contexts, each utterance can synthesize independently,
// while contexts and their audio/word/end frames drain downstream in FIFO order.
type TTSProcessor struct {
	*BaseProcessor
	taskCtx          *TaskContext
	metrics          *ProcessorMetrics
	phonetic         *phoneticFilter
	modelID          string
	outputSampleRate int

	// Orchestrator-owned. turn receives LLM tokens; outputQueue owns emission order.
	turn         *ttsContext
	outputQueue  []ttsOutputItem
	contextsByID map[string]*ttsContext
	commands     chan ttsCommand
	ttsEvents    chan ttsEvent
	connected    chan struct{}

	connMu               sync.Mutex
	websocketConn        *websocket.Conn
	closing              bool
	providerErrorWindows map[string]ttsProviderErrorWindow
}

type CartesiaTTSMessage struct {
	Type       string `json:"type"`
	Error      string `json:"error"`
	Message    string `json:"message"`
	ContextId  string `json:"context_id"`
	StatusCode int    `json:"status_code"`
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
// modelID selects the Cartesia model (e.g. for the call_tts_variant_flag
// A/B test); an empty string falls back to defaultCartesiaModelID. Voice
// id, language, and output format are unaffected by this parameter.
func NewTTSProcessor(taskCtx *TaskContext, phoneticDict map[string]string, modelID string) *TTSProcessor {
	if modelID == "" {
		modelID = defaultCartesiaModelID
	}
	t := &TTSProcessor{
		metrics:              NewProcessorMetrics("tts"),
		taskCtx:              taskCtx,
		contextsByID:         make(map[string]*ttsContext),
		phonetic:             newPhoneticFilter(phoneticDict),
		modelID:              modelID,
		outputSampleRate:     outputSampleRateFromRoom(taskCtx),
		commands:             make(chan ttsCommand, 100),
		ttsEvents:            make(chan ttsEvent, 100),
		connected:            make(chan struct{}),
		providerErrorWindows: make(map[string]ttsProviderErrorWindow),
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
// Idempotent: BaseProcessor's cancelling flag + closeTTSConnection is
// safe to call repeatedly.
func (t *TTSProcessor) Stop() {
	t.BaseProcessor.Stop()
	t.closeTTSConnection()
}

// closeTTSConnection is the intentional close (EndFrame forward or
// Stop). It marks the processor as closing — so the reader exits on its
// next read error instead of reconnecting — and closes whatever
// connection is current, including one installed by a reconnect.
// Safe to call more than once; closing an already-closed conn is a no-op
// error we ignore.
func (t *TTSProcessor) closeTTSConnection() {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	t.closing = true
	if t.websocketConn != nil {
		t.websocketConn.Close()
	}
}

// isClosing reports whether an intentional close has happened.
func (t *TTSProcessor) isClosing() bool {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	return t.closing
}

// currentConn returns the current Cartesia websocket under connMu.
func (t *TTSProcessor) currentConn() *websocket.Conn {
	t.connMu.Lock()
	defer t.connMu.Unlock()
	return t.websocketConn
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
// forwarded the EndFrame downstream). The orchestrator cancels synthesis
// before forwarding interruption, so old audio cannot follow the interrupt.
func (t *TTSProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	if dir == Upstream {
		t.PushFrame(frame, dir)
		return
	}
	cmd := ttsCommand{ctx: ctx, frame: frame, dir: dir}
	if _, interrupted := frame.(InterruptFrame); interrupted {
		select {
		case <-t.connected:
		default:
			// No synthesis can exist before the initial connection. Relayed
			// commands from before this interrupt carry cancelled contexts.
			t.PushFrame(frame, dir)
			return
		}
	}
	if _, ok := frame.(EndFrame); ok {
		cmd.done = make(chan struct{})
	}
	select {
	case t.commands <- cmd:
	case <-ctx.Done():
		return
	}
	if cmd.done != nil {
		select {
		case <-cmd.done:
		case <-ctx.Done():
		}
	}
}

func (t *TTSProcessor) orchestrator() {
	select {
	case <-t.connected:
	case <-t.ctx.Done():
		return
	}
	var pendingEnd *ttsCommand
	var endTimer *time.Timer
	var endTimeout <-chan time.Time
	defer func() {
		if endTimer != nil {
			endTimer.Stop()
		}
	}()
	finishEnd := func() bool {
		if pendingEnd == nil || len(t.outputQueue) != 0 {
			return false
		}
		t.PushFrame(pendingEnd.frame, pendingEnd.dir)
		close(pendingEnd.done)
		t.closeTTSConnection()
		return true
	}
	for {
		select {
		case <-t.ctx.Done():
			return
		case cmd := <-t.commands:
			// Commands already relayed to this queue retain their interrupt context.
			if cmd.frame.IsInterruptible() && cmd.ctx.Err() != nil {
				continue
			}
			switch f := cmd.frame.(type) {
			case InterruptFrame:
				t.cancelContexts()
				t.PushFrame(f, cmd.dir)
			case EndFrame:
				if pendingEnd != nil {
					close(cmd.done)
					continue
				}
				pendingEnd = &cmd
				if shouldStopPlaybackImmediately(f.Reason) {
					t.cancelContexts()
				} else {
					t.closeTurn()
					endTimer = time.NewTimer(ttsPendingEndTimeout)
					endTimeout = endTimer.C
				}
			case LLMResponseStartFrame:
				if pendingEnd == nil {
					t.outputQueue = append(t.outputQueue, ttsOutputItem{frame: f})
					t.turn = t.newContext()
				}
			case TextFrame:
				if pendingEnd == nil {
					t.addText(f.Text)
				}
			case LLMResponseEndFrame:
				if pendingEnd == nil {
					t.closeTurn()
					t.outputQueue = append(t.outputQueue, ttsOutputItem{frame: f})
					t.drainOutput()
				}
			case TTSSpeakFrame:
				if pendingEnd != nil {
					continue
				}
				pushAggregation := t.turn == nil
				c := t.newContext()
				c.firstSentence = true
				t.metrics.Start(MetricTTFB)
				c.sent = t.sendTextToTTS(c, f.Text)
				t.closeContext(c)
				if pushAggregation {
					t.outputQueue = append(t.outputQueue, ttsOutputItem{frame: NewLLMAssistantPushAggregationFrame()})
					t.drainOutput()
				}
			default:
				if pendingEnd == nil {
					// Pipecat's serialization queue also orders ordinary control
					// frames behind preceding speech contexts.
					if !f.IsSystem() {
						t.outputQueue = append(t.outputQueue, ttsOutputItem{frame: f})
						t.drainOutput()
					} else {
						t.PushFrame(f, cmd.dir)
					}
				}
			}
		case event := <-t.ttsEvents:
			switch event.eventType {
			case ttsEventReconnected:
				// The provider forgot every outstanding context on this connection.
				for _, c := range t.contextsByID {
					t.finishContext(c)
				}
			case ttsEventProviderError:
				if event.contextID != "" {
					c := t.contextsByID[event.contextID]
					if c == nil || c.done {
						continue
					}
					t.finishContext(c)
				} else {
					for _, c := range t.contextsByID {
						t.finishContext(c)
					}
				}
				t.reportProviderError(event)
			default:
				t.handleTTSEvent(event)
			}
		case <-endTimeout:
			t.taskCtx.Logger.Println("TTS pending EndFrame timed out waiting for synthesis")
			sentryutil.Capture(sentryutil.Event{
				Hub: t.taskCtx.SentryHub(), Message: "TTS pending EndFrame timed out waiting for Cartesia done",
				Tags:    map[string]string{"component": "tts", "operation": "pending_end_timeout"},
				Details: map[string]any{"timeout": ttsPendingEndTimeout.String()},
			})
			for _, c := range t.contextsByID {
				t.finishContext(c)
			}
		}
		if finishEnd() {
			return
		}
	}
}

func (t *TTSProcessor) newContext() *ttsContext {
	c := &ttsContext{id: uuid.NewString()}
	t.outputQueue = append(t.outputQueue, ttsOutputItem{context: c})
	t.contextsByID[c.id] = c
	t.emit(c, NewTTSStartedFrame(c.id))
	return c
}

func (t *TTSProcessor) emit(c *ttsContext, frame Frame) {
	if mf, ok := frame.(MetricsFrame); ok {
		t.PushFrame(mf, Downstream)
		return
	}
	c.output = append(c.output, frame)
	t.drainOutput()
}

func (t *TTSProcessor) drainOutput() {
	for len(t.outputQueue) > 0 {
		item := t.outputQueue[0]
		if c := item.context; c != nil {
			for _, f := range c.output {
				t.PushFrame(f, Downstream)
			}
			c.output = nil
			if !c.done {
				return
			}
			delete(t.contextsByID, c.id)
		} else {
			t.PushFrame(item.frame, Downstream)
		}
		t.outputQueue[0] = ttsOutputItem{}
		t.outputQueue = t.outputQueue[1:]
	}
}

func (t *TTSProcessor) addText(text string) {
	c := t.turn
	if c == nil || c.done {
		return
	}
	if !c.firstText {
		c.firstText = true
		t.metrics.Start(MetricTextAggregation)
	}
	c.aggregation += text
	if endsWithPunctuation(c.aggregation) {
		t.flushText(c)
	}
}

func (t *TTSProcessor) flushText(c *ttsContext) {
	if strings.TrimSpace(c.aggregation) == "" {
		c.aggregation = ""
		return
	}
	if !c.firstSentence {
		c.firstSentence = true
		if mf := t.metrics.Stop(MetricTextAggregation); mf != nil {
			t.emit(c, *mf)
		}
		t.metrics.Start(MetricTTFB)
	}
	if t.sendTextToTTS(c, c.aggregation) {
		c.sent = true
	}
	c.aggregation = ""
}

func (t *TTSProcessor) closeTurn() {
	if t.turn != nil {
		if !t.turn.done {
			t.flushText(t.turn)
			t.closeContext(t.turn)
		}
		t.turn = nil
	}
}

func (t *TTSProcessor) closeContext(c *ttsContext) {
	if c.closed || c.done {
		return
	}
	c.closed = true
	if !c.sent || !t.ResetTTSContext(c) {
		t.finishContext(c)
	}
}

func (t *TTSProcessor) finishContext(c *ttsContext) {
	if c.done {
		return
	}
	t.pushRemainingAudioFrames(c)
	done := NewTTSDoneFrame()
	done.ContextID = c.id
	t.emit(c, done)
	c.done = true
	t.drainOutput()
}

func (t *TTSProcessor) cancelContexts() {
	t.metrics.Reset()
	for _, c := range t.contextsByID {
		if !c.done {
			t.CancelTTSContext(c)
		}
	}
	t.outputQueue = nil
	t.contextsByID = make(map[string]*ttsContext)
	t.turn = nil
}

func (t *TTSProcessor) reportProviderError(event ttsEvent) {
	key := event.errorKey
	if key == "" {
		key = event.error
	}
	if !t.shouldReportProviderError(key, time.Now()) {
		return
	}
	summary := event.summary
	if summary == "" {
		summary = event.error
	}
	sentryutil.Capture(sentryutil.Event{
		Hub:     t.taskCtx.SentryHub(),
		Message: "TTS error: " + summary,
		Tags: map[string]string{
			"component": "tts",
			"provider":  "cartesia",
			"operation": "provider_error",
		},
		Details: map[string]any{
			"context_id": event.contextID,
			"error":      event.error,
		},
	})
	t.PushError(event.error, false)
}

func (t *TTSProcessor) shouldReportProviderError(key string, now time.Time) bool {
	if t.providerErrorWindows == nil {
		t.providerErrorWindows = make(map[string]ttsProviderErrorWindow)
	}
	window := t.providerErrorWindows[key]
	if window.startedAt.IsZero() || now.Before(window.startedAt) || now.Sub(window.startedAt) >= ttsProviderErrorReportWindow {
		t.providerErrorWindows[key] = ttsProviderErrorWindow{startedAt: now, count: 1}
		return true
	}
	if window.count >= ttsProviderErrorReportLimit {
		return false
	}
	window.count++
	t.providerErrorWindows[key] = window
	return true
}

// --- Cartesia interactions (called only from orchestrator goroutine) ---

func (t *TTSProcessor) sendTextToTTS(c *ttsContext, text string) bool {
	speakable := text
	if t.phonetic != nil {
		speakable = t.phonetic.apply(text)
		if strings.TrimSpace(speakable) == "" {
			t.taskCtx.Logger.Printf("TTS filter dropped non-speakable fragment: %q\n", text)
			return false
		}
	}
	payload := map[string]interface{}{
		"model_id":       t.modelID,
		"transcript":     speakable,
		"voice":          map[string]interface{}{"mode": "id", "id": "95d51f79-c397-46f9-b49a-23763d3eaa2d"},
		"output_format":  map[string]interface{}{"container": "raw", "encoding": "pcm_s16le", "sample_rate": t.outputRate()},
		"language":       "hi",
		"context_id":     c.id,
		"continue":       true,
		"add_timestamps": true,
	}
	if err := t.currentConn().WriteJSON(payload); err != nil {
		t.taskCtx.Logger.Println("failed to send TTS payload:", err)
		return false
	}
	// Surface the unfiltered text to the UI/debug log — phonetics are
	// a Cartesia-pronunciation artifact, not user-visible content.
	t.taskCtx.UIEvents.BotTranscription(text, time.Now())
	return true
}

func (t *TTSProcessor) ResetTTSContext(c *ttsContext) bool {
	if c.id == "" {
		return false
	}
	payload := map[string]interface{}{
		"model_id":      t.modelID,
		"transcript":    "",
		"voice":         map[string]interface{}{"mode": "id", "id": "95d51f79-c397-46f9-b49a-23763d3eaa2d"},
		"output_format": map[string]interface{}{"container": "raw", "encoding": "pcm_s16le", "sample_rate": t.outputRate()},
		"context_id":    c.id,
		"continue":      false,
	}
	if err := t.currentConn().WriteJSON(payload); err != nil {
		t.taskCtx.Logger.Println("failed to reset TTS context:", err)
		return false
	}
	return true
}

func (t *TTSProcessor) CancelTTSContext(c *ttsContext) bool {
	if c.id == "" {
		return false
	}
	payload := map[string]interface{}{
		"context_id": c.id,
		"cancel":     true,
	}
	if err := t.currentConn().WriteJSON(payload); err != nil {
		t.taskCtx.Logger.Println("failed to cancel TTS context:", err)
		return false
	}
	return true
}

// Provider context IDs are validated by the state-owning orchestrator, not
// against a single active ID in the reader. B may finish before A.
func (t *TTSProcessor) handleTTSEvent(event ttsEvent) {
	c := t.contextsByID[event.contextID]
	if c == nil || c.done {
		return
	}
	switch event.eventType {
	case ttsEventAudioChunk:
		if !c.firstAudio {
			c.firstAudio = true
			if mf := t.metrics.Stop(MetricTTFB); mf != nil {
				t.emit(c, *mf)
			}
		}
		c.pcmBuffer = append(c.pcmBuffer, event.audioData...)
		for len(c.pcmBuffer) >= t.frameBytes() {
			t.emit(c, NewAudioFrame(t.nextPCMFrame(c)))
			c.audioTimePushed += 0.02
			t.emitPendingWords(c)
		}
	case ttsEventWordTimestamps:
		c.pendingWords = append(c.pendingWords, event.words...)
		t.emitPendingWords(c)
	case ttsEventDone:
		t.finishContext(c)
	}
}

func (t *TTSProcessor) emitWord(c *ttsContext, word string) {
	f := NewWordTimestampFrame([]string{word})
	f.ContextID = c.id
	t.emit(c, f)
}

func (t *TTSProcessor) emitPendingWords(c *ttsContext) {
	if c.audioTimePushed == 0 {
		return
	}
	for len(c.pendingWords) > 0 && c.pendingWords[0].start <= c.audioTimePushed {
		w := c.pendingWords[0]
		c.pendingWords = c.pendingWords[1:]
		t.emitWord(c, w.word)
	}
}

func (t *TTSProcessor) pushRemainingAudioFrames(c *ttsContext) {
	if len(c.pcmBuffer) > 0 {
		for len(c.pcmBuffer) < t.frameBytes() {
			c.pcmBuffer = append(c.pcmBuffer, 0)
		}
		t.emit(c, NewAudioFrame(t.nextPCMFrame(c)))
		c.audioTimePushed += 0.02
	}
	t.emitPendingWords(c)
	c.pendingWords = nil
}

func (t *TTSProcessor) nextPCMFrame(c *ttsContext) []byte {
	if !t.audioTimingEnabled() {
		return c.nextPCMFrame(t.frameBytes())
	}
	startedAt := time.Now()
	frame := c.nextPCMFrame(t.frameBytes())
	t.recordAudioTiming("go_tts_pcm_frame_copy", time.Since(startedAt))
	return frame
}

func (c *ttsContext) nextPCMFrame(frameBytes int) []byte {
	if len(c.pcmBuffer) < frameBytes {
		return nil
	}
	frame := append([]byte(nil), c.pcmBuffer[:frameBytes]...)
	c.pcmBuffer = c.pcmBuffer[frameBytes:]
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
		if t.ctx.Err() != nil || t.isClosing() {
			return false
		}
		conn, _, err := websocket.DefaultDialer.Dial(ttsDialURL+os.Getenv("CARTESIA_API_KEY"), nil)
		if err == nil {
			t.connMu.Lock()
			if t.closing {
				// An intentional close raced this dial; never install a
				// socket nobody will close.
				t.connMu.Unlock()
				conn.Close()
				return false
			}
			t.websocketConn = conn
			t.connMu.Unlock()
			t.taskCtx.Logger.Printf("TTS websocket connected model_id=%s\n", t.modelID)
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
		_, msg, err := t.currentConn().ReadMessage()
		if err != nil {
			if t.ctx.Err() != nil {
				t.taskCtx.Logger.Println("TTS reader exiting")
				return
			}
			if t.isClosing() {
				t.taskCtx.Logger.Println("TTS reader exiting after intentional close")
				return
			}
			t.taskCtx.Logger.Println("TTS read error, reconnecting:", err)
			if !t.connect() {
				return
			}
			if !t.pushTTSEvent(ttsEvent{eventType: ttsEventReconnected}) {
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

		rawMessage := strings.TrimSpace(string(msg))
		if resp.Type == "error" || resp.Error != "" {
			if strings.Contains(resp.Error, "Invalid context ID") ||
				strings.Contains(resp.Message, "Invalid context ID") ||
				strings.Contains(rawMessage, "Invalid context ID") {
				continue
			}
			summary := strings.TrimSpace(resp.Error)
			if summary == "" {
				summary = strings.TrimSpace(resp.Message)
			}
			if summary == "" {
				summary = "Cartesia provider error"
			}
			if !t.pushTTSEvent(ttsEvent{
				eventType: ttsEventProviderError,
				contextID: resp.ContextId,
				error:     "Error: " + rawMessage,
				errorKey:  fmt.Sprintf("%s|%d|%s|%s", resp.Type, resp.StatusCode, resp.Error, resp.Message),
				summary:   summary,
			}) {
				return
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
			if !t.pushTTSEvent(ttsEvent{
				eventType: ttsEventDone,
				contextID: doneMsg.ContextId,
			}) {
				return
			}
		default:
			if !t.pushTTSEvent(ttsEvent{
				eventType: ttsEventProviderError,
				contextID: resp.ContextId,
				error:     "Error, unknown message type: " + rawMessage,
				errorKey:  "unknown_message_type|" + resp.Type,
				summary:   fmt.Sprintf("unknown message type %q", resp.Type),
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
