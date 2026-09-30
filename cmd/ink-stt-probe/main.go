// Command ink-stt-probe streams prepared 16 kHz mono s16le PCM to Cartesia's
// AUTO/MANUAL Ink or Soniox WebSocket API and records transcripts and timing.
// It is standalone investigation tooling; it does not use the calling pipeline.
//
// Example (run from the repository root):
//
//	go run ./cmd/ink-stt-probe -env-file .env -mode auto -file /tmp/ink-probe/en16k.raw -json-out /tmp/ink-probe/out/example.json
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/gorilla/websocket"
)

type options struct {
	Mode           string  `json:"mode"`
	Model          string  `json:"model"`
	File           string  `json:"file,omitempty"`
	Language       string  `json:"language,omitempty"`
	ChunkMS        int     `json:"chunk_ms"`
	TailSeconds    float64 `json:"tail_silence_secs"`
	IdleSeconds    float64 `json:"idle_secs"`
	KeepaliveText  string  `json:"keepalive_text,omitempty"`
	Finalize       bool    `json:"finalize"`
	TimeoutSeconds float64 `json:"timeout_secs"`
}

// Event.Raw preserves every JSON field, including unknown future event shapes.
// Transport/HTTP observations are explicitly distinguished from server messages.
type event struct {
	At           time.Time `json:"timestamp"`
	ConnectMS    *float64  `json:"ms_since_connect"`
	SpeechMS     *float64  `json:"ms_since_speech_end"`
	Kind         string    `json:"kind"`
	Type         string    `json:"type"`
	MessageType  int       `json:"websocket_message_type,omitempty"`
	Raw          any       `json:"raw,omitempty"`
	RawText      string    `json:"raw_text,omitempty"`
	BinaryBase64 string    `json:"binary_base64,omitempty"`
}

type speechBoundary struct {
	FileMS    float64    `json:"file_ms"`
	SentAt    *time.Time `json:"sent_at,omitempty"`
	ConnectMS *float64   `json:"ms_since_connect,omitempty"`
	sample    int
}

type audioWrite struct {
	At        time.Time `json:"timestamp"`
	ConnectMS float64   `json:"ms_since_connect"`
	Bytes     int       `json:"bytes"`
	FileEndMS *float64  `json:"file_end_ms,omitempty"`
}

type action struct {
	At        time.Time `json:"timestamp"`
	ConnectMS float64   `json:"ms_since_connect"`
	Text      string    `json:"text"`
	Error     string    `json:"error,omitempty"`
}

type dialAttempt struct {
	At         time.Time `json:"timestamp"`
	DurationMS float64   `json:"duration_ms"`
	HTTPStatus int       `json:"http_status,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type finalTranscript struct {
	Text            string   `json:"text"`
	ConnectMS       *float64 `json:"ms_since_connect"`
	SpeechMS        *float64 `json:"ms_since_speech_end"`
	UtteranceMS     *float64 `json:"ms_since_utterance_end,omitempty"`
	UtteranceFileMS *float64 `json:"utterance_end_file_ms,omitempty"`
}

type summary struct {
	FirstEventMS            *float64          `json:"first_event_ms"`
	Updates                 int               `json:"turn_update_count"`
	NonFinals               int               `json:"non_final_count"`
	InterimMedianIntervalMS *float64          `json:"interim_median_interval_ms,omitempty"`
	Finals                  []finalTranscript `json:"finals"`
	InterimVerdict          string            `json:"interim_verdict,omitempty"`
	InterimRule             string            `json:"interim_rule,omitempty"`
	InterimEvidence         []string          `json:"interim_evidence,omitempty"`
	Errors                  []any             `json:"errors"`
}

type result struct {
	Run struct {
		options
		Endpoint     string           `json:"endpoint"`
		Query        url.Values       `json:"query"`
		StartedAt    time.Time        `json:"started_at"`
		ConnectedAt  *time.Time       `json:"connected_at,omitempty"`
		FinishedAt   time.Time        `json:"finished_at"`
		ConnectMS    *float64         `json:"connect_ms"`
		DialAttempts []dialAttempt    `json:"dial_attempts"`
		AudioSHA256  string           `json:"audio_sha256,omitempty"`
		AudioMS      *float64         `json:"audio_duration_ms,omitempty"`
		SpeechEndMS  *float64         `json:"speech_end_ms,omitempty"`
		SpeechSentAt *time.Time       `json:"speech_sent_at,omitempty"`
		Boundaries   []speechBoundary `json:"speech_boundaries,omitempty"`
		BoundaryRule string           `json:"speech_boundary_rule,omitempty"`
		AudioWrites  []audioWrite     `json:"audio_writes,omitempty"`
		Commands     []action         `json:"commands,omitempty"`
		Termination  string           `json:"termination"`
		Failure      string           `json:"failure,omitempty"`
	} `json:"run"`
	Events  []event `json:"events"`
	Summary summary `json:"summary"`
}

type probe struct {
	result
	key string
	t0  time.Time
}

func main() {
	os.Exit(runCLI())
}

func runCLI() int {
	var o options
	envFile := flag.String("env-file", ".env", "dotenv file (real environment wins); empty skips loading")
	jsonOut := flag.String("json-out", "", "write run metadata, ordered events, and summary as JSON")
	repeat := flag.Int("repeat", 1, "sequential runs, with 2 seconds between runs (<=1 runs once)")
	flag.StringVar(&o.Mode, "mode", "auto", "auto, manual, or soniox endpoint")
	flag.StringVar(&o.Model, "model", "ink-preview", "Cartesia STT model")
	flag.StringVar(&o.File, "file", "", "raw s16le/16000 Hz/mono PCM file (required unless idle)")
	flag.StringVar(&o.Language, "language", "", "manual-only language query parameter")
	flag.IntVar(&o.ChunkMS, "chunk-ms", 100, "PCM chunk duration and ticker cadence in ms")
	flag.Float64Var(&o.TailSeconds, "tail-silence-secs", 6, "seconds of PCM silence after the file")
	flag.Float64Var(&o.IdleSeconds, "idle-secs", 0, "idle without any audio for this many seconds")
	flag.StringVar(&o.KeepaliveText, "keepalive-text", "", "idle-only exact text frame every 5 seconds")
	flag.BoolVar(&o.Finalize, "finalize", false, "manual-only: send bare finalize immediately after file audio")
	flag.Float64Var(&o.TimeoutSeconds, "timeout-secs", 60, "total safety ceiling including dial/retry/teardown")
	flag.Float64("turn-start-threshold", 0, "auto-only explicit turn_start_threshold query parameter")
	flag.Float64("turn-eager-end-threshold", 0, "auto-only explicit turn_eager_end_threshold query parameter")
	flag.Float64("turn-end-threshold", 0, "auto-only explicit turn_end_threshold query parameter")
	flag.Int("turn-end-timeout-ms", 0, "auto-only explicit turn_end_timeout_ms query parameter")
	var keyterms []string
	flag.Func("keyterm", "auto-only comma-separated keyterms (repeatable)", func(s string) error {
		for _, term := range strings.Split(s, ",") {
			if term = strings.TrimSpace(term); term != "" {
				keyterms = append(keyterms, term)
			}
		}
		return nil
	})
	flag.Parse()
	if *envFile != "" {
		if err := loadEnvFile(*envFile); err != nil {
			// Do not print dotenv parser errors: they could contain a line's value.
			fmt.Fprintln(os.Stderr, "could not load env file")
			return 2
		}
	}
	keyVar := "CARTESIA_API_KEY"
	if o.Mode == "soniox" {
		keyVar = "SONIOX_API_KEY"
		o.Model = "stt-rt-v5" // Fixed production configuration, not a Cartesia model.
	}
	p := &probe{key: strings.TrimSpace(os.Getenv(keyVar))}
	if p.key == "" {
		fmt.Fprintln(os.Stderr, "missing "+keyVar)
		return 2
	}
	if o.Mode != "auto" && o.Mode != "manual" && o.Mode != "soniox" {
		p.printf(os.Stderr, "-mode must be auto, manual, or soniox\n")
		return 2
	}
	for _, n := range []float64{o.TailSeconds, o.IdleSeconds, o.TimeoutSeconds} {
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 86400 {
			p.printf(os.Stderr, "durations must be finite and between 0 and 86400 seconds\n")
			return 2
		}
	}
	if o.TimeoutSeconds == 0 || o.ChunkMS < 1 || o.ChunkMS > 10000 || (o.IdleSeconds == 0 && o.File == "") {
		p.printf(os.Stderr, "need positive timeout, chunk-ms in 1..10000, and -file unless -idle-secs > 0\n")
		return 2
	}
	query := url.Values{
		"model": {o.Model}, "encoding": {"pcm_s16le"}, "sample_rate": {"16000"},
		"cartesia_version": {"2026-08-14"},
	}
	var invalidModeFlag bool
	flag.Visit(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "turn-") {
			invalidModeFlag = invalidModeFlag || o.Mode != "auto"
			query.Set(strings.ReplaceAll(f.Name, "-", "_"), f.Value.String())
		}
		if f.Name == "language" || f.Name == "finalize" {
			invalidModeFlag = invalidModeFlag || o.Mode != "manual"
		}
		if f.Name == "keyterm" {
			invalidModeFlag = invalidModeFlag || o.Mode != "auto"
		}
	})
	if invalidModeFlag || (o.KeepaliveText != "" && o.IdleSeconds == 0) {
		p.printf(os.Stderr, "mode-specific flag used outside its mode (keepalive-text requires idle)\n")
		return 2
	}
	if o.Language != "" {
		query.Set("language", o.Language)
	}
	for _, term := range keyterms {
		query.Add("keyterm", term)
	}
	if o.Mode == "soniox" {
		query = url.Values{}
	}
	status := 0
	for k := 1; k <= max(1, *repeat); k++ {
		path := *jsonOut
		if *repeat > 1 && path != "" {
			path = repeatedJSONPath(path, k)
		}
		run, code := runSingle(o, query, p.key, path)
		if code != 0 {
			status = code
		}
		if *repeat > 1 {
			var firstSpeechMS *float64
			if len(run.Summary.Finals) > 0 {
				firstSpeechMS = run.Summary.Finals[0].SpeechMS
			}
			run.printf(os.Stdout, "REPEAT run %d/%d termination=%s finals=%d first_speech_latency_ms=%s errors=%d\n",
				k, *repeat, run.Run.Termination, len(run.Summary.Finals), formatMS(firstSpeechMS), len(run.Summary.Errors))
			if k < *repeat {
				time.Sleep(2 * time.Second)
			}
		}
	}
	return status
}

func repeatedJSONPath(path string, k int) string {
	ext := filepath.Ext(path)
	return strings.TrimSuffix(path, ext) + fmt.Sprintf(".run%d", k) + ext
}

func runSingle(o options, query url.Values, key, jsonOut string) (*probe, int) {
	p := &probe{key: key}
	p.Run.options = o
	p.Run.Query = query
	p.Run.Endpoint = "wss://api.cartesia.ai/stt/turns/websocket"
	if o.Mode == "manual" {
		p.Run.Endpoint = "wss://api.cartesia.ai/stt/websocket"
	} else if o.Mode == "soniox" {
		p.Run.Endpoint = "wss://stt-rt.soniox.com/transcribe-websocket"
	}
	p.Events = []event{}
	p.Run.StartedAt = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), seconds(o.TimeoutSeconds))
	defer cancel()
	var pcm []byte
	var err error
	if o.IdleSeconds == 0 {
		pcm, err = os.ReadFile(o.File)
		if err == nil && (len(pcm) == 0 || len(pcm)%2 != 0) {
			err = errors.New("PCM must be nonempty and contain whole int16 samples")
		}
		if err == nil {
			p.analyzePCM(pcm)
		}
	}
	if err == nil {
		err = p.run(ctx, pcm)
	}
	if err != nil {
		p.Run.Failure = p.safe(err.Error())
	}
	p.Run.FinishedAt = time.Now()
	p.summarize()
	p.printSummary()
	if jsonOut != "" {
		data, marshalErr := json.MarshalIndent(p.result, "", "  ")
		if marshalErr == nil {
			marshalErr = os.MkdirAll(filepath.Dir(jsonOut), 0700)
		}
		if marshalErr == nil {
			// Defense in depth: never persist the credential, even if reflected by a server.
			marshalErr = os.WriteFile(jsonOut, []byte(p.safe(string(data))+"\n"), 0600)
		}
		if marshalErr != nil {
			p.printf(os.Stderr, "write JSON: %v\n", marshalErr)
			return p, 1
		}
	}
	if err != nil {
		return p, 1
	}
	// An HTTP failure recovered by the one dial retry remains in the error
	// history, but does not turn a subsequently successful run into a failure.
	for _, e := range p.Events {
		if e.Type == "error" || e.Type == "read_error" {
			return p, 1
		}
		if e.Type == "websocket.close" {
			raw, _ := e.Raw.(map[string]any)
			if raw["code"] != 1000 && raw["code"] != 1005 {
				return p, 1
			}
		}
	}
	return p, 0
}

func (p *probe) analyzePCM(pcm []byte) {
	p.Run.AudioSHA256 = fmt.Sprintf("%x", sha256.Sum256(pcm))
	p.Run.AudioMS = number(float64(len(pcm)) / 32)
	p.Run.BoundaryRule = "abs(int16 sample) > 500; split at >=1000ms between active samples; sample timestamp = index/16000"
	last := -1
	for i := 0; i < len(pcm)/2; i++ {
		v := int(int16(binary.LittleEndian.Uint16(pcm[2*i:])))
		if v >= -500 && v <= 500 {
			continue
		}
		if last >= 0 && i-last >= 16000 {
			p.Run.Boundaries = append(p.Run.Boundaries, speechBoundary{FileMS: float64(last) / 16, sample: last})
		}
		last = i
	}
	if last >= 0 {
		p.Run.Boundaries = append(p.Run.Boundaries, speechBoundary{FileMS: float64(last) / 16, sample: last})
		p.Run.SpeechEndMS = number(float64(last) / 16)
	}
}

func (p *probe) dial(ctx context.Context) (*websocket.Conn, error) {
	// Soniox authenticates in the first JSON message; Cartesia uses a header.
	// Neither provider's URL ever contains a credential.
	var headers http.Header
	endpoint := p.Run.Endpoint
	if p.Run.Mode != "soniox" {
		headers = http.Header{"X-Api-Key": {p.key}}
		endpoint += "?" + p.Run.Query.Encode()
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	for attempt := 0; attempt < 2; attempt++ {
		started := time.Now()
		conn, response, err := dialer.DialContext(ctx, endpoint, headers)
		da := dialAttempt{At: started, DurationMS: ms(time.Since(started))}
		if response != nil {
			da.HTTPStatus = response.StatusCode
		}
		if err == nil {
			p.t0 = time.Now()
			p.Run.ConnectedAt = &p.t0
			p.Run.ConnectMS = number(da.DurationMS)
			p.Run.DialAttempts = append(p.Run.DialAttempts, da)
			p.printf(os.Stdout, "connected mode=%s model=%s handshake=%.1fms\n", p.Run.Mode, p.Run.Model, da.DurationMS)
			return conn, nil
		}
		da.Error = p.safe(err.Error())
		p.Run.DialAttempts = append(p.Run.DialAttempts, da)
		if response != nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			raw := map[string]any{"status_code": response.StatusCode, "body": p.safe(string(body)), "attempt": attempt + 1}
			if readErr != nil {
				raw["body_read_error"] = p.safe(readErr.Error())
			}
			p.record(event{At: time.Now(), Kind: "http", Type: "http_error", Raw: raw})
			p.printf(os.Stderr, "HTTP %d body: %s\n", response.StatusCode, body)
		} else {
			p.record(event{At: time.Now(), Kind: "transport", Type: "dial_error", Raw: map[string]any{"error": da.Error}})
		}
		if response == nil || attempt == 1 {
			p.Run.Termination = "dial_failed"
			return nil, fmt.Errorf("dial failed: %s", da.Error)
		}
		p.printf(os.Stderr, "retrying HTTP-level dial failure once after 10s\n")
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("dial attempts exhausted")
}

// One reader goroutine timestamps arrivals immediately; the main select loop is
// the sole data writer and owner of the event slice and measured speech markers.
func (p *probe) run(ctx context.Context, pcm []byte) error {
	conn, err := p.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetReadDeadline(deadline)
	incoming := make(chan event, 256)
	readerDone := make(chan struct{})
	stopReader := make(chan struct{})
	defer close(stopReader)
	enqueue := func(e event) {
		select {
		case incoming <- e:
		case <-stopReader:
		}
	}
	conn.SetPingHandler(func(s string) error {
		enqueue(event{At: time.Now(), Kind: "websocket_control", Type: "ping", MessageType: websocket.PingMessage, RawText: p.safe(s)})
		return conn.WriteControl(websocket.PongMessage, []byte(s), time.Now().Add(time.Second))
	})
	conn.SetPongHandler(func(s string) error {
		enqueue(event{At: time.Now(), Kind: "websocket_control", Type: "pong", MessageType: websocket.PongMessage, RawText: p.safe(s)})
		return nil
	})
	closeReceived := false // accessed only by the reader goroutine and its callbacks
	conn.SetCloseHandler(func(code int, reason string) error {
		closeReceived = true
		enqueue(event{At: time.Now(), Kind: "websocket_control", Type: "websocket.close", MessageType: websocket.CloseMessage,
			Raw: map[string]any{"code": code, "reason": p.safe(reason)}})
		return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""), time.Now().Add(time.Second))
	})
	go func() {
		defer close(readerDone)
		defer close(incoming)
		for {
			mt, data, readErr := conn.ReadMessage()
			now := time.Now()
			if readErr != nil {
				if !closeReceived {
					typeName := "read_error"
					if p.Run.Mode == "soniox" && errors.Is(readErr, net.ErrClosed) {
						// forceClose after finished:true closes our own socket. Keep
						// that observation without misreporting a provider failure.
						typeName = "local_close"
					}
					raw := map[string]any{"error": p.safe(readErr.Error())}
					var ce *websocket.CloseError
					if errors.As(readErr, &ce) {
						raw["code"], raw["reason"] = ce.Code, p.safe(ce.Text)
					}
					enqueue(event{At: now, Kind: "transport", Type: typeName, Raw: raw})
				}
				return
			}
			e := event{At: now, Kind: "server_message", MessageType: mt, Type: "unknown"}
			data = []byte(p.safe(string(data)))
			var head struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(data, &head) == nil && head.Type != "" {
				e.Type = head.Type
			}
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			if json.Valid(data) && decoder.Decode(&e.Raw) == nil {
				if p.Run.Mode == "soniox" {
					e.Type = "soniox_tokens"
					raw, _ := e.Raw.(map[string]any)
					if sonioxProviderError(raw) {
						e.Type = "error"
					}
				}
				if mt == websocket.TextMessage {
					e.RawText = string(data)
				}
			} else if mt == websocket.BinaryMessage {
				e.BinaryBase64 = base64.StdEncoding.EncodeToString(data)
			} else {
				e.RawText = string(data)
			}
			enqueue(e)
		}
	}()
	defer func() {
		_ = conn.Close()
		<-readerDone
	}()
	var ticks <-chan time.Time
	var ticker *time.Ticker
	var wait <-chan time.Time
	var idleTimer *time.Timer
	if p.Run.IdleSeconds > 0 {
		idleTimer = time.NewTimer(seconds(p.Run.IdleSeconds))
		defer idleTimer.Stop()
		wait = idleTimer.C
		if p.Run.KeepaliveText != "" {
			ticker = time.NewTicker(5 * time.Second)
		}
	} else {
		ticker = time.NewTicker(time.Duration(p.Run.ChunkMS) * time.Millisecond)
	}
	if ticker != nil {
		defer ticker.Stop()
		ticks = ticker.C
	}
	var teardownTimer *time.Timer
	var teardown <-chan time.Time
	defer func() {
		if teardownTimer != nil {
			teardownTimer.Stop()
		}
	}()
	closing := false
	var runErr error
	sendCommand := func(messageType int, data []byte, text, label string) {
		writeDeadline := time.Now().Add(5 * time.Second)
		if deadline.Before(writeDeadline) {
			writeDeadline = deadline
		}
		_ = conn.SetWriteDeadline(writeDeadline)
		writeErr := conn.WriteMessage(messageType, data)
		now := time.Now()
		a := action{At: now, ConnectMS: ms(now.Sub(p.t0)), Text: p.safe(text)}
		if writeErr != nil {
			a.Error = p.safe(writeErr.Error())
			runErr = writeErr
		}
		p.Run.Commands = append(p.Run.Commands, a)
		p.printf(os.Stdout, "+%.1fms client %s=%q error=%q\n", a.ConnectMS, label, a.Text, a.Error)
	}
	sendText := func(text string) {
		sendCommand(websocket.TextMessage, []byte(text), text, "text")
	}
	sendBinary := func(data []byte, description string) {
		sendCommand(websocket.BinaryMessage, data, description, "binary")
	}
	beginClose := func(reason string) {
		if closing {
			return
		}
		closing = true
		ticks, wait = nil, nil
		p.Run.Termination = reason
		if p.Run.Mode == "auto" {
			sendText(`{"type":"close"}`)
		} else if p.Run.Mode == "soniox" {
			sendBinary(nil, "<empty audio frame: end-of-audio marker>")
		} else {
			sendText("close")
		}
		teardownTimer = time.NewTimer(10 * time.Second)
		teardown = teardownTimer.C
	}
	forceClose := func(reason string) {
		p.Run.Termination = reason
		_ = conn.Close()
		// Drain queued events and the final read error so the artifact is complete.
		for e := range incoming {
			p.record(e)
		}
	}
	if p.Run.Mode == "soniox" {
		// Mirrors voicepipelinecore/stt_processor.go:sttConfigPayload. The
		// reader is already running, so an immediate rejection is captured.
		// Production's idle keepalive is unnecessary while streaming PCM.
		config, _ := json.Marshal(map[string]any{
			"api_key": p.key, "model": "stt-rt-v5", "audio_format": "s16le",
			"sample_rate": 16000, "num_channels": 1, "language_hints": []string{"hi"},
			"enable_endpoint_detection": true, "endpoint_latency_adjustment_level": 0,
			"endpoint_sensitivity": 0.0, "max_endpoint_delay_ms": 2000,
		})
		sendText(string(config))
		if runErr != nil {
			beginClose("write_error")
		}
	}
	offset := 0
	silence := make([]byte, p.Run.ChunkMS*32)
	var tailStarted time.Time
	for {
		select {
		case <-ctx.Done():
			forceClose("safety_timeout")
			return ctx.Err()
		case <-teardown:
			forceClose("close_wait_timeout")
			return errors.New("server did not close within 10s of close command")
		case e, ok := <-incoming:
			if !ok {
				if !closing {
					p.Run.Termination = "server_closed_before_client_close"
				}
				return runErr
			}
			p.record(e)
			if e.Type == "error" {
				beginClose("server_error")
			}
			if p.Run.Mode == "soniox" {
				raw, _ := e.Raw.(map[string]any)
				if finished, _ := raw["finished"].(bool); finished {
					forceClose("soniox_finished")
					return runErr
				}
			}
		case <-wait:
			beginClose("idle_wait_complete")
		case <-ticks:
			if p.Run.IdleSeconds > 0 {
				sendText(p.Run.KeepaliveText)
				if runErr != nil {
					beginClose("write_error")
				}
				continue
			}
			isFile := offset < len(pcm)
			data := silence
			end := offset
			if isFile {
				end = min(offset+len(silence), len(pcm))
				data = pcm[offset:end]
			}
			writeDeadline := time.Now().Add(5 * time.Second)
			if deadline.Before(writeDeadline) {
				writeDeadline = deadline
			}
			_ = conn.SetWriteDeadline(writeDeadline)
			if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				runErr = err
				beginClose("write_error")
				continue
			}
			now := time.Now()
			w := audioWrite{At: now, ConnectMS: ms(now.Sub(p.t0)), Bytes: len(data)}
			if isFile {
				w.FileEndMS = number(float64(end) / 32)
				for i := range p.Run.Boundaries {
					b := &p.Run.Boundaries[i]
					if b.sample*2 >= offset && b.sample*2 < end {
						b.SentAt, b.ConnectMS = &now, number(w.ConnectMS)
						if i == len(p.Run.Boundaries)-1 {
							p.Run.SpeechSentAt = &now
						}
						p.printf(os.Stdout, "+%.1fms speech boundary file=%.3fms chunk written\n", w.ConnectMS, b.FileMS)
					}
				}
				offset = end
				if offset == len(pcm) {
					tailStarted = now
					p.printf(os.Stdout, "+%.1fms file complete; tail silence starts\n", w.ConnectMS)
					if p.Run.Finalize {
						sendText("finalize")
					}
				}
			}
			p.Run.AudioWrites = append(p.Run.AudioWrites, w)
			if runErr != nil {
				beginClose("write_error")
			}
			if !tailStarted.IsZero() && now.Sub(tailStarted) >= seconds(p.Run.TailSeconds) {
				beginClose("tail_silence_complete")
			}
		}
	}
}

func (p *probe) record(e event) {
	if !p.t0.IsZero() {
		e.ConnectMS = number(ms(e.At.Sub(p.t0)))
	}
	if p.Run.SpeechSentAt != nil {
		e.SpeechMS = number(ms(e.At.Sub(*p.Run.SpeechSentAt)))
	}
	p.Events = append(p.Events, e)
	content := e.RawText
	if content == "" {
		b, _ := json.Marshal(e.Raw)
		content = string(b)
	}
	runes := []rune(content)
	if len(runes) > 240 {
		content = string(runes[:240]) + "…"
	}
	p.printf(os.Stdout, "%s connect=%sms speech=%sms %-18s %s\n", e.At.Format(time.RFC3339Nano), formatMS(e.ConnectMS), formatMS(e.SpeechMS), e.Type, content)
}

func (p *probe) summarize() {
	s := summary{Finals: []finalTranscript{}, Errors: []any{}}
	var interims []string
	var manual strings.Builder
	var lastFinal *event
	var sonioxFinal strings.Builder
	var lastInterimAt time.Time
	var interimIntervals []float64
	for i := range p.Events {
		e := &p.Events[i]
		// Earlier arrivals get a negative offset once the actual last-speech write
		// is known. Idle/all-silent/early-failure runs retain null, never guesses.
		if p.Run.SpeechSentAt != nil {
			e.SpeechMS = number(ms(e.At.Sub(*p.Run.SpeechSentAt)))
		}
		if e.Kind == "server_message" && s.FirstEventMS == nil {
			s.FirstEventMS = e.ConnectMS
		}
		raw, _ := e.Raw.(map[string]any)
		if p.Run.Mode == "soniox" && e.Kind == "server_message" {
			// Final tokens are deltas across messages. Non-finals are snapshots
			// of the uncommitted tail; never concatenate them into the finals.
			tokens, _ := raw["tokens"].([]any)
			hasNonFinal := false
			for _, item := range tokens {
				token, _ := item.(map[string]any)
				final, hasFinal := token["is_final"].(bool)
				if !hasFinal {
					continue
				}
				if !final {
					hasNonFinal = true
					continue
				}
				if text := stringField(token, "text"); text == "<end>" {
					s.Finals = append(s.Finals, finalTranscript{Text: sonioxFinal.String(), ConnectMS: e.ConnectMS, SpeechMS: e.SpeechMS})
					sonioxFinal.Reset()
				} else {
					sonioxFinal.WriteString(text)
				}
			}
			if hasNonFinal {
				s.NonFinals++
				if !lastInterimAt.IsZero() {
					interimIntervals = append(interimIntervals, ms(e.At.Sub(lastInterimAt)))
				}
				lastInterimAt = e.At
			}
		}
		switch e.Type {
		case "turn.update":
			s.Updates++
		case "turn.end":
			f := finalTranscript{Text: stringField(raw, "transcript"), ConnectMS: e.ConnectMS, SpeechMS: e.SpeechMS}
			// Assign the most recent *completed* acoustic region, never the whole
			// file's later speech end. Raw offsets remain available independently.
			for _, b := range p.Run.Boundaries {
				if b.SentAt != nil && !b.SentAt.After(e.At) {
					f.UtteranceMS, f.UtteranceFileMS = number(ms(e.At.Sub(*b.SentAt))), number(b.FileMS)
				}
			}
			s.Finals = append(s.Finals, f)
		case "transcript":
			final, hasFinal := raw["is_final"].(bool)
			if hasFinal && final {
				text := stringField(raw, "text")
				manual.WriteString(text)
				// A later empty final acknowledges silence; it does not change
				// when the concatenated transcript became available. Preserve
				// that event in the timeline without inflating transcript latency.
				if text != "" || lastFinal == nil {
					lastFinal = e
				}
			}
			if hasFinal && !final {
				interims = append(interims, stringField(raw, "text"))
			}
		case "error", "http_error", "dial_error", "read_error":
			s.Errors = append(s.Errors, e.Raw)
		case "websocket.close":
			if raw["code"] != 1000 && raw["code"] != 1005 {
				s.Errors = append(s.Errors, e.Raw)
			}
		}
	}
	if p.Run.Failure != "" {
		s.Errors = append(s.Errors, map[string]any{"probe_error": p.Run.Failure})
	}
	if p.Run.Mode == "manual" {
		if lastFinal != nil {
			s.Finals = append(s.Finals, finalTranscript{Text: manual.String(), ConnectMS: lastFinal.ConnectMS, SpeechMS: lastFinal.SpeechMS})
		}
		s.NonFinals = len(interims)
		s.InterimVerdict, s.InterimRule = interimVerdict(interims)
		s.InterimEvidence = interims[:min(3, len(interims))]
	}
	if p.Run.Mode == "soniox" && len(interimIntervals) > 0 {
		sort.Float64s(interimIntervals)
		n := len(interimIntervals)
		s.InterimMedianIntervalMS = number((interimIntervals[(n-1)/2] + interimIntervals[n/2]) / 2)
	}
	p.Summary = s
}

func sonioxProviderError(raw map[string]any) bool {
	if stringField(raw, "error_message") != "" {
		return true
	}
	switch code := raw["error_code"].(type) {
	case nil:
		return false
	case json.Number:
		n, err := code.Float64()
		return err != nil || n != 0
	case float64:
		return code != 0
	default:
		return true // Unexpected non-null codes must not silently look successful.
	}
}

func interimVerdict(texts []string) (string, string) {
	if len(texts) < 2 {
		return "inconclusive", fmt.Sprintf("only %d non-final events; at least two are needed for a pair comparison", len(texts))
	}
	prefixes, overlaps, empty := 0, 0, 0
	for i := 1; i < len(texts); i++ {
		a, b := texts[i-1], texts[i]
		if a == "" || b == "" {
			empty++
		}
		if strings.HasPrefix(b, a) {
			prefixes++
		}
		if fragmentsOverlap(a, b) {
			overlaps++
		}
	}
	rule := fmt.Sprintf("all %d consecutive pairs checked: %d exact earlier-text prefixes, %d lexical overlaps, %d empty pairs", len(texts)-1, prefixes, overlaps, empty)
	if empty == 0 && prefixes == len(texts)-1 {
		return "cumulative snapshot", rule + "; every later text extends or equals the earlier text"
	}
	if empty == 0 && prefixes == 0 && overlaps == 0 {
		return "delta", rule + "; adjacent texts are disjoint fragments, consistent with concatenation (inspect raw evidence for meaning)"
	}
	return "inconclusive / revised snapshots or mixed behavior", rule + "; neither the strict prefix rule nor the disjoint-fragment rule holds"
}

func fragmentsOverlap(a, b string) bool {
	// Whole-word overlap is intentionally conservative: revisions/resetting
	// hypotheses must not be mislabeled as deltas just because prefix fails.
	words := func(s string) []string {
		return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) && !unicode.IsMark(r) })
	}
	seen := map[string]bool{}
	for _, w := range words(a) {
		seen[w] = true
	}
	for _, w := range words(b) {
		if seen[w] {
			return true
		}
	}
	return strings.Contains(a, b) || strings.Contains(b, a)
}

func (p *probe) printSummary() {
	s := p.Summary
	p.printf(os.Stdout, "\nSUMMARY mode=%s model=%s\nhandshake=%sms first_event=%sms speechEndMs=%s termination=%s\n", p.Run.Mode, p.Run.Model, formatMS(p.Run.ConnectMS), formatMS(s.FirstEventMS), formatMS(p.Run.SpeechEndMS), p.Run.Termination)
	for i, f := range s.Finals {
		p.printf(os.Stdout, "final[%d] speech_latency=%sms utterance_latency=%sms utterance_file_end=%sms text=%q\n", i+1, formatMS(f.SpeechMS), formatMS(f.UtteranceMS), formatMS(f.UtteranceFileMS), f.Text)
	}
	if p.Run.Mode == "auto" {
		p.printf(os.Stdout, "turn.update count=%d\n", s.Updates)
	} else if p.Run.Mode == "soniox" {
		p.printf(os.Stdout, "non-final count=%d median interval=%sms\n", s.NonFinals, formatMS(s.InterimMedianIntervalMS))
	} else {
		p.printf(os.Stdout, "non-final count=%d interim verdict=%s\nrule: %s\n", s.NonFinals, s.InterimVerdict, s.InterimRule)
		for i, t := range s.InterimEvidence {
			p.printf(os.Stdout, "interim[%d]=%q\n", i+1, t)
		}
	}
	p.printf(os.Stdout, "errors (%d):\n", len(s.Errors))
	for _, e := range s.Errors {
		b, _ := json.Marshal(e)
		p.printf(os.Stdout, "%s\n", b)
	}
}

// No request headers are rendered. Redact even unexpected reflected credentials
// before stdout/stderr, raw event storage, and final JSON serialization.
func (p *probe) safe(s string) string {
	if p.key == "" {
		return s
	}
	escaped, _ := json.Marshal(p.key)
	for _, secret := range []string{p.key, strings.Trim(string(escaped), `"`), url.QueryEscape(p.key)} {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
	}
	return s
}

func (p *probe) printf(w io.Writer, format string, args ...any) {
	_, _ = io.WriteString(w, p.safe(fmt.Sprintf(format, args...)))
}
func number(n float64) *float64       { return &n }
func ms(d time.Duration) float64      { return float64(d) / float64(time.Millisecond) }
func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }
func formatMS(n *float64) string {
	if n == nil {
		return "n/a"
	}
	return fmt.Sprintf("%.1f", *n)
}
func stringField(m map[string]any, key string) string { s, _ := m[key].(string); return s }

// Same dotenv loader as luna-ws-probe: pre-existing environment always wins.
func loadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		value := strings.TrimSpace(parts[1])
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
}
