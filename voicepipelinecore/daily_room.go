package voicepipelinecore

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaideep329/talk-go/internal/perfdiag"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const botIdentity = "bot"

const dailyJoinRetries = 3

const rtviProtocolVersion = "1.2.0"

var (
	dailyJoinRetryDelay = time.Second
	dailyJoinTimeout    = 20 * time.Second
)

type dailyBridgeEvent struct {
	Event         string          `json:"event"`
	ParticipantID string          `json:"participant_id"`
	MeetingID     string          `json:"meeting_id"`
	Reason        string          `json:"reason"`
	Message       json.RawMessage `json:"message"`
	Data          string          `json:"data"`
	SampleRate    int             `json:"sample_rate"`
	Channels      int             `json:"channels"`
}

type dailyBridgeCommand struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

type dailyAppMessage struct {
	Label string          `json:"label"`
	Type  string          `json:"type"`
	ID    string          `json:"id"`
	Data  json.RawMessage `json:"data"`
}

type rtviClientMessageData struct {
	T string `json:"t"`
	D any    `json:"d,omitempty"`
}

// DailyRoom owns the Daily media bridge process. Daily's official
// headless/server-side media SDK is Python, so Go keeps the pipeline and
// lifecycle while the bridge only joins Daily, relays PCM audio, and sends
// app messages.
type DailyRoom struct {
	roomURL     string
	roomName    string
	taskCtx     *TaskContext
	audioSource *AudioSourceProcessor
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	audioTiming *audioTimingAggregator
	perfDiag    bool
	writeMu     sync.Mutex
	waitDone    chan struct{}
	closed      atomic.Bool
	joinOnce    sync.Once
	joinResult  chan error
	greetOnce   sync.Once
}

func JoinDailyRoom(roomURL, token string, taskCtx *TaskContext, audioSource *AudioSourceProcessor) (*DailyRoom, error) {
	roomURL = strings.TrimSpace(roomURL)
	if roomURL == "" {
		return nil, errors.New("daily room url is required")
	}
	python, script := dailyBridgeConfig()
	var lastErr error
	for attempt := 1; attempt <= dailyJoinRetries; attempt++ {
		room, err := startDailyRoomAttempt(roomURL, token, python, script, taskCtx, audioSource)
		if err == nil {
			return room, nil
		}
		lastErr = err
		if taskCtx != nil && taskCtx.Logger != nil {
			taskCtx.Logger.Printf("Daily join attempt %d/%d failed: %v\n", attempt, dailyJoinRetries, err)
		}
		if attempt < dailyJoinRetries {
			done := taskDone(taskCtx)
			select {
			case <-time.After(dailyJoinRetryDelay):
			case <-done:
				return nil, taskCtx.Ctx.Err()
			}
		}
	}
	err := fmt.Errorf("failed to join Daily room after %d attempts: %w", dailyJoinRetries, lastErr)
	sentryutil.Capture(sentryutil.Event{
		Err:  err,
		Tags: map[string]string{"component": "daily", "operation": "join"},
		Details: map[string]any{
			"room_url": roomURL,
			"attempts": dailyJoinRetries,
		},
	})
	return nil, err
}

func dailyBridgeConfig() (python, script string) {
	python = strings.TrimSpace(os.Getenv("DAILY_BRIDGE_PYTHON"))
	if python == "" {
		python = "python3"
	}
	script = strings.TrimSpace(os.Getenv("DAILY_BRIDGE_SCRIPT"))
	if script == "" {
		script = "daily_bridge.py"
	}
	if !filepath.IsAbs(script) {
		if wd, err := os.Getwd(); err == nil {
			script = filepath.Join(wd, script)
		}
	}
	return python, script
}

func startDailyRoomAttempt(roomURL, token, python, script string, taskCtx *TaskContext, audioSource *AudioSourceProcessor) (*DailyRoom, error) {
	cmd := exec.Command(python, script, "--room-url", roomURL, "--token", token, "--bot-name", "Chatbot")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("daily bridge stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("daily bridge stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("daily bridge stderr: %w", err)
	}

	perfDiagEnabled := perfdiag.Enabled()
	room := &DailyRoom{
		roomURL:     roomURL,
		roomName:    dailyRoomNameFromURL(roomURL),
		taskCtx:     taskCtx,
		audioSource: audioSource,
		cmd:         cmd,
		stdin:       stdin,
		perfDiag:    perfDiagEnabled,
		waitDone:    make(chan struct{}),
		joinResult:  make(chan error, 1),
	}
	if perfDiagEnabled {
		room.audioTiming = newAudioTimingAggregator()
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start daily bridge: %w", err)
	}
	if perfDiagEnabled && cmd.Process != nil {
		room.goTracked(func() { room.monitorProcessUsage(os.Getpid(), cmd.Process.Pid) })
	}
	if perfDiagEnabled {
		room.goTracked(func() { room.monitorAudioTiming() })
	}
	room.goTracked(func() { room.readStdout(stdout) })
	room.goTracked(func() { room.readStderr(stderr) })
	room.goTracked(func() {
		err := cmd.Wait()
		if err != nil && !room.closed.Load() {
			room.log("Daily bridge exited: %v", err)
			room.finishJoin(fmt.Errorf("daily bridge exited before join: %w", err))
		}
		close(room.waitDone)
	})

	select {
	case err := <-room.joinResult:
		if err != nil {
			room.Disconnect()
			return nil, err
		}
	case <-time.After(dailyJoinTimeout):
		room.Disconnect()
		return nil, errors.New("daily bridge join timed out")
	}
	return room, nil
}

func taskDone(taskCtx *TaskContext) <-chan struct{} {
	if taskCtx == nil || taskCtx.Ctx == nil {
		return nil
	}
	return taskCtx.Ctx.Done()
}

func (r *DailyRoom) RoomURL() string {
	return r.roomURL
}

func (r *DailyRoom) RoomName() string {
	return r.roomName
}

func (r *DailyRoom) OutputSampleRate() int {
	return defaultOutputSampleRate
}

func (r *DailyRoom) SendAppMessage(v interface{}) error {
	return r.writeCommand(dailyBridgeCommand{Type: "message", Data: v})
}

func (r *DailyRoom) WriteAudioPCM(pcm []byte) error {
	if len(pcm) == 0 {
		return nil
	}
	if !r.perfDiagnosticsEnabled() {
		encoded := base64.StdEncoding.EncodeToString(pcm)
		return r.writeCommand(dailyBridgeCommand{Type: "audio", Data: encoded})
	}
	start := time.Now()
	encoded := base64.StdEncoding.EncodeToString(pcm)
	r.recordAudioTiming("go_bridge_out_base64_encode", time.Since(start))
	start = time.Now()
	err := r.writeCommand(dailyBridgeCommand{Type: "audio", Data: encoded})
	r.recordAudioTiming("go_bridge_out_json_stdin", time.Since(start))
	return err
}

func (r *DailyRoom) ClearAudioBuffer() {}

func (r *DailyRoom) Disconnect() {
	if r == nil || !r.closed.CompareAndSwap(false, true) {
		return
	}
	_ = r.writeCommandAllowClosed(dailyBridgeCommand{Type: "leave"}, true)
	if r.stdin != nil {
		_ = r.stdin.Close()
	}
	select {
	case <-r.waitDone:
	case <-time.After(5 * time.Second):
		if r.cmd != nil && r.cmd.Process != nil {
			_ = r.cmd.Process.Kill()
		}
		<-r.waitDone
	}
}

func (r *DailyRoom) writeCommand(cmd dailyBridgeCommand) error {
	return r.writeCommandAllowClosed(cmd, false)
}

func (r *DailyRoom) writeCommandAllowClosed(cmd dailyBridgeCommand, allowClosed bool) error {
	if r == nil || r.stdin == nil || r.closed.Load() {
		if !allowClosed {
			return nil
		}
	}
	if r == nil || r.stdin == nil {
		return nil
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return json.NewEncoder(r.stdin).Encode(cmd)
}

func (r *DailyRoom) readStdout(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var event dailyBridgeEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			r.log("Daily bridge event decode error: %v", err)
			continue
		}
		r.handleEvent(event)
	}
	if err := scanner.Err(); err != nil && !r.closed.Load() {
		r.log("Daily bridge stdout error: %v", err)
	}
}

func (r *DailyRoom) readStderr(stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		r.log("Daily bridge: %s", scanner.Text())
	}
}

func (r *DailyRoom) handleEvent(event dailyBridgeEvent) {
	switch event.Event {
	case "joined":
		at := time.Now()
		r.log("[%s] Bot joined Daily room meeting_id=%s participant_id=%s", r.roomName, event.MeetingID, event.ParticipantID)
		if r.taskCtx != nil {
			r.taskCtx.UIEvents.SetDailyMeeting(event.MeetingID, event.ParticipantID)
		}
		if r.taskCtx != nil && r.taskCtx.callEvents != nil {
			r.taskCtx.callEvents.fireBotJoined(at)
		}
		r.finishJoin(nil)
	case "participant_joined":
		r.markUserJoined(event.ParticipantID)
	case "participant_left":
		at := time.Now()
		r.markUserLeft(event.ParticipantID)
		r.log("[%s] Daily participant %q left, requesting EndFrame: %s", r.roomName, event.ParticipantID, event.Reason)
		if r.taskCtx != nil {
			r.taskCtx.UIEvents.ServerMessage("Participant left: "+event.Reason, at)
		}
		r.endTask(EndReasonClientDisconnect)
	case "audio":
		var raw []byte
		var err error
		if r.perfDiagnosticsEnabled() {
			start := time.Now()
			raw, err = base64.StdEncoding.DecodeString(event.Data)
			r.recordAudioTiming("go_bridge_in_base64_decode", time.Since(start))
		} else {
			raw, err = base64.StdEncoding.DecodeString(event.Data)
		}
		if err != nil {
			r.log("Daily bridge audio decode error: %v", err)
			return
		}
		if r.perfDiagnosticsEnabled() {
			start := time.Now()
			r.audioSource.PushPCM(raw, event.SampleRate, event.Channels)
			r.recordAudioTiming("go_bridge_in_push_pcm", time.Since(start))
		} else {
			r.audioSource.PushPCM(raw, event.SampleRate, event.Channels)
		}
	case "app_message":
		r.handleAppMessage(event.Message)
	case "left":
		r.log("[%s] Daily bridge left room", r.roomName)
	case "error":
		var message string
		if len(event.Message) > 0 {
			_ = json.Unmarshal(event.Message, &message)
		}
		if message == "" {
			message = "daily bridge error"
		}
		r.log("[%s] %s", r.roomName, message)
		err := errors.New(message)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "daily", "operation": "bridge_event"},
			Details: map[string]any{
				"room_name": r.roomName,
			},
		})
		r.finishJoin(err)
	}
}

func (r *DailyRoom) handleAppMessage(raw json.RawMessage) {
	var msg dailyAppMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		r.log("[%s] Daily app message malformed: %v", r.roomName, err)
		return
	}
	if msg.Type == "end_call" {
		r.log("[%s] UI requested end_call via Daily app message", r.roomName)
		r.endTask(EndReasonClientDisconnect)
		return
	}
	if msg.Type == "ping" {
		r.sendLegacyPong()
		return
	}
	if msg.Label == rtviDebugLabel && msg.Type == "client-ready" {
		r.sendRTVIBotReady(msg.ID)
		return
	}
	if msg.Label != rtviDebugLabel || msg.Type != "client-message" {
		return
	}
	var data rtviClientMessageData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		r.log("[%s] RTVI client-message malformed: %v", r.roomName, err)
		return
	}
	switch data.T {
	case "ping":
		r.sendRTVIServerResponse(msg.ID, data.T, "pong")
	}
}

func (r *DailyRoom) sendLegacyPong() {
	if err := r.SendAppMessage(map[string]any{"type": "pong"}); err != nil {
		r.log("[%s] Daily pong publish error: %v", r.roomName, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "daily", "operation": "legacy_pong"},
		})
	}
}

func (r *DailyRoom) sendRTVIServerResponse(id, messageType string, data any) {
	id = rtviMessageID(id)
	resp := map[string]any{
		"label": rtviDebugLabel,
		"type":  "server-response",
		"id":    id,
		"data": map[string]any{
			"t": messageType,
			"d": data,
		},
	}
	if err := r.SendAppMessage(resp); err != nil {
		r.log("[%s] RTVI server-response publish error: %v", r.roomName, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "daily", "operation": "rtvi_server_response"},
			Details: map[string]any{
				"id":   id,
				"type": messageType,
			},
		})
	}
}

func (r *DailyRoom) sendRTVIBotReady(id string) {
	id = rtviMessageID(id)
	msg := map[string]any{
		"label": rtviDebugLabel,
		"type":  "bot-ready",
		"id":    id,
		"data": map[string]any{
			"version": rtviProtocolVersion,
			"about": map[string]any{
				"library": "talk-go",
			},
		},
	}
	if err := r.SendAppMessage(msg); err != nil {
		r.log("[%s] RTVI bot-ready publish error: %v", r.roomName, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "daily", "operation": "rtvi_bot_ready"},
			Details: map[string]any{
				"id": id,
			},
		})
	}
}

func rtviMessageID(id string) string {
	if strings.TrimSpace(id) != "" {
		return id
	}
	return fmt.Sprintf("go-%d", time.Now().UnixNano())
}

func (r *DailyRoom) markUserJoined(participantID string) {
	if participantID == "" || participantID == botIdentity {
		return
	}
	at := time.Now()
	r.log("[%s] Daily participant %q joined", r.roomName, participantID)
	if r.taskCtx == nil {
		return
	}
	r.taskCtx.UIEvents.SetUserSessionID(participantID)
	if r.taskCtx.callStats != nil {
		r.taskCtx.callStats.MarkUserJoined(at)
	}
	if r.taskCtx.callEvents != nil {
		r.taskCtx.callEvents.fireUserJoined(at)
	}
	// The user is present; the bot always speaks first. Queue an
	// LLMMessagesAppendFrame (no new messages, run the LLM) so
	// UserContextAggregator runs the first turn from the initial context.
	// Fires once per call. Mirrors Pipecat's on_client_connected ->
	// queue_frames([... run_llm ...]).
	//
	// Use QueueFrame, not PushFrame: participant_joined can fire during
	// JoinDailyRoom — before SetPipeline links neighbours and before
	// Start — and daily_bridge.py replays it for already-present
	// participants right after `joined`. PushFrame would drop the frame
	// (next is still nil) and greetOnce would prevent a retry. QueueFrame
	// buffers it on the audio source's own input channel until its loops
	// start, so the greeting survives the pre-start/link timing.
	if r.audioSource != nil {
		r.greetOnce.Do(func() {
			r.log("[%s] User joined; starting bot greeting (first turn)", r.roomName)
			r.audioSource.QueueFrame(NewLLMMessagesAppendFrame(nil, true), Downstream)
		})
	}
}

func (r *DailyRoom) markUserLeft(participantID string) {
	if participantID == "" || participantID == botIdentity || r.taskCtx == nil || r.taskCtx.callStats == nil {
		return
	}
	r.taskCtx.callStats.MarkUserLeft(time.Now())
}

func (r *DailyRoom) endTask(reason EndReason) {
	if r.taskCtx != nil && r.taskCtx.EndTask != nil {
		r.taskCtx.EndTask(reason)
	}
}

func (r *DailyRoom) finishJoin(err error) {
	r.joinOnce.Do(func() {
		r.joinResult <- err
	})
}

func (r *DailyRoom) goTracked(fn func()) {
	if r.taskCtx != nil && r.taskCtx.wg != nil {
		r.taskCtx.wg.Add(1)
		go func() {
			defer r.taskCtx.wg.Done()
			fn()
		}()
		return
	}
	go fn()
}

func (r *DailyRoom) log(format string, args ...interface{}) {
	if r.taskCtx != nil && r.taskCtx.Logger != nil {
		r.taskCtx.Logger.Printf(format, args...)
	}
}

func dailyRoomNameFromURL(roomURL string) string {
	roomURL = strings.TrimRight(roomURL, "/")
	idx := strings.LastIndex(roomURL, "/")
	if idx < 0 || idx == len(roomURL)-1 {
		return roomURL
	}
	return roomURL[idx+1:]
}
