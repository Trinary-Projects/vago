package voicepipelinecore

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type testWriteCloser struct {
	bytes.Buffer
}

func (w *testWriteCloser) Close() error { return nil }

func TestDailyRoomRespondsToRTVIPing(t *testing.T) {
	var out testWriteCloser
	room := &DailyRoom{
		roomName: "room-1",
		stdin:    &out,
	}

	room.handleAppMessage(json.RawMessage(`{
		"label":"rtvi-ai",
		"type":"client-message",
		"id":"msg-1",
		"data":{"t":"ping"}
	}`))

	var cmd dailyBridgeCommand
	if err := json.NewDecoder(&out).Decode(&cmd); err != nil {
		t.Fatalf("Decode command: %v", err)
	}
	if cmd.Type != "message" {
		t.Fatalf("command type = %q, want message", cmd.Type)
	}
	msg, ok := cmd.Data.(map[string]any)
	if !ok {
		t.Fatalf("command data = %#v, want object", cmd.Data)
	}
	if msg["label"] != "rtvi-ai" || msg["type"] != "server-response" || msg["id"] != "msg-1" {
		t.Fatalf("RTVI response mismatch: %+v", msg)
	}
	data, ok := msg["data"].(map[string]any)
	if !ok {
		t.Fatalf("response data = %#v, want object", msg["data"])
	}
	if data["t"] != "ping" || data["d"] != "pong" {
		t.Fatalf("response payload = %+v, want ping/pong", data)
	}
}

func TestDailyRoomRespondsToRTVIClientReady(t *testing.T) {
	var out testWriteCloser
	room := &DailyRoom{
		roomName: "room-1",
		stdin:    &out,
	}

	room.handleAppMessage(json.RawMessage(`{
		"label":"rtvi-ai",
		"type":"client-ready",
		"id":"ready-1",
		"data":{"version":"1.2.0","about":{"library":"test-client"}}
	}`))

	var cmd dailyBridgeCommand
	if err := json.NewDecoder(&out).Decode(&cmd); err != nil {
		t.Fatalf("Decode command: %v", err)
	}
	if cmd.Type != "message" {
		t.Fatalf("command type = %q, want message", cmd.Type)
	}
	msg, ok := cmd.Data.(map[string]any)
	if !ok {
		t.Fatalf("command data = %#v, want object", cmd.Data)
	}
	if msg["label"] != "rtvi-ai" || msg["type"] != "bot-ready" || msg["id"] != "ready-1" {
		t.Fatalf("RTVI bot-ready mismatch: %+v", msg)
	}
	data, ok := msg["data"].(map[string]any)
	if !ok {
		t.Fatalf("bot-ready data = %#v, want object", msg["data"])
	}
	if data["version"] != rtviProtocolVersion {
		t.Fatalf("bot-ready version = %v, want %s", data["version"], rtviProtocolVersion)
	}
	about, ok := data["about"].(map[string]any)
	if !ok || about["library"] != "talk-go" {
		t.Fatalf("bot-ready about = %#v, want talk-go library", data["about"])
	}
}

func TestDailyRoomWriteAudioPCMSkipsTimingWhenDiagnosticsDisabled(t *testing.T) {
	var out testWriteCloser
	room := &DailyRoom{
		stdin:       &out,
		audioTiming: newAudioTimingAggregator(),
		perfDiag:    false,
	}

	if err := room.WriteAudioPCM([]byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("WriteAudioPCM: %v", err)
	}
	if got := room.audioTiming.snapshotAndReset(); len(got) != 0 {
		t.Fatalf("audio timing entries = %+v, want none", got)
	}
}

func TestDailyRoomWriteAudioPCMRecordsTimingWhenDiagnosticsEnabled(t *testing.T) {
	var out testWriteCloser
	room := &DailyRoom{
		stdin:       &out,
		audioTiming: newAudioTimingAggregator(),
		perfDiag:    true,
	}

	if err := room.WriteAudioPCM([]byte{1, 2, 3, 4}); err != nil {
		t.Fatalf("WriteAudioPCM: %v", err)
	}
	if got := room.audioTiming.snapshotAndReset(); len(got) != 2 {
		t.Fatalf("audio timing entry count = %d, want 2: %+v", len(got), got)
	}
}

// TestDailyRoomParticipantLeftEndsTaskWhenConfigured verifies the
// existing sales/follow-up behavior is unchanged: EndOnParticipantLeft:
// true ends the task immediately on the non-bot participant leaving.
func TestDailyRoomParticipantLeftEndsTaskWhenConfigured(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.callStats = newCallStatsTracker()
	var ended []EndReason
	fix.TaskCtx.EndTask = func(reason EndReason) { ended = append(ended, reason) }

	room := &DailyRoom{
		roomName:             "room-1",
		taskCtx:              fix.TaskCtx,
		endOnParticipantLeft: true,
	}
	room.markUserJoined("user-1")
	room.handleEvent(dailyBridgeEvent{Event: "participant_left", ParticipantID: "user-1", Reason: "left"})

	if len(ended) != 1 || ended[0] != EndReasonClientDisconnect {
		t.Fatalf("expected EndTask(client_disconnect) exactly once, got %v", ended)
	}
}

// TestDailyRoomParticipantLeftDoesNotEndTaskWhenDisabled verifies
// onboarding's rejoin policy (item 5): a participant leaving with
// EndOnParticipantLeft: false records the leave + RTVI but does not end
// the task, mirrors Python's on_participant_left, and a rejoin followed
// by a second leave still does not end the task while call stats
// correctly accumulate both join/leave spans.
func TestDailyRoomParticipantLeftDoesNotEndTaskWhenDisabled(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.callStats = newCallStatsTracker()
	var ended []EndReason
	fix.TaskCtx.EndTask = func(reason EndReason) { ended = append(ended, reason) }

	room := &DailyRoom{
		roomName:             "room-1",
		taskCtx:              fix.TaskCtx,
		endOnParticipantLeft: false,
	}

	room.markUserJoined("user-1")
	if !fix.TaskCtx.callStats.Present() {
		t.Fatal("expected user present after join")
	}
	room.handleEvent(dailyBridgeEvent{Event: "participant_left", ParticipantID: "user-1", Reason: "left"})

	if len(ended) != 0 {
		t.Fatalf("expected no EndTask call, got %v", ended)
	}
	if fix.TaskCtx.callStats.Present() {
		t.Error("expected user marked not present after leave")
	}

	found := false
	for _, e := range fix.TaskCtx.UIEvents.Snapshot() {
		if e.Type == "server-message" {
			if s, ok := e.Data.(string); ok && strings.HasPrefix(s, "Participant left:") {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected 'Participant left: ...' RTVI message even without ending the task")
	}

	// Rejoin: call stats accumulate a second span, no second lifecycle
	// event, and a second leave still must not end the task.
	room.markUserJoined("user-1")
	if !fix.TaskCtx.callStats.Present() {
		t.Fatal("expected user present after rejoin")
	}
	room.handleEvent(dailyBridgeEvent{Event: "participant_left", ParticipantID: "user-1", Reason: "left again"})
	if len(ended) != 0 {
		t.Fatalf("expected still no EndTask call after second leave, got %v", ended)
	}
	if got := fix.TaskCtx.callStats.TotalDurationSec(time.Now()); got <= 0 {
		t.Errorf("expected accumulated duration across both join/leave spans, got %.3f", got)
	}
}

// TestDailyRoomParticipantJoinedLifecycleFiresOnceAcrossRejoin verifies
// OnUserJoined stays once-only even when the user rejoins mid-call
// (needed once EndOnParticipantLeft: false allows a rejoin to happen).
func TestDailyRoomParticipantJoinedLifecycleFiresOnceAcrossRejoin(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.callStats = newCallStatsTracker()
	var fires int
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnUserJoined: func(at time.Time) { fires++ },
	})
	defer fix.TaskCtx.callEvents.stopAndDrain()

	room := &DailyRoom{
		roomName:             "room-1",
		taskCtx:              fix.TaskCtx,
		endOnParticipantLeft: false,
	}
	room.markUserJoined("user-1")
	room.handleEvent(dailyBridgeEvent{Event: "participant_left", ParticipantID: "user-1", Reason: "left"})
	room.markUserJoined("user-1")
	fix.TaskCtx.callEvents.stopAndDrain()

	if fires != 1 {
		t.Errorf("expected OnUserJoined to fire exactly once across rejoin, got %d", fires)
	}
}

func TestJoinDailyRoomRetriesBridgeJoin(t *testing.T) {
	fix := newTestFixture(t)
	tmp := t.TempDir()
	countFile := filepath.Join(tmp, "count")
	script := filepath.Join(tmp, "bridge.sh")
	body := `#!/bin/sh
count=0
if [ -f "$DAILY_BRIDGE_TEST_COUNT" ]; then
  count=$(cat "$DAILY_BRIDGE_TEST_COUNT")
fi
count=$((count + 1))
echo "$count" > "$DAILY_BRIDGE_TEST_COUNT"
if [ "$count" -lt 3 ]; then
  printf '%s\n' '{"event":"error","message":"join failed"}'
  exit 0
fi
printf '%s\n' '{"event":"joined","participant_id":"bot","meeting_id":"meeting-1"}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"leave"'*) break ;;
  esac
done
printf '%s\n' '{"event":"left"}'
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile script: %v", err)
	}
	t.Setenv("DAILY_BRIDGE_PYTHON", "/bin/sh")
	t.Setenv("DAILY_BRIDGE_SCRIPT", script)
	t.Setenv("DAILY_BRIDGE_TEST_COUNT", countFile)

	oldRetryDelay := dailyJoinRetryDelay
	oldJoinTimeout := dailyJoinTimeout
	dailyJoinRetryDelay = 10 * time.Millisecond
	dailyJoinTimeout = time.Second
	t.Cleanup(func() {
		dailyJoinRetryDelay = oldRetryDelay
		dailyJoinTimeout = oldJoinTimeout
	})

	audioSource := NewAudioSourceProcessor(fix.TaskCtx)
	room, err := JoinDailyRoom("https://example.daily.co/test-room", "token", fix.TaskCtx, audioSource, DailyRoomOptions{EndOnParticipantLeft: true})
	if err != nil {
		t.Fatalf("JoinDailyRoom: %v", err)
	}
	room.Disconnect()
	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
	raw, err := os.ReadFile(countFile)
	if err != nil {
		t.Fatalf("ReadFile count: %v", err)
	}
	if got := string(bytes.TrimSpace(raw)); got != "3" {
		t.Fatalf("join attempts = %s, want 3", got)
	}
}

// endTaskRecorder captures EndTask(reason) calls from a background
// goroutine (the write-timeout/bridge-exit shutdown paths call it off
// the caller's goroutine) with the same mutex-guarded capture pattern
// used by the QueueProcessor/callStatsTracker helpers elsewhere in this
// package.
type endTaskRecorder struct {
	mu      sync.Mutex
	reasons []EndReason
}

func (e *endTaskRecorder) record(reason EndReason) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reasons = append(e.reasons, reason)
}

func (e *endTaskRecorder) snapshot() []EndReason {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]EndReason, len(e.reasons))
	copy(out, e.reasons)
	return out
}

// writeJoinedThenBlockScript writes a fake bridge shell script that
// reports "joined" and then stops reading/writing entirely (simulating
// daily-python's write_frames wedging forever on a dead send transport).
func writeJoinedThenBlockScript(t *testing.T, dir string) string {
	t.Helper()
	script := filepath.Join(dir, "bridge.sh")
	body := `#!/bin/sh
printf '%s\n' '{"event":"joined","participant_id":"bot","meeting_id":"meeting-1"}'
exec sleep 60
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile script: %v", err)
	}
	return script
}

// TestDailyRoomWriteTimeoutDeclaresBridgeDead covers the prod deadlock
// fix: a bridge that stops reading stdin mid-call (dead send transport)
// must not be allowed to hang WriteAudioPCM/SendAppMessage callers
// forever. The write deadline must fire, the bridge must be declared
// dead exactly once (EndTask(error) exactly once), further writes must
// no-op immediately, and the room must still shut down.
func TestDailyRoomWriteTimeoutDeclaresBridgeDead(t *testing.T) {
	fix := newTestFixture(t)
	rec := &endTaskRecorder{}
	fix.TaskCtx.EndTask = rec.record

	tmp := t.TempDir()
	script := writeJoinedThenBlockScript(t, tmp)
	t.Setenv("DAILY_BRIDGE_PYTHON", "/bin/sh")
	t.Setenv("DAILY_BRIDGE_SCRIPT", script)

	oldWriteTimeout := dailyBridgeWriteTimeout
	dailyBridgeWriteTimeout = 300 * time.Millisecond
	t.Cleanup(func() { dailyBridgeWriteTimeout = oldWriteTimeout })

	audioSource := NewAudioSourceProcessor(fix.TaskCtx)
	room, err := JoinDailyRoom("https://example.daily.co/test-room", "token", fix.TaskCtx, audioSource, DailyRoomOptions{EndOnParticipantLeft: true})
	if err != nil {
		t.Fatalf("JoinDailyRoom: %v", err)
	}

	// The OS pipe buffer is ~64KB; pump a few-KB payload in a bounded
	// loop until a write times out (pipe fills fast once the bridge
	// stops reading) or a generous overall deadline expires, so a
	// regression that removes the deadline fails the test instead of
	// hanging it.
	payload := make([]byte, 4*1024)
	overallDeadline := time.Now().Add(5 * time.Second)
	var timedOut bool
	for i := 0; i < 100000 && time.Now().Before(overallDeadline); i++ {
		writeErr := room.WriteAudioPCM(payload)
		if writeErr != nil {
			if errors.Is(writeErr, os.ErrDeadlineExceeded) {
				timedOut = true
			} else {
				t.Fatalf("WriteAudioPCM returned unexpected error: %v", writeErr)
			}
			break
		}
	}
	if !timedOut {
		t.Fatalf("expected a WriteAudioPCM call to time out with os.ErrDeadlineExceeded within the bound")
	}

	// Once dead, further writes must return immediately without
	// blocking or erroring.
	if err := room.WriteAudioPCM(payload); err != nil {
		t.Fatalf("WriteAudioPCM after bridge declared dead: %v", err)
	}

	// Disconnect runs on a separate goroutine spawned by the timeout
	// path and waits up to 5s before killing the wedged bridge process;
	// budget generously.
	if err := waitForWG(fix.WG, 10*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}

	reasons := rec.snapshot()
	if len(reasons) != 1 || reasons[0] != EndReasonError {
		t.Fatalf("expected EndTask(error) exactly once, got %v", reasons)
	}
}

// TestDailyRoomPostJoinBridgeExitEndsTask covers the second half of the
// deadlock fix: a bridge process that exits unexpectedly after join
// (not via Disconnect) must end the task instead of leaving a
// deaf-and-mute call alive until the 120s watchdog.
func TestDailyRoomPostJoinBridgeExitEndsTask(t *testing.T) {
	fix := newTestFixture(t)
	rec := &endTaskRecorder{}
	fix.TaskCtx.EndTask = rec.record

	tmp := t.TempDir()
	script := filepath.Join(tmp, "bridge.sh")
	body := `#!/bin/sh
printf '%s\n' '{"event":"joined","participant_id":"bot","meeting_id":"meeting-1"}'
exit 0
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile script: %v", err)
	}
	t.Setenv("DAILY_BRIDGE_PYTHON", "/bin/sh")
	t.Setenv("DAILY_BRIDGE_SCRIPT", script)

	audioSource := NewAudioSourceProcessor(fix.TaskCtx)
	room, err := JoinDailyRoom("https://example.daily.co/test-room", "token", fix.TaskCtx, audioSource, DailyRoomOptions{EndOnParticipantLeft: true})
	if err != nil {
		t.Fatalf("JoinDailyRoom: %v", err)
	}

	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}

	reasons := rec.snapshot()
	if len(reasons) != 1 || reasons[0] != EndReasonError {
		t.Fatalf("expected EndTask(error) exactly once, got %v", reasons)
	}

	// Disconnect afterwards must be safe/idempotent (closed is already
	// false since nothing called it, but the process is already gone).
	room.Disconnect()
	reasons = rec.snapshot()
	if len(reasons) != 1 {
		t.Fatalf("expected Disconnect to not trigger an extra EndTask, got %v", reasons)
	}
}

// TestDailyRoomIntentionalDisconnectDoesNotEndTask verifies a normal
// Disconnect()-driven shutdown (bridge exits only because it was told
// to leave) never fires EndTask via either new code path.
func TestDailyRoomIntentionalDisconnectDoesNotEndTask(t *testing.T) {
	fix := newTestFixture(t)
	rec := &endTaskRecorder{}
	fix.TaskCtx.EndTask = rec.record

	tmp := t.TempDir()
	script := filepath.Join(tmp, "bridge.sh")
	body := `#!/bin/sh
printf '%s\n' '{"event":"joined","participant_id":"bot","meeting_id":"meeting-1"}'
while IFS= read -r line; do
  case "$line" in
    *'"type":"leave"'*) break ;;
  esac
done
printf '%s\n' '{"event":"left"}'
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile script: %v", err)
	}
	t.Setenv("DAILY_BRIDGE_PYTHON", "/bin/sh")
	t.Setenv("DAILY_BRIDGE_SCRIPT", script)

	audioSource := NewAudioSourceProcessor(fix.TaskCtx)
	room, err := JoinDailyRoom("https://example.daily.co/test-room", "token", fix.TaskCtx, audioSource, DailyRoomOptions{EndOnParticipantLeft: true})
	if err != nil {
		t.Fatalf("JoinDailyRoom: %v", err)
	}

	room.Disconnect()
	if err := waitForWG(fix.WG, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}

	if reasons := rec.snapshot(); len(reasons) != 0 {
		t.Fatalf("expected no EndTask call on intentional disconnect, got %v", reasons)
	}
}
