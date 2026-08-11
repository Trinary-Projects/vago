package voicepipelinecore

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"gopkg.in/hraban/opus.v2"
)

func newLiveKitLifecycleTestRoom(endReasons chan EndReason) *LiveKitRoom {
	return &LiveKitRoom{
		roomName: "test-room",
		taskCtx: &TaskContext{
			Ctx:       context.Background(),
			Logger:    log.New(io.Discard, "", 0),
			UIEvents:  NewUIEventSender(log.New(io.Discard, "", 0)),
			callStats: newCallStatsTracker(),
			EndTask: func(reason EndReason) {
				endReasons <- reason
			},
		},
	}
}

func TestLiveKitUnexpectedTerminalDisconnectEndsTask(t *testing.T) {
	endReasons := make(chan EndReason, 1)
	room := newLiveKitLifecycleTestRoom(endReasons)

	room.handleDisconnected(lksdk.Failed)

	select {
	case reason := <-endReasons:
		if reason != EndReasonError {
			t.Fatalf("end reason = %q, want %q", reason, EndReasonError)
		}
	default:
		t.Fatal("terminal disconnect did not request task end")
	}
}

func TestLiveKitLocalDisconnectCallbackIsIgnored(t *testing.T) {
	endReasons := make(chan EndReason, 1)
	room := newLiveKitLifecycleTestRoom(endReasons)
	room.closed.Store(true)

	room.handleDisconnected(lksdk.LeaveRequested)

	select {
	case reason := <-endReasons:
		t.Fatalf("local disconnect unexpectedly ended task with %q", reason)
	default:
	}
}

func TestLiveKitLocalCleanupParticipantDisconnectIsIgnored(t *testing.T) {
	endReasons := make(chan EndReason, 1)
	room := newLiveKitLifecycleTestRoom(endReasons)
	room.closed.Store(true)

	room.handleParticipantDisconnected("user-1")

	select {
	case reason := <-endReasons:
		t.Fatalf("local cleanup unexpectedly ended task with %q", reason)
	default:
	}
}

func TestLiveKitReconnectSuppressesSyntheticParticipantLeave(t *testing.T) {
	endReasons := make(chan EndReason, 1)
	room := newLiveKitLifecycleTestRoom(endReasons)
	room.taskCtx.callStats.MarkUserJoined(time.Now())

	room.handleReconnecting()
	room.handleParticipantDisconnected("user-1")

	if !room.reconnecting.Load() {
		t.Fatal("room should be marked reconnecting")
	}
	if !room.taskCtx.callStats.Present() {
		t.Fatal("synthetic reconnect leave should not mark user absent")
	}
	select {
	case reason := <-endReasons:
		t.Fatalf("reconnect leave unexpectedly ended task with %q", reason)
	default:
	}

	room.handleReconnected()
	if room.reconnecting.Load() {
		t.Fatal("room should clear reconnecting after recovery")
	}
}

func TestLiveKitWriteAudioPCMEncodesOpusLocally(t *testing.T) {
	track, err := lksdk.NewLocalTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus})
	if err != nil {
		t.Fatalf("NewLocalTrack: %v", err)
	}
	room := &LiveKitRoom{outTrack: track}
	if err := room.resetOpusEncoder(); err != nil {
		t.Fatalf("resetOpusEncoder: %v", err)
	}

	if err := room.WriteAudioPCM(make([]byte, framePCMBytes)); err != nil {
		t.Fatalf("WriteAudioPCM: %v", err)
	}
}

func TestLiveKitWriteAudioPCMEncodesPCMULocally(t *testing.T) {
	track, err := lksdk.NewLocalTrack(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: liveKitPCMUOutputRate})
	if err != nil {
		t.Fatalf("NewLocalTrack: %v", err)
	}
	room := &LiveKitRoom{outTrack: track, outCodec: liveKitOutputCodecPCMU}

	if err := room.WriteAudioPCM(make([]byte, pcmFrameBytesForRate(liveKitPCMUOutputRate))); err != nil {
		t.Fatalf("WriteAudioPCM: %v", err)
	}
}

func TestLiveKitClearAudioBufferResetsOpusEncoder(t *testing.T) {
	room := &LiveKitRoom{}
	if err := room.resetOpusEncoder(); err != nil {
		t.Fatalf("resetOpusEncoder: %v", err)
	}
	oldEncoder := room.opusEncoder

	room.ClearAudioBuffer()

	if room.opusEncoder == nil {
		t.Fatal("opus encoder was not recreated")
	}
	if room.opusEncoder == oldEncoder {
		t.Fatal("expected ClearAudioBuffer to reset encoder state")
	}
}

func TestLiveKitClearAudioBufferSkipsPCMUEncoder(t *testing.T) {
	room := &LiveKitRoom{outCodec: liveKitOutputCodecPCMU}
	room.ClearAudioBuffer()
	if room.opusEncoder != nil {
		t.Fatal("PCMU ClearAudioBuffer should not create an Opus encoder")
	}
}

func TestLiveKitPCMUCodecUsesEightKilohertz(t *testing.T) {
	room := &LiveKitRoom{outCodec: liveKitOutputCodecPCMU}
	if got := room.OutputSampleRate(); got != liveKitPCMUOutputRate {
		t.Fatalf("PCMU output sample rate = %d, want %d", got, liveKitPCMUOutputRate)
	}
	codec := room.outCodecCapability()
	if codec.MimeType != webrtc.MimeTypePCMU || codec.ClockRate != liveKitPCMUOutputRate {
		t.Fatalf("PCMU codec = %+v", codec)
	}
}

func TestLiveKitOutputCodecFromEnv(t *testing.T) {
	t.Setenv(liveKitOutCodecEnv, "pcmu")

	codec, err := liveKitOutputCodecFromEnv()
	if err != nil {
		t.Fatalf("liveKitOutputCodecFromEnv: %v", err)
	}
	if codec != liveKitOutputCodecPCMU {
		t.Fatalf("codec = %q, want %q", codec, liveKitOutputCodecPCMU)
	}
}

func TestLiveKitOutputCodecRejectsInvalidEnv(t *testing.T) {
	t.Setenv(liveKitOutCodecEnv, "aac")

	_, err := liveKitOutputCodecFromEnv()
	if err == nil {
		t.Fatal("expected invalid output codec to fail")
	}
	if !strings.Contains(err.Error(), liveKitOutCodecEnv) {
		t.Fatalf("error %q does not mention %s", err, liveKitOutCodecEnv)
	}
}

func TestLiveKitOpusEncoderConfigFromEnv(t *testing.T) {
	t.Setenv(liveKitOpusComplexityEnv, "3")
	t.Setenv(liveKitOpusBitrateEnv, "24000")
	t.Setenv(liveKitOpusMaxBandwidthEnv, "superwideband")

	room := &LiveKitRoom{}
	if err := room.resetOpusEncoder(); err != nil {
		t.Fatalf("resetOpusEncoder: %v", err)
	}

	complexity, err := room.opusEncoder.Complexity()
	if err != nil {
		t.Fatalf("Complexity: %v", err)
	}
	if complexity != 3 {
		t.Fatalf("complexity = %d, want 3", complexity)
	}

	bitrate, err := room.opusEncoder.Bitrate()
	if err != nil {
		t.Fatalf("Bitrate: %v", err)
	}
	if bitrate != 24000 {
		t.Fatalf("bitrate = %d, want 24000", bitrate)
	}

	bandwidth, err := room.opusEncoder.MaxBandwidth()
	if err != nil {
		t.Fatalf("MaxBandwidth: %v", err)
	}
	if bandwidth != opus.SuperWideband {
		t.Fatalf("max bandwidth = %v, want %v", bandwidth, opus.SuperWideband)
	}
}

func TestLiveKitOpusEncoderConfigRejectsInvalidEnv(t *testing.T) {
	t.Setenv(liveKitOpusComplexityEnv, "11")

	room := &LiveKitRoom{}
	err := room.resetOpusEncoder()
	if err == nil {
		t.Fatal("expected invalid opus complexity to fail")
	}
	if !strings.Contains(err.Error(), liveKitOpusComplexityEnv) {
		t.Fatalf("error %q does not mention %s", err, liveKitOpusComplexityEnv)
	}
}

func TestPCM16BytesToPCMU(t *testing.T) {
	samples := []int16{-32768, -30000, -20000, -10000, -5000, -1000, 0, 1, 1000, 5000, 10000, 20000, 30000, 32767}
	want := []byte{0x00, 0x02, 0x0c, 0x1c, 0x2b, 0x4e, 0xff, 0xff, 0xce, 0xab, 0x9c, 0x8c, 0x82, 0x80}
	raw := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(raw[i*2:], uint16(sample))
	}

	got := pcm16BytesToPCMU(raw)
	if len(got) != len(want) {
		t.Fatalf("encoded length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sample %d encoded to %#x, want %#x", i, got[i], want[i])
		}
	}
}
