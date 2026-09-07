package voicepipelinecore

import (
	"bytes"
	"testing"
	"time"
)

const customOutputSampleRate = 48000

type testOutputRoom struct {
	outputSampleRate int
}

func (r *testOutputRoom) RoomURL() string          { return "test-room-url" }
func (r *testOutputRoom) RoomName() string         { return "test-room" }
func (r *testOutputRoom) OutputSampleRate() int    { return r.outputSampleRate }
func (r *testOutputRoom) SendAppMessage(any) error { return nil }
func (r *testOutputRoom) WriteAudioPCM([]byte) error {
	return nil
}
func (r *testOutputRoom) ClearAudioBuffer()                       {}
func (r *testOutputRoom) Disconnect()                             {}
func (r *testOutputRoom) recordAudioTiming(string, time.Duration) {}
func (r *testOutputRoom) perfDiagnosticsEnabled() bool            { return false }

func TestTTSUsesRoomOutputSampleRateForFrames(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.Room = &testOutputRoom{outputSampleRate: customOutputSampleRate}
	p := NewTTSProcessor(fix.TaskCtx, nil, "")

	wantFrameBytes := pcmFrameBytesForRate(customOutputSampleRate)
	input := make([]byte, wantFrameBytes+4)
	for i := range input {
		input[i] = byte(i % 251)
	}

	p.pcmBuffer = append([]byte(nil), input...)
	frame := p.nextPCMFrame()

	if len(frame) != wantFrameBytes {
		t.Fatalf("expected %d-byte PCM frame, got %d", wantFrameBytes, len(frame))
	}
	if !bytes.Equal(frame, input[:wantFrameBytes]) {
		t.Fatal("PCM frame bytes were modified")
	}
	if !bytes.Equal(p.pcmBuffer, input[wantFrameBytes:]) {
		t.Fatal("PCM buffer was not advanced by exactly one frame")
	}
}

func TestPlaybackUsesRoomOutputSampleRateForFrames(t *testing.T) {
	fix := newTestFixture(t)
	room := &testOutputRoom{outputSampleRate: customOutputSampleRate}
	fix.TaskCtx.Room = room
	p := NewPlaybackSinkProcessor(fix.TaskCtx)

	wantSamples := pcmSamplesPerFrameForRate(customOutputSampleRate)
	wantBytes := pcmFrameBytesForRate(customOutputSampleRate)
	if p.frameSamples != wantSamples || p.frameBytes != wantBytes {
		t.Fatalf("playback frame shape = %d samples/%d bytes, want %d/%d", p.frameSamples, p.frameBytes, wantSamples, wantBytes)
	}

	got := p.botPCMFrame(make([]byte, wantBytes))
	if len(got) != wantSamples {
		t.Fatalf("expected %d samples, got %d", wantSamples, len(got))
	}
	if len(p.mixPCM(got)) != wantBytes {
		t.Fatalf("mixed frame length mismatch")
	}
}

func TestRoomOutputSampleRates(t *testing.T) {
	if got := (&DailyRoom{}).OutputSampleRate(); got != defaultOutputSampleRate {
		t.Fatalf("Daily output sample rate = %d, want %d", got, defaultOutputSampleRate)
	}
	if got := (&LiveKitRoom{}).OutputSampleRate(); got != defaultOutputSampleRate {
		t.Fatalf("LiveKit output sample rate = %d, want %d", got, defaultOutputSampleRate)
	}
	if got := (&LiveKitRoom{outCodec: liveKitOutputCodecPCMU}).OutputSampleRate(); got != liveKitPCMUOutputRate {
		t.Fatalf("LiveKit PCMU output sample rate = %d, want %d", got, liveKitPCMUOutputRate)
	}
}
