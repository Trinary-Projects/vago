package voicepipelinecore

import (
	"encoding/binary"
	"testing"
	"time"
)

func TestPlaybackBotPCMFrameConvertsRawPCMBytes(t *testing.T) {
	fix := newTestFixture(t)
	p := &PlaybackSinkProcessor{taskCtx: fix.TaskCtx}

	input := make([]byte, framePCMBytes)
	want := []int16{-32768, -1234, 0, 1234, 32767}
	for i, sample := range want {
		binary.LittleEndian.PutUint16(input[i*2:], uint16(sample))
	}

	got := p.botPCMFrame(input)
	if len(got) != framePCM {
		t.Fatalf("expected %d samples, got %d", framePCM, len(got))
	}
	for i, sample := range want {
		if got[i] != sample {
			t.Fatalf("sample %d: expected %d, got %d", i, sample, got[i])
		}
	}
	for i := len(want); i < len(got); i++ {
		if got[i] != 0 {
			t.Fatalf("expected trailing sample %d to remain silent, got %d", i, got[i])
		}
	}
}

func TestResampleMonoPCMDownsamplesBackground(t *testing.T) {
	input := make([]int16, 240)
	for i := range input {
		input[i] = int16(i)
	}

	got := resampleMonoPCM(input, 24000, 8000)
	if len(got) != 80 {
		t.Fatalf("resampled length = %d, want 80", len(got))
	}
	for i := 0; i < 5; i++ {
		if got[i] != input[i*3] {
			t.Fatalf("sample %d = %d, want %d", i, got[i], input[i*3])
		}
	}
}

func TestPlaybackSinkForwardsInterruptDownstream(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.Room = &testOutputRoom{outputSampleRate: defaultOutputSampleRate}
	p := NewPlaybackSinkProcessor(fix.TaskCtx)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewInterruptFrame(),
		},
		settleDelay:  30 * time.Millisecond,
		sendEndFrame: true,
	})

	if c := countFrames[InterruptFrame](down); c != 1 {
		t.Fatalf("expected InterruptFrame forwarded downstream, got %d in %s", c, describeFrameTypes(down))
	}
}

func TestPlaybackSinkInterruptDropsQueuedUnplayedReconciliationFrames(t *testing.T) {
	fix := newTestFixture(t)
	p := NewPlaybackSinkProcessor(fix.TaskCtx)
	sink := newQueueProcessor(fix.TaskCtx, "test-sink", Downstream)
	p.Link(sink)
	sink.Start(fix.RootCtx)

	p.playbackQueue = []Frame{
		NewAudioFrame(make([]byte, framePCMBytes)),
		NewWordTimestampFrame([]string{"unheard"}),
		NewTTSDoneFrame(),
		NewEndFrame("shutdown"),
	}

	p.handleQueueFrame(NewInterruptFrame())
	time.Sleep(20 * time.Millisecond)

	if len(p.playbackQueue) != 1 {
		t.Fatalf("expected only EndFrame to survive playback interrupt, got %s", describeFrameTypes(p.playbackQueue))
	}
	if _, ok := p.playbackQueue[0].(EndFrame); !ok {
		t.Fatalf("expected EndFrame to survive playback interrupt, got %s", describeFrameTypes(p.playbackQueue))
	}

	down := sink.Captured()
	if c := countFrames[InterruptFrame](down); c != 1 {
		t.Fatalf("expected InterruptFrame forwarded downstream, got %d in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[WordTimestampFrame](down); c != 0 {
		t.Fatalf("queued WordTimestampFrame should be dropped as unplayed, got %d in %s", c, describeFrameTypes(down))
	}
	if c := countFrames[TTSDoneFrame](down); c != 0 {
		t.Fatalf("queued TTSDoneFrame should be dropped as unplayed, got %d in %s", c, describeFrameTypes(down))
	}

	stopProcessorsAndWait(t, fix, 3*time.Second, sink)
}
