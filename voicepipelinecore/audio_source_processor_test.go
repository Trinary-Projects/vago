package voicepipelinecore

import (
	"testing"
	"time"
)

// TestAudioSource_ForwardsEndFrame verifies AudioSourceProcessor's
// ProcessFrame forwards EndFrame downstream. Daily bridge audio ingress is
// covered by PushPCM-focused checks and integration testing.
func TestAudioSource_ForwardsEndFrame(t *testing.T) {
	fix := newTestFixture(t)
	a := NewAudioSourceProcessor(fix.TaskCtx)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor:    a,
		framesToSend: []Frame{},
		sendEndFrame: true,
	})

	// EndFrame is stripped from `down` by runProcessorTest, so down
	// should be empty. The fact that the test reached this point without
	// timing out means the EndFrame propagated correctly.
	if len(down) != 0 {
		t.Errorf("expected no downstream frames besides EndFrame, got %s", describeFrameTypes(down))
	}
}

// TestAudioSource_PassesThroughUnknownFrames verifies the default-forward
// behaviour for unhandled frame types.
func TestAudioSource_PassesThroughUnknownFrames(t *testing.T) {
	fix := newTestFixture(t)
	a := NewAudioSourceProcessor(fix.TaskCtx)

	down, _ := runProcessorTest(t, fix, runConfig{
		processor: a,
		framesToSend: []Frame{
			TextFrame{Text: "pass through"},
		},
		sendEndFrame: true,
	})

	if c := countFrames[TextFrame](down); c != 1 {
		t.Errorf("expected TextFrame to pass through, got %d in: %s", c, describeFrameTypes(down))
	}
}

func TestAudioSource_MarksFirstAudibleUserAudioOnce(t *testing.T) {
	fix := newTestFixture(t)
	fix.TaskCtx.callStats = newCallStatsTracker()
	calls := make(chan time.Time, 2)
	fix.TaskCtx.callEvents = newCallEventDispatcher(fix.Logger, CallEvents{
		OnFirstUserAudio: func(at time.Time) { calls <- at },
	})
	defer fix.TaskCtx.callEvents.stopAndDrain()
	a := NewAudioSourceProcessor(fix.TaskCtx)

	a.maybeMarkFirstUserAudio([]int16{0, 999, -1000}, time.Now())
	select {
	case <-calls:
		t.Fatal("quiet samples should not fire OnFirstUserAudio")
	default:
	}

	a.maybeMarkFirstUserAudio([]int16{0, 1001}, time.Now())
	a.maybeMarkFirstUserAudio([]int16{2000}, time.Now())
	fix.TaskCtx.callEvents.stopAndDrain()

	select {
	case <-calls:
	default:
		t.Fatal("audible samples should fire OnFirstUserAudio")
	}
	select {
	case <-calls:
		t.Fatal("OnFirstUserAudio should only fire once")
	default:
	}
	if fix.TaskCtx.callStats.FirstUserAudioFrameAt().IsZero() {
		t.Fatal("call stats first audio timestamp was not recorded")
	}
}
