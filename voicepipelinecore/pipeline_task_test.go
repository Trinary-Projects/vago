package voicepipelinecore

import (
	"context"
	"io"
	"log"
	"testing"
	"time"
)

func TestPipelineTaskRunCleanupCallsOnCallEndedWithStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.New(io.Discard, "", 0)
	callStats := newCallStatsTracker()
	start := time.Now().Add(-3 * time.Second)
	callStats.MarkUserJoined(start)
	callStats.MarkFirstUserAudio(start.Add(time.Second))

	got := make(chan struct {
		reason EndReason
		stats  CallStats
	}, 1)
	task := &PipelineTask{
		TaskCtx: &TaskContext{
			Ctx:      ctx,
			Logger:   logger,
			UIEvents: NewUIEventSender(logger),
		},
		Cancel:    cancel,
		Pipeline:  NewPipeline(nil),
		callStats: callStats,
		onCallEnded: func(reason EndReason, stats CallStats) {
			got <- struct {
				reason EndReason
				stats  CallStats
			}{reason: reason, stats: stats}
		},
	}

	task.runCleanup(NewEndFrame(string(EndReasonClientDisconnect)))

	select {
	case call := <-got:
		if call.reason != EndReasonClientDisconnect {
			t.Fatalf("reason = %q, want %q", call.reason, EndReasonClientDisconnect)
		}
		if call.stats.TotalUserDurationSec <= 0 {
			t.Fatalf("total user duration should be positive, got %.3f", call.stats.TotalUserDurationSec)
		}
		if call.stats.FirstUserAudioFrameAt.IsZero() {
			t.Fatal("first user audio timestamp should be present")
		}
		if call.stats.EndedAt.IsZero() {
			t.Fatal("ended_at should be present")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnCallEnded was not called")
	}
}

func TestPipelineTaskEndCanQueueBeforePipelineAttached(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task, err := NewPipelineTask(ctx, TaskConfig{
		Logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatalf("NewPipelineTask: %v", err)
	}

	source := NewPipelineSourceProcessor(task.TaskCtx)
	task.AttachSource(source)

	// This is the BuildTask race window: the room transport can request an end
	// after the source exists but before the full processor chain is attached.
	task.End(EndReasonClientDisconnect)

	gotEnd := make(chan EndFrame, 1)
	sink := NewPipelineSinkProcessor(task.TaskCtx, func(frame EndFrame) {
		gotEnd <- frame
	})
	task.SetPipeline(source, NewPipeline([]Processor{source, sink}))
	task.Start()

	select {
	case frame := <-gotEnd:
		if frame.Reason != string(EndReasonClientDisconnect) {
			t.Fatalf("EndFrame reason = %q, want %q", frame.Reason, EndReasonClientDisconnect)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued EndFrame did not reach pipeline sink")
	}

	task.Pipeline.Stop()
	task.Cancel()
	if err := waitForWG(&task.wg, 2*time.Second); err != nil {
		t.Fatalf("waitForWG: %v", err)
	}
}
