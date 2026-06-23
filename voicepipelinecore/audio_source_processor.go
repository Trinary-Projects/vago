package voicepipelinecore

import (
	"context"
	"encoding/binary"
	"time"
)

type AudioSourceProcessor struct {
	*BaseProcessor
	taskCtx *TaskContext
}

func NewAudioSourceProcessor(taskCtx *TaskContext) *AudioSourceProcessor {
	a := &AudioSourceProcessor{taskCtx: taskCtx}
	a.BaseProcessor = NewBaseProcessor("AudioSource", a, taskCtx)
	return a
}

func hasAudibleSamples(samples []int16) bool {
	const audibleThreshold = 1000
	for _, sample := range samples {
		v := int32(sample)
		if v < 0 {
			v = -v
		}
		if v > audibleThreshold {
			return true
		}
	}
	return false
}

func (a *AudioSourceProcessor) maybeMarkFirstUserAudio(samples []int16, at time.Time) {
	if a.taskCtx == nil || a.taskCtx.callStats == nil || !hasAudibleSamples(samples) {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	if a.taskCtx.callStats.MarkFirstUserAudio(at) {
		if a.taskCtx.callEvents != nil {
			a.taskCtx.callEvents.fireFirstUserAudio(at)
		}
	}
}

// PushPCM is called by the Daily bridge with raw 16kHz mono PCM from
// remote participants.
func (a *AudioSourceProcessor) PushPCM(pcmBytes []byte, sampleRate, channels int) {
	if sampleRate != 16000 || channels != 1 {
		a.taskCtx.Logger.Printf("unexpected Daily audio format: sample_rate=%d channels=%d\n", sampleRate, channels)
	}
	if len(pcmBytes) == 0 {
		return
	}
	at := time.Now()

	timingEnabled := a.audioTimingEnabled()
	var start time.Time
	if timingEnabled {
		start = at
	}
	samples := make([]int16, len(pcmBytes)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(pcmBytes[i*2:]))
	}
	if timingEnabled {
		a.recordAudioTiming("go_audio_source_pcm_scan", time.Since(start))
	}
	a.maybeMarkFirstUserAudio(samples, at)
	a.PushFrame(NewAudioFrame(pcmBytes), Downstream)
}

func (a *AudioSourceProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case EndFrame:
		a.taskCtx.Logger.Printf("EndFrame at AudioSourceProcessor: reason=%q\n", f.Reason)
		a.PushFrame(f, dir)
	default:
		a.PushFrame(frame, dir)
	}
}

func (a *AudioSourceProcessor) recordAudioTiming(name string, elapsed time.Duration) {
	if a == nil || a.taskCtx == nil || a.taskCtx.Room == nil {
		return
	}
	a.taskCtx.Room.recordAudioTiming(name, elapsed)
}

func (a *AudioSourceProcessor) audioTimingEnabled() bool {
	return a != nil && a.taskCtx != nil && a.taskCtx.Room != nil && a.taskCtx.Room.perfDiagnosticsEnabled()
}
