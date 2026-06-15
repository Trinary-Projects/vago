package voicepipelinecore

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"time"

	gomp3 "github.com/hajimehoshi/go-mp3"
)

const (
	bgVolume        = 0.5 // background audio volume
	botVolume       = 0.3 // bot speech volume when mixed
	playbackEndTail = 1 * time.Second
)

// PlaybackSinkProcessor is the bottom of the chain. It writes raw PCM to
// the Daily bridge and owns a 20ms ticker that paces playback. Frame
// ownership is split:
//   - ProcessFrame (on processLoop / inputLoop) forwards incoming frames
//     to runPlayback via queueCh.
//   - runPlayback owns playbackQueue, interrupted, playbackStarted, and
//     all writes to the Daily bridge.
//
// On EndFrame, ProcessFrame blocks until runPlayback has drained the
// queue, written the silence tail, and forwarded EndFrame downstream,
// so task cleanup cannot cancel ctx mid-drain.
type PlaybackSinkProcessor struct {
	*BaseProcessor
	taskCtx          *TaskContext
	metrics          *ProcessorMetrics
	outputSampleRate int
	frameSamples     int
	frameBytes       int
	bgPCM            []int16 // background audio loop buffer at outputSampleRate mono

	queueCh chan Frame    // ProcessFrame → runPlayback
	endDone chan struct{} // closed by runPlayback after EndFrame is forwarded

	// Owned by runPlayback only:
	bgPos           int
	interrupted     bool
	playbackStarted bool
	playbackQueue   []Frame
}

func loadBackgroundPCM(path string, targetSampleRate int, logger interface{ Printf(string, ...interface{}) }) []int16 {
	f, err := os.Open(path)
	if err != nil {
		logger.Printf("Background audio not found (%s), mixing disabled\n", path)
		return nil
	}
	defer f.Close()

	dec, err := gomp3.NewDecoder(f)
	if err != nil {
		logger.Printf("Background MP3 decode error: %v\n", err)
		return nil
	}

	raw, err := io.ReadAll(dec)
	if err != nil {
		logger.Printf("Background MP3 read error: %v\n", err)
		return nil
	}

	numSamples := len(raw) / 4
	mono := make([]int16, numSamples)
	for i := 0; i < numSamples; i++ {
		left := int16(binary.LittleEndian.Uint16(raw[i*4:]))
		right := int16(binary.LittleEndian.Uint16(raw[i*4+2:]))
		mono[i] = int16((int32(left) + int32(right)) / 2)
	}
	sourceSampleRate := dec.SampleRate()
	if targetSampleRate > 0 && sourceSampleRate > 0 && targetSampleRate != sourceSampleRate {
		mono = resampleMonoPCM(mono, sourceSampleRate, targetSampleRate)
	}
	logSampleRate := sourceSampleRate
	if targetSampleRate > 0 {
		logSampleRate = targetSampleRate
	}
	logger.Printf("Background audio loaded: %d samples (%.1fs at %dHz)\n", len(mono), float64(len(mono))/float64(logSampleRate), logSampleRate)
	return mono
}

func NewPlaybackSinkProcessor(taskCtx *TaskContext) *PlaybackSinkProcessor {
	outputSampleRate := outputSampleRateFromRoom(taskCtx)
	bgPCM := loadBackgroundPCM("background-office-sound.mp3", outputSampleRate, taskCtx.Logger)

	p := &PlaybackSinkProcessor{
		taskCtx:          taskCtx,
		metrics:          NewProcessorMetrics("playback"),
		outputSampleRate: outputSampleRate,
		frameSamples:     pcmSamplesPerFrameForRate(outputSampleRate),
		frameBytes:       pcmFrameBytesForRate(outputSampleRate),
		bgPCM:            bgPCM,
		queueCh:          make(chan Frame, 256),
		endDone:          make(chan struct{}),
	}
	p.BaseProcessor = NewBaseProcessor("PlaybackSink", p, taskCtx)
	return p
}

func resampleMonoPCM(samples []int16, sourceRate, targetRate int) []int16 {
	if len(samples) == 0 || sourceRate <= 0 || targetRate <= 0 || sourceRate == targetRate {
		return samples
	}
	outLen := (len(samples)*targetRate + sourceRate/2) / sourceRate
	if outLen <= 0 {
		return nil
	}
	out := make([]int16, outLen)
	for i := range out {
		posNum := i * sourceRate
		idx := posNum / targetRate
		if idx >= len(samples)-1 {
			out[i] = samples[len(samples)-1]
			continue
		}
		rem := posNum % targetRate
		a := int(samples[idx])
		b := int(samples[idx+1])
		out[i] = int16((a*(targetRate-rem) + b*rem) / targetRate)
	}
	return out
}

func (p *PlaybackSinkProcessor) Start(ctx context.Context) {
	p.BaseProcessor.Start(ctx)
	p.Go(p.runPlayback)
}

func (p *PlaybackSinkProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch frame.(type) {
	case EndFrame:
		// EndFrame must drain through runPlayback (audio tail + silence)
		// before task cleanup stops the processor.
		select {
		case p.queueCh <- frame:
		case <-p.ctx.Done():
			return
		}
		select {
		case <-p.endDone:
		case <-p.ctx.Done():
		}
	case AudioFrame, WordTimestampFrame, TTSDoneFrame,
		LLMResponseStartFrame, TTSSpeakFrame, InterruptFrame:
		select {
		case p.queueCh <- frame:
		case <-p.ctx.Done():
		}
	case LLMResponseEndFrame:
		// ignore; playback doesn't care about LLM stream boundaries
	default:
		// Unknown frames terminate here.
	}
}

func (p *PlaybackSinkProcessor) runPlayback() {
	ticker := time.NewTicker(playbackFrameDuration)
	defer ticker.Stop()

	var lastTick time.Time
	for {
		select {
		case <-p.ctx.Done():
			return
		case f := <-p.queueCh:
			p.handleQueueFrame(f)
		case tickAt := <-ticker.C:
			timingEnabled := p.audioTimingEnabled()
			if timingEnabled && !lastTick.IsZero() {
				if lag := tickAt.Sub(lastTick) - playbackFrameDuration; lag > 0 {
					p.recordAudioTiming("go_playback_tick_lag", lag)
				}
			}
			lastTick = tickAt
			var start time.Time
			if timingEnabled {
				start = time.Now()
			}
			if p.tick() {
				if timingEnabled {
					p.recordAudioTiming("go_playback_tick_work", time.Since(start))
				}
				close(p.endDone)
				return
			}
			if timingEnabled {
				p.recordAudioTiming("go_playback_tick_work", time.Since(start))
			}
		}
	}
}

func (p *PlaybackSinkProcessor) handleQueueFrame(f Frame) {
	switch v := f.(type) {
	case AudioFrame, WordTimestampFrame, TTSDoneFrame:
		if !p.interrupted {
			p.playbackQueue = append(p.playbackQueue, v)
		}
	case EndFrame:
		if shouldStopPlaybackImmediately(v.Reason) {
			p.taskCtx.Logger.Printf("EndFrame stopping PlaybackSink immediately, dropping %d pending playback frames: reason=%q\n", len(p.playbackQueue), v.Reason)
			p.playbackQueue = nil
			p.interrupted = true
		} else {
			p.taskCtx.Logger.Printf("EndFrame queued in PlaybackSink after %d pending playback frames: reason=%q\n", len(p.playbackQueue), v.Reason)
		}
		p.playbackQueue = append(p.playbackQueue, v)
	case LLMResponseStartFrame:
		p.interrupted = false
		p.playbackStarted = false
		p.playbackQueue = nil
		p.metrics.StartAt(MetricE2ELatency, v.StartedAt)
	case TTSSpeakFrame:
		p.interrupted = false
		p.playbackStarted = false
		p.playbackQueue = nil
	case InterruptFrame:
		p.interrupted = true
		// Only shutdown survives the playback queue purge. Queued
		// WordTimestampFrame/TTSDoneFrame values are still future output here:
		// if they have not left PlaybackSink yet, their audio was not played.
		kept := p.playbackQueue[:0]
		for _, qf := range p.playbackQueue {
			if survivesPlaybackInterrupt(qf) {
				kept = append(kept, qf)
			}
		}
		dropped := len(p.playbackQueue) - len(kept)
		p.playbackQueue = kept
		if p.taskCtx != nil && p.taskCtx.Room != nil {
			p.taskCtx.Room.ClearAudioBuffer()
		}
		p.taskCtx.Logger.Printf("Playback interrupted (dropped %d queued playback frames, kept %d EndFrames)\n", dropped, len(kept))
		p.PushFrame(v, Downstream)
	}
}

func survivesPlaybackInterrupt(f Frame) bool {
	_, ok := f.(EndFrame)
	return ok
}

// tick advances playback by one 20ms frame. Returns true iff an
// EndFrame was processed and runPlayback should exit.
func (p *PlaybackSinkProcessor) tick() bool {
	var botPCM []int16
	for len(p.playbackQueue) > 0 {
		switch f := p.playbackQueue[0].(type) {
		case AudioFrame:
			if !p.playbackStarted {
				p.playbackStarted = true
				at := time.Now()
				if p.taskCtx.callEvents != nil {
					p.taskCtx.callEvents.fireBotFirstSpeech(at)
				}
				p.taskCtx.UIEvents.BotStartedSpeaking(at)
				// Broadcast both ways: upstream lets UserIdleProcessor
				// cancel its idle timer; downstream is informational for
				// any future post-Playback consumer. Mirrors Pipecat's
				// BaseOutputTransport.MediaSender._bot_started_speaking.
				p.Broadcast(NewBotStartedSpeakingFrame())
				if mf := p.metrics.Stop(MetricE2ELatency); mf != nil {
					p.PushFrame(*mf, Downstream)
				}
			}
			botPCM = p.botPCMFrame(f.Data)
			p.playbackQueue = p.playbackQueue[1:]
			goto mix
		case WordTimestampFrame:
			p.PushFrame(f, Downstream)
			p.playbackQueue = p.playbackQueue[1:]
		case TTSDoneFrame:
			p.taskCtx.Logger.Println("Playback complete")
			p.taskCtx.UIEvents.BotStoppedSpeaking(time.Now())
			p.PushFrame(f, Downstream)
			// Broadcast: upstream tells UserContextAggregator/UserIdle that
			// the bot finished speaking; downstream tells the assistant
			// context aggregator to commit the played words before EndFrame
			// can reach PipelineSink.
			p.Broadcast(NewBotStoppedSpeakingFrame())
			p.playbackQueue = p.playbackQueue[1:]
		case EndFrame:
			p.playbackQueue = p.playbackQueue[1:]
			if !shouldStopPlaybackImmediately(f.Reason) {
				p.writeSilenceTail()
			}
			p.taskCtx.Logger.Printf("EndFrame leaving PlaybackSink after playback handling: reason=%q\n", f.Reason)
			p.PushFrame(f, Downstream)
			return true
		default:
			p.playbackQueue = p.playbackQueue[1:]
		}
	}
mix:
	timingEnabled := p.audioTimingEnabled()
	var start time.Time
	if timingEnabled {
		start = time.Now()
	}
	pcm := p.mixPCM(botPCM)
	if timingEnabled {
		p.recordAudioTiming("go_playback_mix_pcm", time.Since(start))
	}
	_ = p.taskCtx.Room.WriteAudioPCM(pcm)
	return false
}

func shouldStopPlaybackImmediately(reason string) bool {
	switch EndReason(reason) {
	case EndReasonClientDisconnect, EndReasonError:
		return true
	default:
		return false
	}
}

func (p *PlaybackSinkProcessor) getBgFrame() []int16 {
	if len(p.bgPCM) == 0 {
		return nil
	}
	frameSamples := p.samplesPerFrame()
	frame := make([]int16, frameSamples)
	for i := 0; i < frameSamples; i++ {
		frame[i] = p.bgPCM[p.bgPos]
		p.bgPos++
		if p.bgPos >= len(p.bgPCM) {
			p.bgPos = 0
		}
	}
	return frame
}

func (p *PlaybackSinkProcessor) mixPCM(botPCM []int16) []byte {
	bgFrame := p.getBgFrame()
	frameSamples := p.samplesPerFrame()
	mixed := make([]int16, frameSamples)

	for i := 0; i < frameSamples; i++ {
		var sample float64
		if bgFrame != nil {
			sample += float64(bgFrame[i]) * bgVolume
		}
		if botPCM != nil {
			sample += float64(botPCM[i]) * botVolume
		}
		if sample > 32767 {
			sample = 32767
		} else if sample < -32768 {
			sample = -32768
		}
		mixed[i] = int16(sample)
	}

	pcmBytes := make([]byte, p.bytesPerFrame())
	for i, sample := range mixed {
		binary.LittleEndian.PutUint16(pcmBytes[i*2:], uint16(sample))
	}
	return pcmBytes
}

func (p *PlaybackSinkProcessor) botPCMFrame(pcmBytes []byte) []int16 {
	if len(pcmBytes) == 0 {
		return nil
	}
	frameBytes := p.bytesPerFrame()
	frameSamples := p.samplesPerFrame()
	if len(pcmBytes) != frameBytes {
		if p.taskCtx != nil && p.taskCtx.Logger != nil {
			p.taskCtx.Logger.Printf("playback bot PCM frame has %d bytes, expected %d; padding/truncating\n", len(pcmBytes), frameBytes)
		}
	}
	pcm := make([]int16, frameSamples)
	samples := len(pcmBytes) / 2
	if samples > frameSamples {
		samples = frameSamples
	}
	timingEnabled := p.audioTimingEnabled()
	var start time.Time
	if timingEnabled {
		start = time.Now()
	}
	for i := 0; i < samples; i++ {
		pcm[i] = int16(binary.LittleEndian.Uint16(pcmBytes[i*2:]))
	}
	if timingEnabled {
		p.recordAudioTiming("go_playback_pcm_bytes_to_samples", time.Since(start))
	}
	return pcm
}

func (p *PlaybackSinkProcessor) recordAudioTiming(name string, elapsed time.Duration) {
	if p == nil || p.taskCtx == nil || p.taskCtx.Room == nil {
		return
	}
	p.taskCtx.Room.recordAudioTiming(name, elapsed)
}

func (p *PlaybackSinkProcessor) audioTimingEnabled() bool {
	return p != nil && p.taskCtx != nil && p.taskCtx.Room != nil && p.taskCtx.Room.perfDiagnosticsEnabled()
}

func (p *PlaybackSinkProcessor) writeSilenceTail() {
	if !p.playbackStarted || playbackEndTail <= 0 {
		return
	}
	p.taskCtx.Logger.Printf("Writing %s playback silence before EndFrame\n", playbackEndTail)

	silence := make([]byte, p.bytesPerFrame())
	frames := int(playbackEndTail / playbackFrameDuration)
	for i := 0; i < frames; i++ {
		_ = p.taskCtx.Room.WriteAudioPCM(silence)
		time.Sleep(playbackFrameDuration)
	}
}

func (p *PlaybackSinkProcessor) samplesPerFrame() int {
	if p == nil || p.frameSamples <= 0 {
		return framePCM
	}
	return p.frameSamples
}

func (p *PlaybackSinkProcessor) bytesPerFrame() int {
	if p == nil || p.frameBytes <= 0 {
		return framePCMBytes
	}
	return p.frameBytes
}
