package voicepipelinecore

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// captureDeadMicSentry is a package-level var (like sttDialURL/ttsDialURL)
// so tests can redirect Sentry capture without hitting the real SDK.
var captureDeadMicSentry = sentryutil.Capture

const (
	idleTimeout    = 7 * time.Second
	maxIdlePrompts = 7
	idlePromptText = "Hello?"
	// Fire the diagnostic after a few idle nudges instead of the first one:
	// first-idle was too noisy for users who responded shortly after the nudge.
	deadMicSentryIdleRetry = 5

	// cancelOnIdleTimeout ends the call if there is no activity at all
	// (user speech / interim transcript / bot speaking) for this long.
	// Mirrors Pipecat's PipelineTask(cancel_on_idle_timeout=True,
	// idle_timeout_secs=120). It's a backstop: the idle-prompt logic
	// above normally ends a quiet call first (its prompts count as bot
	// activity, which resets this timer).
	cancelOnIdleTimeout = 120 * time.Second
)

type UserIdleProcessor struct {
	*BaseProcessor
	taskCtx         *TaskContext
	idleTimer       *time.Timer
	idlePromptCount atomic.Int32
	cancelResetCh   chan struct{}
}

func NewUserIdleProcessor(taskCtx *TaskContext) *UserIdleProcessor {
	p := &UserIdleProcessor{taskCtx: taskCtx, cancelResetCh: make(chan struct{}, 1)}
	p.BaseProcessor = NewBaseProcessor("UserIdle", p, taskCtx)
	return p
}

// Start launches the base loops plus the inactivity watchdog goroutine.
func (p *UserIdleProcessor) Start(ctx context.Context) {
	p.BaseProcessor.Start(ctx)
	p.Go(p.runCancelWatchdog)
}

// markActivity resets the inactivity watchdog. Non-blocking so it's cheap
// to call on every activity frame.
func (p *UserIdleProcessor) markActivity() {
	select {
	case p.cancelResetCh <- struct{}{}:
	default:
	}
}

// runCancelWatchdog ends the call when no activity is seen for
// cancelOnIdleTimeout. Reset by any user speech / interim transcript /
// bot speaking (see markActivity). Exits when the processor's context is
// cancelled (EndFrame / Stop).
func (p *UserIdleProcessor) runCancelWatchdog() {
	timer := time.NewTimer(cancelOnIdleTimeout)
	defer timer.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.cancelResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(cancelOnIdleTimeout)
		case <-timer.C:
			p.taskCtx.Logger.Printf("No activity for %s; ending call (idle timeout)\n", cancelOnIdleTimeout)
			if p.taskCtx.EndTask != nil {
				p.taskCtx.EndTask(EndReasonUserIdle)
			}
			return
		}
	}
}

func (p *UserIdleProcessor) cancelIdleTimer() {
	if p.idleTimer != nil {
		p.idleTimer.Stop()
		p.idleTimer = nil
	}
}

func (p *UserIdleProcessor) startIdleTimer() {
	p.cancelIdleTimer()
	if p.idlePromptCount.Load() >= maxIdlePrompts {
		return
	}
	p.idleTimer = time.AfterFunc(idleTimeout, p.onIdleTimeout)
}

// onIdleTimeout is the idle timer's fire body, pulled out of the
// time.AfterFunc closure so tests can invoke it directly/deterministically
// instead of waiting on the real idleTimeout.
func (p *UserIdleProcessor) onIdleTimeout() {
	count := p.idlePromptCount.Add(1)
	if count > maxIdlePrompts {
		// We've already issued the final prompt and asked the
		// pipeline to end; ignore any straggling fires.
		return
	}

	// Dead-mic Sentry check: Python's base_pipeline_manager.py fires this
	// on the FIRST idle retry, when zero audio frames were ever received
	// AND more than the bot alone is in the room:
	//   if (self.transport.input().total_audio_frame_count == 0
	//       and self._idle_retry_count <= 1
	//       and len(self.transport.participants().keys()) > 1):
	//       sentry_sdk.capture_exception(Exception(
	//           "No audio frames received after user idle timeout, try reconnecting"))
	// Go deliberately waits until the fifth retry to avoid alerting on
	// users who answer right after the first "Hello?". Its proxy for "any
	// audio frame ever received" is the first-AUDIBLE-frame mark
	// (AudioSourceProcessor only marks it above a magnitude threshold), and
	// "more than the bot in the room" is callStatsTracker's current-presence
	// flag. The exact retry check keeps this naturally once per call.
	if count == deadMicSentryIdleRetry && p.taskCtx.callStats.FirstUserAudioFrameAt().IsZero() && p.taskCtx.callStats.Present() {
		p.taskCtx.Logger.Println("User is idle, no audio frames received")
		captureDeadMicSentry(sentryutil.Event{
			Message: "No audio frames received after user idle timeout, try reconnecting",
			Tags: map[string]string{
				"component": "voicepipeline",
				"operation": "user_idle_no_audio",
				"session":   sessionIdentity(p.taskCtx),
			},
		})
	}

	if count == maxIdlePrompts {
		// Final attempt: speak the prompt one last time, then ask
		// the pipeline source to inject EndFrame so every processor
		// shuts down in order. Matches Python sales_call's
		// handle_idle returning False after retry == 6.
		p.taskCtx.Logger.Printf("User idle (%d/%d), final prompt; ending task\n", count, maxIdlePrompts)
		p.taskCtx.UIEvents.ServerMessage(fmt.Sprintf("User idle nudge (retry %d): hello?", count), time.Now())
		p.PushFrame(NewTTSSpeakFrame(idlePromptText), Downstream)
		if p.taskCtx.EndTask != nil {
			p.taskCtx.EndTask(EndReasonUserIdle)
		}
		return
	}
	p.taskCtx.Logger.Printf("User idle (%d/%d), injecting prompt\n", count, maxIdlePrompts)
	p.taskCtx.UIEvents.ServerMessage(fmt.Sprintf("User idle nudge (retry %d): hello?", count), time.Now())
	p.PushFrame(NewTTSSpeakFrame(idlePromptText), Downstream)
}

// sessionIdentity returns whatever call/session identity core has
// available. Core has no conversation_id; disha's call logger is
// constructed with a "[conv=<id>] " prefix (call_startup.go), so the
// trimmed Logger prefix is the closest generic identity available here.
func sessionIdentity(taskCtx *TaskContext) string {
	if taskCtx == nil || taskCtx.Logger == nil {
		return ""
	}
	return strings.TrimSpace(taskCtx.Logger.Prefix())
}

func (p *UserIdleProcessor) ProcessFrame(ctx context.Context, frame Frame, dir Direction) {
	switch f := frame.(type) {
	case EndFrame:
		p.taskCtx.Logger.Printf("EndFrame at UserIdleProcessor: reason=%q\n", f.Reason)
		p.cancelIdleTimer()
		p.PushFrame(f, dir)
	case TranscriptFrame:
		// User speech / interim transcript: activity for the task idle
		// watchdog (Pipecat's UserSpeaking/InterimTranscription frames).
		p.markActivity()
		p.cancelIdleTimer()
		p.idlePromptCount.Store(0)
		p.PushFrame(frame, dir)
	case BotStartedSpeakingFrame:
		// Bot speaking: activity for the task idle watchdog.
		p.markActivity()
		p.cancelIdleTimer()
		// consumed upstream here — UserIdle is the terminal upstream consumer
	case BotStoppedSpeakingFrame:
		p.markActivity()
		p.startIdleTimer()
		// consumed upstream here
	case FunctionCallInProgressFrame:
		p.markActivity()
		p.cancelIdleTimer()
		p.PushFrame(frame, dir)
	case FunctionCallResultFrame:
		p.markActivity()
		p.startIdleTimer()
		p.PushFrame(frame, dir)
	default:
		p.PushFrame(frame, dir)
	}
}
