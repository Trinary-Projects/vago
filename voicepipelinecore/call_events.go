package voicepipelinecore

import (
	"log"
	"sync"
	"time"
)

const callEventQueueSize = 512

type callEventTask struct {
	name string
	fn   func()
}

type callEventDispatcher struct {
	logger *log.Logger

	queue    chan callEventTask
	done     chan struct{}
	mu       sync.Mutex
	closing  bool
	stopOnce sync.Once

	onBotJoined              func(time.Time)
	onUserJoined             func(time.Time)
	onUserFirstSpeech        func(time.Time)
	onBotFirstSpeech         func(time.Time)
	onFirstUserAudio         func(time.Time)
	onUserTurnCommitted      func(text string, at time.Time, promptKey string, replacePrevious bool)
	onAssistantTurnCommitted func(text string, at time.Time, metrics TurnMetrics, promptKey string)
	onToolResultCommitted    func(assistantToolCall Message, toolResult Message, at time.Time)
	onLLMCallCompleted       func(text string, interrupted bool)

	botJoinedOnce       sync.Once
	userJoinedOnce      sync.Once
	userFirstSpeechOnce sync.Once
	botFirstSpeechOnce  sync.Once
	firstUserAudioOnce  sync.Once
}

func newCallEventDispatcher(logger *log.Logger, events CallEvents) *callEventDispatcher {
	d := &callEventDispatcher{
		logger:                   logger,
		queue:                    make(chan callEventTask, callEventQueueSize),
		done:                     make(chan struct{}),
		onBotJoined:              events.OnBotJoined,
		onUserJoined:             events.OnUserJoined,
		onUserFirstSpeech:        events.OnUserFirstSpeech,
		onBotFirstSpeech:         events.OnBotFirstSpeech,
		onFirstUserAudio:         events.OnFirstUserAudio,
		onUserTurnCommitted:      events.OnUserTurnCommitted,
		onAssistantTurnCommitted: events.OnAssistantTurnCommitted,
		onToolResultCommitted:    events.OnToolResultCommitted,
		onLLMCallCompleted:       events.OnLLMCallCompleted,
	}
	go d.run()
	return d
}

func (l *callEventDispatcher) run() {
	defer close(l.done)
	for task := range l.queue {
		l.runSafely(task.name, task.fn)
	}
}

func (l *callEventDispatcher) fireBotJoined(at time.Time) {
	if l == nil || l.onBotJoined == nil {
		return
	}
	l.botJoinedOnce.Do(func() {
		l.dispatch("OnBotJoined", func() { l.onBotJoined(at) })
	})
}

func (l *callEventDispatcher) fireUserJoined(at time.Time) {
	if l == nil || l.onUserJoined == nil {
		return
	}
	l.userJoinedOnce.Do(func() {
		l.dispatch("OnUserJoined", func() { l.onUserJoined(at) })
	})
}

func (l *callEventDispatcher) fireUserFirstSpeech(at time.Time) {
	if l == nil || l.onUserFirstSpeech == nil {
		return
	}
	l.userFirstSpeechOnce.Do(func() {
		l.dispatch("OnUserFirstSpeech", func() { l.onUserFirstSpeech(at) })
	})
}

func (l *callEventDispatcher) fireBotFirstSpeech(at time.Time) {
	if l == nil || l.onBotFirstSpeech == nil {
		return
	}
	l.botFirstSpeechOnce.Do(func() {
		l.dispatch("OnBotFirstSpeech", func() { l.onBotFirstSpeech(at) })
	})
}

func (l *callEventDispatcher) fireFirstUserAudio(at time.Time) {
	if l == nil || l.onFirstUserAudio == nil {
		return
	}
	l.firstUserAudioOnce.Do(func() {
		l.dispatch("OnFirstUserAudio", func() { l.onFirstUserAudio(at) })
	})
}

func (l *callEventDispatcher) fireUserTurnCommitted(text string, at time.Time, promptKey string, replacePrevious bool) {
	if l == nil || l.onUserTurnCommitted == nil {
		return
	}
	l.dispatch("OnUserTurnCommitted", func() { l.onUserTurnCommitted(text, at, promptKey, replacePrevious) })
}

func (l *callEventDispatcher) fireAssistantTurnCommitted(text string, at time.Time, metrics TurnMetrics, promptKey string) {
	if l == nil || l.onAssistantTurnCommitted == nil {
		return
	}
	l.dispatch("OnAssistantTurnCommitted", func() { l.onAssistantTurnCommitted(text, at, metrics, promptKey) })
}

func (l *callEventDispatcher) fireToolResultCommitted(assistantToolCall Message, toolResult Message, at time.Time) {
	if l == nil || l.onToolResultCommitted == nil {
		return
	}
	l.dispatch("OnToolResultCommitted", func() { l.onToolResultCommitted(assistantToolCall, toolResult, at) })
}

func (l *callEventDispatcher) fireLLMCallCompleted(text string, interrupted bool) {
	if l == nil || l.onLLMCallCompleted == nil {
		return
	}
	l.dispatch("OnLLMCallCompleted", func() { l.onLLMCallCompleted(text, interrupted) })
}

func (l *callEventDispatcher) dispatch(name string, fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing {
		return
	}
	select {
	case l.queue <- callEventTask{name: name, fn: fn}:
	default:
		if l.logger != nil {
			l.logger.Printf("call event queue full (cap=%d), dropping %s\n", callEventQueueSize, name)
		}
	}
}

func (l *callEventDispatcher) runSafely(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil && l.logger != nil {
			l.logger.Printf("%s callback panicked: %v\n", name, r)
		}
	}()
	fn()
}

func (l *callEventDispatcher) stopAndDrain() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() {
		l.mu.Lock()
		l.closing = true
		close(l.queue)
		l.mu.Unlock()
	})
	<-l.done
}
