package voicepipelinecore

import (
	"io"
	"log"
	"testing"
	"time"
)

func TestLifecycleCallbacksAreOnceAndDrained(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	calls := make(chan string, 4)
	l := newCallEventDispatcher(logger, CallEvents{
		OnBotJoined:       func(time.Time) { calls <- "bot_joined" },
		OnUserJoined:      func(time.Time) { calls <- "user_joined" },
		OnBotFirstSpeech:  func(time.Time) { calls <- "bot_first_speech" },
		OnUserFirstSpeech: func(time.Time) { calls <- "user_first_speech" },
	})
	defer l.stopAndDrain()

	l.fireBotJoined(time.Now())
	l.fireBotJoined(time.Now())
	l.fireUserJoined(time.Now())
	l.fireUserJoined(time.Now())
	l.fireBotFirstSpeech(time.Now())
	l.fireBotFirstSpeech(time.Now())
	l.fireUserFirstSpeech(time.Now())
	l.fireUserFirstSpeech(time.Now())

	l.stopAndDrain()
	close(calls)
	got := map[string]int{}
	for name := range calls {
		got[name]++
	}
	for _, name := range []string{"bot_joined", "user_joined", "bot_first_speech", "user_first_speech"} {
		if got[name] != 1 {
			t.Fatalf("%s callback count = %d, want 1 (all counts: %+v)", name, got[name], got)
		}
	}
}

func TestCallEventsDispatchCommittedTurnsInOrder(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	var got []string
	l := newCallEventDispatcher(logger, CallEvents{
		OnUserTurnCommitted: func(text string, at time.Time, promptKey string) {
			got = append(got, "user:"+text+":"+promptKey)
		},
		OnAssistantTurnCommitted: func(text string, at time.Time, metrics TurnMetrics, promptKey string) {
			got = append(got, "assistant:"+text+":"+promptKey)
			if metrics.LLMTTFBMs != 12 {
				t.Fatalf("LLMTTFBMs = %.1f, want 12", metrics.LLMTTFBMs)
			}
		},
	})

	l.fireUserTurnCommitted("one", time.Now(), "prompt-v1")
	l.fireAssistantTurnCommitted("two", time.Now(), TurnMetrics{LLMTTFBMs: 12}, "prompt-v1")
	l.stopAndDrain()

	want := []string{"user:one:prompt-v1", "assistant:two:prompt-v1"}
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

func TestCallEventsRecoversAndContinuesAfterCallbackPanic(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	var count int
	l := newCallEventDispatcher(logger, CallEvents{
		OnUserTurnCommitted: func(text string, at time.Time, promptKey string) {
			count++
			if count == 1 {
				panic("first callback panic")
			}
		},
	})

	l.fireUserTurnCommitted("first", time.Now(), "")
	l.fireUserTurnCommitted("second", time.Now(), "")
	l.stopAndDrain()

	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}
