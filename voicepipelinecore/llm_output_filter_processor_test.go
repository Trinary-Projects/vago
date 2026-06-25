package voicepipelinecore

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

func TestLLMOutputFilter_CleanTextPassesThrough(t *testing.T) {
	got := runLLMOutputFilterResponse(t, []string{"Hello", " ", "world", "!"})
	want := []string{"Hello", " ", "world", "!"}
	assertStringSlicesEqual(t, got, want)
}

func TestLLMOutputFilter_KillPrefixesSuppressFromPrefixOnward(t *testing.T) {
	cases := []struct {
		name   string
		tokens []string
		want   []string
	}{
		{
			name:   "XML tag kills from prefix onward",
			tokens: []string{"Hello", " ", "<think>", "internal stuff"},
			want:   []string{"Hello", " "},
		},
		{
			name:   "underscore prefix kills entire garbage response",
			tokens: []string{"_thought", "blah blah"},
			want:   nil,
		},
		{
			name:   "JSON leak kills from prefix",
			tokens: []string{"Sure! ", "{\"key\": \"value\"}"},
			want:   []string{"Sure! "},
		},
		{
			name:   "startcall prefix kills",
			tokens: []string{"startcall:", "some_function"},
			want:   nil,
		},
		{
			name:   "text before kill prefix is preserved",
			tokens: []string{"Hi there", "<leaked_tag>", "more stuff"},
			want:   []string{"Hi there"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runLLMOutputFilterResponse(t, tc.tokens)
			assertStringSlicesEqual(t, got, tc.want)
		})
	}
}

func TestLLMOutputFilter_KillAfterPrefixKeepsPrefixAndFlushes(t *testing.T) {
	got := runLLMOutputFilterResponse(t, []string{"Really", "? some garbage"})
	want := []string{"Really", "?", "."}
	assertStringSlicesEqual(t, got, want)
}

func TestLLMOutputFilter_KillAfterPrefixDoesNotTriggerAtEndOfChunk(t *testing.T) {
	got := runLLMOutputFilterResponse(t, []string{"Really", "?"})
	want := []string{"Really", "?"}
	assertStringSlicesEqual(t, got, want)
}

func TestLLMOutputFilter_SuppressionResetsBetweenResponses(t *testing.T) {
	fix := newTestFixture(t)
	p := NewLLMOutputFilterProcessor(fix.TaskCtx)
	down, _ := runProcessorTest(t, fix, runConfig{
		processor: p,
		framesToSend: []Frame{
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("<garbage>"),
			NewLLMResponseEndFrame(),
			NewLLMResponseStartFrame(time.Now()),
			NewTextFrame("Hello"),
			NewTextFrame(" "),
			NewTextFrame("again"),
			NewLLMResponseEndFrame(),
		},
		sendEndFrame: true,
	})

	got := textFrameTexts(down)
	want := []string{"Hello", " ", "again"}
	assertStringSlicesEqual(t, got, want)
}

func TestLLMOutputFilter_EntireSuppressedResponseCapturesSentryWarning(t *testing.T) {
	oldClient := sentry.CurrentHub().Client()
	t.Cleanup(func() {
		sentry.CurrentHub().BindClient(oldClient)
	})

	transport := &captureSentryTransport{}
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:       "https://public@example.com/1",
		Transport: transport,
	}); err != nil {
		t.Fatalf("sentry.Init: %v", err)
	}

	got := runLLMOutputFilterResponse(t, []string{"<think>", "internal reasoning"})
	if len(got) != 0 {
		t.Fatalf("got text frames %q, want none", got)
	}

	events := transport.Events()
	if len(events) != 1 {
		t.Fatalf("captured %d Sentry events, want 1", len(events))
	}
	if events[0].Level != sentry.LevelWarning {
		t.Fatalf("Sentry level = %q, want %q", events[0].Level, sentry.LevelWarning)
	}
	if !strings.Contains(events[0].Message, "entire response suppressed by kill-prefix") {
		t.Fatalf("Sentry message = %q", events[0].Message)
	}
}

func runLLMOutputFilterResponse(t *testing.T, tokens []string) []string {
	t.Helper()
	fix := newTestFixture(t)
	p := NewLLMOutputFilterProcessor(fix.TaskCtx)
	frames := []Frame{NewLLMResponseStartFrame(time.Now())}
	for _, token := range tokens {
		frames = append(frames, NewTextFrame(token))
	}
	frames = append(frames, NewLLMResponseEndFrame())

	down, _ := runProcessorTest(t, fix, runConfig{
		processor:    p,
		framesToSend: frames,
		sendEndFrame: true,
	})
	return textFrameTexts(down)
}

func textFrameTexts(frames []Frame) []string {
	var out []string
	for _, frame := range frames {
		if tf, ok := frame.(TextFrame); ok {
			out = append(out, tf.Text)
		}
	}
	return out
}

func assertStringSlicesEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

type captureSentryTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (t *captureSentryTransport) Flush(timeout time.Duration) bool { return true }

func (t *captureSentryTransport) FlushWithContext(ctx context.Context) bool { return true }

func (t *captureSentryTransport) Configure(options sentry.ClientOptions) {}

func (t *captureSentryTransport) Close() {}

func (t *captureSentryTransport) SendEvent(event *sentry.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}

func (t *captureSentryTransport) Events() []*sentry.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*sentry.Event, len(t.events))
	copy(out, t.events)
	return out
}
