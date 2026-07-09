package sentryutil

import (
	"errors"
	"testing"

	"github.com/getsentry/sentry-go"
)

// newTestHub builds a Hub with its own Client/MockTransport (no DSN, no
// network) so tests can inspect exactly what gets sent, independent of
// whatever (if anything) sentry.Init did at the process level.
func newTestHub(t *testing.T) (*sentry.Hub, *sentry.MockTransport) {
	t.Helper()
	transport := &sentry.MockTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Transport: transport})
	if err != nil {
		t.Fatalf("sentry.NewClient: %v", err)
	}
	hub := sentry.NewHub(client, sentry.NewScope())
	return hub, transport
}

func TestNewTaskHubSetsTagsOnScope(t *testing.T) {
	hub, transport := newTestHub(t)

	// Simulate NewTaskHub's Clone-then-tag behavior against our own test
	// hub instead of the process-global CurrentHub(), so the test doesn't
	// depend on (or mutate) global Sentry state.
	taskHub := hub.Clone()
	tags := map[string]string{"conversation_id": "conv-1", "bot_type": "onboarding_call"}
	scope := taskHub.Scope()
	if scope == nil {
		t.Fatal("cloned hub has nil scope")
	}
	scope.SetTags(tags)

	taskHub.CaptureMessage("hello")
	got := transport.Events()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if got[0].Tags["conversation_id"] != "conv-1" || got[0].Tags["bot_type"] != "onboarding_call" {
		t.Fatalf("expected tags to be set on captured event, got %v", got[0].Tags)
	}
}

func TestNewTaskHubNilAndEmptyTags(t *testing.T) {
	// NewTaskHub must never panic and must always return a usable hub,
	// including when Sentry was never initialized (CurrentHub's default
	// client is nil).
	hub := NewTaskHub(nil)
	if hub == nil {
		t.Fatal("expected non-nil hub for nil tags")
	}
	hub2 := NewTaskHub(map[string]string{})
	if hub2 == nil {
		t.Fatal("expected non-nil hub for empty tags")
	}

	// No client bound (process-global default), so capture must no-op
	// rather than panic.
	Capture(Event{Hub: hub, Message: "should be a no-op without a client"})
}

func TestNewTaskHubUsesProcessGlobalClient(t *testing.T) {
	// NewTaskHub clones sentry.CurrentHub(), so binding a client on the
	// current hub (as sentry.Init would) makes it show up on the clone.
	transport := &sentry.MockTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Transport: transport})
	if err != nil {
		t.Fatalf("sentry.NewClient: %v", err)
	}
	original := sentry.CurrentHub().Client()
	sentry.CurrentHub().BindClient(client)
	t.Cleanup(func() { sentry.CurrentHub().BindClient(original) })

	taskHub := NewTaskHub(map[string]string{"conversation_id": "conv-2"})
	taskHub.CaptureMessage("from task hub")

	got := transport.Events()
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if got[0].Tags["conversation_id"] != "conv-2" {
		t.Fatalf("expected conversation_id tag, got %v", got[0].Tags)
	}
}

func TestCaptureWithHubRoutesThroughThatHub(t *testing.T) {
	hub, transport := newTestHub(t)
	hub.Scope().SetTag("component", "test")

	Capture(Event{
		Hub:     hub,
		Err:     errors.New("boom"),
		Tags:    map[string]string{"operation": "unit_test"},
		Details: map[string]any{"key": "value"},
	})

	got := transport.Events()
	if len(got) != 1 {
		t.Fatalf("expected 1 event on the hub-specific transport, got %d", len(got))
	}
	event := got[0]
	if event.Tags["component"] != "test" {
		t.Fatalf("expected scope tag to survive, got %v", event.Tags)
	}
	if event.Tags["operation"] != "unit_test" {
		t.Fatalf("expected event tag to be applied, got %v", event.Tags)
	}
	if len(event.Exception) == 0 || event.Exception[0].Value != "boom" {
		t.Fatalf("expected exception value 'boom', got %+v", event.Exception)
	}
}

func TestCaptureWithoutHubDoesNotPanic(t *testing.T) {
	// No Hub set: falls back to sentry.CurrentHub(), matching pre-existing
	// call sites unchanged. With no client bound at the process level this
	// must be a safe no-op.
	original := sentry.CurrentHub().Client()
	sentry.CurrentHub().BindClient(nil)
	t.Cleanup(func() { sentry.CurrentHub().BindClient(original) })

	Capture(Event{Message: "no hub, no client"})
	Capture(Event{Err: errors.New("no hub, no client, with error")})
}

func TestCaptureIgnoresEmptyEvent(t *testing.T) {
	hub, transport := newTestHub(t)
	Capture(Event{Hub: hub})
	if len(transport.Events()) != 0 {
		t.Fatalf("expected no event for an empty Event, got %d", len(transport.Events()))
	}
}
