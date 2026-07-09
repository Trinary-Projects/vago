package sentryutil

import (
	"strings"

	"github.com/getsentry/sentry-go"
)

// Event describes a single Sentry capture. Hub is optional: when set,
// the capture runs against that hub's scope/client instead of the
// process-global hub. This lets task-scoped callers (see NewTaskHub)
// attach per-call identity tags to every event a shared core processor
// emits, without core itself knowing what those tags mean.
type Event struct {
	Hub     *sentry.Hub
	Err     error
	Message string
	Level   sentry.Level
	Tags    map[string]string
	Details map[string]any
}

// NewTaskHub returns a Hub cloned from the process-global hub (so it
// inherits whatever client sentry.Init bound, or none if Sentry was
// never initialized) with tags set on its scope. Safe to call whether
// or not Sentry has been initialized: a clone with a nil client simply
// captures nothing, mirroring the package-level sentry.Capture* no-op
// behavior. Callers get a usable, non-nil hub even when tags is
// nil/empty.
func NewTaskHub(tags map[string]string) *sentry.Hub {
	hub := sentry.CurrentHub().Clone()
	if len(tags) == 0 {
		return hub
	}
	if scope := hub.Scope(); scope != nil {
		scope.SetTags(tags)
	}
	return hub
}

func Capture(event Event) {
	if event.Err == nil && strings.TrimSpace(event.Message) == "" {
		return
	}
	hub := event.Hub
	if hub == nil {
		hub = sentry.CurrentHub()
	}
	hub.WithScope(func(scope *sentry.Scope) {
		if len(event.Tags) > 0 {
			scope.SetTags(event.Tags)
		}
		if len(event.Details) > 0 {
			scope.SetContext("details", sentry.Context(event.Details))
		}
		if event.Err != nil {
			hub.CaptureException(event.Err)
			return
		}
		level := event.Level
		if level == "" {
			level = sentry.LevelError
		}
		scope.SetLevel(level)
		hub.CaptureMessage(event.Message)
	})
}
