package disha

import (
	"sync/atomic"

	"github.com/getsentry/sentry-go"
)

// taskSentryHub is the late-bound task-scoped Sentry hub, embedded by any
// disha component that captures Sentry events during a live task (today:
// the onboarding stage manager/tracker, deep-thinking and careplan
// managers). Set once at BuildTask wiring time (taskCtx.SentryHub()) and
// read on every capture path. atomic.Pointer needs no mutex.
// sentryHub() returns nil until wired — sentryutil.Capture treats a nil
// Event.Hub as "use the global hub", which is safe for any call in the
// window before this is wired.
type taskSentryHub struct {
	hub atomic.Pointer[sentry.Hub]
}

// SetSentryHub injects the task-scoped Sentry hub. nil is a no-op so a
// manager that is never wired keeps falling back to the global hub.
func (h *taskSentryHub) SetSentryHub(hub *sentry.Hub) {
	if hub != nil {
		h.hub.Store(hub)
	}
}

// sentryHub returns the late-bound task-scoped Sentry hub, or nil before
// SetSentryHub runs.
func (h *taskSentryHub) sentryHub() *sentry.Hub {
	return h.hub.Load()
}
