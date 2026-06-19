package worker

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

func (r *Runtime) MarkGracefulShutdownCompleted() {
	r.gracefulShutdownComplete.Store(true)
}

func (r *Runtime) ReportAbruptShutdownOnExit() {
	if r.gracefulShutdownComplete.Load() || !r.abruptShutdownReported.CompareAndSwap(false, true) {
		return
	}
	identifier := strings.TrimSpace(os.Getenv("FLY_MACHINE_ID"))
	if identifier == "" {
		identifier = strings.TrimSpace(os.Getenv("HOSTNAME"))
	}
	if identifier == "" {
		identifier = "unknown"
	}
	message := fmt.Sprintf("Abrupt machine kill detected: %s", identifier)
	log.Printf("ALERT: %s\n", message)
	sentryutil.Capture(sentryutil.Event{
		Message: message,
		Tags:    map[string]string{"component": "shutdown"},
		Details: map[string]any{
			"identifier": identifier,
		},
	})
	sentry.Flush(2 * time.Second)
}
