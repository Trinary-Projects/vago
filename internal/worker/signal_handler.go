package worker

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

func (r *Runtime) RegisterSignalHandlers() {
	log.Println("Registering cleanup handlers...")
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for sig := range signals {
			r.HandleShutdownSignal(sig)
		}
	}()
}

func (r *Runtime) HandleShutdownSignal(sig os.Signal) {
	if !r.shutdownInitiated.CompareAndSwap(false, true) {
		log.Printf("Received duplicate %s, ignoring (shutdown already in progress)...\n", sig)
		return
	}

	r.MarkGracefulShutdownCompleted()
	log.Printf("Received %s, checking worker status...\n", sig)

	if sig == syscall.SIGINT || sig == os.Interrupt {
		sentry.Flush(2 * time.Second)
		r.exitProcess(0)
		return
	}

	podName := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if podName == "" {
		log.Println("HOSTNAME is empty; skipping worker graceful shutdown enqueue")
		sentry.Flush(2 * time.Second)
		r.exitProcess(0)
		return
	}
	podUID := strings.TrimSpace(os.Getenv("POD_UID"))

	log.Println("Allowing graceful shutdown to proceed...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := EnqueueWorkerGracefulShutdown(ctx, r.deps, podName, podUID); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "signal_handler"},
			Details: map[string]any{
				"pod_name": podName,
				"pod_uid":  podUID,
				"signal":   sig.String(),
			},
		})
		log.Printf("failed to enqueue graceful shutdown for pod=%s: %v\n", podName, err)
	}

	// Never exit on SIGTERM, even when idle (mirrors the Python worker's
	// sigterm_handler): disha-backend clears this pod's gkeworkermachines row
	// before deleting the pod. Exiting here inverts that order and leaves a
	// dead pod reservable from the available pool until the shutdown job is
	// consumed, which times out call forwarding.
	log.Println("keeping process alive; disha-backend deletes the pod after clearing its machine record")
}
