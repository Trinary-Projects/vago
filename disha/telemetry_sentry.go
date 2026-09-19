package disha

import (
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// Telemetry jobs (per-turn LLM logs, Daily metrics, stage analytics) are
// best-effort by decision: they have no retry, so their first failure is
// also their last and reporting it is correct. The problem was never
// correctness but volume — these sites produced the bulk of VAGO-7's
// 3,936 events, drowning the failures that actually lost call data.
//
// A token bucket per job keeps one representative event per minute, so a
// sustained outage still surfaces without burying everything else.
const sentryReportWindow = time.Minute

var (
	sentryReportMu   sync.Mutex
	sentryReportLast = map[string]time.Time{}
)

// allowSentryReport reports whether a capture for subject should be
// emitted now, keeping at most one per subject per window. Used for
// sites that can fail repeatedly for a single underlying cause — a
// best-effort job on every turn, or the drainer's claim on a 5s timer
// while Redis is unhealthy.
func allowSentryReport(subject string, now time.Time) bool {
	sentryReportMu.Lock()
	defer sentryReportMu.Unlock()
	if last, ok := sentryReportLast[subject]; ok && now.Sub(last) < sentryReportWindow {
		return false
	}
	sentryReportLast[subject] = now
	return true
}

// reportTelemetryDrop captures at most one event per job per minute.
func reportTelemetryDrop(job, conversationID string, cause error) {
	if cause == nil || !allowSentryReport("job_dropped:"+job, time.Now()) {
		return
	}
	captureOutboxSentry(sentryutil.Event{
		Err: cause,
		Tags: map[string]string{
			"component": "disha_api",
			"operation": "enqueue_job_dropped",
		},
		Details: map[string]any{
			"job":             job,
			"conversation_id": conversationID,
			"note":            "best-effort telemetry job; rate-limited to 1 report per minute per job",
		},
	})
}
