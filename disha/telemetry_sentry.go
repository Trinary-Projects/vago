package disha

import (
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// reportTelemetryDrop captures a dropped best-effort telemetry job
// (per-turn LLM logs, Daily metrics, stage analytics). These have no
// retry by decision, so their first failure is also their last and
// reporting it is correct.
func reportTelemetryDrop(job, conversationID string, cause error) {
	if cause == nil {
		return
	}
	captureSentry(sentryutil.Event{
		Err: cause,
		Tags: map[string]string{
			"component": "disha_api",
			"operation": "enqueue_job_dropped",
		},
		Details: map[string]any{
			"job":             job,
			"conversation_id": conversationID,
		},
	})
}
