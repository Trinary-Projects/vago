package disha

import (
	"testing"
	"time"
)

// A Disha outage makes the same operation fail on every call that ends
// during it. Without a bucket that alone would out-noise the issues this
// change exists to remove.
func TestSentryReportBucketKeepsOnePerWindow(t *testing.T) {
	now := time.Now()
	// The bucket is package-level state, so each test uses its own
	// subject rather than leaking a window into the next test.
	subject := "test:window:api_undelivered"

	if !allowSentryReport(subject, now) {
		t.Fatal("first report should be allowed")
	}
	// 5s apart, faster than calls realistically end, for the rest of the minute.
	for i := 1; i*5 < int(sentryReportWindow/time.Second); i++ {
		at := now.Add(time.Duration(i) * 5 * time.Second)
		if allowSentryReport(subject, at) {
			t.Fatalf("report at +%ds is inside the window and should have been suppressed", i*5)
		}
	}
	if !allowSentryReport(subject, now.Add(sentryReportWindow)) {
		t.Fatal("the next window should report again")
	}
}

func TestSentryReportBucketIsPerSubject(t *testing.T) {
	now := time.Now()
	subjects := []string{
		"test:subject:api_undelivered",
		"test:subject:telemetry_drop",
		"test:subject:job_dropped",
	}
	// Each subject gets its own window; one noisy subject must not
	// silence an unrelated one.
	for _, s := range subjects {
		if !allowSentryReport(s, now) {
			t.Fatalf("subject %q should be allowed on first report", s)
		}
	}
	for _, s := range subjects {
		if allowSentryReport(s, now.Add(time.Second)) {
			t.Fatalf("subject %q should be suppressed within its own window", s)
		}
	}
}
