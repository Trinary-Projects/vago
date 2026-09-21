package disha

import (
	"context"
	"encoding/json"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const (
	outboxDrainInterval = 5 * time.Second
	outboxClaimLease    = 60 * time.Second
	outboxClaimBatch    = 20
	outboxDrainWorkers  = 4
)

// captureOutboxSentry is a package-var seam so tests can assert that
// exactly one event fires when an item is given up on, mirroring the
// captureDeadMicSentry pattern used elsewhere in the repo.
var captureOutboxSentry = sentryutil.Capture

// outboxAttempter performs one delivery attempt for a claimed item.
// *APIClient satisfies it; tests substitute a stub.
type outboxAttempter interface {
	attemptOutbox(ctx context.Context, item *OutboxItem) error
}

// OutboxDrainer retries persisted operations until they succeed or the
// budget runs out. It is process-wide, not per-PipelineTask: calls end,
// but the queue outlives them, and an item written by a pod that has
// since died must still be picked up by whoever is alive.
type OutboxDrainer struct {
	outbox    *Outbox
	attempter outboxAttempter
	logger    *log.Logger

	interval time.Duration
	lease    time.Duration
	batch    int
	workers  int

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

func NewOutboxDrainer(outbox *Outbox, attempter outboxAttempter, logger *log.Logger) *OutboxDrainer {
	return &OutboxDrainer{
		outbox:    outbox,
		attempter: attempter,
		logger:    logger,
		interval:  outboxDrainInterval,
		lease:     outboxClaimLease,
		batch:     outboxClaimBatch,
		workers:   outboxDrainWorkers,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start runs the drain loop until ctx is cancelled or Stop is called. It
// is a no-op when the outbox is disabled, so VAGO_OUTBOX_ENABLED=0
// leaves no background goroutine behind.
func (d *OutboxDrainer) Start(ctx context.Context) {
	if d == nil || !d.outbox.Enabled() || d.attempter == nil {
		return
	}
	go d.run(ctx)
}

// Stop asks the loop to finish the current tick and exit. In-flight
// attempts are allowed to complete; anything still leased simply falls
// due again for another pod once the lease expires, so SIGTERM needs no
// special handling beyond this.
func (d *OutboxDrainer) Stop() {
	if d == nil {
		return
	}
	d.stopOnce.Do(func() { close(d.stop) })
	select {
	case <-d.done:
	case <-time.After(2 * time.Second):
	}
}

func (d *OutboxDrainer) run(ctx context.Context) {
	defer func() {
		outboxLogf(d.logger, "drainer stopped")
		close(d.done)
	}()
	outboxLogf(d.logger, "drainer started interval=%s lease=%s batch=%d workers=%d due_key=%s",
		d.interval, d.lease, d.batch, d.workers, outboxDueKey())
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stop:
			return
		case <-time.After(d.nextInterval()):
		}
		d.drainOnce(ctx)
	}
}

// nextInterval jitters the tick by +/-20% so pods that started together
// do not claim together.
func (d *OutboxDrainer) nextInterval() time.Duration {
	jitter := time.Duration(rand.Int64N(int64(d.interval) / 5 * 2))
	return d.interval - d.interval/5 + jitter
}

func (d *OutboxDrainer) drainOnce(ctx context.Context) {
	records, err := d.outbox.store.ClaimOutboxItems(ctx, time.Now(), d.lease, d.batch)
	if err != nil {
		// Already reported by the Redis layer; a failed claim just means
		// this tick does nothing and the next one retries.
		return
	}
	if len(records) == 0 {
		// Deliberately silent: an idle queue is the normal state and a
		// line here would be ~17k entries a day per pod saying nothing.
		return
	}
	// TODO: remove this when merging PR
	outboxLogf(d.logger, "claimed n=%d lease=%s (leased until %s)",
		len(records), d.lease, time.Now().Add(d.lease).Format(time.RFC3339))

	work := make(chan OutboxRecord)
	var wg sync.WaitGroup
	for i := 0; i < d.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rec := range work {
				d.processRecord(ctx, rec)
			}
		}()
	}
	for _, rec := range records {
		select {
		case <-ctx.Done():
		case work <- rec:
			continue
		}
		break
	}
	close(work)
	wg.Wait()
}

func (d *OutboxDrainer) processRecord(ctx context.Context, rec OutboxRecord) {
	var item OutboxItem
	if err := json.Unmarshal(rec.Payload, &item); err != nil {
		// Undecodable payloads can never succeed; park rather than spin.
		item = OutboxItem{ID: rec.ID, Operation: "unknown"}
		d.giveUp(ctx, &item, err, "corrupt_payload")
		return
	}
	item.ID = rec.ID
	item.Attempts++

	// TODO: remove this when merging PR
	outboxLogf(d.logger, "attempt operation=%s id=%s attempt=%d/%d source=drainer %s %s queued_for=%s",
		item.Operation, item.ID, item.Attempts, outboxMaxAttempts, item.Method, item.Path,
		outboxQueuedFor(&item))

	attemptCtx, cancel := context.WithTimeout(ctx, defaultAPITimeout)
	start := time.Now()
	err := d.attempter.attemptOutbox(attemptCtx, &item)
	cancel()
	logOutboxAttempt(d.logger, &item, "drainer", time.Since(start), err)

	if err == nil {
		if cerr := d.outbox.Complete(ctx, item.ID); cerr != nil && d.logger != nil {
			d.logger.Printf("disha: outbox complete failed operation=%s id=%s: %v\n", item.Operation, item.ID, cerr)
		}
		outboxLogf(d.logger, "delivered operation=%s id=%s attempts=%d source=drainer", item.Operation, item.ID, item.Attempts)
		return
	}

	if !outboxRetryable(err) {
		d.giveUp(ctx, &item, err, "permanent")
		return
	}
	if item.Attempts >= outboxMaxAttempts {
		d.giveUp(ctx, &item, err, "exhausted")
		return
	}

	// Intermediate failures are logged only — one Sentry event per
	// operation, after the budget is spent (AGENTS.md S3 retry policy).
	if rerr := d.outbox.Retry(ctx, &item, err); rerr != nil && d.logger != nil {
		d.logger.Printf("disha: outbox reschedule failed operation=%s id=%s: %v\n", item.Operation, item.ID, rerr)
	}
}

// outboxQueuedFor is how long this item has been failing, which is the
// number that says whether a backlog is draining or stuck.
func outboxQueuedFor(item *OutboxItem) string {
	if item.FirstFailedAt.IsZero() {
		return "0s"
	}
	return time.Since(item.FirstFailedAt).Truncate(time.Second).String()
}

// giveUp parks the item and reports it exactly once. The Sentry hub is
// rebuilt from the tags persisted on the item, because the drainer runs
// outside any PipelineTask and so has no task hub of its own.
func (d *OutboxDrainer) giveUp(ctx context.Context, item *OutboxItem, cause error, reason string) {
	if perr := d.outbox.Park(ctx, item, cause); perr != nil && d.logger != nil {
		d.logger.Printf("disha: outbox park failed operation=%s id=%s: %v\n", item.Operation, item.ID, perr)
	}
	outboxLogf(d.logger, "GAVE UP operation=%s id=%s attempts=%d reason=%s source=drainer queued_for=%s: %v",
		item.Operation, item.ID, item.Attempts, reason, outboxQueuedFor(item), cause)

	details := map[string]any{
		"item_id":  item.ID,
		"attempts": item.Attempts,
		"reason":   reason,
	}
	if !item.FirstFailedAt.IsZero() {
		details["first_failed_at"] = item.FirstFailedAt.Format(time.RFC3339)
	}
	if item.LastError != "" {
		details["last_error"] = item.LastError
	}
	for k, v := range item.SentryTags {
		details[k] = v
	}

	captureOutboxSentry(sentryutil.Event{
		Hub: sentryutil.NewTaskHub(item.SentryTags),
		Err: cause,
		Tags: map[string]string{
			"component": "disha_outbox",
			"operation": item.Operation,
			"reason":    reason,
		},
		Details: details,
	})
}
