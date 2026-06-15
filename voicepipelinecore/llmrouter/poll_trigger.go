package llmrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// pollLockPrefix matches the lock key used by Python's
	// _trigger_event_polling (poll_openai_models_lock:{group}).
	pollLockPrefix = "poll_openai_models_lock"
	// groupPollLockTTL matches GROUP_POLL_LOCK_SECONDS.
	groupPollLockTTL = 60 * time.Second
	// pollTriggerTimeout bounds the fire-and-forget trigger so it can
	// never delay the call.
	pollTriggerTimeout = 30 * time.Second
)

// triggerPoll fires a fire-and-forget re-poll of the (primary) model
// group, guarded by a Redis lock so concurrent calls/pods don't spam the
// poller. Mirrors CustomOpenAILLMService._trigger_event_polling: the
// lock is keyed by the primary group and the poll targets that group
// even when a fallback group is currently in use, so the exhausted
// primary endpoints get refreshed.
func (r *Router) triggerPoll(reason string) {
	if r.pollTriggerURL == "" || r.cfg.Redis == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), pollTriggerTimeout)
		defer cancel()

		lockKey := pollLockPrefix + ":" + r.cfg.Group
		acquired, err := r.cfg.Redis.AcquireLock(ctx, lockKey, groupPollLockTTL)
		if err != nil {
			r.logf("poll lock error (group=%s reason=%s): %v", r.cfg.Group, reason, err)
			return
		}
		if !acquired {
			r.logf("poll skipped, lock held (group=%s reason=%s)", r.cfg.Group, reason)
			return
		}
		r.logf("poll triggered (group=%s reason=%s)", r.cfg.Group, reason)
		if err := r.postPoll(ctx, reason); err != nil {
			r.logf("poll trigger failed (group=%s reason=%s): %v", r.cfg.Group, reason, err)
		}
	}()
}

func (r *Router) postPoll(ctx context.Context, reason string) error {
	payload, err := json.Marshal(map[string]any{
		"model_group": r.cfg.Group,
		"region":      r.region(),
		"reason":      reason,
		"force":       true,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.pollTriggerURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("poll endpoint status %d", resp.StatusCode)
	}
	return nil
}
