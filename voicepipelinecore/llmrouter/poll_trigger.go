package llmrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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

// pollTrigger fires the on-demand re-poll of one model group. Both the
// Chat-Completions Router and the Responses WebSocket client own one, so
// every health-selected group refreshes its Redis health the same way.
// The zero value (and fixed-endpoint mode, which has no group to re-rank:
// Python's failover service never touches the poller) is disabled.
type pollTrigger struct {
	group      string
	region     string
	url        string
	redis      RedisStore
	httpClient *http.Client
	logf       func(format string, args ...any)
}

func newPollTrigger(cfg Config, httpClient *http.Client, logf func(format string, args ...any)) pollTrigger {
	if cfg.FixedEndpoint != "" {
		return pollTrigger{}
	}
	region := cfg.Region
	if region == "" {
		region = defaultRegion
	}
	return pollTrigger{
		group:      cfg.Group,
		region:     region,
		url:        strings.TrimSpace(os.Getenv(pollTriggerURLEnv)),
		redis:      cfg.Redis,
		httpClient: httpClient,
		logf:       logf,
	}
}

// trigger fires a fire-and-forget re-poll of the (primary) model group,
// guarded by a Redis lock so concurrent calls/pods don't spam the poller.
// Mirrors CustomOpenAILLMService._trigger_event_polling: the lock is keyed
// by the primary group and the poll targets that group even when a
// fallback group is currently in use, so the exhausted primary endpoints
// get refreshed.
func (p pollTrigger) trigger(reason string) {
	if p.group == "" || p.url == "" || p.redis == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), pollTriggerTimeout)
		defer cancel()

		lockKey := pollLockPrefix + ":" + p.group
		acquired, err := p.redis.AcquireLock(ctx, lockKey, groupPollLockTTL)
		if err != nil {
			p.logf("poll lock error (group=%s reason=%s): %v", p.group, reason, err)
			return
		}
		if !acquired {
			p.logf("poll skipped, lock held (group=%s reason=%s)", p.group, reason)
			return
		}
		p.logf("poll triggered (group=%s reason=%s)", p.group, reason)
		if err := p.post(ctx, reason); err != nil {
			p.logf("poll trigger failed (group=%s reason=%s): %v", p.group, reason, err)
		}
	}()
}

func (p pollTrigger) post(ctx context.Context, reason string) error {
	payload, err := json.Marshal(map[string]any{
		"model_group": p.group,
		"region":      p.region,
		"reason":      reason,
		"force":       true,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.httpClient.Do(req)
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
