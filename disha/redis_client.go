package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/redis/go-redis/v9"
)

const (
	conversationDataKeyPrefix  = "conversation_data"
	conversationChunkKeyPrefix = "conversation_chunks"
	outboxKeyPrefix            = "vago_outbox"
)

var ErrConversationDataNotFound = errors.New("disha: conversation_data not found in Redis")

// RedisClient is the narrow Redis surface required by the Disha
// integration.
type RedisClient interface {
	GetConversationData(ctx context.Context, conversationID string) (*ConversationData, error)
	GetCache(ctx context.Context, key string) ([]byte, bool, error)
	MGetCache(ctx context.Context, keys ...string) ([][]byte, error)
	SetCache(ctx context.Context, key string, value any, expiration time.Duration) error
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	AppendChunk(ctx context.Context, userID, conversationID string, chunk ConversationChunk) error

	// Outbox durability primitives. These are phrased as outbox
	// operations rather than raw Redis verbs so the claim's atomicity
	// (a Lua script) stays an implementation detail of this file,
	// matching how AppendChunk hides its RPUSH.
	EnqueueOutboxItem(ctx context.Context, id string, payload []byte, runAt time.Time) error
	ClaimOutboxItems(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]OutboxRecord, error)
	RescheduleOutboxItem(ctx context.Context, id string, payload []byte, runAt time.Time) error
	DeleteOutboxItem(ctx context.Context, id string) error
	ParkOutboxItem(ctx context.Context, id string, payload []byte) error

	Close() error
}

type redisClient struct {
	rdb    *redis.Client
	logger *log.Logger
}

func NewRedisClient(addr, password string, db int, logger *log.Logger) RedisClient {
	return &redisClient{
		rdb:    redis.NewClient(redisOptions(addr, password, db)),
		logger: logger,
	}
}

func redisOptions(addr, password string, db int) *redis.Options {
	var opts *redis.Options
	if strings.Contains(addr, "://") {
		parsed, err := redis.ParseURL(addr)
		if err == nil {
			opts = parsed
		}
	}
	if opts == nil {
		opts = &redis.Options{Addr: normalizeRedisAddr(addr)}
	}
	if password != "" {
		opts.Password = password
	}
	opts.DB = db
	opts.MaxRetries = 3
	opts.MinRetryBackoff = 100 * time.Millisecond
	opts.MaxRetryBackoff = time.Second
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 3 * time.Second
	opts.WriteTimeout = 3 * time.Second
	return opts
}

func normalizeRedisAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "localhost:6379"
	}
	if strings.Contains(addr, "://") {
		return addr
	}
	if strings.Contains(addr, ":") {
		return addr
	}
	return addr + ":6379"
}

func (c *redisClient) Close() error {
	return c.rdb.Close()
}

func (c *redisClient) GetConversationData(ctx context.Context, conversationID string) (*ConversationData, error) {
	key := conversationDataKey(conversationID)
	var raw []byte
	err := withRedisTimeoutRetry(ctx, func() error {
		var getErr error
		raw, getErr = c.rdb.Get(ctx, key).Bytes()
		return getErr
	})
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("%w: %s", ErrConversationDataNotFound, conversationID)
	}
	if err != nil {
		wrapped := fmt.Errorf("disha: redis GET %s failed: %w", key, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "GET"},
			Details: map[string]any{
				"key": key,
			},
		})
		return nil, wrapped
	}

	var data ConversationData
	if err := json.Unmarshal(raw, &data); err != nil {
		wrapped := fmt.Errorf("disha: conversation_data malformed for %s: %w", conversationID, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "UNMARSHAL"},
			Details: map[string]any{
				"key": key,
			},
		})
		return nil, wrapped
	}
	return &data, nil
}

func (c *redisClient) GetCache(ctx context.Context, key string) ([]byte, bool, error) {
	var raw []byte
	err := withRedisTimeoutRetry(ctx, func() error {
		var getErr error
		raw, getErr = c.rdb.Get(ctx, key).Bytes()
		return getErr
	})
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		wrapped := fmt.Errorf("disha: redis GET %s failed: %w", key, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "GET"},
			Details: map[string]any{
				"key": key,
			},
		})
		return nil, false, wrapped
	}
	return raw, true, nil
}

// MGetCache fetches multiple cache keys in a single round trip. The
// returned slice has one entry per requested key, in order; a nil entry
// means that key was absent (redis.Nil). Used by the LLM router to read
// all of a model group's endpoint-health keys at once.
func (c *redisClient) MGetCache(ctx context.Context, keys ...string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	var vals []any
	err := withRedisTimeoutRetry(ctx, func() error {
		var getErr error
		vals, getErr = c.rdb.MGet(ctx, keys...).Result()
		return getErr
	})
	if err != nil {
		return nil, fmt.Errorf("disha: redis MGET failed: %w", err)
	}
	out := make([][]byte, len(keys))
	for i := range keys {
		if i >= len(vals) || vals[i] == nil {
			continue
		}
		switch v := vals[i].(type) {
		case string:
			out[i] = []byte(v)
		case []byte:
			out[i] = v
		}
	}
	return out, nil
}

// AcquireLock performs a SET NX with expiry, returning whether the lock
// was acquired. Mirrors Disha's acquire_redis_lock; used to gate the
// LLM re-poll trigger so it isn't fired concurrently.
func (c *redisClient) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	var acquired bool
	err := withRedisTimeoutRetry(ctx, func() error {
		var setErr error
		acquired, setErr = c.rdb.SetNX(ctx, key, "1", ttl).Result()
		return setErr
	})
	if err != nil {
		return false, fmt.Errorf("disha: redis SETNX %s failed: %w", key, err)
	}
	return acquired, nil
}

func (c *redisClient) SetCache(ctx context.Context, key string, value any, expiration time.Duration) error {
	payload, err := json.Marshal(value)
	if err != nil {
		wrapped := fmt.Errorf("disha: cache marshal failed: %w", err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "MARSHAL"},
			Details: map[string]any{
				"key": key,
			},
		})
		return wrapped
	}
	if err := withRedisTimeoutRetry(ctx, func() error {
		return c.rdb.Set(ctx, key, payload, expiration).Err()
	}); err != nil {
		wrapped := fmt.Errorf("disha: redis SET %s failed: %w", key, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "SET"},
			Details: map[string]any{
				"key": key,
			},
		})
		return wrapped
	}
	return nil
}

func (c *redisClient) AppendChunk(ctx context.Context, userID, conversationID string, chunk ConversationChunk) error {
	key := conversationChunksKey(userID, conversationID)
	payload, err := json.Marshal(chunk)
	if err != nil {
		wrapped := fmt.Errorf("disha: chunk marshal failed: %w", err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "MARSHAL"},
			Details: map[string]any{
				"key": key,
			},
		})
		return wrapped
	}
	if err := withRedisTimeoutRetry(ctx, func() error {
		return c.rdb.RPush(ctx, key, payload).Err()
	}); err != nil {
		wrapped := fmt.Errorf("disha: redis RPUSH %s failed: %w", key, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "RPUSH"},
			Details: map[string]any{
				"key": key,
			},
		})
		return wrapped
	}
	if c.logger != nil {
		c.logger.Printf("Appended chunk %s to Redis conversation %s\n", chunk.ID, conversationID)
	}
	return nil
}

// outboxClaimScript atomically leases due items. A ZRANGEBYSCORE
// followed by a separate ZADD would race: two pods can read the same
// member before either re-scores it, and the item then runs twice. The
// script re-scores each claimed member to now+lease, so the new score IS
// the lease — a pod that dies mid-attempt simply lets the item fall due
// again, with no separate lock and no reaper. Members whose item key has
// expired are dropped from the set as we go.
//
// KEYS[1] due-set key, KEYS[2] item-key prefix
// ARGV[1] now (unix ms), ARGV[2] lease deadline (unix ms), ARGV[3] limit
// returns a flat [id, payload, id, payload, ...]
var outboxClaimScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
local out = {}
for i = 1, #due do
  local id = due[i]
  local payload = redis.call('GET', KEYS[2] .. id)
  if payload then
    redis.call('ZADD', KEYS[1], ARGV[2], id)
    out[#out + 1] = id
    out[#out + 1] = payload
  else
    redis.call('ZREM', KEYS[1], id)
  end
end
return out
`)

func (c *redisClient) EnqueueOutboxItem(ctx context.Context, id string, payload []byte, runAt time.Time) error {
	return c.writeOutboxItem(ctx, id, payload, runAt, "ENQUEUE")
}

func (c *redisClient) RescheduleOutboxItem(ctx context.Context, id string, payload []byte, runAt time.Time) error {
	return c.writeOutboxItem(ctx, id, payload, runAt, "RESCHEDULE")
}

func (c *redisClient) writeOutboxItem(ctx context.Context, id string, payload []byte, runAt time.Time, operation string) error {
	itemKey := outboxItemKey(id)
	if err := withRedisTimeoutRetry(ctx, func() error {
		_, err := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, itemKey, payload, outboxItemTTL)
			pipe.ZAdd(ctx, outboxDueKey(), redis.Z{
				Score:  float64(runAt.UnixMilli()),
				Member: id,
			})
			return nil
		})
		return err
	}); err != nil {
		wrapped := fmt.Errorf("disha: redis outbox %s %s failed: %w", operation, itemKey, err)
		// TODO: remove this when merging PR — Sentry is rate-limited to
		// one event a minute, so the log is the only place the real
		// frequency of a Redis outage shows up.
		outboxLogf(c.logger, "redis %s FAILED key=%s: %v", operation, itemKey, err)
		captureOutboxRedisFailure(wrapped, "OUTBOX_"+operation, itemKey)
		return wrapped
	}
	return nil
}

func (c *redisClient) ClaimOutboxItems(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]OutboxRecord, error) {
	if limit <= 0 {
		return nil, nil
	}
	var raw []any
	if err := withRedisTimeoutRetry(ctx, func() error {
		res, err := outboxClaimScript.Run(ctx, c.rdb,
			[]string{outboxDueKey(), outboxItemKeyPrefix()},
			now.UnixMilli(),
			now.Add(lease).UnixMilli(),
			limit,
		).Result()
		if err != nil {
			return err
		}
		values, ok := res.([]any)
		if !ok {
			return fmt.Errorf("unexpected claim result type %T", res)
		}
		raw = values
		return nil
	}); err != nil {
		wrapped := fmt.Errorf("disha: redis outbox CLAIM failed: %w", err)
		outboxLogf(c.logger, "redis CLAIM FAILED key=%s: %v", outboxDueKey(), err)
		captureOutboxRedisFailure(wrapped, "OUTBOX_CLAIM", outboxDueKey())
		return nil, wrapped
	}

	records := make([]OutboxRecord, 0, len(raw)/2)
	for i := 0; i+1 < len(raw); i += 2 {
		id, ok := raw[i].(string)
		if !ok {
			continue
		}
		payload, ok := raw[i+1].(string)
		if !ok {
			continue
		}
		records = append(records, OutboxRecord{ID: id, Payload: []byte(payload)})
	}
	return records, nil
}

func (c *redisClient) DeleteOutboxItem(ctx context.Context, id string) error {
	itemKey := outboxItemKey(id)
	if err := withRedisTimeoutRetry(ctx, func() error {
		_, err := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, itemKey)
			pipe.ZRem(ctx, outboxDueKey(), id)
			return nil
		})
		return err
	}); err != nil {
		wrapped := fmt.Errorf("disha: redis outbox DELETE %s failed: %w", itemKey, err)
		outboxLogf(c.logger, "redis DELETE FAILED key=%s: %v", itemKey, err)
		captureOutboxRedisFailure(wrapped, "OUTBOX_DELETE", itemKey)
		return wrapped
	}
	return nil
}

func (c *redisClient) ParkOutboxItem(ctx context.Context, id string, payload []byte) error {
	deadKey := outboxDeadKey()
	if err := withRedisTimeoutRetry(ctx, func() error {
		_, err := c.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.RPush(ctx, deadKey, payload)
			pipe.LTrim(ctx, deadKey, -outboxDeadListCap, -1)
			pipe.Del(ctx, outboxItemKey(id))
			pipe.ZRem(ctx, outboxDueKey(), id)
			return nil
		})
		return err
	}); err != nil {
		wrapped := fmt.Errorf("disha: redis outbox PARK %s failed: %w", id, err)
		outboxLogf(c.logger, "redis PARK FAILED id=%s key=%s: %v", id, deadKey, err)
		captureOutboxRedisFailure(wrapped, "OUTBOX_PARK", deadKey)
		return wrapped
	}
	return nil
}

// captureOutboxRedisFailure rate-limits outbox Redis reporting. The
// drainer claims every ~5s per pod, so an unconditional capture here
// would turn a brief Redis blip into thousands of events — reproducing
// the exact VAGO-7 problem this change exists to remove. One event per
// operation per minute is enough to see that Redis is unhealthy.
func captureOutboxRedisFailure(err error, operation, key string) {
	if !allowSentryReport("outbox_redis:"+operation, time.Now()) {
		return
	}
	sentryutil.Capture(sentryutil.Event{
		Err:     err,
		Tags:    map[string]string{"component": "disha_redis", "operation": operation},
		Details: map[string]any{"key": key},
	})
}

func outboxDueKey() string {
	return outboxKeyPrefix + ":due"
}

func outboxDeadKey() string {
	return outboxKeyPrefix + ":dead"
}

func outboxItemKeyPrefix() string {
	return outboxKeyPrefix + ":item:"
}

func outboxItemKey(id string) string {
	return outboxItemKeyPrefix() + id
}

func conversationDataKey(conversationID string) string {
	return fmt.Sprintf("%s:%s", conversationDataKeyPrefix, conversationID)
}

func conversationChunksKey(userID, conversationID string) string {
	return fmt.Sprintf("%s:%s:%s", conversationChunkKeyPrefix, userID, conversationID)
}

func withRedisTimeoutRetry(ctx context.Context, op func() error) error {
	const maxAttempts = 3
	backoff := 100 * time.Millisecond
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = op()
		if err == nil || !isRedisTimeout(err) || attempt == maxAttempts {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
		if backoff > time.Second {
			backoff = time.Second
		}
	}
	return err
}

func isRedisTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
