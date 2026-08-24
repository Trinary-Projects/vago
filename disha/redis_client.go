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
)

var (
	ErrConversationDataNotFound  = errors.New("disha: conversation_data not found in Redis")
	ErrConversationChunkNotFound = errors.New("disha: conversation chunk not found in Redis")
)

// RedisClient is the narrow Redis surface required by the Disha
// integration.
type RedisClient interface {
	GetConversationData(ctx context.Context, conversationID string) (*ConversationData, error)
	GetCache(ctx context.Context, key string) ([]byte, bool, error)
	MGetCache(ctx context.Context, keys ...string) ([][]byte, error)
	SetCache(ctx context.Context, key string, value any, expiration time.Duration) error
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	AppendChunk(ctx context.Context, userID, conversationID string, chunk ConversationChunk) error
	ReplaceChunk(ctx context.Context, userID, conversationID, chunkID string, chunk ConversationChunk) error
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

// ReplaceChunk updates one unsynced conversation chunk in place. The
// optimistic transaction keeps the lookup-by-ID and LSET atomic even when a
// debug chunk is appended concurrently. Retrying is safe because the chunk ID
// and replacement payload are stable.
func (c *redisClient) ReplaceChunk(ctx context.Context, userID, conversationID, chunkID string, chunk ConversationChunk) error {
	key := conversationChunksKey(userID, conversationID)
	payload, err := json.Marshal(chunk)
	if err != nil {
		wrapped := fmt.Errorf("disha: replacement chunk marshal failed: %w", err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "MARSHAL"},
			Details: map[string]any{
				"key":      key,
				"chunk_id": chunkID,
			},
		})
		return wrapped
	}

	err = withRedisTimeoutRetry(ctx, func() error {
		const maxTransactionAttempts = 3
		for attempt := 1; attempt <= maxTransactionAttempts; attempt++ {
			txErr := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
				items, rangeErr := tx.LRange(ctx, key, 0, -1).Result()
				if rangeErr != nil {
					return rangeErr
				}

				index := -1
				for i := len(items) - 1; i >= 0; i-- {
					var candidate struct {
						ID string `json:"id"`
					}
					if json.Unmarshal([]byte(items[i]), &candidate) == nil && candidate.ID == chunkID {
						index = i
						break
					}
				}
				if index < 0 {
					return fmt.Errorf("%w: %s", ErrConversationChunkNotFound, chunkID)
				}

				_, pipeErr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.LSet(ctx, key, int64(index), payload)
					return nil
				})
				return pipeErr
			}, key)
			if !errors.Is(txErr, redis.TxFailedErr) {
				return txErr
			}
		}
		return redis.TxFailedErr
	})
	if err != nil {
		wrapped := fmt.Errorf("disha: redis replace chunk %s in %s failed: %w", chunkID, key, err)
		sentryutil.Capture(sentryutil.Event{
			Err:  wrapped,
			Tags: map[string]string{"component": "disha_redis", "operation": "LSET"},
			Details: map[string]any{
				"key":      key,
				"chunk_id": chunkID,
			},
		})
		return wrapped
	}
	if c.logger != nil {
		c.logger.Printf("Replaced chunk %s in Redis conversation %s\n", chunkID, conversationID)
	}
	return nil
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
