package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

// PhoneticDict loads Disha's phonetic dictionary from S3 (the JSON blob
// behind Python's bots/onboarding_call/phonetic_text_filter.py). It is
// purely a data source: it downloads the token → replacement map once
// and hands it to voicepipelinecore, which owns the actual TTS text
// filtering. Loaded lazily on the first Dictionary() (or explicit
// Preload), thread-safe afterwards.
type PhoneticDict struct {
	logger    *log.Logger
	client    S3GetClient
	objectKey string
	loadOnce  sync.Once
	loadErr   error
	dict      map[string]string
}

// NewPhoneticDictFromEnv constructs a PhoneticDict that reads the JSON
// blob at s3://${AWS_US_BUCKET_NAME}/${PHONETICS_DICT_KEY}.
// Returns nil (with a log line) when env is incomplete so callers can
// no-op gracefully.
func NewPhoneticDictFromEnv(logger *log.Logger) *PhoneticDict {
	objectKey := strings.TrimSpace(os.Getenv("PHONETICS_DICT_KEY"))
	if objectKey == "" {
		if logger != nil {
			logger.Println("disha: phonetic dict disabled; PHONETICS_DICT_KEY is empty")
		}
		return nil
	}
	// The client resolves its own bucket from env; GetObject is called
	// with an empty bucket below so it uses that one. The US bucket lives
	// in AWS_US_REGION (us-east-1), not AWS_MAIN_REGION.
	client := NewS3GetClientFromEnv(logger, "AWS_US_BUCKET_NAME", "AWS_US_REGION")
	if client == nil {
		return nil
	}
	return &PhoneticDict{
		logger:    logger,
		client:    client,
		objectKey: objectKey,
	}
}

// Preload eagerly fetches the dictionary. Safe to call from app
// startup; subsequent calls are no-ops. Returns the load error (if
// any) so the caller can log it.
func (p *PhoneticDict) Preload(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.ensureLoaded(ctx)
	return p.loadErr
}

// Dictionary returns the loaded token → replacement map, fetching it on
// first use. Returns nil when the dictionary can't be loaded; callers
// treat a nil/empty map as "no phonetic filtering".
func (p *PhoneticDict) Dictionary(ctx context.Context) map[string]string {
	if p == nil {
		return nil
	}
	p.ensureLoaded(ctx)
	return p.dict
}

func (p *PhoneticDict) ensureLoaded(ctx context.Context) {
	p.loadOnce.Do(func() {
		if p.client == nil {
			p.loadErr = errors.New("disha: phonetic dict client is nil")
			return
		}
		fetchCtx, cancel := context.WithTimeout(ctx, defaultS3OperationTimeout)
		defer cancel()
		raw, err := p.client.GetObject(fetchCtx, "", p.objectKey)
		if err != nil {
			p.loadErr = fmt.Errorf("disha: fetch phonetic dict: %w", err)
			return
		}
		var dict map[string]string
		if err := json.Unmarshal(raw, &dict); err != nil {
			p.loadErr = fmt.Errorf("disha: parse phonetic dict: %w", err)
			return
		}
		p.dict = dict
		if p.logger != nil {
			p.logger.Printf("disha: phonetic dict loaded from %s with %d entries\n", p.objectKey, len(dict))
		}
	})
}
