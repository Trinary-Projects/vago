package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const (
	documentCacheTTL  = 20 * time.Minute
	documentKeyPrefix = "document"
)

// DocumentVersion is the shape Disha stores under
// `document:{name}:{env}` (or `document:{name}:v{version}`).
type DocumentVersion struct {
	ID         string         `json:"id"`
	PromptText string         `json:"prompt_text"`
	ConfigJSON map[string]any `json:"config_json"`
	Version    int            `json:"version"`
}

// DocumentStore reads Disha's Langfuse-backed prompts from Redis. It
// mirrors documents/document_manager.py:_fetch_from_redis — same key
// shape, same JSON payload — but never falls through to Postgres
// (talk-go has no DB connection by design).
//
// The store keeps a per-process in-memory cache to avoid touching
// Redis once a prompt has been resolved. TTL matches Python (20 min).
type DocumentStore struct {
	redis    RedisClient
	logger   *log.Logger
	env      string
	renderer TemplateRenderer

	mu    sync.Mutex
	cache map[string]documentCacheEntry
}

type documentCacheEntry struct {
	doc       DocumentVersion
	expiresAt time.Time
}

// NewDocumentStore wires the store to the shared Disha Redis client.
// Environment selection follows Python's _resolve_environment:
//   - ENVIRONMENT in {"staging", "dev"} → key suffix "latest"
//   - anything else                     → key suffix "production"
func NewDocumentStore(redis RedisClient, logger *log.Logger) *DocumentStore {
	return newDocumentStore(redis, logger, NewGonjaJinjaRenderer(logger))
}

func newDocumentStore(redis RedisClient, logger *log.Logger, renderer TemplateRenderer) *DocumentStore {
	if redis == nil {
		return nil
	}
	return &DocumentStore{
		redis:    redis,
		logger:   logger,
		env:      resolveDocumentEnvironment(),
		renderer: renderer,
		cache:    make(map[string]documentCacheEntry),
	}
}

func resolveDocumentEnvironment() string {
	switch strings.TrimSpace(strings.ToLower(os.Getenv("ENVIRONMENT"))) {
	case "staging", "dev":
		return "latest"
	default:
		return "production"
	}
}

// GetDocument fetches the prompt for `name` (or a specific version
// when version != 0) and renders it with the provided variables.
// Returns the rendered text and the resolved version (useful for
// pinning system_message_prompt_key on chunks).
func (s *DocumentStore) GetDocument(ctx context.Context, name string, version int, variables DocumentVariables) (string, int, error) {
	text, resolvedVersion, _, err := s.GetDocumentWithConfig(ctx, name, version, variables)
	return text, resolvedVersion, err
}

// GetDocumentWithConfig is the Go equivalent of Disha's
// get_document_with_config/get_document_and_version combination: it renders
// the prompt and also returns the Langfuse document config JSON. Dynamic
// follow-up uses the config's `tools` block to build OpenAI tool schemas.
func (s *DocumentStore) GetDocumentWithConfig(ctx context.Context, name string, version int, variables DocumentVariables) (string, int, map[string]any, error) {
	if s == nil {
		return "", 0, nil, errors.New("disha: document store is nil")
	}
	if s.renderer == nil {
		return "", 0, nil, errors.New("disha: document renderer is nil")
	}
	doc, err := s.resolve(ctx, name, version)
	if err != nil {
		return "", 0, nil, err
	}
	rendered, err := s.renderer.Render(ctx, TemplateRenderRequest{
		DocumentName:    name,
		DocumentVersion: doc.Version,
		Text:            doc.PromptText,
		Variables:       variables,
	})
	if err != nil {
		return "", 0, nil, err
	}
	if s.logger != nil {
		if rendered.UndefinedError != "" {
			s.logger.Printf("disha: document %q (version=%d) has undefined jinja variables after strict validation: %s missing=%v\n", name, doc.Version, rendered.UndefinedError, rendered.UnresolvedVariables)
		} else if len(rendered.UnresolvedVariables) > 0 {
			s.logger.Printf("disha: document %q (version=%d) has compile-time missing jinja variables: %v\n", name, doc.Version, rendered.UnresolvedVariables)
		}
		if rendered.StrictValidationError != "" {
			s.logger.Printf("disha: document %q (version=%d) strict jinja validation warning: %s\n", name, doc.Version, rendered.StrictValidationError)
		}
	}
	reportMissingJinjaVariables(name, doc.Version, rendered, variables)
	return rendered.Output, doc.Version, cloneDocumentConfig(doc.ConfigJSON), nil
}

// GetDocumentConfig resolves a document WITHOUT rendering it and returns
// its config_json, mirroring Python's document_manager.get_document_config:
// a copy of config_json with the resolved "version" and document "id"
// injected as top-level keys (overriding same-named keys, like Python).
// The onboarding stage tracker uses this to read trigger statements —
// rendering with empty variables would fire the missing-jinja-variable
// Sentry path for no reason. Uses the same resolve/caching as GetDocument.
func (s *DocumentStore) GetDocumentConfig(ctx context.Context, name string, version int) (map[string]any, int, error) {
	if s == nil {
		return nil, 0, errors.New("disha: document store is nil")
	}
	doc, err := s.resolve(ctx, name, version)
	if err != nil {
		return nil, 0, err
	}
	config := cloneDocumentConfig(doc.ConfigJSON)
	if config == nil {
		config = map[string]any{}
	}
	config["version"] = doc.Version
	if doc.ID != "" {
		config["id"] = doc.ID
	}
	return config, doc.Version, nil
}

func cloneDocumentConfig(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// reportMissingJinjaVariables raises a single Sentry warning naming every
// referenced variable with no usable value. It serves BOTH renderers through
// the shared TemplateRenderResult: the gonja renderer (production) fills
// UnresolvedVariables with the merged missing (key absent) + nil (key present,
// value nil) set, and the Python oracle (jinja_renderer.go, used only by the
// parity test) fills it with its compile-time missing set.
//
// Divergence vs. Python's original two-warning scheme: gonja detects these
// statically at top-level-name granularity, branch-agnostically, so it cannot
// restrict to render-site / taken-branch access the way Python's StrictUndefined
// render and per-output finalize hook did. This over-reports (dead branches,
// loop/set/test names) but never misses a genuinely-unresolved variable.
func reportMissingJinjaVariables(name string, version int, rendered TemplateRenderResult, variables DocumentVariables) {
	if rendered.UndefinedError == "" && len(rendered.UnresolvedVariables) == 0 {
		return
	}
	supplied := make([]string, 0, len(variables))
	for key := range variables {
		supplied = append(supplied, key)
	}
	sort.Strings(supplied)
	sentryutil.Capture(sentryutil.Event{
		Message: fmt.Sprintf("Undefined Jinja variables in document %q v%d", name, version),
		Tags: map[string]string{
			"component":        "disha_document_store",
			"operation":        "render_jinja",
			"document_name":    name,
			"document_version": fmt.Sprintf("%d", version),
		},
		Details: map[string]any{
			"document_name":                     name,
			"document_version":                  version,
			"runtime_missing_variable":          rendered.UndefinedError,
			"compile_time_unresolved_variables": rendered.UnresolvedVariables,
			"strict_validation_error":           rendered.StrictValidationError,
			"supplied_variables":                supplied,
		},
	})
}

func (s *DocumentStore) resolve(ctx context.Context, name string, version int) (DocumentVersion, error) {
	cacheKey := documentCacheKey(name, version)
	if cached, ok := s.lookupCache(cacheKey); ok {
		return cached, nil
	}
	doc, err := s.fetchFromRedis(ctx, name, version)
	if err != nil {
		return DocumentVersion{}, err
	}
	s.storeCache(cacheKey, doc)
	return doc, nil
}

func (s *DocumentStore) fetchFromRedis(ctx context.Context, name string, version int) (DocumentVersion, error) {
	key := s.redisKey(name, version)
	raw, ok, err := s.redis.GetCache(ctx, key)
	if err != nil {
		return DocumentVersion{}, fmt.Errorf("disha: document %q redis GET failed: %w", name, err)
	}
	if !ok {
		return DocumentVersion{}, fmt.Errorf("disha: document %q (version=%d) not in redis key %s", name, version, key)
	}
	var doc DocumentVersion
	if err := json.Unmarshal(raw, &doc); err != nil {
		return DocumentVersion{}, fmt.Errorf("disha: document %q payload malformed: %w", name, err)
	}
	if s.logger != nil {
		s.logger.Printf("disha: document loaded name=%s version=%d key=%s\n", name, doc.Version, key)
	}
	return doc, nil
}

func (s *DocumentStore) lookupCache(key string) (DocumentVersion, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[key]
	if !ok || time.Now().After(entry.expiresAt) {
		if ok {
			delete(s.cache, key)
		}
		return DocumentVersion{}, false
	}
	return entry.doc, true
}

func (s *DocumentStore) storeCache(key string, doc DocumentVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[key] = documentCacheEntry{doc: doc, expiresAt: time.Now().Add(documentCacheTTL)}
}

func (s *DocumentStore) redisKey(name string, version int) string {
	if version > 0 {
		return fmt.Sprintf("%s:%s:v%d", documentKeyPrefix, name, version)
	}
	return fmt.Sprintf("%s:%s:%s", documentKeyPrefix, name, s.env)
}

func documentCacheKey(name string, version int) string {
	if version > 0 {
		return fmt.Sprintf("%s:v%d", name, version)
	}
	return name
}

// PromptKey is the canonical "system_message_prompt_key" we persist on
// chunks (matches Python's `{name}_v{version}` format).
func PromptKey(name string, version int) string {
	return fmt.Sprintf("%s_v%d", name, version)
}

func (s *DocumentStore) Close() error {
	if s == nil || s.renderer == nil {
		return nil
	}
	return s.renderer.Close()
}
