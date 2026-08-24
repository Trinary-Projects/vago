package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/getsentry/sentry-go"
)

type simpleTemplateRenderer struct{}

func (simpleTemplateRenderer) Render(_ context.Context, req TemplateRenderRequest) (TemplateRenderResult, error) {
	out := req.Text
	for key, val := range req.Variables {
		rendered := fmt.Sprint(val)
		out = strings.ReplaceAll(out, "{{ "+key+" }}", rendered)
		out = strings.ReplaceAll(out, "{{"+key+"}}", rendered)
	}
	return TemplateRenderResult{Output: out}, nil
}

func (simpleTemplateRenderer) Close() error {
	return nil
}

func TestDocumentStoreUsesInjectedTemplateEngine(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	redisServer := miniredis.RunT(t)
	redisClient := NewRedisClient(redisServer.Addr(), "", 0, log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = redisClient.Close() })

	doc := DocumentVersion{
		ID:         "doc-1",
		PromptText: "Hi {{ name }}",
		Version:    3,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal document: %v", err)
	}
	redisServer.Set("document:test/prompt:production", string(raw))

	store := newDocumentStore(redisClient, log.New(io.Discard, "", 0), simpleTemplateRenderer{})
	got, version, err := store.GetDocument(context.Background(), "test/prompt", 0, DocumentVariables{"name": "Riya"})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if version != 3 {
		t.Fatalf("version = %d, want 3", version)
	}
	if want := "Hi Riya"; got != want {
		t.Fatalf("rendered prompt = %q, want %q", got, want)
	}
}

func TestDocumentStoreReturnsConfigCopy(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	redisServer := miniredis.RunT(t)
	redisClient := NewRedisClient(redisServer.Addr(), "", 0, log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = redisClient.Close() })

	doc := DocumentVersion{
		ID:         "doc-1",
		PromptText: "Hi {{ name }}",
		ConfigJSON: map[string]any{
			"tools": []any{map[string]any{"name": "get_guidance"}},
		},
		Version: 7,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal document: %v", err)
	}
	redisServer.Set("document:test/config:production", string(raw))

	store := newDocumentStore(redisClient, log.New(io.Discard, "", 0), simpleTemplateRenderer{})
	got, version, config, err := store.GetDocumentWithConfig(context.Background(), "test/config", 0, DocumentVariables{"name": "Riya"})
	if err != nil {
		t.Fatalf("GetDocumentWithConfig: %v", err)
	}
	if got != "Hi Riya" || version != 7 {
		t.Fatalf("rendered/version = %q/%d, want Hi Riya/7", got, version)
	}
	if tools, ok := config["tools"].([]any); !ok || len(tools) != 1 {
		t.Fatalf("tools config = %#v, want one tool", config["tools"])
	}
	config["tools"] = nil

	_, _, secondConfig, err := store.GetDocumentWithConfig(context.Background(), "test/config", 0, DocumentVariables{"name": "Riya"})
	if err != nil {
		t.Fatalf("GetDocumentWithConfig second: %v", err)
	}
	if tools, ok := secondConfig["tools"].([]any); !ok || len(tools) != 1 {
		t.Fatalf("cached config was mutated: %#v", secondConfig["tools"])
	}
}

func bindMockSentry(t *testing.T) *sentry.MockTransport {
	t.Helper()
	transport := &sentry.MockTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Transport: transport})
	if err != nil {
		t.Fatalf("sentry.NewClient: %v", err)
	}
	original := sentry.CurrentHub().Client()
	sentry.CurrentHub().BindClient(client)
	t.Cleanup(func() { sentry.CurrentHub().BindClient(original) })
	return transport
}

type scriptedCacheRedis struct {
	get func(ctx context.Context, key string) ([]byte, bool, error)
}

func (s scriptedCacheRedis) GetCache(ctx context.Context, key string) ([]byte, bool, error) {
	if s.get != nil {
		return s.get(ctx, key)
	}
	return nil, false, nil
}

func (s scriptedCacheRedis) GetConversationData(context.Context, string) (*ConversationData, error) {
	return nil, errors.New("unused")
}

func (s scriptedCacheRedis) MGetCache(context.Context, ...string) ([][]byte, error) {
	return nil, errors.New("unused")
}

func (s scriptedCacheRedis) SetCache(context.Context, string, any, time.Duration) error {
	return errors.New("unused")
}

func (s scriptedCacheRedis) AcquireLock(context.Context, string, time.Duration) (bool, error) {
	return false, errors.New("unused")
}

func (s scriptedCacheRedis) AppendChunk(context.Context, string, string, ConversationChunk) error {
	return errors.New("unused")
}

func (s scriptedCacheRedis) ReplaceChunk(context.Context, string, string, string, ConversationChunk) error {
	return errors.New("unused")
}

func (s scriptedCacheRedis) Close() error { return nil }

func TestDocumentStoreReportsMissingDocumentToSentry(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	transport := bindMockSentry(t)
	redisServer := miniredis.RunT(t)
	redisClient := NewRedisClient(redisServer.Addr(), "", 0, log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = redisClient.Close() })

	store := newDocumentStore(redisClient, log.New(io.Discard, "", 0), simpleTemplateRenderer{})
	_, _, err := store.GetDocument(context.Background(), "missing/prompt", 0, DocumentVariables{"name": "Riya"})
	if err == nil {
		t.Fatal("GetDocument: expected missing-document error")
	}
	if !strings.Contains(err.Error(), "not in redis key document:missing/prompt:production") {
		t.Fatalf("GetDocument error = %v, want redis miss", err)
	}

	events := transport.Events()
	if len(events) != 1 {
		t.Fatalf("Sentry events = %d, want 1", len(events))
	}
	event := events[0]
	if event.Tags["component"] != "disha_document_store" {
		t.Fatalf("component tag = %q", event.Tags["component"])
	}
	if event.Tags["operation"] != "fetch_document" {
		t.Fatalf("operation tag = %q", event.Tags["operation"])
	}
	if event.Tags["document_name"] != "missing/prompt" {
		t.Fatalf("document_name tag = %q", event.Tags["document_name"])
	}
	details := event.Contexts["details"]
	if details["redis_key"] != "document:missing/prompt:production" {
		t.Fatalf("redis_key details = %#v", details["redis_key"])
	}
}

func TestDocumentStoreDoesNotReportCanceledFetchToSentry(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	transport := bindMockSentry(t)
	store := newDocumentStore(scriptedCacheRedis{
		get: func(ctx context.Context, key string) ([]byte, bool, error) {
			return nil, false, ctx.Err()
		},
	}, log.New(io.Discard, "", 0), simpleTemplateRenderer{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := store.GetDocument(ctx, "canceled/prompt", 0, nil)
	if err == nil {
		t.Fatal("GetDocument: expected canceled error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetDocument error = %v, want context.Canceled", err)
	}
	if len(transport.Events()) != 0 {
		t.Fatalf("Sentry events = %d, want 0 for canceled fetch", len(transport.Events()))
	}
}

func TestDocumentStoreDoesNotReportPresentDocumentToSentry(t *testing.T) {
	t.Setenv("ENVIRONMENT", "prod")
	transport := bindMockSentry(t)
	redisServer := miniredis.RunT(t)
	redisClient := NewRedisClient(redisServer.Addr(), "", 0, log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = redisClient.Close() })

	doc := DocumentVersion{
		ID:         "doc-1",
		PromptText: "Hi {{ name }}",
		Version:    3,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal document: %v", err)
	}
	redisServer.Set("document:test/prompt:production", string(raw))

	store := newDocumentStore(redisClient, log.New(io.Discard, "", 0), simpleTemplateRenderer{})
	got, _, err := store.GetDocument(context.Background(), "test/prompt", 0, DocumentVariables{"name": "Riya"})
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if got != "Hi Riya" {
		t.Fatalf("rendered prompt = %q, want Hi Riya", got)
	}
	if len(transport.Events()) != 0 {
		t.Fatalf("Sentry events = %d, want 0 for a present document", len(transport.Events()))
	}
}
