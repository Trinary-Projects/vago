package disha

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"strings"
	"sync"
	"testing"
)

type stubJSONUploader struct {
	mu   sync.Mutex
	err  error
	keys []string
	last any
}

func (u *stubJSONUploader) UploadJSON(_ context.Context, objectKey string, value any) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.err != nil {
		return u.err
	}
	u.keys = append(u.keys, objectKey)
	u.last = value
	return nil
}

func (u *stubJSONUploader) uploaded() ([]string, any) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.keys, u.last
}

func testProtocolRetrievalRecord() protocolRetrievalRecord {
	top := 0.88
	return protocolRetrievalRecord{
		QueryText:      "Disha: a\nUser: b",
		Candidates:     []protocolCandidate{{InstructionID: "instr-A", Similarity: 0.88, Qualified: true, TurnThreshold: 3}},
		Injected:       []residentProtocol{{InstructionID: "instr-A", Title: "t", ScoreAtAdd: 0.88, RemainingTurns: 3, Threshold: 3}},
		ResidentAfter:  []residentProtocol{{InstructionID: "instr-A", Title: "t", ScoreAtAdd: 0.88, RemainingTurns: 3, Threshold: 3}},
		Qualified:      1,
		LatencyMs:      31.5,
		QueryLatencyMs: 24.25,
		TopSimilarity:  &top,
		InsertIndex:    5,
		Status:         "ok",
	}
}

func newTestDecorator(t *testing.T, uploader JSONUploader) (func(*ConversationChunk), *protocolRecordBox) {
	t.Helper()
	box := &protocolRecordBox{}
	decorator := newRetrievalChunkDecorator(
		box, uploader, log.New(io.Discard, "", 0), "user-1", "conv-1", FollowUpBotType,
	)
	return decorator, box
}

func TestChunkDecoratorAttachesToSpokenAssistantChunk(t *testing.T) {
	uploader := &stubJSONUploader{}
	decorate, box := newTestDecorator(t, uploader)
	box.put(testProtocolRetrievalRecord())

	chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	decorate(chunk)

	if chunk.ChunkRetrievalMetrics.protocolOrNil() == nil {
		t.Fatal("metrics not attached")
	}
	metrics := chunk.ChunkRetrievalMetrics.Protocol
	if metrics.Status != "ok" || metrics.InjectedCount != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
	if metrics.RetrievalLatencyMs != 31.5 || metrics.VectorQueryLatencyMs != 24.25 {
		t.Fatalf("latencies = %+v", metrics)
	}
	if metrics.TopSimilarityScore == nil || *metrics.TopSimilarityScore != 0.88 {
		t.Fatalf("top similarity = %+v", metrics.TopSimilarityScore)
	}

	keys, payload := uploader.uploaded()
	wantKey := "protocol_retrieval/conv-1/chunk-1.json"
	if len(keys) != 1 || keys[0] != wantKey {
		t.Fatalf("uploaded keys = %v, want [%s]", keys, wantKey)
	}
	if metrics.ProtocolsS3Key != wantKey {
		t.Fatalf("chunk key = %q, want %q", metrics.ProtocolsS3Key, wantKey)
	}

	// The blob is the only durable copy until the backend table exists, so it
	// must be self-describing.
	body, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T", payload)
	}
	for _, key := range []string{
		"chunk_id", "conversation_id", "user_id", "bot_type", "retrieved_at",
		"query_text", "threshold", "latency_ms", "candidates",
		"injected_protocol_ids", "resident_after", "qualified_count", "insert_index", "status",
		"top_similarity",
	} {
		if _, present := body[key]; !present {
			t.Errorf("payload missing %q", key)
		}
	}
	if body["chunk_id"] != "chunk-1" || body["conversation_id"] != "conv-1" {
		t.Errorf("payload identity wrong: %v / %v", body["chunk_id"], body["conversation_id"])
	}
}

// Only the spoken Disha turn carries retrieval metrics. The tool-pair chunk is
// also assistant-role, and must not consume the record.
func TestChunkDecoratorIgnoresOtherChunks(t *testing.T) {
	tests := []struct {
		name  string
		chunk ConversationChunk
	}{
		{"user chunk", ConversationChunk{ID: "c", Role: "user"}},
		{"debug-log chunk", ConversationChunk{ID: "c", Role: "assistant", IsDebugLog: true}},
		{"tool-pair assistant chunk", ConversationChunk{
			ID: "c", Role: "assistant",
			AdditionalData: map[string]any{"tool_calls": []any{}},
		}},
		{"tool result chunk", ConversationChunk{
			ID: "c", Role: "tool",
			AdditionalData: map[string]any{"tool_call_id": "call-1"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uploader := &stubJSONUploader{}
			decorate, box := newTestDecorator(t, uploader)
			box.put(testProtocolRetrievalRecord())

			chunk := tc.chunk
			decorate(&chunk)

			if chunk.ChunkRetrievalMetrics.protocolOrNil() != nil {
				t.Errorf("metrics attached to %s", tc.name)
			}
			if keys, _ := uploader.uploaded(); len(keys) != 0 {
				t.Errorf("%s triggered an upload: %v", tc.name, keys)
			}
			// The record must still be waiting for the real assistant chunk.
			if box.take() == nil {
				t.Errorf("%s consumed the pending record", tc.name)
			}
		})
	}
}

// One retrieval round maps to exactly one chunk.
func TestChunkDecoratorConsumesRecordOnce(t *testing.T) {
	uploader := &stubJSONUploader{}
	decorate, box := newTestDecorator(t, uploader)
	box.put(testProtocolRetrievalRecord())

	first := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	second := &ConversationChunk{ID: "chunk-2", Role: "assistant"}
	decorate(first)
	decorate(second)

	if first.ChunkRetrievalMetrics.protocolOrNil() == nil {
		t.Fatal("first chunk should have metrics")
	}
	if second.ChunkRetrievalMetrics.protocolOrNil() != nil {
		t.Fatal("second chunk in the same turn must not reuse the record")
	}
	if keys, _ := uploader.uploaded(); len(keys) != 1 {
		t.Fatalf("uploads = %v, want exactly one", keys)
	}
}

// No retrieval (greet-first turn) means no metrics rather than empty ones.
func TestChunkDecoratorNoPendingRecord(t *testing.T) {
	uploader := &stubJSONUploader{}
	decorate, _ := newTestDecorator(t, uploader)

	chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	decorate(chunk)

	if chunk.ChunkRetrievalMetrics.protocolOrNil() != nil {
		t.Fatalf("metrics attached with no pending record: %+v", chunk.ChunkRetrievalMetrics)
	}
	if keys, _ := uploader.uploaded(); len(keys) != 0 {
		t.Fatalf("unexpected upload: %v", keys)
	}
}

// Upload is best-effort: the chunk is still written, just without a key.
func TestChunkDecoratorUploadFailureKeepsMetrics(t *testing.T) {
	uploader := &stubJSONUploader{err: errors.New("s3 down")}
	decorate, box := newTestDecorator(t, uploader)
	box.put(testProtocolRetrievalRecord())

	chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	decorate(chunk)

	if chunk.ChunkRetrievalMetrics.protocolOrNil() == nil {
		t.Fatal("metrics should survive an upload failure")
	}
	if chunk.ChunkRetrievalMetrics.Protocol.ProtocolsS3Key != "" {
		t.Errorf("key = %q, want empty on upload failure", chunk.ChunkRetrievalMetrics.Protocol.ProtocolsS3Key)
	}
	if chunk.ChunkRetrievalMetrics.Protocol.Status != "ok" {
		t.Errorf("upload failure must not rewrite the retrieval status: %q", chunk.ChunkRetrievalMetrics.Protocol.Status)
	}
}

// An incomplete S3 env yields a nil uploader; that must not panic.
func TestChunkDecoratorNilUploader(t *testing.T) {
	decorate, box := newTestDecorator(t, nil)
	box.put(testProtocolRetrievalRecord())

	chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	decorate(chunk)

	if chunk.ChunkRetrievalMetrics.protocolOrNil() == nil {
		t.Fatal("metrics should still be attached without an uploader")
	}
	if chunk.ChunkRetrievalMetrics.Protocol.ProtocolsS3Key != "" {
		t.Errorf("key = %q, want empty", chunk.ChunkRetrievalMetrics.Protocol.ProtocolsS3Key)
	}
}

func TestChunkDecoratorNilChunk(t *testing.T) {
	decorate, box := newTestDecorator(t, &stubJSONUploader{})
	box.put(testProtocolRetrievalRecord())
	decorate(nil) // must not panic
}

// Every other bot's chunk JSON must be byte-identical to before: the new field
// is omitempty and nothing else sets it.
func TestChunkRetrievalMetricsOmittedWhenAbsent(t *testing.T) {
	encoded, err := json.Marshal(ConversationChunk{ID: "c", Role: "user"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), "chunk_retrieval_metrics") {
		t.Fatalf("chunk_retrieval_metrics must be omitted when unset: %s", encoded)
	}

	// ...and present, with the S3 key, once retrieval has run.
	withMetrics, err := json.Marshal(ConversationChunk{
		ID: "c", Role: "assistant",
		ChunkRetrievalMetrics: &ChunkRetrievalMetrics{
			Protocol: &ProtocolRetrievalMetrics{Status: "ok", ProtocolsS3Key: "k"},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"chunk_retrieval_metrics"`, `"protocol"`, `"protocols_s3_key":"k"`, `"status":"ok"`} {
		if !strings.Contains(string(withMetrics), want) {
			t.Errorf("encoded chunk missing %s: %s", want, withMetrics)
		}
	}
}

// protocolOrNil lets the tests assert "no protocol metrics" without caring
// whether the umbrella itself was allocated. The guardrail step will allocate
// the umbrella independently once it lands.
func (m *ChunkRetrievalMetrics) protocolOrNil() *ProtocolRetrievalMetrics {
	if m == nil {
		return nil
	}
	return m.Protocol
}

// Resident-set lifecycle events are logged, not persisted (decided 2026-07-31):
// the per-turn resident_after snapshot already records where the set landed.
func TestChunkDecoratorDoesNotPersistStoreEvents(t *testing.T) {
	uploader := &stubJSONUploader{}
	decorate, box := newTestDecorator(t, uploader)
	box.put(testProtocolRetrievalRecord())

	decorate(&ConversationChunk{ID: "chunk-1", Role: "assistant"})

	_, payload := uploader.uploaded()
	body, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T", payload)
	}
	if _, present := body["events"]; present {
		t.Error("S3 payload must not carry the store event list")
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, action := range []string{`"add"`, `"evict"`, `"expire"`, `"refresh"`} {
		if strings.Contains(string(encoded), action) {
			t.Errorf("payload still contains a store event action %s", action)
		}
	}
	if body["qualified_count"] != 1 {
		t.Errorf("qualified_count = %v, want the aggregate 1", body["qualified_count"])
	}
}
