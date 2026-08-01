package disha

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// Per-turn retrieval telemetry, attached to the spoken Disha chunk.
//
// Not bot- or call-type-specific: any bot that runs a retrieval step can wire
// this decorator through CallEventCallbacks.SetChunkDecorator.
//
// The chunk carries a compact chunk_retrieval_metrics object: the latencies,
// the top score, the query text, and an S3 key. Everything disha-backend needs
// to fill its conversationchunkretrievallog row and to seed
// ProtocolLiveQueryAnchor is inline, so the chunk-sync job needs no S3 read.
//
// The full candidate list (every score, qualifying or not) is the one thing
// that stays S3-only, reachable through protocols_s3_key: it is the
// threshold-calibration dataset, it is large, and nothing on the write path
// needs it. Inline it too if Postgres should become self-sufficient.
//
// Extra top-level chunk keys are safe: Python's
// ConversationChunkManager.redis_dict_to_model reads named keys via explicit
// data.get(...), so keys it does not know are ignored rather than raising.

const protocolRetrievalUploadTimeout = 5 * time.Second

// newRetrievalChunkDecorator returns a chunk decorator that attaches the
// pending retrieval record to the assistant turn it produced.
//
// Only the spoken Disha turn qualifies: role assistant, not a debug-log chunk,
// and no additional_data. That last condition is load-bearing —
// OnToolResultCommitted also writes an assistant-role chunk (the tool_calls
// half of the pair), and it must not consume the record.
func newRetrievalChunkDecorator(
	box *protocolRecordBox,
	uploader JSONUploader,
	logger *log.Logger,
	userID, conversationID, botType string,
) func(*ConversationChunk) {
	return func(chunk *ConversationChunk) {
		if chunk == nil || chunk.Role != "assistant" || chunk.IsDebugLog || chunk.AdditionalData != nil {
			return
		}
		record := box.take()
		if record == nil {
			// Greet-first turn, or a turn whose retrieval was skipped before
			// any record existed.
			return
		}

		protocol := &ProtocolRetrievalMetrics{
			RetrievalLatencyMs:   record.LatencyMs,
			VectorQueryLatencyMs: record.QueryLatencyMs,
			TopSimilarityScore:   record.TopSimilarity,
			InjectedCount:        len(record.Injected),
			QueryText:            record.QueryText,
			Status:               record.Status,
			Error:                record.Err,
		}

		if key, err := uploadProtocolRetrievalRecord(
			uploader, logger, chunk.ID, userID, conversationID, botType, *record,
		); err == nil {
			protocol.ProtocolsS3Key = key
		}

		// Merge rather than assign: the planned guardrail step will populate a
		// sibling field on the same umbrella, and the two run at different
		// points in the turn.
		if chunk.ChunkRetrievalMetrics == nil {
			chunk.ChunkRetrievalMetrics = &ChunkRetrievalMetrics{}
		}
		chunk.ChunkRetrievalMetrics.Protocol = protocol
	}
}

// uploadProtocolRetrievalRecord writes the full record to the US bucket, synchronously
// before the caller's Redis write, so the persisted key never points at a
// missing object (same ordering rule as onboarding's per-chunk conversation
// state). A nil uploader means the S3 env is incomplete: the chunk still gets
// its metrics, just without a key.
func uploadProtocolRetrievalRecord(
	uploader JSONUploader,
	logger *log.Logger,
	chunkID, userID, conversationID, botType string,
	record protocolRetrievalRecord,
) (string, error) {
	if uploader == nil {
		return "", fmt.Errorf("disha: no US bucket uploader for protocol retrieval")
	}
	key := fmt.Sprintf("%s/%s/%s.json", protocolS3KeyPrefix, conversationID, chunkID)

	ctx, cancel := context.WithTimeout(context.Background(), protocolRetrievalUploadTimeout)
	defer cancel()

	if err := uploader.UploadJSON(ctx, key, protocolRetrievalRecordPayload(
		chunkID, userID, conversationID, botType, record,
	)); err != nil {
		if logger != nil {
			logger.Printf("disha: protocol retrieval record upload failed chunk=%s: %v\n", chunkID, err)
		}
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "disha_followup",
				"operation": "protocol_retrieval_upload",
			},
			Details: map[string]any{
				"conversation_id": conversationID,
				"user_id":         userID,
				"chunk_id":        chunkID,
				"object_key":      key,
			},
		})
		return "", err
	}
	return key, nil
}

// protocolRetrievalRecordPayload is deliberately self-describing: until the backend
// table exists this blob is the only durable copy of the round.
func protocolRetrievalRecordPayload(
	chunkID, userID, conversationID, botType string,
	record protocolRetrievalRecord,
) map[string]any {
	candidateProtocols := make([]map[string]any, 0, len(record.Candidates))
	for _, candidate := range record.Candidates {
		candidateProtocols = append(candidateProtocols, map[string]any{
			"instruction_id":        candidate.InstructionID,
			"anchor_id":             candidate.AnchorID,
			"anchor_text":           candidate.AnchorText,
			"title":                 candidate.Title,
			"document_version_path": candidate.DocumentPath,
			"turn_threshold_count":  candidate.TurnThreshold,
			"similarity":            candidate.Similarity,
			"qualified":             candidate.Qualified,
		})
	}

	// similarity here is the score the protocol had when it was ADMITTED, not
	// this round's score — it is what the eviction tie-break compares, so a
	// protocol can persist through a round in which it scored poorly.
	injectedProtocols := make([]map[string]any, 0, len(record.Injected))
	for _, protocol := range record.Injected {
		injectedProtocols = append(injectedProtocols, map[string]any{
			"instruction_id":        protocol.InstructionID,
			"title":                 protocol.Title,
			"document_version_path": protocol.DocumentPath,
			"remaining_turns":       protocol.RemainingTurns,
			"turn_threshold":        protocol.Threshold,
			"similarity":            protocol.ScoreAtAdd,
		})
	}

	payload := map[string]any{
		"chunk_id":        chunkID,
		"conversation_id": conversationID,
		"user_id":         userID,
		"bot_type":        botType,
		"retrieved_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"query_text":      record.QueryText,
		"threshold": map[string]any{
			"metric": "cosine_similarity",
			"value":  protocolSimilarityThreshold,
		},
		"latency_ms": map[string]any{
			"vector_query": record.QueryLatencyMs,
			"total":        record.LatencyMs,
		},
		"candidate_protocols": candidateProtocols,
		"qualified_count":     record.Qualified,
		"injected_protocols":  injectedProtocols,
		"insert_index":        record.InsertIndex,
		"status":              record.Status,
	}
	if record.TopSimilarity != nil {
		payload["top_similarity"] = *record.TopSimilarity
	}
	if record.Err != "" {
		payload["error"] = record.Err
	}
	return payload
}
