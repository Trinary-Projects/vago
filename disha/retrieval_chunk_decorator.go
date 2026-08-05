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

const (
	protocolRetrievalUploadTimeout = 5 * time.Second
	guardrailCheckUploadTimeout    = 5 * time.Second
)

// newRetrievalChunkDecorator returns a chunk decorator that attaches the
// pending retrieval record to the assistant turn it produced.
//
// Only the spoken Disha turn qualifies: role assistant, not a debug-log chunk,
// and no additional_data. That last condition is load-bearing —
// OnToolResultCommitted also writes an assistant-role chunk (the tool_calls
// half of the pair), and it must not consume the record.
func newRetrievalChunkDecorator(
	protocolBox *protocolRecordBox,
	guardrailBox *guardrailRecordBox,
	uploader JSONUploader,
	logger *log.Logger,
	userID, conversationID, botType string,
) func(*ConversationChunk) {
	return func(chunk *ConversationChunk) {
		if chunk == nil || chunk.Role != "assistant" || chunk.IsDebugLog || chunk.AdditionalData != nil {
			return
		}

		if protocolBox != nil {
			record := protocolBox.take()
			if record != nil {
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

				if chunk.ChunkRetrievalMetrics == nil {
					chunk.ChunkRetrievalMetrics = &ChunkRetrievalMetrics{}
				}
				chunk.ChunkRetrievalMetrics.Protocol = protocol
			}
		}

		if guardrailBox != nil {
			record := guardrailBox.take()
			if record != nil {
				record.ChunkID = chunk.ID
				guardrail := &GuardrailCheckMetrics{
					E2EMs:           record.slowestTotalMs,
					SimilarityScore: record.highestSimilarity,
					Interrupted:     record.Interrupted,
					CheckCount:      record.CheckCount,
					ChecksFired:     record.ChecksFired,
					QueryText:       record.TurnText,
					Status:          record.Status,
					Error:           record.Error,
				}
				if record.ChecksFired != record.CheckCount && logger != nil {
					logger.Printf(
						"disha: guardrail checks truncated chunk=%s checks_fired=%d check_count=%d\n",
						chunk.ID, record.ChecksFired, record.CheckCount,
					)
				}

				if key, err := uploadGuardrailCheckRecord(
					uploader, logger, userID, conversationID, *record,
				); err == nil {
					guardrail.RawDataS3Key = key
				}

				// Merge rather than assign: protocol retrieval and guardrail checks
				// can both populate the same turn, in either order.
				if chunk.ChunkRetrievalMetrics == nil {
					chunk.ChunkRetrievalMetrics = &ChunkRetrievalMetrics{}
				}
				chunk.ChunkRetrievalMetrics.Guardrail = guardrail
			}
		}
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

// uploadGuardrailCheckRecord writes the self-describing sentence record to the
// US bucket synchronously before the caller's Redis write. A nil uploader
// leaves compact metrics on the chunk without publishing a dangling key.
func uploadGuardrailCheckRecord(
	uploader JSONUploader,
	logger *log.Logger,
	userID, conversationID string,
	record guardrailCheckRecord,
) (string, error) {
	if uploader == nil {
		return "", fmt.Errorf("disha: no US bucket uploader for guardrail check")
	}
	key := fmt.Sprintf("%s/%s/%s.json", guardrailS3KeyPrefix, conversationID, record.ChunkID)

	ctx, cancel := context.WithTimeout(context.Background(), guardrailCheckUploadTimeout)
	defer cancel()

	if err := uploader.UploadJSON(ctx, key, record); err != nil {
		if logger != nil {
			logger.Printf("disha: guardrail check record upload failed chunk=%s: %v\n", record.ChunkID, err)
		}
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "disha_followup",
				"operation": "guardrail_check_upload",
			},
			Details: map[string]any{
				"conversation_id": conversationID,
				"user_id":         userID,
				"chunk_id":        record.ChunkID,
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

	// similarity_at_add, not similarity: this is the score the protocol had when
	// it was ADMITTED, not this round's. It is what the eviction tie-break
	// compares, so a protocol can persist through a round in which it scored
	// poorly — and candidate_protocols.similarity one level away IS the current
	// round's score, so the two must not share a name.
	injectedProtocols := make([]map[string]any, 0, len(record.Injected))
	for _, protocol := range record.Injected {
		injectedProtocols = append(injectedProtocols, map[string]any{
			"instruction_id":        protocol.InstructionID,
			"title":                 protocol.Title,
			"document_version_path": protocol.DocumentPath,
			"remaining_turns":       protocol.RemainingTurns,
			"turn_threshold":        protocol.Threshold,
			"similarity_at_add":     protocol.ScoreAtAdd,
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
