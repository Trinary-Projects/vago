package disha

import (
	"sync"
	"time"
)

const (
	guardrailAnchorClass           = "GuardrailAnchor"
	guardrailInstructionClass      = "GuardrailInstruction"
	guardrailInterruptThreshold    = 0.85
	guardrailOfflineJudgeThreshold = 0.75
	guardrailQueryLimit            = 10
	guardrailFanoutSentryThreshold = 10
	guardrailCheckTimeout          = 3 * time.Second
	guardrailS3KeyPrefix           = "guardrail_check"
	guardrailCheckEnabledEnv       = "FOLLOWUP_GUARDRAIL_CHECK_ENABLED"
)

type guardrailThresholds struct {
	Metric       string  `json:"metric"`
	Interrupt    float64 `json:"interrupt"`
	OfflineJudge float64 `json:"offline_judge"`
}

type guardrailCheckLatency struct {
	VectorQuery float64 `json:"vector_query"`
	Total       float64 `json:"total"`
}

// guardrailCheckRecord is one generated turn's complete in-call calibration
// record. ChunkID is filled by the chunk decorator at take-time, immediately
// before the record is uploaded.
type guardrailCheckRecord struct {
	ChunkID        string                   `json:"chunk_id"`
	ConversationID string                   `json:"conversation_id"`
	UserID         string                   `json:"user_id"`
	BotType        string                   `json:"bot_type"`
	CheckedAt      string                   `json:"checked_at"`
	TurnText       string                   `json:"turn_text"`
	Thresholds     guardrailThresholds      `json:"thresholds"`
	Interrupted    bool                     `json:"interrupted"`
	CheckCount     int                      `json:"check_count"`
	ChecksFired    int                      `json:"checks_fired"`
	Checks         []guardrailSentenceCheck `json:"checks"`
	Status         string                   `json:"status"`
	Error          string                   `json:"error,omitempty"`

	// Chunk aggregates are carried on the record snapshot but not duplicated
	// in the full S3 payload; their durable home is chunk_retrieval_metrics.
	highestSimilarity *float64 `json:"-"`
	slowestTotalMs    float64  `json:"-"`
}

type guardrailSentenceCheck struct {
	Index      int                   `json:"index"`
	Fragment   string                `json:"fragment"`
	Similarity *float64              `json:"similarity,omitempty"`
	Band       string                `json:"band"`
	Violated   bool                  `json:"violated"`
	Status     string                `json:"status"`
	Error      string                `json:"error"`
	LatencyMs  guardrailCheckLatency `json:"latency_ms"`
	Top        *guardrailTopHit      `json:"top,omitempty"`
	Candidates []guardrailCandidate  `json:"candidates"`
}

type guardrailTopHit struct {
	InstructionID       string  `json:"instruction_id"`
	AnchorID            string  `json:"anchor_id"`
	AnchorText          string  `json:"anchor_text"`
	Title               string  `json:"title"`
	DocumentVersionPath string  `json:"document_version_path"`
	InstructionText     string  `json:"instruction_text"`
	Similarity          float64 `json:"similarity"`
}

type guardrailCandidate struct {
	InstructionID string  `json:"instruction_id"`
	AnchorID      string  `json:"anchor_id"`
	Similarity    float64 `json:"similarity"`
}

// guardrailRecordBox hands one turn record from the response-check goroutines
// to the chunk decorator on the call-events dispatcher goroutine.
type guardrailRecordBox struct {
	mu      sync.Mutex
	pending *guardrailCheckRecord
}

func (b *guardrailRecordBox) put(record guardrailCheckRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = &record
}

// take removes and returns the pending record, so one guarded turn maps to
// exactly one assistant chunk.
func (b *guardrailRecordBox) take() *guardrailCheckRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	record := b.pending
	b.pending = nil
	return record
}
