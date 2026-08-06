package disha

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/internal/weaviate"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// In-flight guardrail checking for follow-up calls. Each completed sentence
// is sent RAW to the US Weaviate instance (the GuardrailAnchor collection
// vectorizes server-side through TEI, which applies the model prefix itself).
// Similarity alone decides whether core interrupts and regenerates; all
// sentence results are retained for the later offline judge.
//
// Design note: reports/followup-guardrail-check-design-note.md

// guardrailAnchorFields is the GraphQL selection set. The client appends
// _additional{id distance} for the anchor itself.
const guardrailAnchorFields = `anchorText
answeredBy { ... on GuardrailInstruction { instructionText title documentVersionPath _additional { id } } }`

// guardrailChecker implements voicepipelinecore.ResponseGuard and supplies
// the consume-once correction MessagesEnricher used by the retry turn.
type guardrailChecker struct {
	client *weaviate.Client
	box    *guardrailRecordBox
	logger *log.Logger
	taskSentryHub

	userID         string
	conversationID string

	mu sync.Mutex
	ui serverMessageEmitter

	turnCtx           context.Context
	record            *guardrailCheckRecord
	checksFired       int
	fanoutReported    bool
	violationRecorded bool
	pendingCorrection string
}

func newGuardrailChecker(
	client *weaviate.Client,
	box *guardrailRecordBox,
	logger *log.Logger,
	userID, conversationID string,
) *guardrailChecker {
	return &guardrailChecker{
		client:         client,
		box:            box,
		logger:         logger,
		userID:         userID,
		conversationID: conversationID,
	}
}

// SetUI injects the late-bound RTVI emitter.
func (c *guardrailChecker) SetUI(ui serverMessageEmitter) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ui = ui
}

// Check is the ResponseGuard. It never returns true on an infrastructure
// failure: failing open is safer than truncating a valid response and burning
// the turn's one regeneration.
func (c *guardrailChecker) Check(ctx context.Context, fragment string) bool {
	startedAt := time.Now()

	c.mu.Lock()
	if ctx != c.turnCtx {
		c.turnCtx = ctx
		c.record = &guardrailCheckRecord{
			ConversationID: c.conversationID,
			UserID:         c.userID,
			BotType:        FollowUpBotType,
			CheckedAt:      time.Now().UTC().Format(time.RFC3339Nano),
			Thresholds: guardrailThresholds{
				Metric:       "cosine_similarity",
				Interrupt:    guardrailInterruptThreshold,
				OfflineJudge: guardrailOfflineJudgeThreshold,
			},
			Status: "skipped",
		}
		c.checksFired = 0
		c.fanoutReported = false
		c.violationRecorded = false
		if c.logger != nil {
			c.logger.Printf("disha: guardrail turn started, record reset\n")
		}

		// Do not clear the box here. A violation before speech produces no
		// partial chunk, and its unguarded regeneration fires no checks, so the
		// violating record must survive for whichever assistant chunk commits
		// next. If another GUARDED turn starts, its first snapshot replaces the
		// old one; by then the old chunk was taken or can no longer commit first.
	}

	index := c.checksFired + 1
	c.checksFired++
	c.record.ChecksFired = c.checksFired
	// Copy-on-write keeps every shallow record snapshot in guardrailRecordBox
	// immutable while another sentence finishes on a different goroutine.
	checks := make([]guardrailSentenceCheck, len(c.record.Checks)+1)
	copy(checks, c.record.Checks)
	checks[len(checks)-1] = guardrailSentenceCheck{
		Index:      index,
		Fragment:   fragment,
		Status:     "cancelled",
		Candidates: []guardrailCandidate{},
	}
	c.record.Checks = checks
	c.record.TurnText = guardrailTurnText(c.record.Checks)
	c.box.put(*c.record)
	if c.logger != nil {
		c.logger.Printf("disha: guardrail check %d fired: %q\n", index, fragment)
	}

	reportFanout := c.checksFired > guardrailFanoutSentryThreshold && !c.fanoutReported
	if reportFanout {
		c.fanoutReported = true
	}
	c.mu.Unlock()

	if reportFanout {
		c.reportFanout(index)
	}

	queryCtx, cancel := context.WithTimeout(ctx, guardrailCheckTimeout)
	defer cancel()
	queryStartedAt := time.Now()
	hits, err := c.client.NearText(queryCtx, weaviate.NearTextQuery{
		Class:    guardrailAnchorClass,
		Concepts: []string{fragment}, // RAW: TEI applies the model prefix
		Fields:   guardrailAnchorFields,
		Where: weaviate.EqualBool(
			[]string{"answeredBy", guardrailInstructionClass, weaviateEnvFlagField()}, true),
		Limit: guardrailQueryLimit,
	})
	vectorQueryMs := guardrailElapsedMs(queryStartedAt)

	if err != nil {
		if ctx.Err() != nil {
			c.logDropped(index, fragment, startedAt)
			return false
		}
		c.mu.Lock()
		if ctx != c.turnCtx || ctx.Err() != nil {
			c.mu.Unlock()
			c.logDropped(index, fragment, startedAt)
			return false
		}
		totalMs := guardrailElapsedMs(startedAt)
		checks := append([]guardrailSentenceCheck(nil), c.record.Checks...)
		check := &checks[index-1]
		check.Status = "error"
		check.Band = "error"
		check.Error = err.Error()
		check.LatencyMs = guardrailCheckLatency{VectorQuery: vectorQueryMs, Total: totalMs}
		c.record.Checks = checks
		c.record.CheckCount = guardrailCompletedCheckCount(c.record.Checks)
		c.record.Status = "error"
		c.record.Error = err.Error()
		if totalMs > c.record.slowestTotalMs {
			c.record.slowestTotalMs = totalMs
		}
		c.box.put(*c.record)
		c.mu.Unlock()

		c.reportFailure(ctx, err, fragment, index, totalMs)
		return false
	}

	decoded := guardrailDecodeCandidates(hits)
	// The query can finish concurrently with a violation, barge-in, turn
	// advance, or call end. Once its turn is done the placeholder is the only
	// durable truth: it stays cancelled, with no fabricated zero similarity.
	if ctx.Err() != nil {
		c.logDropped(index, fragment, startedAt)
		return false
	}

	var top *guardrailTopHit
	var similarity *float64
	band := "below"
	violated := false
	if len(decoded) > 0 {
		best := decoded[0]
		top = &best
		score := best.Similarity
		similarity = &score
		band = guardrailBand(score)
		violated = score > guardrailInterruptThreshold
	}
	candidates := make([]guardrailCandidate, 0, len(decoded))
	for _, candidate := range decoded {
		candidates = append(candidates, guardrailCandidate{
			InstructionID: candidate.InstructionID,
			AnchorID:      candidate.AnchorID,
			Similarity:    candidate.Similarity,
		})
	}

	c.mu.Lock()
	if ctx != c.turnCtx || ctx.Err() != nil {
		c.mu.Unlock()
		c.logDropped(index, fragment, startedAt)
		return false
	}
	totalMs := guardrailElapsedMs(startedAt)
	checks = append([]guardrailSentenceCheck(nil), c.record.Checks...)
	check := &checks[index-1]
	check.Similarity = similarity
	check.Band = band
	check.Violated = violated
	check.Status = "ok"
	check.LatencyMs = guardrailCheckLatency{VectorQuery: vectorQueryMs, Total: totalMs}
	check.Top = top
	check.Candidates = candidates
	c.record.Checks = checks
	c.record.CheckCount = guardrailCompletedCheckCount(c.record.Checks)
	if c.record.Status != "error" {
		c.record.Status = "ok"
	}
	if similarity != nil && (c.record.highestSimilarity == nil || *similarity > *c.record.highestSimilarity) {
		highest := *similarity
		c.record.highestSimilarity = &highest
	}
	if totalMs > c.record.slowestTotalMs {
		c.record.slowestTotalMs = totalMs
	}
	similarityText := "none"
	if similarity != nil {
		similarityText = fmt.Sprintf("%.4f", *similarity)
	}
	topTitle := ""
	if top != nil {
		topTitle = top.Title
	}
	if c.logger != nil {
		c.logger.Printf(
			"disha: guardrail check %d completed in %.1fms (query %.1fms): similarity=%s band=%s violated=%v top=%q candidates=%d\n",
			index, totalMs, vectorQueryMs, similarityText, band, violated, topTitle, len(candidates),
		)
	}

	if violated {
		if !c.violationRecorded {
			c.violationRecorded = true
			c.record.Interrupted = true
			c.pendingCorrection = top.InstructionText
			if c.logger != nil {
				c.logger.Printf("disha: guardrail check %d violation recorded, correction pending (title=%q)\n", index, top.Title)
			}
		}
		// Simultaneous violating sentences can race with core's own CAS. The
		// correction recorded here and the sentence that wins that CAS may be
		// different, which is harmless: both independently exceeded the gate.
	}
	c.box.put(*c.record)
	ui := c.ui
	c.mu.Unlock()

	if (band == "offline_judge" || violated) && top != nil && ui != nil {
		ui.ServerMessage(map[string]any{
			"type":           "guardrail_check",
			"index":          index,
			"similarity":     *similarity,
			"band":           band,
			"violated":       violated,
			"title":          top.Title,
			"instruction_id": top.InstructionID,
		}, time.Now())
	}
	return violated
}

// EnrichCorrection appends the matched instruction to the regeneration's
// ephemeral message snapshot. It is consume-once and never mutates the
// caller's slice or backing array.
func (c *guardrailChecker) EnrichCorrection(_ context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
	if c == nil {
		return messages
	}
	c.mu.Lock()
	correction := c.pendingCorrection
	c.pendingCorrection = ""
	c.mu.Unlock()
	if correction == "" {
		return messages
	}
	if c.logger != nil {
		c.logger.Printf(
			"disha: guardrail correction injected into regeneration (guardrail: %q)\n",
			guardrailLogExcerpt(correction, 120),
		)
	}

	out := make([]voicepipelinecore.Message, len(messages), len(messages)+1)
	copy(out, messages)
	out = append(out, voicepipelinecore.Message{
		Role: "user",
		Content: fmt.Sprintf(`<system_message>
Your response violates the following guardrail -
%s
please regenerate with correction
</system_message>`, correction),
	})
	return out
}

// guardrailDecodeCandidates skips malformed hits, dedupes by instruction id
// keeping its best anchor, and returns the distinct instructions best-first.
func guardrailDecodeCandidates(hits []weaviate.Hit) []guardrailTopHit {
	best := make(map[string]guardrailTopHit, len(hits))
	order := make([]string, 0, len(hits))
	for _, hit := range hits {
		if !hit.DistancePresent {
			continue
		}
		instruction, ok := hit.CrossRef("answeredBy")
		if !ok {
			continue
		}
		instructionText := strings.TrimSpace(instruction.String("instructionText"))
		instructionID := instruction.ID()
		if instructionText == "" || instructionID == "" {
			continue
		}
		candidate := guardrailTopHit{
			InstructionID:       instructionID,
			AnchorID:            hit.ID,
			AnchorText:          hit.String("anchorText"),
			Title:               instruction.String("title"),
			DocumentVersionPath: instruction.String("documentVersionPath"),
			InstructionText:     instructionText,
			Similarity:          hit.Similarity(),
		}
		if existing, seen := best[instructionID]; seen {
			if candidate.Similarity > existing.Similarity {
				best[instructionID] = candidate
			}
			continue
		}
		best[instructionID] = candidate
		order = append(order, instructionID)
	}

	candidates := make([]guardrailTopHit, 0, len(order))
	for _, instructionID := range order {
		candidates = append(candidates, best[instructionID])
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].Similarity > candidates[j].Similarity
	})
	return candidates
}

func guardrailBand(similarity float64) string {
	switch {
	case similarity > guardrailInterruptThreshold:
		return "interrupt"
	case similarity >= guardrailOfflineJudgeThreshold:
		return "offline_judge"
	default:
		return "below"
	}
}

func guardrailTurnText(checks []guardrailSentenceCheck) string {
	fragments := make([]string, 0, len(checks))
	for _, check := range checks {
		fragments = append(fragments, check.Fragment)
	}
	return strings.Join(fragments, " ")
}

func guardrailCompletedCheckCount(checks []guardrailSentenceCheck) int {
	count := 0
	for _, check := range checks {
		if check.Status == "ok" || check.Status == "error" {
			count++
		}
	}
	return count
}

func guardrailElapsedMs(startedAt time.Time) float64 {
	return float64(time.Since(startedAt).Microseconds()) / 1000.0
}

func guardrailLogExcerpt(text string, maxRunes int) string {
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return text
	}
	return string(runes[:maxRunes]) + "…"
}

func (c *guardrailChecker) logDropped(index int, fragment string, startedAt time.Time) {
	if c.logger != nil {
		c.logger.Printf(
			"disha: guardrail check %d cancelled after %.1fms, dropped: %q\n",
			index, guardrailElapsedMs(startedAt), fragment,
		)
	}
}

func (c *guardrailChecker) reportFailure(ctx context.Context, err error, fragment string, index int, elapsedMs float64) {
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return
	}
	if c.logger != nil {
		c.logger.Printf(
			"disha: guardrail check %d failed after %.1fms for %q: %v\n",
			index, elapsedMs, fragment, err,
		)
	}
	sentryutil.Capture(sentryutil.Event{
		Hub: c.sentryHub(),
		Err: err,
		Tags: map[string]string{
			"component": "disha_followup",
			"operation": "guardrail_check",
		},
		Details: map[string]any{
			"conversation_id": c.conversationID,
			"user_id":         c.userID,
			"index":           index,
			"fragment_chars":  len(fragment),
		},
	})
}

func (c *guardrailChecker) reportFanout(checksFired int) {
	if c.logger != nil {
		c.logger.Printf("disha: guardrail check fan-out conversation=%s checks_fired=%d\n", c.conversationID, checksFired)
	}
	sentryutil.Capture(sentryutil.Event{
		Hub:     c.sentryHub(),
		Message: "follow-up guardrail check fan-out exceeded threshold",
		Tags: map[string]string{
			"component": "disha_followup",
			"operation": "guardrail_check_fanout",
		},
		Details: map[string]any{
			"conversation_id": c.conversationID,
			"user_id":         c.userID,
			"checks_fired":    checksFired,
		},
	})
}
