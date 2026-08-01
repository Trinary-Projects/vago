package disha

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/internal/weaviate"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// Blocking protocol retrieval for follow-up calls (both the dynamic check-in
// path and the agenda-based path).
//
// Before every LLM generation the trailing "Disha: … / User: …" exchange is
// sent to the US Weaviate instance as RAW text (the ProtocolAnchor collection
// vectorizes server-side through the in-cluster TEI, which applies the model's
// "Document: " prefix itself — prefixing here would double-prefix and silently
// degrade ranking). Matching protocols are held in a small rolling set and
// re-injected into the message array on every turn.
//
// Design note: reports/followup-protocol-retrieval-design-note.md

const (
	protocolAnchorClass      = "ProtocolAnchor"
	protocolInstructionClass = "ProtocolInstruction"

	// protocolSimilarityThreshold gates admission, as cosine similarity
	// (1 - distance) on the collection's cosine index.
	//
	// Calibration data against jina-embeddings-v5-text-small:
	//   - fixture probe (2026-07-29): true positives 0.7143-0.8808, highest
	//     false positive 0.6648.
	//   - live staging call f234dafb (2026-07-31): real top hits 0.7067 /
	//     0.7013 / 0.7276 against a best-irrelevant of 0.6472.
	//   - the first real authored protocol (Acidity, 2026-08-01) scored 0.9198
	//     on an on-topic query.
	//
	// At 0.80 the gate is conservative: it clears the authored protocol
	// comfortably but rejects the 0.71-0.79 band, where several measured TRUE
	// positives sat. Whether that costs real recall depends on whether the real
	// corpus scores like the authored document or like the fixtures — every
	// candidate's score reaches the S3 record whether or not it qualified, so a
	// few QA calls answer it with data.
	//
	// The model has a high similarity floor (irrelevant protocols still score
	// 0.54-0.67), so the usable band is narrow; a relative gate (top hit must
	// beat the second by a margin) is the known next lever.
	protocolSimilarityThreshold = 0.8

	// protocolCapacity is how many protocols stay resident at once.
	protocolCapacity = 3

	// protocolDefaultTurnThreshold is the residency used when an instruction
	// has no turnsThresholdCount (the property comes back as JSON null, not
	// absent, when unset).
	protocolDefaultTurnThreshold = 3

	// protocolBlockTurnsFromTail places the injected block this many ASSISTANT
	// turns above the tail, recomputed every turn. Assistant turns, not speaker
	// turns, so it counts the same unit as turnsThresholdCount: one bot
	// generation. Counting both roles made the same "3" mean 1.5 exchanges here
	// and 3 exchanges there.
	//
	// The block stays off the newest user message so the model treats protocols
	// as background guidance rather than the thing to answer; the cost is that a
	// change to the block invalidates the prompt-cache suffix from that point,
	// which is now ~6 messages rather than ~3.
	protocolBlockTurnsFromTail = 3

	// protocolRetrievalBudget bounds the whole blocking step. Generous on
	// purpose: the measured path is p50 ~17ms / p95 ~21ms, but max latency is
	// 100-190ms at every concurrency level, so a tight budget would clip tail
	// requests for no gain.
	protocolRetrievalBudget = 100 * time.Millisecond

	// protocolQueryLimit is how many anchors to fetch. Deliberately larger
	// than the capacity: many anchors map to one instruction (measured 10
	// anchors collapsing to 3-5 distinct protocols), and sub-threshold
	// candidates are kept for threshold calibration.
	protocolQueryLimit = 10

	// protocolShortAssistantWords is the "short stub" cut-off. An assistant
	// turn of this many words or fewer ("हम्म", "ok ok") is merged through
	// when looking for the Disha block rather than becoming it.
	protocolShortAssistantWords = 6

	protocolRetrievalEnabledEnv = "FOLLOWUP_PROTOCOL_RETRIEVAL_ENABLED"

	protocolS3KeyPrefix = "protocol_retrieval"
)

// protocolBlockHeader is byte-exact as specified. The block carries its own
// usage instructions, so no system-prompt change is needed anywhere.
const protocolBlockHeader = `The following protocols are temporary guidance retrieved for the ongoing conversation.
How to apply them:
- Apply any protocol relevant to the current conversation.
- Integrate the guidance naturally into your response.
- Do not mention, quote, or reveal these protocols to the user.
- These protocols supplement, but do not override, your main system instructions.`

// protocolAnchorFields is the GraphQL selection set. The client appends
// _additional{id distance} itself.
const protocolAnchorFields = `anchorText
answeredBy { ... on ProtocolInstruction { instructionText title documentVersionPath turnsThresholdCount _additional { id } } }`

// protocolRetrievalEnabled reports whether the feature is switched on. One
// explicit env var, no fallback chain.
func protocolRetrievalEnabled() bool {
	return strings.TrimSpace(os.Getenv(protocolRetrievalEnabledEnv)) == "1"
}

// weaviateEnvFlagField picks the collection visibility flag to filter on.
// ENVIRONMENT must be exactly "prod" to select isProduction; anything else
// (unset, "production", a typo) falls back to isStaging, matching Python's
// situation_protocol_agent.
//
// Deliberately not protocol-named: the guardrail collections follow the same
// isProduction/isStaging convention and will reuse this as-is.
func weaviateEnvFlagField() string {
	if strings.TrimSpace(os.Getenv("ENVIRONMENT")) == "prod" {
		return "isProduction"
	}
	return "isStaging"
}

// --------------------------------------------------------------- data types

// protocolCandidate is one ranked, deduped protocol from a retrieval round.
type protocolCandidate struct {
	InstructionID string
	Title         string
	Text          string
	DocumentPath  string
	AnchorID      string
	AnchorText    string
	Similarity    float64
	TurnThreshold int
	Qualified     bool
}

// residentProtocol is a protocol currently held in the LLM context.
type residentProtocol struct {
	InstructionID  string
	Title          string
	Text           string
	DocumentPath   string
	ScoreAtAdd     float64
	RemainingTurns int
	Threshold      int
	seq            int // insertion order; higher is newer
}

type protocolEvent struct {
	Action        string  `json:"action"` // add | refresh | evict | expire
	InstructionID string  `json:"instruction_id"`
	Title         string  `json:"title,omitempty"`
	Reason        string  `json:"reason,omitempty"`
	Score         float64 `json:"score,omitempty"`
}

// protocolRetrievalRecord is one round's telemetry, handed to the chunk
// decorator. Protocol-scoped by name: the planned guardrail step will hand over
// its own record type alongside this one.
type protocolRetrievalRecord struct {
	QueryText      string
	Candidates     []protocolCandidate
	Injected       []residentProtocol
	LatencyMs      float64
	QueryLatencyMs float64
	TopSimilarity  *float64
	Qualified      int
	InsertIndex    int
	Status         string // ok | skipped | error | timeout
	Err            string
}

// --------------------------------------------------------------- the store

// ProtocolStore holds the resident protocol set for one call. Safe for
// concurrent use: the enricher writes it on the pipeline's frame loop while the
// chunk decorator reads it on the call-events dispatcher goroutine.
type ProtocolStore struct {
	mu       sync.Mutex
	resident []residentProtocol
	seq      int
}

func NewProtocolStore() *ProtocolStore { return &ProtocolStore{} }

// apply runs one retrieval round against the resident set and returns the
// lifecycle events.
//
// Three ordered phases, and the order is the correctness argument:
//
//  1. Expire — age every resident, drop those that ran out.
//  2. Refresh — reset the TTL and score of every resident that re-matched.
//  3. Admit — add genuinely new protocols unconditionally, evicting only from
//     the protocols resident BEFORE this round (fewest remaining turns first,
//     ties broken by the lower similarity recorded at the time of addition).
//
// Refreshing before admitting is what makes eviction pick the right victim, and
// what stops a re-matched protocol from being evicted and re-added as new.
func (s *ProtocolStore) apply(qualified []protocolCandidate) []protocolEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := make([]protocolEvent, 0, len(qualified)+protocolCapacity)

	// 1. Age the resident set and drop anything that ran out.
	kept := s.resident[:0]
	for _, protocol := range s.resident {
		protocol.RemainingTurns--
		if protocol.RemainingTurns <= 0 {
			events = append(events, protocolEvent{
				Action: "expire", InstructionID: protocol.InstructionID, Title: protocol.Title,
			})
			continue
		}
		kept = append(kept, protocol)
	}
	s.resident = kept

	// 2. Refresh pass. Every re-matched resident has its TTL and score reset
	// BEFORE anything is admitted, for two reasons.
	//
	// First, the eviction comparator picks the fewest remaining turns — so a
	// protocol that just re-matched must already carry its reset count by the
	// time eviction runs, or it looks like the weakest resident and gets thrown
	// out at the exact moment it proved most relevant. (Previously the passes
	// were interleaved in score order: a higher-scoring newcomer could evict a
	// resident whose own refresh had not been reached yet, and that resident was
	// then re-added as brand new — two evictions, and a lost insertion order.)
	//
	// Second, a refresh consumes no admission slot: the protocol already holds
	// one. Counting it against the per-round budget starved genuinely new
	// protocols whenever the resident set kept matching, which is the common
	// case because the conversation stays on topic.
	refreshed := make(map[string]bool, len(qualified))
	for _, candidate := range qualified {
		if refreshed[candidate.InstructionID] {
			continue
		}
		index := s.indexOf(candidate.InstructionID)
		if index < 0 {
			continue
		}
		s.resident[index].RemainingTurns = candidate.TurnThreshold
		s.resident[index].ScoreAtAdd = candidate.Similarity
		refreshed[candidate.InstructionID] = true
		events = append(events, protocolEvent{
			Action: "refresh", InstructionID: candidate.InstructionID,
			Title: candidate.Title, Score: candidate.Similarity,
		})
	}

	// Eviction may only take protocols that were resident before this round —
	// including ones just refreshed, which are protected not by exclusion but by
	// now having the most remaining turns. Anything admitted below is exempt.
	eligible := make(map[string]bool, len(s.resident))
	for _, protocol := range s.resident {
		eligible[protocol.InstructionID] = true
	}

	// 3. Admission pass. Only genuinely new protocols, and the budget counts
	// admissions rather than candidates examined.
	admitted := 0
	for _, candidate := range qualified {
		if admitted >= protocolCapacity {
			break
		}
		// Already handled by the refresh pass, or a duplicate id later in the
		// same round's list — either way it must not consume a slot.
		if refreshed[candidate.InstructionID] || s.indexOf(candidate.InstructionID) >= 0 {
			continue
		}

		s.seq++
		s.resident = append(s.resident, residentProtocol{
			InstructionID:  candidate.InstructionID,
			Title:          candidate.Title,
			Text:           candidate.Text,
			DocumentPath:   candidate.DocumentPath,
			ScoreAtAdd:     candidate.Similarity,
			RemainingTurns: candidate.TurnThreshold,
			Threshold:      candidate.TurnThreshold,
			seq:            s.seq,
		})
		admitted++
		events = append(events, protocolEvent{
			Action: "add", InstructionID: candidate.InstructionID,
			Title: candidate.Title, Score: candidate.Similarity,
		})

		for len(s.resident) > protocolCapacity {
			victim := s.evictionTarget(eligible)
			if victim < 0 {
				break
			}
			events = append(events, protocolEvent{
				Action: "evict", InstructionID: s.resident[victim].InstructionID,
				Title: s.resident[victim].Title, Reason: "capacity",
				Score: s.resident[victim].ScoreAtAdd,
			})
			delete(eligible, s.resident[victim].InstructionID)
			s.resident = append(s.resident[:victim], s.resident[victim+1:]...)
		}
	}
	return events
}

func (s *ProtocolStore) indexOf(instructionID string) int {
	for i := range s.resident {
		if s.resident[i].InstructionID == instructionID {
			return i
		}
	}
	return -1
}

// evictionTarget picks the protocol to drop: fewest remaining turns, ties
// broken by the lower score recorded when it was added. Only protocols that
// were resident before this round are eligible.
func (s *ProtocolStore) evictionTarget(eligible map[string]bool) int {
	target := -1
	for i := range s.resident {
		if !eligible[s.resident[i].InstructionID] {
			continue
		}
		if target < 0 {
			target = i
			continue
		}
		switch {
		case s.resident[i].RemainingTurns < s.resident[target].RemainingTurns:
			target = i
		case s.resident[i].RemainingTurns == s.resident[target].RemainingTurns &&
			s.resident[i].ScoreAtAdd < s.resident[target].ScoreAtAdd:
			target = i
		}
	}
	return target
}

// snapshot returns the resident protocols newest-addition-first.
func (s *ProtocolStore) snapshot() []residentProtocol {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]residentProtocol, len(s.resident))
	copy(out, s.resident)
	sort.SliceStable(out, func(i, j int) bool { return out[i].seq > out[j].seq })
	return out
}

// --------------------------------------------------- query text construction

// buildProtocolQueryText renders the retrieval query from the conversation
// snapshot: the trailing user block plus the preceding Disha block.
//
// Returns "" when there is no trailing user turn (greet-first, or an injected
// nudge that didn't come from the user) — the caller then skips retrieval and
// only re-injects the resident set.
func buildProtocolQueryText(messages []voicepipelinecore.Message) string {
	index := len(messages) - 1

	// Trailing user block: consecutive user messages merged into one turn.
	// Out-of-band injections (the resume nudge, our own protocol block) also
	// travel as user-role messages, and merging their text into the query would
	// embed bot-directed instructions instead of user speech — so they are
	// skipped and the walk continues past them to the real turn.
	var userParts []string
	for ; index >= 0; index-- {
		if messages[index].Role != "user" {
			break
		}
		if isOutOfBandUserMessage(messages[index]) {
			continue
		}
		if text := strings.TrimSpace(messages[index].Content); text != "" {
			userParts = append([]string{text}, userParts...)
		}
	}
	userText := strings.TrimSpace(strings.Join(userParts, " "))
	if userText == "" {
		return ""
	}

	// Preceding Disha block, skipping tool turns and short stubs.
	dishaText := ""
	for ; index >= 0; index-- {
		message := messages[index]
		if message.Role != "assistant" {
			// Tool results (and anything else) never end the search.
			continue
		}
		// A tool-call turn carries no spoken text.
		if len(message.ToolCalls) > 0 {
			continue
		}
		text := strings.TrimSpace(message.Content)
		if text == "" {
			continue
		}
		// Short acknowledgements are merged through, not treated as the block.
		if len(strings.Fields(text)) <= protocolShortAssistantWords {
			continue
		}
		dishaText = text
		break
	}

	if dishaText == "" {
		return "User: " + userText
	}
	return "Disha: " + dishaText + "\nUser: " + userText
}

// ------------------------------------------------------- weaviate decoding

// queryProtocols runs one nearText round and returns candidates deduped by
// instruction id, best-first, with Qualified set per the threshold. Every
// candidate is returned (not just qualifying ones) so the telemetry keeps the
// full score distribution for calibration.
func queryProtocols(ctx context.Context, client *weaviate.Client, query string) ([]protocolCandidate, error) {
	hits, err := client.NearText(ctx, weaviate.NearTextQuery{
		Class:    protocolAnchorClass,
		Concepts: []string{query}, // RAW: TEI applies the "Document: " prefix
		Fields:   protocolAnchorFields,
		Where: weaviate.EqualBool(
			[]string{"answeredBy", protocolInstructionClass, weaviateEnvFlagField()}, true),
		Limit: protocolQueryLimit,
	})
	if err != nil {
		return nil, err
	}

	best := make(map[string]protocolCandidate, len(hits))
	order := make([]string, 0, len(hits))
	for _, hit := range hits {
		if !hit.DistancePresent {
			continue
		}
		instruction, ok := hit.CrossRef("answeredBy")
		if !ok {
			continue
		}
		text := strings.TrimSpace(instruction.String("instructionText"))
		instructionID := instruction.ID()
		if text == "" || instructionID == "" {
			continue
		}
		candidate := protocolCandidate{
			InstructionID: instructionID,
			Title:         instruction.String("title"),
			Text:          text,
			DocumentPath:  instruction.String("documentVersionPath"),
			AnchorID:      hit.ID,
			AnchorText:    hit.String("anchorText"),
			Similarity:    hit.Similarity(),
			// Unset arrives as JSON null rather than absent; Ref.Int maps
			// null / non-numeric / non-positive to the fallback.
			TurnThreshold: instruction.Int("turnsThresholdCount", protocolDefaultTurnThreshold),
		}
		candidate.Qualified = candidate.Similarity >= protocolSimilarityThreshold

		// Dedupe by instruction: many anchors point at one protocol, and
		// without this one protocol would consume every slot.
		if existing, seen := best[instructionID]; seen {
			if candidate.Similarity > existing.Similarity {
				best[instructionID] = candidate
			}
			continue
		}
		best[instructionID] = candidate
		order = append(order, instructionID)
	}

	candidates := make([]protocolCandidate, 0, len(order))
	for _, id := range order {
		candidates = append(candidates, best[id])
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].Similarity > candidates[j].Similarity
	})
	return candidates, nil
}

// --------------------------------------------------------- block rendering

// renderProtocolBlock builds the injected message content. Returns "" for an
// empty resident set so no block is inserted at all.
func renderProtocolBlock(protocols []residentProtocol) string {
	if len(protocols) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<system_message>\n")
	b.WriteString(protocolBlockHeader)
	b.WriteString("\n")
	for _, protocol := range protocols {
		b.WriteString("\n- ")
		b.WriteString(strings.TrimSpace(protocol.Text))
		b.WriteString("\n")
	}
	b.WriteString("</system_message>")
	return b.String()
}

// outOfBandUserWrappers are the tags this codebase uses to smuggle
// bot-directed instructions into the conversation as user-role messages:
// <system_message> for the sales/follow-up resume nudge and for the protocol
// block, <system_instruction> for onboarding's resume text.
var outOfBandUserWrappers = []string{"<system_message>", "<system_instruction>"}

// isOutOfBandUserMessage reports whether a user-role message is really an
// injected instruction rather than something the user said.
//
// Found live on staging conversation f234dafb (2026-07-31): on a resumed call
// buildInitialMessages appends the resume nudge as a user message, so the
// trailing user block merged 285 characters of "resume this conversation by
// saying hi…" onto 28 characters of actual speech and embedded that as the
// retrieval query.
func isOutOfBandUserMessage(message voicepipelinecore.Message) bool {
	if message.Role != "user" {
		return false
	}
	content := strings.TrimSpace(message.Content)
	for _, wrapper := range outOfBandUserWrappers {
		if strings.HasPrefix(content, wrapper) {
			return true
		}
	}
	return false
}

// isProtocolBlockMessage reports whether a message is a previously injected
// block. The block is never written into shared aggregator state, so this
// should never match in practice; it keeps the invariant enforced locally
// rather than assumed.
func isProtocolBlockMessage(message voicepipelinecore.Message) bool {
	return message.Role == "user" && strings.Contains(message.Content, protocolBlockHeader)
}

// stripProtocolBlock returns a new slice: it must not filter in place, because
// the caller's slice is the turn's message snapshot and reusing its backing
// array would corrupt whatever else holds a reference to it.
func stripProtocolBlock(messages []voicepipelinecore.Message) []voicepipelinecore.Message {
	out := make([]voicepipelinecore.Message, 0, len(messages))
	for _, message := range messages {
		if isProtocolBlockMessage(message) {
			continue
		}
		out = append(out, message)
	}
	return out
}

// hasAssistantTurn reports whether the bot has spoken yet in this context.
//
// Used to skip retrieval on the greet-first turn. A fresh call's context is
// [system, user:"hello?"], where "hello?" is a synthetic seed rather than
// anything the user said, so retrieving on it is meaningless — and that turn is
// the most latency-sensitive moment of the call (greet latency is tracked). A
// resumed call replays real assistant chunks, so retrieval runs normally there.
func hasAssistantTurn(messages []voicepipelinecore.Message) bool {
	for _, message := range messages {
		if message.Role == "assistant" {
			return true
		}
	}
	return false
}

// conversationTurnStarts returns the index of each conversation turn, skipping
// the leading system message. A turn is a contiguous run by one logical speaker;
// a tool pair (assistant tool_calls -> tool result -> spoken reply) counts as
// part of the assistant turn that issued it, so a block can never be inserted
// inside one.
//
// Not protocol-specific: this is plain conversation shape, reusable by any step
// that needs to reason about turn boundaries.
func conversationTurnStarts(messages []voicepipelinecore.Message) []int {
	starts := make([]int, 0, len(messages))
	for i := 1; i < len(messages); i++ {
		role := messages[i].Role
		if role == "tool" {
			continue
		}
		previous := messages[i-1].Role
		switch role {
		case "user":
			if previous != "user" {
				starts = append(starts, i)
			}
		default: // assistant and anything else
			// Continuing an assistant turn: another assistant message, or the
			// spoken reply that follows a tool result.
			if previous != "assistant" && previous != "tool" {
				starts = append(starts, i)
			}
		}
	}
	return starts
}

func protocolQueryHash(query string) string {
	sum := sha256.Sum256([]byte(query))
	return hex.EncodeToString(sum[:8])
}

// assistantTurnStarts narrows conversationTurnStarts to bot turns only. One
// entry per generation: a tool pair (assistant tool_calls -> tool result ->
// spoken reply) is a single assistant turn, and inserting immediately before a
// start is therefore always safe — the message before it is a user message or
// the system prompt, never the middle of a tool pair.
func assistantTurnStarts(messages []voicepipelinecore.Message) []int {
	starts := make([]int, 0, len(messages))
	for _, index := range conversationTurnStarts(messages) {
		if messages[index].Role == "assistant" {
			starts = append(starts, index)
		}
	}
	return starts
}

// protocolInsertIndex computes where the block goes: immediately before the
// start of the Nth-from-last ASSISTANT turn, recomputed every turn. Clamped so
// the system message always stays first.
func protocolInsertIndex(messages []voicepipelinecore.Message) int {
	starts := assistantTurnStarts(messages)
	if len(starts) < protocolBlockTurnsFromTail {
		if len(messages) == 0 {
			return 0
		}
		return 1
	}
	index := starts[len(starts)-protocolBlockTurnsFromTail]
	if index < 1 {
		return 1
	}
	if index > len(messages) {
		return len(messages)
	}
	return index
}

// injectProtocolBlock returns messages with the block inserted, plus the index
// used. It never mutates the input backing array beyond the copy it makes.
func injectProtocolBlock(messages []voicepipelinecore.Message, block string) ([]voicepipelinecore.Message, int) {
	if block == "" {
		return messages, -1
	}
	index := protocolInsertIndex(messages)
	out := make([]voicepipelinecore.Message, 0, len(messages)+1)
	out = append(out, messages[:index]...)
	out = append(out, voicepipelinecore.Message{Role: "user", Content: block})
	out = append(out, messages[index:]...)
	return out, index
}

// ------------------------------------------------------------- the enricher

// protocolRecordBox hands one retrieval record from the pipeline's frame loop
// to the chunk decorator on the call-events dispatcher goroutine.
type protocolRecordBox struct {
	mu      sync.Mutex
	pending *protocolRetrievalRecord
}

func (b *protocolRecordBox) put(record protocolRetrievalRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = &record
}

// take removes and returns the pending record, so one retrieval maps to
// exactly one assistant chunk.
func (b *protocolRecordBox) take() *protocolRetrievalRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	record := b.pending
	b.pending = nil
	return record
}

// protocolEnricher implements voicepipelinecore.MessagesEnricher.
type protocolEnricher struct {
	client *weaviate.Client
	store  *ProtocolStore
	box    *protocolRecordBox
	logger *log.Logger

	router promptMetadataSetter
	ui     serverMessageEmitter
	taskSentryHub

	baseMetadata   map[string]any
	baseVariables  DocumentVariables
	conversationID string
	userID         string

	mu            sync.Mutex
	lastQueryHash string
}

func newProtocolEnricher(
	client *weaviate.Client,
	store *ProtocolStore,
	box *protocolRecordBox,
	logger *log.Logger,
	baseMetadata map[string]any,
	baseVariables DocumentVariables,
	userID, conversationID string,
) *protocolEnricher {
	return &protocolEnricher{
		client:         client,
		store:          store,
		box:            box,
		logger:         logger,
		baseMetadata:   baseMetadata,
		baseVariables:  baseVariables,
		userID:         userID,
		conversationID: conversationID,
	}
}

// SetInfrastructure injects the conversation router handle (for the per-call
// prompt-metadata refresh) and the RTVI emitter, both of which only exist once
// the task is built. The Sentry hub arrives separately via SetSentryHub.
func (e *protocolEnricher) SetInfrastructure(router promptMetadataSetter, ui serverMessageEmitter) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.router = router
	e.ui = ui
}

func (e *protocolEnricher) infrastructure() (promptMetadataSetter, serverMessageEmitter) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.router, e.ui
}

// Enrich is the MessagesEnricher. It always returns usable messages: every
// failure path keeps the existing resident set, injects it, and lets the turn
// proceed.
func (e *protocolEnricher) Enrich(ctx context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
	startedAt := time.Now()
	messages = stripProtocolBlock(messages)

	query := buildProtocolQueryText(messages)
	retrieved := false
	record := protocolRetrievalRecord{Status: "skipped"}
	if query != "" && hasAssistantTurn(messages) && e.shouldRetrieve(query) {
		retrieved = true
		record = e.retrieve(ctx, query)
	}

	injected := e.store.snapshot()
	block := renderProtocolBlock(injected)
	out, index := injectProtocolBlock(messages, block)

	record.QueryText = query
	record.Injected = injected
	record.InsertIndex = index
	record.LatencyMs = float64(time.Since(startedAt).Microseconds()) / 1000.0

	// Only a real retrieval round claims the upcoming assistant chunk. A
	// tool-result re-run reaches this function a second time within the same
	// turn; publishing its skipped record would overwrite the round that
	// actually happened and the chunk would report no retrieval at all.
	if retrieved {
		e.box.put(record)
	}
	e.publish(record)
	e.refreshPromptMetadata(injected)

	return out
}

// shouldRetrieve gates retrieval on the query text changing, so a tool-result
// re-run re-injects without re-querying and without ageing the resident set:
// residency is measured in user turns, not LLM calls.
//
// Known limitation, accepted deliberately: two genuinely different turns can
// render an identical query once short assistant stubs are skipped ("haan" /
// bot "hmm" / "haan"), and that collision suppresses the second turn's
// retrieval and ageing. A monotonic turn id from upstream would be exact, but
// this is simpler and the collision is rare.
func (e *protocolEnricher) shouldRetrieve(query string) bool {
	hash := protocolQueryHash(query)
	e.mu.Lock()
	defer e.mu.Unlock()
	if hash == e.lastQueryHash {
		return false
	}
	e.lastQueryHash = hash
	return true
}

func (e *protocolEnricher) retrieve(ctx context.Context, query string) protocolRetrievalRecord {
	record := protocolRetrievalRecord{Status: "ok"}

	queryCtx, cancel := context.WithTimeout(ctx, protocolRetrievalBudget)
	defer cancel()

	queryStartedAt := time.Now()
	candidates, err := queryProtocols(queryCtx, e.client, query)
	record.QueryLatencyMs = float64(time.Since(queryStartedAt).Microseconds()) / 1000.0
	record.Candidates = candidates

	if err != nil {
		record.Status = "error"
		if errors.Is(err, context.DeadlineExceeded) {
			record.Status = "timeout"
		}
		record.Err = err.Error()
		e.reportFailure(ctx, err, query)
		// Fail open on CONTENT — the resident set keeps its protocols and is
		// still injected — but the turn still happened, so it must still be
		// counted against every resident's TTL. Skipping the ageing let a run of
		// failures pin protocols in context indefinitely.
		e.logEvents(e.store.apply(nil))
		return record
	}

	if len(candidates) > 0 {
		top := candidates[0].Similarity
		record.TopSimilarity = &top
	}

	qualified := make([]protocolCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Qualified {
			qualified = append(qualified, candidate)
		}
	}
	record.Qualified = len(qualified)

	// Resident-set lifecycle is logged, not persisted: the events are for
	// eyeballing a call in app.log, and the per-turn resident_after snapshot in
	// the S3 record already says what the set ended up as.
	e.logEvents(e.store.apply(qualified))
	return record
}

// logEvents writes one line per round describing what the round did to the
// resident set. Nothing here is stored.
func (e *protocolEnricher) logEvents(events []protocolEvent) {
	if e.logger == nil || len(events) == 0 {
		return
	}
	parts := make([]string, 0, len(events))
	for _, event := range events {
		part := event.Action + ":" + shortID(event.InstructionID)
		if event.Title != "" {
			part += " (" + event.Title + ")"
		}
		if event.Reason != "" {
			part += " reason=" + event.Reason
		}
		parts = append(parts, part)
	}
	e.logger.Printf("disha: protocol store events: %s\n", strings.Join(parts, "; "))
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// reportFailure sends retrieval failures to Sentry. Cancellation just means the
// turn was abandoned (barge-in or call end) and is not an error.
func (e *protocolEnricher) reportFailure(ctx context.Context, err error, query string) {
	if errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return
	}
	if e.logger != nil {
		e.logger.Printf("disha: protocol retrieval failed conversation=%s: %v\n", e.conversationID, err)
	}
	sentryutil.Capture(sentryutil.Event{
		Hub: e.sentryHub(),
		Err: err,
		Tags: map[string]string{
			"component": "disha_followup",
			"operation": "protocol_retrieval",
		},
		Details: map[string]any{
			"conversation_id": e.conversationID,
			"user_id":         e.userID,
			"query_chars":     len(query),
		},
	})
}

// publish logs the round and emits the RTVI trace event through the existing
// single event stream (which also lands in the S3 debug log).
func (e *protocolEnricher) publish(record protocolRetrievalRecord) {
	top := "none"
	if record.TopSimilarity != nil {
		top = fmt.Sprintf("%.4f", *record.TopSimilarity)
	}
	if e.logger != nil {
		e.logger.Printf(
			"disha: protocol retrieval status=%s candidates=%d qualified=%d injected=%d top_sim=%s query_ms=%.1f total_ms=%.1f insert_index=%d\n",
			record.Status, len(record.Candidates), record.Qualified, len(record.Injected),
			top, record.QueryLatencyMs, record.LatencyMs, record.InsertIndex,
		)
	}

	_, ui := e.infrastructure()
	if ui == nil {
		return
	}
	data := map[string]any{
		"type":               "protocol_retrieval",
		"status":             record.Status,
		"candidate_count":    len(record.Candidates),
		"qualified_count":    record.Qualified,
		"injected_count":     len(record.Injected),
		"total_ms":           record.LatencyMs,
		"vector_query_ms":    record.QueryLatencyMs,
		"insert_index":       record.InsertIndex,
		"top_similarity":     nil,
		"injected_protocols": protocolTitles(record.Injected),
	}
	if record.TopSimilarity != nil {
		data["top_similarity"] = *record.TopSimilarity
	}
	if record.Err != "" {
		data["error"] = record.Err
	}
	ui.ServerMessage(data, time.Now())
}

func protocolTitles(protocols []residentProtocol) []string {
	titles := make([]string, 0, len(protocols))
	for _, protocol := range protocols {
		titles = append(titles, protocol.Title)
	}
	return titles
}

// refreshPromptMetadata puts the injected protocols into the LLM log's variable
// section. It clones the base metadata and variables every time so the maps
// built in plan() are never mutated.
func (e *protocolEnricher) refreshPromptMetadata(injected []residentProtocol) {
	router, _ := e.infrastructure()
	if router == nil {
		return
	}
	metadata := make(map[string]any, len(e.baseMetadata)+1)
	for key, value := range e.baseMetadata {
		metadata[key] = clonePromptMetadataValue(value)
	}
	variables := cloneDocumentVariables(e.baseVariables)
	variables["retrieved_protocols"] = protocolLogVariables(injected)
	metadata["system_prompt_variables"] = variables
	router.SetPromptMetadata(metadata)
}

func protocolLogVariables(protocols []residentProtocol) []any {
	out := make([]any, 0, len(protocols))
	for _, protocol := range protocols {
		out = append(out, map[string]any{
			"instruction_id":  protocol.InstructionID,
			"title":           protocol.Title,
			"similarity":      protocol.ScoreAtAdd,
			"remaining_turns": protocol.RemainingTurns,
			"text":            protocol.Text,
		})
	}
	return out
}

// warmUp fires one throwaway query so the first real retrieval doesn't pay TLS
// and connection setup. Best-effort and silent: a failure here is not a call
// problem, and the real path reports its own errors.
func (e *protocolEnricher) warmUp(ctx context.Context) {
	warmCtx, cancel := context.WithTimeout(ctx, protocolRetrievalBudget)
	defer cancel()
	if _, err := queryProtocols(warmCtx, e.client, "warm up"); err != nil && e.logger != nil {
		e.logger.Printf("disha: protocol retrieval warm-up failed (ignored): %v\n", err)
	}
}
