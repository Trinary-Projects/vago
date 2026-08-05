package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/internal/weaviate"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// ------------------------------------------------------------- query builder

func msg(role, content string) voicepipelinecore.Message {
	return voicepipelinecore.Message{Role: role, Content: content}
}

func toolCallMsg() voicepipelinecore.Message {
	return voicepipelinecore.Message{
		Role: "assistant",
		ToolCalls: []voicepipelinecore.ToolCall{{
			ID: "call-1", Type: "function",
			Function: voicepipelinecore.ToolCallFunction{Name: "get_guidance", Arguments: "{}"},
		}},
	}
}

func TestBuildProtocolQueryText(t *testing.T) {
	longDisha := "aapka weight kaisa chal raha hai bataiye zara detail me"

	tests := []struct {
		name     string
		messages []voicepipelinecore.Message
		want     string
	}{
		{
			name:     "simple pair",
			messages: []voicepipelinecore.Message{msg("system", "sys"), msg("assistant", longDisha), msg("user", "acidity hai")},
			want:     "Disha: " + longDisha + "\nUser: acidity hai",
		},
		{
			name: "multiple trailing user messages merge into one turn",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("user", "haan"), msg("user", "acidity hai"),
			},
			want: "Disha: " + longDisha + "\nUser: haan acidity hai",
		},
		{
			name: "tool-call turn and tool result are skipped",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				toolCallMsg(), {Role: "tool", Content: "guidance", ToolCallID: "call-1"},
				msg("user", "acidity hai"),
			},
			want: "Disha: " + longDisha + "\nUser: acidity hai",
		},
		{
			name: "short assistant stub is merged through",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("user", "haan"), msg("assistant", "हम्म"), msg("user", "acidity hai"),
			},
			want: "Disha: " + longDisha + "\nUser: acidity hai",
		},
		{
			name: "exactly six words is still a stub",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("assistant", "one two three four five six"), msg("user", "acidity hai"),
			},
			want: "Disha: " + longDisha + "\nUser: acidity hai",
		},
		{
			name: "seven words is the Disha block",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("assistant", "one two three four five six seven"), msg("user", "acidity hai"),
			},
			want: "Disha: one two three four five six seven\nUser: acidity hai",
		},
		{
			name:     "no Disha block yields the user line alone",
			messages: []voicepipelinecore.Message{msg("system", "sys"), msg("user", "hello?")},
			want:     "User: hello?",
		},
		{
			name:     "no trailing user turn skips the round",
			messages: []voicepipelinecore.Message{msg("system", "sys"), msg("user", "hi"), msg("assistant", longDisha)},
			want:     "",
		},
		{
			name:     "blank trailing user content skips the round",
			messages: []voicepipelinecore.Message{msg("system", "sys"), msg("assistant", longDisha), msg("user", "   ")},
			want:     "",
		},
		{
			// Regression, staging conv f234dafb (2026-07-31): on a resumed call
			// buildInitialMessages appends the resume nudge as a USER message, so
			// the trailing user block merged bot-directed instructions into the
			// retrieval query instead of user speech.
			name: "resume nudge is skipped, not merged into the user turn",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("user", "हाँ, हाँ, हाँ, समझ गया मैं।"),
				msg("user", "<system_message>This conversation was interrupted because the call ended. "+
					"Now you have to resume this conversation by saying hi and acknowledge the things "+
					"that have been discussed very briefly and inform the next agenda.</system_message>"),
			},
			want: "Disha: " + longDisha + "\nUser: हाँ, हाँ, हाँ, समझ गया मैं।",
		},
		{
			name: "onboarding-style wrapper is skipped too",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("user", "real speech"),
				msg("user", "<system_instruction>do something</system_instruction>"),
			},
			want: "Disha: " + longDisha + "\nUser: real speech",
		},
		{
			name: "a resume nudge with no real user turn behind it skips the round",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("user", "<system_message>resume please</system_message>"),
			},
			want: "",
		},
		{
			name: "a previously injected block never becomes the Disha block",
			messages: []voicepipelinecore.Message{
				msg("system", "sys"), msg("assistant", longDisha),
				msg("user", renderProtocolBlock([]residentProtocol{{Text: "some protocol body"}})),
				msg("user", "acidity hai"),
			},
			want: "Disha: " + longDisha + "\nUser: acidity hai",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildProtocolQueryText(stripProtocolBlock(tc.messages))
			if got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// stripProtocolBlock must not reuse the caller's backing array.
func TestStripProtocolBlockDoesNotMutateInput(t *testing.T) {
	block := renderProtocolBlock([]residentProtocol{{Text: "body"}})
	input := []voicepipelinecore.Message{msg("system", "sys"), msg("user", block), msg("user", "real")}
	_ = stripProtocolBlock(input)
	if input[1].Content != block || input[2].Content != "real" {
		t.Fatalf("input was mutated: %+v", input)
	}
}

// ----------------------------------------------------------------- the store

func candidate(id string, similarity float64, threshold int) protocolCandidate {
	return protocolCandidate{
		InstructionID: id,
		Title:         "title-" + id,
		Text:          "text-" + id,
		Similarity:    similarity,
		TurnThreshold: threshold,
		Qualified:     true,
	}
}

func residentIDs(protocols []residentProtocol) []string {
	ids := make([]string, 0, len(protocols))
	for _, protocol := range protocols {
		ids = append(ids, protocol.InstructionID)
	}
	return ids
}

func TestProtocolStoreTTLExpiry(t *testing.T) {
	store := NewProtocolStore()
	store.apply([]protocolCandidate{candidate("a", 0.9, 3)})

	// Present for the round it was added plus the next two.
	for round := 2; round <= 3; round++ {
		store.apply(nil)
		if len(store.snapshot()) != 1 {
			t.Fatalf("round %d: protocol should still be resident", round)
		}
	}
	events := store.apply(nil)
	if len(store.snapshot()) != 0 {
		t.Fatal("round 4: protocol should have expired")
	}
	if len(events) != 1 || events[0].Action != "expire" {
		t.Fatalf("expected one expire event, got %+v", events)
	}
}

func TestProtocolStoreHonoursPerProtocolThreshold(t *testing.T) {
	store := NewProtocolStore()
	store.apply([]protocolCandidate{candidate("long", 0.9, 5)})
	for round := 2; round <= 5; round++ {
		store.apply(nil)
		if len(store.snapshot()) != 1 {
			t.Fatalf("round %d: threshold 5 should keep it resident", round)
		}
	}
	store.apply(nil)
	if len(store.snapshot()) != 0 {
		t.Fatal("round 6: should have expired")
	}
}

func TestProtocolStoreEvictsFewestRemaining(t *testing.T) {
	store := NewProtocolStore()
	// a: threshold 5, b: 4, c: 3 -> after the next round c has the fewest left.
	store.apply([]protocolCandidate{candidate("a", 0.9, 5), candidate("b", 0.9, 4), candidate("c", 0.9, 3)})

	events := store.apply([]protocolCandidate{candidate("d", 0.95, 3)})

	got := residentIDs(store.snapshot())
	if len(got) != protocolCapacity {
		t.Fatalf("resident = %v, want %d entries", got, protocolCapacity)
	}
	if contains(got, "c") {
		t.Errorf("c had the fewest remaining turns and should have been evicted: %v", got)
	}
	if !contains(got, "d") {
		t.Errorf("the new protocol must always be admitted: %v", got)
	}
	if !hasEvent(events, "evict", "c") {
		t.Errorf("expected an evict event for c, got %+v", events)
	}
}

func TestProtocolStoreEvictionTieBreaksOnScore(t *testing.T) {
	store := NewProtocolStore()
	// Same threshold, so remaining turns tie; b was added with the lower score.
	store.apply([]protocolCandidate{candidate("a", 0.90, 3), candidate("b", 0.72, 3), candidate("c", 0.88, 3)})

	store.apply([]protocolCandidate{candidate("d", 0.95, 3)})

	got := residentIDs(store.snapshot())
	if contains(got, "b") {
		t.Errorf("b had the lowest score-at-addition and should have been evicted: %v", got)
	}
	if !contains(got, "d") {
		t.Errorf("new protocol not admitted: %v", got)
	}
}

func TestProtocolStoreRefreshDoesNotEvict(t *testing.T) {
	store := NewProtocolStore()
	store.apply([]protocolCandidate{candidate("a", 0.9, 3), candidate("b", 0.9, 3), candidate("c", 0.9, 3)})

	events := store.apply([]protocolCandidate{candidate("a", 0.99, 3)})

	got := residentIDs(store.snapshot())
	if len(got) != 3 {
		t.Fatalf("resident = %v, want all three kept", got)
	}
	if !hasEvent(events, "refresh", "a") {
		t.Errorf("expected a refresh event for a, got %+v", events)
	}
	for _, event := range events {
		if event.Action == "evict" {
			t.Errorf("a re-retrieved protocol must not cause an eviction: %+v", events)
		}
	}
	// TTL reset, so a survives longer than the untouched b/c.
	store.apply(nil)
	store.apply(nil)
	if got := residentIDs(store.snapshot()); len(got) != 1 || got[0] != "a" {
		t.Fatalf("resident = %v, want only the refreshed a", got)
	}
}

func TestProtocolStoreNeverEvictsSameRoundAdditions(t *testing.T) {
	store := NewProtocolStore()
	store.apply([]protocolCandidate{candidate("old", 0.9, 9)})

	// Three new qualifying protocols against a capacity of 3 with one already
	// resident: the pre-existing one goes, and all three newcomers stay.
	store.apply([]protocolCandidate{
		candidate("n1", 0.95, 3), candidate("n2", 0.94, 3), candidate("n3", 0.93, 3),
	})

	got := residentIDs(store.snapshot())
	if len(got) != protocolCapacity {
		t.Fatalf("resident = %v, want %d", got, protocolCapacity)
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		if !contains(got, id) {
			t.Errorf("%s was added this round and must not be evicted: %v", id, got)
		}
	}
}

func TestProtocolStoreSnapshotIsNewestFirst(t *testing.T) {
	store := NewProtocolStore()
	store.apply([]protocolCandidate{candidate("a", 0.9, 3)})
	store.apply([]protocolCandidate{candidate("b", 0.9, 3)})
	if got := residentIDs(store.snapshot()); len(got) != 2 || got[0] != "b" {
		t.Fatalf("snapshot = %v, want newest-addition first", got)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func hasEvent(events []protocolEvent, action, id string) bool {
	for _, event := range events {
		if event.Action == action && event.InstructionID == id {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------- turn placement

func TestProtocolInsertIndexAndBlockPlacement(t *testing.T) {
	long := "this is a full length assistant turn with plenty of words"

	t.Run("three assistant turns follow the block", func(t *testing.T) {
		messages := []voicepipelinecore.Message{
			msg("system", "sys"),
			msg("user", "u1"), msg("assistant", long),
			msg("user", "u2"), msg("assistant", long),
			msg("user", "u3"), msg("assistant", long),
			msg("user", "u4"),
		}
		out, index := injectProtocolBlock(messages, renderProtocolBlock([]residentProtocol{{Text: "p"}}))
		if index < 1 {
			t.Fatalf("index = %d", index)
		}
		// Count ASSISTANT turns after the block, in the original array. The unit
		// is bot generations, the same unit turnsThresholdCount uses.
		var turnsAfter int
		for _, start := range assistantTurnStarts(messages) {
			if start >= index {
				turnsAfter++
			}
		}
		if turnsAfter != protocolBlockTurnsFromTail {
			t.Fatalf("turns after block = %d, want %d (index=%d)", turnsAfter, protocolBlockTurnsFromTail, index)
		}
		if !isProtocolBlockMessage(out[index]) {
			t.Fatalf("block is not at the reported index %d", index)
		}
		if out[index+1].Content != messages[index].Content {
			t.Fatalf("insertion displaced the wrong message: %q", out[index+1].Content)
		}
	})

	t.Run("short history clamps below the system message", func(t *testing.T) {
		messages := []voicepipelinecore.Message{msg("system", "sys"), msg("user", "u1")}
		out, index := injectProtocolBlock(messages, renderProtocolBlock([]residentProtocol{{Text: "p"}}))
		if index != 1 {
			t.Fatalf("index = %d, want 1", index)
		}
		if out[0].Role != "system" {
			t.Fatalf("system message must stay first, got %q", out[0].Role)
		}
	})

	t.Run("never splits a tool pair", func(t *testing.T) {
		messages := []voicepipelinecore.Message{
			msg("system", "sys"),
			msg("user", "u1"),
			toolCallMsg(), {Role: "tool", Content: "res", ToolCallID: "call-1"}, msg("assistant", long),
			msg("user", "u2"),
			msg("assistant", long),
			msg("user", "u3"),
		}
		_, index := injectProtocolBlock(messages, renderProtocolBlock([]residentProtocol{{Text: "p"}}))
		if messages[index].Role == "tool" {
			t.Fatalf("index %d lands on a tool message", index)
		}
		if index > 0 && len(messages[index-1].ToolCalls) > 0 {
			t.Fatalf("index %d splits an assistant tool_calls message from its result", index)
		}
	})

	t.Run("empty resident set injects nothing", func(t *testing.T) {
		messages := []voicepipelinecore.Message{msg("system", "sys"), msg("user", "u1")}
		out, index := injectProtocolBlock(messages, renderProtocolBlock(nil))
		if index != -1 || len(out) != len(messages) {
			t.Fatalf("index = %d, len = %d; expected no injection", index, len(out))
		}
	})
}

// ------------------------------------------------------------ block rendering

func TestRenderProtocolBlock(t *testing.T) {
	t.Run("empty set renders nothing", func(t *testing.T) {
		if got := renderProtocolBlock(nil); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})

	t.Run("header is byte-exact and bullets match the resident count", func(t *testing.T) {
		for _, count := range []int{1, 2, 3} {
			protocols := make([]residentProtocol, 0, count)
			for i := 0; i < count; i++ {
				protocols = append(protocols, residentProtocol{Text: fmt.Sprintf("protocol %d body", i)})
			}
			got := renderProtocolBlock(protocols)

			if !strings.HasPrefix(got, "<system_message>\n"+protocolBlockHeader) {
				t.Fatalf("count %d: header not byte-exact:\n%s", count, got)
			}
			if !strings.HasSuffix(got, "</system_message>") {
				t.Fatalf("count %d: missing closing tag:\n%s", count, got)
			}
			if bullets := strings.Count(got, "\n- protocol "); bullets != count {
				t.Fatalf("count %d: got %d protocol bullets:\n%s", count, bullets, got)
			}
			// No empty placeholders when fewer than three are resident.
			if strings.Contains(got, "{{protocol_text") || strings.Contains(got, "\n- \n") {
				t.Fatalf("count %d: empty placeholder rendered:\n%s", count, got)
			}
		}
	})
}

// ------------------------------------------------- weaviate decode + ranking

const anchorResponseTemplate = `{"data":{"Get":{"ProtocolAnchor":[%s]}}}`

func anchorHit(anchorID, instructionID, text string, distance float64, threshold any) string {
	thresholdJSON := "null"
	if threshold != nil {
		thresholdJSON = fmt.Sprintf("%v", threshold)
	}
	return fmt.Sprintf(`{"anchorText":%q,
      "answeredBy":[{"instructionText":%q,"title":"t-%s","documentVersionPath":"p/v/1",
                     "turnsThresholdCount":%s,"_additional":{"id":%q}}],
      "_additional":{"id":%q,"distance":%v}}`,
		"anchor for "+instructionID, text, instructionID, thresholdJSON, instructionID, anchorID, distance)
}

func newStubWeaviate(t *testing.T, body string, capture *string) *weaviate.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if capture != nil {
			var payload struct{ Query string }
			_ = json.Unmarshal(raw, &payload)
			*capture = payload.Query
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, err := weaviate.New(weaviate.Config{BaseURL: server.URL, APIKey: "k"})
	if err != nil {
		t.Fatalf("weaviate.New: %v", err)
	}
	return client
}

func TestQueryProtocolsDedupesByInstruction(t *testing.T) {
	// Three anchors, two instructions: the classic shape measured live.
	body := fmt.Sprintf(anchorResponseTemplate, strings.Join([]string{
		anchorHit("anchor-1", "instr-A", "protocol A", 0.30, 4),
		anchorHit("anchor-2", "instr-A", "protocol A", 0.12, 4), // better score, same instruction
		anchorHit("anchor-3", "instr-B", "protocol B", 0.25, nil),
	}, ","))

	candidates, err := queryProtocols(context.Background(), newStubWeaviate(t, body, nil), "q")
	if err != nil {
		t.Fatalf("queryProtocols: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("got %d candidates, want 2 distinct instructions: %+v", len(candidates), candidates)
	}
	if candidates[0].InstructionID != "instr-A" || candidates[0].AnchorID != "anchor-2" {
		t.Errorf("best anchor per instruction not kept: %+v", candidates[0])
	}
	if got := candidates[0].Similarity; got < 0.87999 || got > 0.88001 {
		t.Errorf("similarity = %v, want 0.88", got)
	}
	// Sorted best-first.
	if candidates[0].Similarity < candidates[1].Similarity {
		t.Errorf("candidates not sorted best-first: %+v", candidates)
	}
}

func TestQueryProtocolsThresholdAndTTLFallback(t *testing.T) {
	// Distances chosen to sit exactly at, just above, and well below the gate.
	atGate := 1 - protocolSimilarityThreshold
	body := fmt.Sprintf(anchorResponseTemplate, strings.Join([]string{
		anchorHit("a1", "at-gate", "at gate", atGate, nil),
		anchorHit("a2", "below-gate", "below gate", atGate+0.01, 3),
		anchorHit("a3", "explicit-ttl", "explicit", 0.05, 5),
		anchorHit("a4", "zero-ttl", "zero", 0.05, 0),
	}, ","))

	candidates, err := queryProtocols(context.Background(), newStubWeaviate(t, body, nil), "q")
	if err != nil {
		t.Fatalf("queryProtocols: %v", err)
	}

	byID := map[string]protocolCandidate{}
	for _, candidate := range candidates {
		byID[candidate.InstructionID] = candidate
	}
	if !byID["at-gate"].Qualified {
		t.Errorf("a hit exactly at the threshold must qualify (sim=%v)", byID["at-gate"].Similarity)
	}
	if byID["below-gate"].Qualified {
		t.Errorf("a hit just under the threshold must not qualify (sim=%v)", byID["below-gate"].Similarity)
	}
	if got := byID["at-gate"].TurnThreshold; got != protocolDefaultTurnThreshold {
		t.Errorf("null turnsThresholdCount -> %d, want the default %d", got, protocolDefaultTurnThreshold)
	}
	if got := byID["explicit-ttl"].TurnThreshold; got != 5 {
		t.Errorf("explicit turnsThresholdCount -> %d, want 5", got)
	}
	if got := byID["zero-ttl"].TurnThreshold; got != protocolDefaultTurnThreshold {
		t.Errorf("zero turnsThresholdCount -> %d, want the default %d", got, protocolDefaultTurnThreshold)
	}
}

func TestQueryProtocolsDropsUnusableHits(t *testing.T) {
	body := `{"data":{"Get":{"ProtocolAnchor":[
      {"anchorText":"no cross ref","answeredBy":[],"_additional":{"id":"a1","distance":0.1}},
      {"anchorText":"blank text","answeredBy":[{"instructionText":"  ","_additional":{"id":"i2"}}],"_additional":{"id":"a2","distance":0.1}},
      {"anchorText":"no distance","answeredBy":[{"instructionText":"body","_additional":{"id":"i3"}}],"_additional":{"id":"a3"}},
      {"anchorText":"no instruction id","answeredBy":[{"instructionText":"body"}],"_additional":{"id":"a4","distance":0.1}}
    ]}}}`
	candidates, err := queryProtocols(context.Background(), newStubWeaviate(t, body, nil), "q")
	if err != nil {
		t.Fatalf("queryProtocols: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("expected all unusable hits dropped, got %+v", candidates)
	}
}

func TestQueryProtocolsUsesEnvironmentFlag(t *testing.T) {
	for _, tc := range []struct{ environment, want string }{
		{"prod", "isProduction"},       // Python voice worker's value
		{"production", "isProduction"}, // THIS worker's value — see weaviateEnvFlagField
		{"PRODUCTION", "isProduction"},
		{"staging", "isStaging"},
		{"", "isStaging"},
		{"typo", "isStaging"},
	} {
		t.Run(tc.environment, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", tc.environment)
			var query string
			client := newStubWeaviate(t, fmt.Sprintf(anchorResponseTemplate, ""), &query)
			if _, err := queryProtocols(context.Background(), client, "q"); err != nil {
				t.Fatalf("queryProtocols: %v", err)
			}
			if !strings.Contains(query, tc.want) {
				t.Errorf("query should filter on %s:\n%s", tc.want, query)
			}
		})
	}
}

// The query text must reach Weaviate raw: TEI adds the "Document: " prefix
// server-side, and prefixing here would double-prefix and silently degrade
// ranking.
func TestQueryProtocolsSendsRawText(t *testing.T) {
	var query string
	client := newStubWeaviate(t, fmt.Sprintf(anchorResponseTemplate, ""), &query)
	if _, err := queryProtocols(context.Background(), client, "Disha: a\nUser: b"); err != nil {
		t.Fatalf("queryProtocols: %v", err)
	}
	if strings.Contains(query, "Document: ") {
		t.Fatalf("query text must not be prefixed:\n%s", query)
	}
	if !strings.Contains(query, `concepts: ["Disha: a\nUser: b"]`) {
		t.Fatalf("raw query text not sent:\n%s", query)
	}
}

// ------------------------------------------------------------- the enricher

type stubMetadataSetter struct {
	mu       sync.Mutex
	metadata map[string]any
	calls    int
}

func (s *stubMetadataSetter) SetPromptMetadata(metadata map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata = metadata
	s.calls++
}

func (s *stubMetadataSetter) snapshot() (map[string]any, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metadata, s.calls
}

// stubRenderer stands in for the Jinja subprocess. It does a literal
// `{{ name }}` substitution and nothing else, which is enough to prove the
// enricher routes instruction text through a renderer and handles the outcomes.
type stubRenderer struct {
	mu    sync.Mutex
	err   error
	block bool // hang until the caller's context expires
	vars  []DocumentVariables
}

func (r *stubRenderer) RenderTemplate(ctx context.Context, _ string, text string, variables DocumentVariables) (string, error) {
	r.mu.Lock()
	r.vars = append(r.vars, variables)
	err, block := r.err, r.block
	r.mu.Unlock()
	if block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if err != nil {
		return "", err
	}
	for name, value := range variables {
		text = strings.ReplaceAll(text, "{{"+name+"}}", fmt.Sprint(value))
		text = strings.ReplaceAll(text, "{{ "+name+" }}", fmt.Sprint(value))
	}
	return text, nil
}

func (r *stubRenderer) calls() []DocumentVariables {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]DocumentVariables(nil), r.vars...)
}

func newTestEnricher(t *testing.T, client *weaviate.Client) (*protocolEnricher, *protocolRecordBox, *stubMetadataSetter) {
	t.Helper()
	box := &protocolRecordBox{}
	baseVariables := DocumentVariables{"patient_info": "info"}
	enricher := newProtocolEnricher(
		client, NewProtocolStore(), box,
		log.New(io.Discard, "", 0),
		&stubRenderer{},
		buildPromptTraceMetadata("system", "followup/sys", 3, baseVariables),
		baseVariables,
		"user-1", "conv-1",
	)
	setter := &stubMetadataSetter{}
	enricher.SetInfrastructure(setter, nil)
	return enricher, box, setter
}

func conversation(userText string) []voicepipelinecore.Message {
	return []voicepipelinecore.Message{
		msg("system", "sys"),
		msg("assistant", "aapka weight kaisa chal raha hai bataiye zara detail me"),
		msg("user", userText),
	}
}

func TestEnricherInjectsAndRecords(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate, anchorHit("a1", "instr-A", "protocol A body", 0.1, 3))
	enricher, box, setter := newTestEnricher(t, newStubWeaviate(t, body, nil))

	out := enricher.Enrich(context.Background(), conversation("acidity hai"))

	if len(out) != 4 {
		t.Fatalf("expected one injected message, got %d: %+v", len(out), out)
	}
	var found bool
	for _, message := range out {
		if isProtocolBlockMessage(message) {
			found = true
			if message.Role != "user" {
				t.Errorf("block role = %q, want user", message.Role)
			}
			if !strings.Contains(message.Content, "protocol A body") {
				t.Errorf("block missing the protocol text: %s", message.Content)
			}
		}
	}
	if !found {
		t.Fatal("no protocol block injected")
	}

	record := box.take()
	if record == nil {
		t.Fatal("no retrieval record published")
	}
	if record.Status != "ok" || len(record.Injected) != 1 || record.TopSimilarity == nil {
		t.Fatalf("unexpected record: %+v", record)
	}

	metadata, calls := setter.snapshot()
	if calls != 1 {
		t.Fatalf("SetPromptMetadata calls = %d, want 1", calls)
	}
	variables, ok := metadata["system_prompt_variables"].(DocumentVariables)
	if !ok {
		t.Fatalf("system_prompt_variables missing or wrong type: %#v", metadata["system_prompt_variables"])
	}
	protocols, ok := variables["retrieved_protocols"].([]any)
	if !ok || len(protocols) != 1 {
		t.Fatalf("retrieved_protocols = %#v", variables["retrieved_protocols"])
	}
	if variables["patient_info"] != "info" {
		t.Errorf("base variables lost: %#v", variables)
	}
	// The base maps built in plan() must never be mutated.
	if _, leaked := enricher.baseVariables["retrieved_protocols"]; leaked {
		t.Error("base variables were mutated")
	}
}

// Two of the 30 live protocols carry Jinja. Their instruction text must be
// rendered against the same variable store the call's prompts were rendered
// with, so the model never sees `{% if diet_chart_available %}`.
func TestEnricherRendersInstructionVariables(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate,
		anchorHit("a1", "instr-A", "Today's plan: {{ diet_plan_today }}", 0.1, 3))
	enricher, box, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))
	renderer := enricher.renderer.(*stubRenderer)
	enricher.baseVariables = DocumentVariables{"diet_plan_today": "dal chawal at 1pm"}

	out := enricher.Enrich(context.Background(), conversation("khaana kab khaun"))

	block := findProtocolBlock(t, out)
	if !strings.Contains(block, "Today's plan: dal chawal at 1pm") {
		t.Errorf("instruction text not rendered into block:\n%s", block)
	}
	if strings.Contains(block, "{{") {
		t.Errorf("raw jinja leaked into the block:\n%s", block)
	}
	if calls := renderer.calls(); len(calls) != 1 || calls[0]["diet_plan_today"] != "dal chawal at 1pm" {
		t.Errorf("renderer got the wrong variable store: %#v", calls)
	}
	// The rendered text — not the template — is what telemetry reports.
	record := box.take()
	if record == nil || len(record.Injected) != 1 {
		t.Fatalf("unexpected record: %+v", record)
	}
	if record.Injected[0].Text != "Today's plan: dal chawal at 1pm" {
		t.Errorf("resident text = %q, want the rendered text", record.Injected[0].Text)
	}
}

// Protocols without template syntax — 28 of the 30 live ones — must not pay
// for an IPC round trip on this blocking path.
func TestEnricherSkipsRendererForPlainText(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate, anchorHit("a1", "instr-A", "plain protocol body", 0.1, 3))
	enricher, _, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))
	renderer := enricher.renderer.(*stubRenderer)

	enricher.Enrich(context.Background(), conversation("mera pet kharab hai"))

	if calls := renderer.calls(); len(calls) != 0 {
		t.Errorf("renderer called %d times for plain text, want 0", len(calls))
	}
}

// A protocol whose template will not render is dropped rather than injected
// raw: showing the model both branches of an `{% if %}` is worse than the
// protocol being absent.
func TestEnricherDropsProtocolWhenRenderFails(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate, strings.Join([]string{
		anchorHit("a1", "instr-A", "{% if diet_chart_available %}has plan{% endif %}", 0.1, 3),
		anchorHit("a2", "instr-B", "plain protocol body", 0.2, 3),
	}, ","))
	enricher, box, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))
	enricher.renderer.(*stubRenderer).err = errors.New("undefined variable")

	out := enricher.Enrich(context.Background(), conversation("diet chart nahi mila"))

	record := box.take()
	if record == nil {
		t.Fatal("no retrieval record published")
	}
	if len(record.Injected) != 1 || record.Injected[0].InstructionID != "instr-B" {
		t.Fatalf("unrenderable protocol was not dropped: %+v", record.Injected)
	}
	// qualified_count tracks threshold crossings, not admissions, so it must
	// stay consistent with the two qualified:true candidates in the payload.
	if record.Qualified != 2 {
		t.Errorf("qualified = %d, want 2 (render drops do not change the count)", record.Qualified)
	}
	if block := findProtocolBlock(t, out); strings.Contains(block, "{%") {
		t.Errorf("raw jinja leaked into the block:\n%s", block)
	}
}

// A protocol that stays resident and keeps re-matching must not be re-rendered:
// apply's refresh pass throws the text away, so it is IPC spent for nothing on
// a blocking path.
func TestEnricherDoesNotReRenderResidentProtocol(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate,
		anchorHit("a1", "instr-A", "Plan: {{ diet_plan_today }}", 0.1, 5))
	enricher, _, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))
	renderer := enricher.renderer.(*stubRenderer)
	enricher.baseVariables = DocumentVariables{"diet_plan_today": "dal chawal"}

	// Three rounds, same protocol qualifying every time. Distinct query text per
	// round so the hash gate does not suppress the later retrievals.
	for _, speech := range []string{"khaana kab", "aur uske baad kya", "theek hai samajh gaya"} {
		out := enricher.Enrich(context.Background(), conversation(speech))
		if block := findProtocolBlock(t, out); !strings.Contains(block, "Plan: dal chawal") {
			t.Fatalf("resident text lost after refresh:\n%s", block)
		}
	}
	if calls := renderer.calls(); len(calls) != 1 {
		t.Errorf("rendered %d times across 3 rounds, want 1 (admission only)", len(calls))
	}
}

// Render timing is reported to app.log and RTVI only — never to the chunk or
// the S3 payload, which is why it lives on the record but not in the payload.
func TestEnricherReportsRenderLatency(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate, anchorHit("a1", "instr-A", "plain protocol body", 0.1, 3))
	enricher, box, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))

	enricher.Enrich(context.Background(), conversation("mera pet kharab hai"))

	record := box.take()
	if record == nil {
		t.Fatal("no record")
	}
	if record.RenderCount != 0 || record.RenderLatencyMs != 0 {
		t.Errorf("plain text must not report render work: count=%d ms=%v", record.RenderCount, record.RenderLatencyMs)
	}
	payload := protocolRetrievalRecordPayload("chunk-1", "user-1", "conv-1", FollowUpBotType, *record)
	latency, _ := payload["latency_ms"].(map[string]any)
	if _, present := latency["render"]; present {
		t.Error("render timing must stay out of the S3 payload")
	}
}

// Rendering shares protocolRetrievalBudget with the vector query rather than
// getting a budget of its own, so N templated protocols cannot cost N budgets.
// This sits in front of every LLM generation: the whole step is capped.
func TestEnricherRenderSharesRetrievalBudget(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate, strings.Join([]string{
		anchorHit("a1", "instr-A", "{{ diet_plan_today }}", 0.1, 3),
		anchorHit("a2", "instr-B", "{{ diet_plan_today }} again", 0.11, 3),
		anchorHit("a3", "instr-C", "{{ diet_plan_today }} thrice", 0.12, 3),
	}, ","))
	enricher, box, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))
	enricher.renderer.(*stubRenderer).block = true

	startedAt := time.Now()
	enricher.Enrich(context.Background(), conversation("khaana kab khaun"))
	elapsed := time.Since(startedAt)

	// Three hanging renders under one shared budget: comfortably under two
	// budgets even on a loaded CI box, but far above one if they were additive.
	if elapsed > 2*protocolRetrievalBudget {
		t.Errorf("step took %v for 3 renders, want one shared %v budget", elapsed, protocolRetrievalBudget)
	}
	record := box.take()
	if record == nil || len(record.Injected) != 0 {
		t.Fatalf("timed-out renders must not be injected: %+v", record)
	}
}

// findProtocolBlock returns the injected protocol block, failing if absent.
func findProtocolBlock(t *testing.T, messages []voicepipelinecore.Message) string {
	t.Helper()
	for _, message := range messages {
		if strings.Contains(message.Content, protocolBlockHeader) {
			return message.Content
		}
	}
	t.Fatal("no protocol block injected")
	return ""
}

// The greet-first turn has no real user input ("hello?" is a synthetic seed)
// and is the most latency-sensitive moment of the call.
func TestEnricherSkipsGreetTurn(t *testing.T) {
	var queries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		_, _ = w.Write([]byte(fmt.Sprintf(anchorResponseTemplate, "")))
	}))
	t.Cleanup(server.Close)
	client, _ := weaviate.New(weaviate.Config{BaseURL: server.URL, APIKey: "k"})
	enricher, box, _ := newTestEnricher(t, client)

	greet := []voicepipelinecore.Message{msg("system", "sys"), msg("user", "hello?")}
	out := enricher.Enrich(context.Background(), greet)

	if got := queries.Load(); got != 0 {
		t.Errorf("greet turn issued %d retrieval queries, want 0", got)
	}
	if len(out) != len(greet) {
		t.Errorf("greet context should be untouched, got %+v", out)
	}
	if box.take() != nil {
		t.Error("greet turn must not claim an assistant chunk")
	}
}

// A tool-result re-run reaches Enrich a second time in the same turn. It must
// re-inject without re-querying, and must not overwrite the round that did run.
func TestEnricherToolReRunInjectsWithoutRetrieving(t *testing.T) {
	var queries atomic.Int32
	body := fmt.Sprintf(anchorResponseTemplate, anchorHit("a1", "instr-A", "protocol A body", 0.1, 3))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, _ := weaviate.New(weaviate.Config{BaseURL: server.URL, APIKey: "k"})
	enricher, box, _ := newTestEnricher(t, client)

	messages := conversation("acidity hai")
	enricher.Enrich(context.Background(), messages)

	// Same user turn, now with a tool pair appended (the get_guidance re-run).
	withTool := append(append([]voicepipelinecore.Message{}, messages...),
		toolCallMsg(), voicepipelinecore.Message{Role: "tool", Content: "guidance", ToolCallID: "call-1"})
	out := enricher.Enrich(context.Background(), withTool)

	if got := queries.Load(); got != 1 {
		t.Errorf("queries = %d, want 1 (the re-run must not re-query)", got)
	}
	var injected int
	for _, message := range out {
		if isProtocolBlockMessage(message) {
			injected++
		}
	}
	if injected != 1 {
		t.Errorf("re-run should still inject exactly one block, got %d", injected)
	}

	record := box.take()
	if record == nil {
		t.Fatal("the real retrieval round's record was lost")
	}
	if record.Status != "ok" || len(record.Candidates) == 0 {
		t.Fatalf("skipped re-run overwrote the real record: %+v", record)
	}
}

// TTLs must count user turns, not LLM calls.
func TestEnricherReRunDoesNotAgeResidentSet(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate, anchorHit("a1", "instr-A", "protocol A", 0.1, 3))
	enricher, _, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))

	messages := conversation("acidity hai")
	enricher.Enrich(context.Background(), messages)
	before := enricher.store.snapshot()[0].RemainingTurns

	enricher.Enrich(context.Background(), messages) // identical query -> skipped

	after := enricher.store.snapshot()[0].RemainingTurns
	if before != after {
		t.Fatalf("remaining turns changed on a skipped round: %d -> %d", before, after)
	}
}

func TestEnricherFailsOpen(t *testing.T) {
	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus string
	}{
		{
			name: "server error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantStatus: "error",
		},
		{
			name: "graphql errors payload",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"errors":[{"message":"boom"}]}`))
			},
			wantStatus: "error",
		},
		{
			name: "budget exceeded",
			handler: func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(protocolRetrievalBudget + 200*time.Millisecond)
				_, _ = w.Write([]byte(fmt.Sprintf(anchorResponseTemplate, "")))
			},
			wantStatus: "timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			t.Cleanup(server.Close)
			client, _ := weaviate.New(weaviate.Config{BaseURL: server.URL, APIKey: "k"})
			enricher, box, _ := newTestEnricher(t, client)

			// Seed a resident protocol so we can prove it survives the failure.
			enricher.store.apply([]protocolCandidate{candidate("kept", 0.9, 5)})

			messages := conversation("acidity hai")
			out := enricher.Enrich(context.Background(), messages)

			if len(out) != len(messages)+1 {
				t.Fatalf("resident protocol was not injected despite failing open: %+v", out)
			}
			if got := residentIDs(enricher.store.snapshot()); len(got) != 1 || got[0] != "kept" {
				t.Fatalf("resident set = %v, want it left intact", got)
			}
			record := box.take()
			if record == nil || record.Status != tc.wantStatus {
				t.Fatalf("record = %+v, want status %q", record, tc.wantStatus)
			}
			if record.Err == "" {
				t.Error("record should carry the error message")
			}
		})
	}
}

func TestEnricherNoQualifyingHitsInjectsNothing(t *testing.T) {
	// Derived from the constant, not hardcoded, so re-tuning the threshold
	// doesn't turn this into a false failure: similarity lands one hundredth
	// under whatever the gate currently is.
	belowGate := 1 - protocolSimilarityThreshold + 0.01
	body := fmt.Sprintf(anchorResponseTemplate, anchorHit("a1", "instr-A", "protocol A", belowGate, 3))
	enricher, box, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))

	messages := conversation("theek tha didi")
	out := enricher.Enrich(context.Background(), messages)

	if len(out) != len(messages) {
		t.Fatalf("nothing qualified, so nothing should be injected: %+v", out)
	}
	record := box.take()
	if record == nil || record.Status != "ok" {
		t.Fatalf("record = %+v", record)
	}
	if len(record.Candidates) != 1 || record.Candidates[0].Qualified {
		t.Fatalf("sub-threshold candidate should still be recorded for calibration: %+v", record.Candidates)
	}
	if record.TopSimilarity == nil {
		t.Error("top similarity must be recorded even when nothing qualifies")
	}
}

// Repeated turns must not accumulate blocks: each round strips the previous one.
func TestEnricherInjectsExactlyOneBlockAcrossTurns(t *testing.T) {
	body := fmt.Sprintf(anchorResponseTemplate, anchorHit("a1", "instr-A", "protocol A body", 0.1, 9))
	enricher, _, _ := newTestEnricher(t, newStubWeaviate(t, body, nil))

	messages := conversation("turn one")
	for turn := 0; turn < 3; turn++ {
		out := enricher.Enrich(context.Background(), messages)
		blocks := 0
		for _, message := range out {
			if isProtocolBlockMessage(message) {
				blocks++
			}
		}
		if blocks != 1 {
			t.Fatalf("turn %d: %d blocks present, want exactly 1", turn, blocks)
		}
		// Feed the enriched output back in, as a real conversation would grow.
		messages = append(out, msg("assistant", "theek hai main samajh gayi aapki baat"),
			msg("user", fmt.Sprintf("turn %d follow up", turn+2)))
	}
}

// ---------------------------------------------------------------- the wiring

// The env flag is the only gate: both follow-up paths get retrieval when it is
// on, and neither gets it when it is off.
func TestSetupProtocolRetrievalGating(t *testing.T) {
	tests := []struct {
		name    string
		dynamic bool
		flag    string
		want    bool
	}{
		{"dynamic and enabled", true, "1", true},
		{"agenda follow-up and enabled", false, "1", true},
		{"dynamic but flag off", true, "0", false},
		{"dynamic but flag unset", true, "", false},
		{"agenda follow-up with flag off", false, "0", false},
		{"agenda follow-up with flag unset", false, "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(protocolRetrievalEnabledEnv, tc.flag)
			t.Setenv(guardrailCheckEnabledEnv, "")
			t.Setenv("WEAVIATE_URL", "http://weaviate.staging.svc.cluster.local:8080")
			t.Setenv("WEAVIATE_API_KEY", "key")
			t.Setenv("AWS_US_BUCKET_NAME", "")
			t.Setenv("AWS_US_REGION", "")

			pl := &followUpPlan{
				Startup: CallStartup{
					Logger:         log.New(io.Discard, "", 0),
					UserID:         "user-1",
					ConversationID: "conv-1",
				},
				Dynamic:         tc.dynamic,
				PromptMetadata:  map[string]any{},
				PromptVariables: DocumentVariables{},
				Callbacks:       &CallEventCallbacks{},
			}
			setupFollowUpRetrieval(pl, nil)

			if got := pl.ProtocolEnricher != nil; got != tc.want {
				t.Errorf("enricher present = %v, want %v", got, tc.want)
			}
			if got := pl.Callbacks.chunkDecorator != nil; got != tc.want {
				t.Errorf("chunk decorator registered = %v, want %v", got, tc.want)
			}
		})
	}
}

// An unconfigured Weaviate means "feature off", not a failed call.
func TestSetupProtocolRetrievalMissingWeaviateConfig(t *testing.T) {
	t.Setenv(protocolRetrievalEnabledEnv, "1")
	t.Setenv(guardrailCheckEnabledEnv, "")
	t.Setenv("WEAVIATE_URL", "")
	t.Setenv("WEAVIATE_API_KEY", "")

	pl := &followUpPlan{
		Startup:   CallStartup{Logger: log.New(io.Discard, "", 0)},
		Dynamic:   true,
		Callbacks: &CallEventCallbacks{},
	}
	setupFollowUpRetrieval(pl, nil)

	if pl.ProtocolEnricher != nil {
		t.Error("enricher should not be built without Weaviate config")
	}
	if pl.Callbacks.chunkDecorator != nil {
		t.Error("chunk decorator should not be registered without Weaviate config")
	}
}

// A client that doesn't implement SetPromptMetadata (test stubs) must not break
// the enricher.
func TestRouterPromptMetadataSetter(t *testing.T) {
	if got := routerPromptMetadataSetter(nil); got != nil {
		t.Error("nil client should yield a nil setter")
	}
	enricher, _, _ := newTestEnricher(t, newStubWeaviate(t, fmt.Sprintf(anchorResponseTemplate, ""), nil))
	enricher.SetInfrastructure(nil, nil)
	// Must not panic with no router wired.
	enricher.refreshPromptMetadata(nil)
}

func residentIDsInOrder(store *ProtocolStore) []string {
	return residentIDs(store.snapshot())
}

func eventActions(events []protocolEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.Action+":"+e.InstructionID)
	}
	return out
}

// Regression, found by review 2026-08-01: with a single interleaved pass, a
// higher-scoring newcomer evicted a resident whose own refresh had not been
// processed yet. That resident was then re-added as brand new — two evictions
// instead of one, an unrelated protocol lost, and the insertion order corrupted.
func TestProtocolStoreRefreshBeforeAdmitPreventsChurn(t *testing.T) {
	store := NewProtocolStore()
	store.apply([]protocolCandidate{
		candidate("a", 0.70, 2), candidate("b", 0.80, 5), candidate("c", 0.90, 5),
	})
	seqBefore := map[string]int{}
	for _, p := range store.snapshot() {
		seqBefore[p.InstructionID] = p.seq
	}

	// "a" is about to age to 1 remaining, and re-matches strongly this round —
	// but "d" outranks it, so "d" is processed first.
	events := store.apply([]protocolCandidate{candidate("d", 0.99, 3), candidate("a", 0.98, 10)})

	actions := eventActions(events)
	for _, unwanted := range []string{"evict:a", "add:a"} {
		if contains(actions, unwanted) {
			t.Errorf("a re-matched and must be refreshed, not %s: %v", unwanted, actions)
		}
	}
	if !contains(actions, "refresh:a") {
		t.Errorf("expected refresh:a, got %v", actions)
	}
	if got := countAction(events, "evict"); got != 1 {
		t.Errorf("evictions = %d, want exactly 1 (capacity overflow only): %v", got, actions)
	}

	got := residentIDsInOrder(store)
	if !contains(got, "a") || !contains(got, "d") {
		t.Fatalf("resident = %v, want both the refreshed a and the new d", got)
	}
	for _, p := range store.snapshot() {
		if p.InstructionID == "a" {
			if p.seq != seqBefore["a"] {
				t.Errorf("a's insertion order changed (%d -> %d); a refresh must keep its slot",
					seqBefore["a"], p.seq)
			}
			if p.RemainingTurns != 10 {
				t.Errorf("a remaining = %d, want its refreshed threshold 10", p.RemainingTurns)
			}
		}
	}
}

// Regression: refreshes used to consume the per-round admission budget, so a
// genuinely new protocol was starved whenever the resident set kept matching —
// the common case, since the conversation stays on topic. Spec: the newly
// ranked protocol is added under all circumstances.
func TestProtocolStoreRefreshesDoNotStarveNewAdmission(t *testing.T) {
	store := NewProtocolStore()
	store.apply([]protocolCandidate{
		candidate("a", 0.90, 9), candidate("b", 0.89, 9), candidate("c", 0.88, 9),
	})

	events := store.apply([]protocolCandidate{
		candidate("a", 0.95, 9), candidate("b", 0.94, 9), candidate("c", 0.93, 9),
		candidate("NEW", 0.92, 9),
	})

	got := residentIDsInOrder(store)
	if !contains(got, "NEW") {
		t.Fatalf("resident = %v, want the newly ranked NEW admitted: %v", got, eventActions(events))
	}
	if len(got) != protocolCapacity {
		t.Errorf("resident = %v, want exactly %d", got, protocolCapacity)
	}
	if countAction(events, "refresh") != 3 {
		t.Errorf("all three residents re-matched and should refresh: %v", eventActions(events))
	}
}

// Duplicate ids inside one round must not consume admission budget either.
func TestProtocolStoreDuplicateCandidatesDoNotConsumeBudget(t *testing.T) {
	store := NewProtocolStore()
	events := store.apply([]protocolCandidate{
		candidate("a", 0.99, 3), candidate("a", 0.98, 3),
		candidate("b", 0.97, 3), candidate("c", 0.96, 3),
	})
	got := residentIDsInOrder(store)
	for _, want := range []string{"a", "b", "c"} {
		if !contains(got, want) {
			t.Errorf("resident = %v, missing %q: %v", got, want, eventActions(events))
		}
	}
	if len(got) != 3 {
		t.Errorf("resident = %v, want 3 distinct protocols", got)
	}
}

func countAction(events []protocolEvent, action string) int {
	n := 0
	for _, e := range events {
		if e.Action == action {
			n++
		}
	}
	return n
}

// A failed retrieval must still age the resident set. Fail-open means the
// protocols are KEPT and still injected, not that the turn stops counting —
// otherwise a run of failures (e.g. a tight budget timing out) pins protocols
// in context indefinitely.
func TestEnricherFailedRetrievalStillAgesTheStore(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"server error", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }},
		{"timeout", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(protocolRetrievalBudget + 200*time.Millisecond)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			t.Cleanup(server.Close)
			client, _ := weaviate.New(weaviate.Config{BaseURL: server.URL, APIKey: "k"})
			enricher, _, _ := newTestEnricher(t, client)

			enricher.store.apply([]protocolCandidate{candidate("kept", 0.9, 3)})
			before := enricher.store.snapshot()[0].RemainingTurns

			out := enricher.Enrich(context.Background(), conversation("first user turn"))
			after := enricher.store.snapshot()[0].RemainingTurns

			if after != before-1 {
				t.Errorf("remaining turns %d -> %d, want the failed turn to still age it", before, after)
			}
			// Fail open: the protocol is kept and still injected.
			if len(out) != len(conversation("first user turn"))+1 {
				t.Errorf("resident protocol should still be injected on failure")
			}
		})
	}
}

// ...and enough consecutive failures must still expire it, rather than pinning
// it in context forever.
func TestEnricherRepeatedFailuresEventuallyExpire(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	t.Cleanup(server.Close)
	client, _ := weaviate.New(weaviate.Config{BaseURL: server.URL, APIKey: "k"})
	enricher, _, _ := newTestEnricher(t, client)

	enricher.store.apply([]protocolCandidate{candidate("kept", 0.9, 3)})
	for turn := 1; turn <= 3; turn++ {
		// Distinct text per turn so the re-run gate treats each as a new turn.
		enricher.Enrich(context.Background(), conversation(fmt.Sprintf("user turn %d", turn)))
	}
	if got := len(enricher.store.snapshot()); got != 0 {
		t.Errorf("resident = %d after three failed turns, want it expired", got)
	}
}
