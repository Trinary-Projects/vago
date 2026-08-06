package disha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/weaviate"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

type guardrailStubServer struct {
	mu      sync.Mutex
	queries []string
	respond func(call int) (int, any)
}

func newGuardrailStubClient(t *testing.T, respond func(call int) (int, any)) (*weaviate.Client, *guardrailStubServer) {
	t.Helper()
	stub := &guardrailStubServer{respond: respond}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]string
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		stub.mu.Lock()
		stub.queries = append(stub.queries, request["query"])
		call := len(stub.queries) - 1
		stub.mu.Unlock()

		status, payload := stub.respond(call)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if payload != nil {
			_ = json.NewEncoder(w).Encode(payload)
		}
	}))
	t.Cleanup(server.Close)
	client, err := weaviate.New(weaviate.Config{BaseURL: server.URL, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatalf("weaviate.New: %v", err)
	}
	return client, stub
}

func (s *guardrailStubServer) query(index int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= len(s.queries) {
		return ""
	}
	return s.queries[index]
}

func guardrailGraphQLResponse(hits ...map[string]any) map[string]any {
	return map[string]any{
		"data": map[string]any{
			"Get": map[string]any{
				guardrailAnchorClass: hits,
			},
		},
	}
}

func guardrailHit(anchorID, instructionID, instructionText, title string, similarity float64) map[string]any {
	return map[string]any{
		"anchorText": anchorID + " anchor",
		"answeredBy": []any{map[string]any{
			"instructionText":     instructionText,
			"title":               title,
			"documentVersionPath": "guardrails/" + instructionID,
			"_additional":         map[string]any{"id": instructionID},
		}},
		"_additional": map[string]any{
			"id":       anchorID,
			"distance": 1 - similarity,
		},
	}
}

func newGuardrailTestHub(t *testing.T) (*sentry.Hub, *sentry.MockTransport) {
	t.Helper()
	transport := &sentry.MockTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Transport: transport})
	if err != nil {
		t.Fatalf("sentry.NewClient: %v", err)
	}
	return sentry.NewHub(client, sentry.NewScope()), transport
}

func newGuardrailTestChecker(t *testing.T, client *weaviate.Client) (*guardrailChecker, *guardrailRecordBox, *sentry.MockTransport) {
	t.Helper()
	box := &guardrailRecordBox{}
	checker := newGuardrailChecker(client, box, log.New(io.Discard, "", 0), "user-1", "conv-1")
	hub, transport := newGuardrailTestHub(t)
	checker.SetSentryHub(hub)
	return checker, box, transport
}

type guardrailUIRecorder struct {
	mu       sync.Mutex
	messages []any
}

func (r *guardrailUIRecorder) ServerMessage(data any, _ time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, data)
}

func (r *guardrailUIRecorder) snapshot() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]any, len(r.messages))
	copy(out, r.messages)
	return out
}

func TestGuardrailThresholdRouting(t *testing.T) {
	t.Setenv("ENVIRONMENT", "staging")
	tests := []struct {
		name       string
		similarity float64
		wantBand   string
		wantResult bool
	}{
		{"interrupt", guardrailInterruptThreshold + 0.09, "interrupt", true},
		{"offline judge", guardrailOfflineJudgeThreshold + 0.05, "offline_judge", false},
		{"below", guardrailOfflineJudgeThreshold - 0.15, "below", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, stub := newGuardrailStubClient(t, func(int) (int, any) {
				return http.StatusOK, guardrailGraphQLResponse(
					guardrailHit("anchor-1", "instruction-1", "Do not do this.", "Safety", tc.similarity),
				)
			})
			checker, box, _ := newGuardrailTestChecker(t, client)
			ui := &guardrailUIRecorder{}
			checker.SetUI(ui)

			if got := checker.Check(context.Background(), "raw fragment."); got != tc.wantResult {
				t.Fatalf("Check() = %v, want %v", got, tc.wantResult)
			}
			record := box.take()
			if record == nil || len(record.Checks) != 1 {
				t.Fatalf("record = %+v", record)
			}
			check := record.Checks[0]
			if check.Band != tc.wantBand || check.Violated != tc.wantResult || check.Status != "ok" {
				t.Fatalf("check = %+v", check)
			}
			if record.Interrupted != tc.wantResult {
				t.Errorf("record interrupted = %v, want %v", record.Interrupted, tc.wantResult)
			}
			if check.Similarity == nil || !guardrailFloatEqual(*check.Similarity, tc.similarity) {
				t.Errorf("similarity = %v, want %v", check.Similarity, tc.similarity)
			}
			messages := ui.snapshot()
			wantMessages := 0
			if tc.wantBand == "offline_judge" || tc.wantResult {
				wantMessages = 1
			}
			if len(messages) != wantMessages {
				t.Fatalf("RTVI messages = %v, want %d", messages, wantMessages)
			}
			if len(messages) == 1 {
				data := messages[0].(map[string]any)
				if data["type"] != "guardrail_check" || data["band"] != tc.wantBand || data["violated"] != tc.wantResult {
					t.Fatalf("RTVI data = %+v", data)
				}
			}

			query := stub.query(0)
			for _, want := range []string{
				guardrailAnchorClass,
				`concepts: ["raw fragment."]`,
				"answeredBy", guardrailInstructionClass, "isStaging",
				"limit: 10", "instructionText", "anchorText",
			} {
				if !strings.Contains(query, want) {
					t.Errorf("query missing %q:\n%s", want, query)
				}
			}
		})
	}
}

func TestGuardrailDedupeKeepsBestAnchorAndSelectsTopInstruction(t *testing.T) {
	lowerDuplicate := guardrailOfflineJudgeThreshold + 0.02
	bestDuplicate := guardrailInterruptThreshold + 0.07
	other := guardrailInterruptThreshold + 0.02
	client, _ := newGuardrailStubClient(t, func(int) (int, any) {
		return http.StatusOK, guardrailGraphQLResponse(
			guardrailHit("anchor-a-low", "instruction-a", "Instruction A", "A", lowerDuplicate),
			guardrailHit("anchor-b", "instruction-b", "Instruction B", "B", other),
			guardrailHit("anchor-a-best", "instruction-a", "Instruction A", "A", bestDuplicate),
		)
	})
	checker, box, _ := newGuardrailTestChecker(t, client)

	if !checker.Check(context.Background(), "fragment.") {
		t.Fatal("best deduped candidate should interrupt")
	}
	record := box.take()
	check := record.Checks[0]
	if len(check.Candidates) != 2 {
		t.Fatalf("deduped candidates = %+v", check.Candidates)
	}
	if check.Top == nil || check.Top.InstructionID != "instruction-a" || check.Top.AnchorID != "anchor-a-best" {
		t.Fatalf("top = %+v", check.Top)
	}
	if !guardrailFloatEqual(check.Top.Similarity, bestDuplicate) || !guardrailFloatEqual(check.Candidates[0].Similarity, bestDuplicate) {
		t.Fatalf("best similarity not retained: top=%+v candidates=%+v", check.Top, check.Candidates)
	}
}

func TestGuardrailTurnSimilarityIsMaximumAcrossSentences(t *testing.T) {
	first := guardrailOfflineJudgeThreshold + 0.01
	second := guardrailInterruptThreshold + 0.04
	responses := []float64{first, second}
	client, _ := newGuardrailStubClient(t, func(call int) (int, any) {
		return http.StatusOK, guardrailGraphQLResponse(
			guardrailHit("anchor", "instruction", "Instruction", "Title", responses[call]),
		)
	})
	checker, box, _ := newGuardrailTestChecker(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker.Check(ctx, "first.")
	checker.Check(ctx, "second.")
	record := box.take()
	if record.highestSimilarity == nil || !guardrailFloatEqual(*record.highestSimilarity, second) {
		t.Fatalf("highest similarity = %v, want %v", record.highestSimilarity, second)
	}
	if record.CheckCount != 2 || record.ChecksFired != 2 {
		t.Fatalf("counts = completed %d fired %d", record.CheckCount, record.ChecksFired)
	}
}

func guardrailFloatEqual(got, want float64) bool {
	return math.Abs(got-want) < 1e-9
}

func TestGuardrailQueryErrorFailsOpenAndCapturesSentry(t *testing.T) {
	client, _ := newGuardrailStubClient(t, func(int) (int, any) {
		return http.StatusInternalServerError, map[string]any{"error": "down"}
	})
	checker, box, transport := newGuardrailTestChecker(t, client)

	if checker.Check(context.Background(), "fragment.") {
		t.Fatal("query error must fail open")
	}
	record := box.take()
	if record.Status != "error" || record.Error == "" || record.CheckCount != 1 {
		t.Fatalf("record = %+v", record)
	}
	check := record.Checks[0]
	if check.Status != "error" || check.Band != "error" || check.Similarity != nil || check.Top != nil {
		t.Fatalf("errored check = %+v", check)
	}
	events := transport.Events()
	if len(events) != 1 {
		t.Fatalf("Sentry events = %d, want 1", len(events))
	}
	if events[0].Tags["operation"] != "guardrail_check" {
		t.Fatalf("Sentry tags = %v", events[0].Tags)
	}
}

func TestGuardrailContextCanceledErrorDoesNotCaptureSentry(t *testing.T) {
	checker := &guardrailChecker{userID: "user-1", conversationID: "conv-1", logger: log.New(io.Discard, "", 0)}
	hub, transport := newGuardrailTestHub(t)
	checker.SetSentryHub(hub)
	checker.reportFailure(
		context.Background(), errors.Join(errors.New("request failed"), context.Canceled), "fragment.", 1, 0,
	)
	if len(transport.Events()) != 0 {
		t.Fatalf("context.Canceled error emitted Sentry: %+v", transport.Events())
	}
}

func TestGuardrailCancelledQueryDoesNotCaptureSentryAndLeavesPlaceholder(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(started) })
		<-release
		_ = json.NewEncoder(w).Encode(guardrailGraphQLResponse(
			guardrailHit("anchor", "instruction", "Instruction", "Title", guardrailInterruptThreshold+0.05),
		))
	}))
	t.Cleanup(server.Close)
	client, err := weaviate.New(weaviate.Config{BaseURL: server.URL, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatalf("weaviate.New: %v", err)
	}
	checker, box, transport := newGuardrailTestChecker(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- checker.Check(ctx, "cancel me.") }()
	<-started
	cancel()
	close(release)
	if <-done {
		t.Fatal("cancelled check must fail open")
	}

	record := box.take()
	if record == nil || record.CheckCount != 0 || record.ChecksFired != 1 || len(record.Checks) != 1 {
		t.Fatalf("record = %+v", record)
	}
	check := record.Checks[0]
	if check.Status != "cancelled" || check.Similarity != nil || check.Top != nil {
		t.Fatalf("cancelled placeholder was overwritten: %+v", check)
	}
	if len(transport.Events()) != 0 {
		t.Fatalf("context cancellation emitted Sentry: %+v", transport.Events())
	}
}

func TestGuardrailFanoutSentryFiresOncePerTurn(t *testing.T) {
	below := guardrailOfflineJudgeThreshold - 0.15
	client, _ := newGuardrailStubClient(t, func(int) (int, any) {
		return http.StatusOK, guardrailGraphQLResponse(
			guardrailHit("anchor", "instruction", "Instruction", "Title", below),
		)
	})
	checker, _, transport := newGuardrailTestChecker(t, client)

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	for i := 0; i < guardrailFanoutSentryThreshold+3; i++ {
		checker.Check(ctx1, "sentence.")
	}
	if got := len(transport.Events()); got != 1 {
		t.Fatalf("first turn Sentry events = %d, want 1", got)
	}
	if transport.Events()[0].Tags["operation"] != "guardrail_check_fanout" {
		t.Fatalf("Sentry tags = %v", transport.Events()[0].Tags)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	for i := 0; i < guardrailFanoutSentryThreshold+1; i++ {
		checker.Check(ctx2, "another sentence.")
	}
	if got := len(transport.Events()); got != 2 {
		t.Fatalf("two turns Sentry events = %d, want 2", got)
	}
}

func TestGuardrailCorrectionAppendedLastWithoutMutatingCallerAndConsumedOnce(t *testing.T) {
	checker := &guardrailChecker{pendingCorrection: "Never promise a guaranteed outcome."}
	backing := make([]voicepipelinecore.Message, 2)
	backing[0] = voicepipelinecore.Message{Role: "system", Content: "base"}
	messages := backing[:1]

	out := checker.EnrichCorrection(context.Background(), messages)
	if len(out) != 2 || out[1].Role != "user" {
		t.Fatalf("enriched messages = %+v", out)
	}
	want := `<system_message>
Your response violates the following guardrail -
Never promise a guaranteed outcome.
please regenerate with correction
</system_message>`
	if out[1].Content != want {
		t.Fatalf("correction = %q, want %q", out[1].Content, want)
	}
	if len(messages) != 1 || !reflect.DeepEqual(backing[1], voicepipelinecore.Message{}) {
		t.Fatalf("caller slice/backing array was mutated: messages=%+v backing=%+v", messages, backing)
	}

	secondInput := []voicepipelinecore.Message{{Role: "system", Content: "next"}}
	second := checker.EnrichCorrection(context.Background(), secondInput)
	if len(second) != 1 || !reflect.DeepEqual(second[0], secondInput[0]) {
		t.Fatalf("correction was not consumed once: %+v", second)
	}
}

func TestComposeEnrichersNilHandlingAndOrder(t *testing.T) {
	if composeEnrichers(nil, nil) != nil {
		t.Fatal("all-nil composition should be nil")
	}
	var order []string
	first := voicepipelinecore.MessagesEnricher(func(_ context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
		order = append(order, "first")
		return append(messages, voicepipelinecore.Message{Role: "user", Content: "first"})
	})
	second := voicepipelinecore.MessagesEnricher(func(_ context.Context, messages []voicepipelinecore.Message) []voicepipelinecore.Message {
		order = append(order, "second")
		if messages[len(messages)-1].Content != "first" {
			t.Fatalf("second enricher did not receive first output: %+v", messages)
		}
		return append(messages, voicepipelinecore.Message{Role: "user", Content: "second"})
	})

	sole := composeEnrichers(nil, first)
	if out := sole(context.Background(), nil); len(out) != 1 || out[0].Content != "first" {
		t.Fatalf("sole enricher output = %+v", out)
	}
	order = nil
	composed := composeEnrichers(first, nil, second)
	out := composed(context.Background(), nil)
	if strings.Join(order, ",") != "first,second" {
		t.Fatalf("order = %v", order)
	}
	if len(out) != 2 || out[1].Content != "second" {
		t.Fatalf("composed output = %+v", out)
	}
}

func testGuardrailRecord() guardrailCheckRecord {
	similarity := guardrailInterruptThreshold + 0.04
	return guardrailCheckRecord{
		ConversationID: "conv-1",
		UserID:         "user-1",
		BotType:        FollowUpBotType,
		CheckedAt:      "2026-08-05T00:00:00Z",
		TurnText:       "checked sentence.",
		Thresholds: guardrailThresholds{
			Metric:       "cosine_similarity",
			Interrupt:    guardrailInterruptThreshold,
			OfflineJudge: guardrailOfflineJudgeThreshold,
		},
		Interrupted: true,
		CheckCount:  1,
		ChecksFired: 1,
		Checks: []guardrailSentenceCheck{{
			Index:      1,
			Fragment:   "checked sentence.",
			Similarity: &similarity,
			Band:       "interrupt",
			Violated:   true,
			Status:     "ok",
			LatencyMs:  guardrailCheckLatency{VectorQuery: 12, Total: 15},
			Top: &guardrailTopHit{
				InstructionID: "instruction-1", InstructionText: "Instruction", Similarity: similarity,
			},
			Candidates: []guardrailCandidate{{InstructionID: "instruction-1", Similarity: similarity}},
		}},
		Status:            "ok",
		highestSimilarity: &similarity,
		slowestTotalMs:    15,
	}
}

func TestGuardrailRecordJSONShape(t *testing.T) {
	record := testGuardrailRecord()
	record.ChunkID = "chunk-1"
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal guardrail record: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("unmarshal guardrail record: %v", err)
	}
	wantTopLevel := "bot_type,check_count,checked_at,checks,checks_fired,chunk_id,conversation_id,interrupted,status,thresholds,turn_text,user_id"
	if got := payloadKeys(body); got != wantTopLevel {
		t.Fatalf("top-level keys = %s, want %s", got, wantTopLevel)
	}
	thresholds := body["thresholds"].(map[string]any)
	if thresholds["metric"] != "cosine_similarity" ||
		thresholds["interrupt"] != guardrailInterruptThreshold ||
		thresholds["offline_judge"] != guardrailOfflineJudgeThreshold {
		t.Fatalf("thresholds = %+v", thresholds)
	}
	checks := body["checks"].([]any)
	check := checks[0].(map[string]any)
	wantCheck := "band,candidates,error,fragment,index,latency_ms,similarity,status,top,violated"
	if got := payloadKeys(check); got != wantCheck {
		t.Fatalf("sentence keys = %s, want %s", got, wantCheck)
	}
	wantTop := "anchor_id,anchor_text,document_version_path,instruction_id,instruction_text,similarity,title"
	if got := payloadKeys(check["top"].(map[string]any)); got != wantTop {
		t.Fatalf("top-hit keys = %s, want %s", got, wantTop)
	}
	wantCandidate := "anchor_id,instruction_id,similarity"
	if got := payloadKeys(check["candidates"].([]any)[0].(map[string]any)); got != wantCandidate {
		t.Fatalf("candidate keys = %s, want %s", got, wantCandidate)
	}

	cancelled := record.Checks[0]
	cancelled.Status = "cancelled"
	cancelled.Similarity = nil
	cancelled.Top = nil
	cancelled.Candidates = []guardrailCandidate{}
	cancelledJSON, err := json.Marshal(cancelled)
	if err != nil {
		t.Fatalf("marshal cancelled check: %v", err)
	}
	if strings.Contains(string(cancelledJSON), `"similarity"`) || strings.Contains(string(cancelledJSON), `"top"`) {
		t.Fatalf("cancelled check fabricated a score/top: %s", cancelledJSON)
	}
}

func TestRetrievalDecoratorMergesGuardrailBesideProtocol(t *testing.T) {
	protocolBox := &protocolRecordBox{}
	guardrailBox := &guardrailRecordBox{}
	protocolBox.put(testProtocolRetrievalRecord())
	guardrailBox.put(testGuardrailRecord())
	uploader := &stubJSONUploader{}
	decorate := newRetrievalChunkDecorator(
		protocolBox, guardrailBox, uploader, log.New(io.Discard, "", 0),
		"user-1", "conv-1", FollowUpBotType,
	)
	chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	decorate(chunk)

	if chunk.ChunkRetrievalMetrics.protocolOrNil() == nil || chunk.ChunkRetrievalMetrics.guardrailOrNil() == nil {
		t.Fatalf("merged metrics = %+v", chunk.ChunkRetrievalMetrics)
	}
	guardrail := chunk.ChunkRetrievalMetrics.Guardrail
	if guardrail.SimilarityScore == nil || *guardrail.SimilarityScore != guardrailInterruptThreshold+0.04 {
		t.Fatalf("guardrail metrics = %+v", guardrail)
	}
	if guardrail.RawDataS3Key != "guardrail_check/conv-1/chunk-1.json" {
		t.Fatalf("guardrail key = %q", guardrail.RawDataS3Key)
	}
	keys, payload := uploader.uploaded()
	if len(keys) != 2 || keys[0] != "protocol_retrieval/conv-1/chunk-1.json" || keys[1] != guardrail.RawDataS3Key {
		t.Fatalf("upload keys = %v", keys)
	}
	record, ok := payload.(guardrailCheckRecord)
	if !ok || record.ChunkID != "chunk-1" {
		t.Fatalf("guardrail payload = %#v", payload)
	}
}

func TestRetrievalDecoratorNilGuardrailBoxIsNoOp(t *testing.T) {
	protocolBox := &protocolRecordBox{}
	protocolBox.put(testProtocolRetrievalRecord())
	decorate := newRetrievalChunkDecorator(
		protocolBox, nil, &stubJSONUploader{}, log.New(io.Discard, "", 0),
		"user-1", "conv-1", FollowUpBotType,
	)
	chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	decorate(chunk)
	if chunk.ChunkRetrievalMetrics.protocolOrNil() == nil || chunk.ChunkRetrievalMetrics.guardrailOrNil() != nil {
		t.Fatalf("metrics = %+v", chunk.ChunkRetrievalMetrics)
	}
}

func TestRetrievalDecoratorToolPairConsumesNeitherBox(t *testing.T) {
	protocolBox := &protocolRecordBox{}
	guardrailBox := &guardrailRecordBox{}
	protocolBox.put(testProtocolRetrievalRecord())
	guardrailBox.put(testGuardrailRecord())
	decorate := newRetrievalChunkDecorator(
		protocolBox, guardrailBox, &stubJSONUploader{}, log.New(io.Discard, "", 0),
		"user-1", "conv-1", FollowUpBotType,
	)
	decorate(&ConversationChunk{
		ID: "tool-pair", Role: "assistant", AdditionalData: map[string]any{"tool_calls": []any{}},
	})
	if protocolBox.take() == nil || guardrailBox.take() == nil {
		t.Fatal("tool-pair chunk consumed a pending record")
	}
}

func TestRetrievalDecoratorGuardrailUploadFailureKeepsMetrics(t *testing.T) {
	guardrailBox := &guardrailRecordBox{}
	guardrailBox.put(testGuardrailRecord())
	decorate := newRetrievalChunkDecorator(
		nil, guardrailBox, &stubJSONUploader{err: errors.New("s3 down")}, log.New(io.Discard, "", 0),
		"user-1", "conv-1", FollowUpBotType,
	)
	chunk := &ConversationChunk{ID: "chunk-1", Role: "assistant"}
	decorate(chunk)
	if chunk.ChunkRetrievalMetrics.guardrailOrNil() == nil {
		t.Fatal("guardrail metrics should survive upload failure")
	}
	if chunk.ChunkRetrievalMetrics.Guardrail.RawDataS3Key != "" {
		t.Fatalf("raw key = %q, want empty", chunk.ChunkRetrievalMetrics.Guardrail.RawDataS3Key)
	}
}

func TestRetrievalDecoratorLogsGuardrailCountMismatch(t *testing.T) {
	guardrailBox := &guardrailRecordBox{}
	record := testGuardrailRecord()
	record.ChecksFired = record.CheckCount + 1
	guardrailBox.put(record)
	var logs bytes.Buffer
	decorate := newRetrievalChunkDecorator(
		nil, guardrailBox, &stubJSONUploader{}, log.New(&logs, "", 0),
		"user-1", "conv-1", FollowUpBotType,
	)
	decorate(&ConversationChunk{ID: "chunk-1", Role: "assistant"})
	if !strings.Contains(logs.String(), "checks_fired=2 check_count=1") {
		t.Fatalf("missing count mismatch log: %s", logs.String())
	}
}

func (m *ChunkRetrievalMetrics) guardrailOrNil() *GuardrailCheckMetrics {
	if m == nil {
		return nil
	}
	return m.Guardrail
}

func TestGuardrailNewTurnContextResetsRecordAndCounters(t *testing.T) {
	below := guardrailOfflineJudgeThreshold - 0.15
	client, _ := newGuardrailStubClient(t, func(int) (int, any) {
		return http.StatusOK, guardrailGraphQLResponse(
			guardrailHit("anchor", "instruction", "Instruction", "Title", below),
		)
	})
	checker, box, _ := newGuardrailTestChecker(t, client)
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	checker.Check(ctx1, "old turn.")
	checker.Check(ctx2, "new turn.")
	record := box.take()
	if record.ChecksFired != 1 || record.CheckCount != 1 || len(record.Checks) != 1 {
		t.Fatalf("new-turn counters were not reset: %+v", record)
	}
	if record.Checks[0].Index != 1 || record.TurnText != "new turn." {
		t.Fatalf("new-turn record retained old state: %+v", record)
	}
}

func TestSetupGuardrailCheckGating(t *testing.T) {
	tests := []struct {
		name string
		flag string
		want bool
	}{
		{"enabled", "1", true},
		{"enabled with whitespace", " 1 ", true},
		{"disabled", "0", false},
		{"unset", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(protocolRetrievalEnabledEnv, "")
			t.Setenv(guardrailCheckEnabledEnv, tc.flag)
			t.Setenv("WEAVIATE_URL", "http://weaviate.staging.svc.cluster.local:8080")
			t.Setenv("WEAVIATE_API_KEY", "key")
			t.Setenv("AWS_US_BUCKET_NAME", "")
			t.Setenv("AWS_US_REGION", "")

			pl := newGuardrailSetupPlan()
			setupFollowUpRetrieval(pl, nil)
			if got := pl.GuardrailChecker != nil; got != tc.want {
				t.Errorf("checker present = %v, want %v", got, tc.want)
			}
			if got := pl.Callbacks.chunkDecorator != nil; got != tc.want {
				t.Errorf("chunk decorator present = %v, want %v", got, tc.want)
			}
			if pl.ProtocolEnricher != nil {
				t.Error("guardrail-only setup unexpectedly built protocol retrieval")
			}
		})
	}
}

func TestFollowUpRetrievalFeaturesShareWeaviateClient(t *testing.T) {
	t.Setenv(protocolRetrievalEnabledEnv, "1")
	t.Setenv(guardrailCheckEnabledEnv, "1")
	t.Setenv("WEAVIATE_URL", "http://weaviate.staging.svc.cluster.local:8080")
	t.Setenv("WEAVIATE_API_KEY", "key")
	t.Setenv("AWS_US_BUCKET_NAME", "")
	t.Setenv("AWS_US_REGION", "")

	pl := newGuardrailSetupPlan()
	setupFollowUpRetrieval(pl, nil)
	if pl.ProtocolEnricher == nil || pl.GuardrailChecker == nil {
		t.Fatalf("features not built: protocol=%v guardrail=%v", pl.ProtocolEnricher, pl.GuardrailChecker)
	}
	if pl.ProtocolEnricher.client != pl.GuardrailChecker.client {
		t.Fatal("protocol and guardrail built separate Weaviate clients")
	}
	if pl.Callbacks.chunkDecorator == nil {
		t.Fatal("combined retrieval decorator was not registered")
	}
}

func newGuardrailSetupPlan() *followUpPlan {
	return &followUpPlan{
		Startup: CallStartup{
			Logger:         log.New(io.Discard, "", 0),
			UserID:         "user-1",
			ConversationID: "conv-1",
		},
		PromptMetadata:  map[string]any{},
		PromptVariables: DocumentVariables{},
		Callbacks:       &CallEventCallbacks{},
	}
}
