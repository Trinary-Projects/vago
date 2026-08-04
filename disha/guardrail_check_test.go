package disha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/internal/weaviate"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

// ----------------------------------------------------------------- fixtures

// guardrailAnchorResponseTemplate/guardrailAnchorHit mirror
// anchorResponseTemplate/anchorHit from protocol_retrieval_test.go, adjusted
// for GuardrailAnchor -> GuardrailInstruction (no turnsThresholdCount: that
// property does not exist on GuardrailInstruction).
const guardrailAnchorResponseTemplate = `{"data":{"Get":{"GuardrailAnchor":[%s]}}}`

func guardrailAnchorHit(anchorID, instructionID, instructionText string, distance float64) string {
	return fmt.Sprintf(`{"anchorText":%q,
      "answeredBy":[{"instructionText":%q,"title":"t-%s","documentVersionPath":"p/v/1","_additional":{"id":%q}}],
      "_additional":{"id":%q,"distance":%v}}`,
		"anchor for "+instructionID, instructionText, instructionID, instructionID, anchorID, distance)
}

func distanceFor(similarity float64) float64 { return 1 - similarity }

// newGuardrailTestHub builds a Hub with its own MockTransport, mirroring the
// pattern used in onboarding_stage_tracker_test.go (sentryutil's own
// equivalent helper is unexported to that package).
func newGuardrailTestHub(t *testing.T) (*sentry.Hub, *sentry.MockTransport) {
	t.Helper()
	transport := &sentry.MockTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Transport: transport})
	if err != nil {
		t.Fatalf("sentry.NewClient: %v", err)
	}
	return sentry.NewHub(client, sentry.NewScope()), transport
}

// newTestGuardrailDocs seeds document:{guardrailJudgePromptName}:production
// in a fresh miniredis instance and returns a *DocumentStore backed by the
// simple substitution renderer already defined in document_store_test.go
// (same package).
func newTestGuardrailDocs(t *testing.T, promptText string) *DocumentStore {
	t.Helper()
	t.Setenv("ENVIRONMENT", "prod")
	redisServer := miniredis.RunT(t)
	redisClient := NewRedisClient(redisServer.Addr(), "", 0, log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = redisClient.Close() })

	doc := DocumentVersion{ID: "doc-guardrail-judge", PromptText: promptText, Version: 1}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	redisServer.Set(fmt.Sprintf("document:%s:production", guardrailJudgePromptName), string(raw))
	return newDocumentStore(redisClient, log.New(io.Discard, "", 0), simpleTemplateRenderer{})
}

// newGuardrailDocsWithNoPrompt returns a configured DocumentStore backed by
// an empty miniredis instance, so GetDocument fails with "not in redis key".
func newGuardrailDocsWithNoPrompt(t *testing.T) *DocumentStore {
	t.Helper()
	t.Setenv("ENVIRONMENT", "prod")
	redisServer := miniredis.RunT(t)
	redisClient := NewRedisClient(redisServer.Addr(), "", 0, log.New(io.Discard, "", 0))
	t.Cleanup(func() { _ = redisClient.Close() })
	return newDocumentStore(redisClient, log.New(io.Discard, "", 0), simpleTemplateRenderer{})
}

func newTestGuardrailChecker(
	t *testing.T,
	client *weaviate.Client,
	factory guardrailJudgeClientFactory,
	docs *DocumentStore,
) (*guardrailChecker, *guardrailRecordBox, *sentry.Hub, *sentry.MockTransport) {
	t.Helper()
	box := &guardrailRecordBox{}
	hub, transport := newGuardrailTestHub(t)
	checker := newGuardrailChecker(
		context.Background(),
		client, docs, box,
		log.New(io.Discard, "", 0),
		factory,
		"user-1", "conv-1",
	)
	checker.SetSentryHub(hub)
	return checker, box, hub, transport
}

// stubGuardrailLLMClient is a minimal voicepipelinecore.LLMClient stub for
// the judge call: it either errors, or emits `output` as a single token and
// reports `model`, optionally honouring a delay and ctx cancellation.
type stubGuardrailLLMClient struct {
	output string
	err    error
	delay  time.Duration
	model  string
}

func (s *stubGuardrailLLMClient) Stream(ctx context.Context, _ voicepipelinecore.LLMRequest, onToken func(string)) (voicepipelinecore.LLMResult, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return voicepipelinecore.LLMResult{}, ctx.Err()
		}
	}
	if s.err != nil {
		return voicepipelinecore.LLMResult{}, s.err
	}
	if onToken != nil && s.output != "" {
		onToken(s.output)
	}
	return voicepipelinecore.LLMResult{Model: s.model}, nil
}

func stubJudgeFactory(client voicepipelinecore.LLMClient, err error) guardrailJudgeClientFactory {
	return func(map[string]any) (voicepipelinecore.LLMClient, error) {
		return client, err
	}
}

// notifyingLLMClient wraps another client and signals doneCh right after
// Stream returns, so tests can deterministically wait for a fire-and-forget
// audit-judge goroutine to finish without sleep-polling.
type notifyingLLMClient struct {
	inner voicepipelinecore.LLMClient
	done  chan struct{}
}

func (n *notifyingLLMClient) Stream(ctx context.Context, req voicepipelinecore.LLMRequest, onToken func(string)) (voicepipelinecore.LLMResult, error) {
	res, err := n.inner.Stream(ctx, req, onToken)
	n.done <- struct{}{}
	return res, err
}

// -------------------------------------------------------------- band routing

func TestGuardrailCheckInterruptBandFiresWithoutBlockingOnJudge(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-1", "do not diagnose", distanceFor(0.94)))
	client := newStubWeaviate(t, body, nil)

	var judgeCalls int32
	factory := func(map[string]any) (voicepipelinecore.LLMClient, error) {
		atomic.AddInt32(&judgeCalls, 1)
		return &stubGuardrailLLMClient{output: `{"violated":false}`}, nil
	}
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	violated := checker.Check(context.Background(), "You should stop taking your medication.")
	if !violated {
		t.Fatal("expected violated=true above the interrupt threshold, on similarity alone")
	}

	// The interrupt-band record is deliberately HELD until the audit verdict
	// lands, so the partial chunk committed by this very interrupt cannot
	// consume it. Expire the hold to inspect the record here.
	if held := box.take(); held != nil {
		t.Fatalf("interrupt-band record must be held for the audit verdict, got %+v", held)
	}
	box.mu.Lock()
	box.auditDeadline = time.Now().Add(-time.Millisecond)
	box.mu.Unlock()

	record := box.take()
	if record == nil || !record.Interrupted {
		t.Fatalf("expected an interrupted record, got %+v", record)
	}
	selected := record.Checks[record.SelectedIndex]
	if selected.Band != "interrupt" {
		t.Fatalf("band = %q, want interrupt", selected.Band)
	}
	// The blocking path itself must not have called the judge: only the
	// fire-and-forget audit does, asynchronously.
	if got := atomic.LoadInt32(&judgeCalls); got != 0 {
		t.Fatalf("judge calls at Check() return = %d, want 0 (audit is fire-and-forget)", got)
	}
}

func TestGuardrailCheckJudgeBandViolatedInterrupts(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-2", "borderline instruction", distanceFor(0.83)))
	client := newStubWeaviate(t, body, nil)
	docs := newTestGuardrailDocs(t, "judge instruction={{guardrail}} fragment={{fragment}}")
	factory := stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":true}`, model: "judge-model"}, nil)
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	violated := checker.Check(context.Background(), "some fragment")
	if !violated {
		t.Fatal("expected the judge's violated=true verdict to interrupt")
	}

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	selected := record.Checks[record.SelectedIndex]
	if selected.Band != "judge" {
		t.Fatalf("band = %q, want judge", selected.Band)
	}
	if selected.Judge.Verdict != "yes" || !selected.Judge.Ran || selected.Judge.AuditOnly {
		t.Fatalf("judge detail = %+v, want Ran=true AuditOnly=false Verdict=yes", selected.Judge)
	}
}

func TestGuardrailCheckJudgeBandNotViolatedContinues(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-3", "borderline instruction", distanceFor(0.83)))
	client := newStubWeaviate(t, body, nil)
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")
	factory := stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":false}`}, nil)
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	violated := checker.Check(context.Background(), "some fragment")
	if violated {
		t.Fatal("expected the judge's violated=false verdict to continue the turn")
	}
	record := box.take()
	if record == nil || record.Interrupted {
		t.Fatalf("expected a non-interrupted record, got %+v", record)
	}
}

func TestGuardrailCheckBelowThresholdNeverCallsJudge(t *testing.T) {
	// Derived from the constant, not hardcoded: the judge threshold is a knob
	// that moves (it was 0.75, then 0.70, now 0.55 for staging testing), and a
	// literal here silently becomes a judge-band value the moment it drops.
	body := fmt.Sprintf(guardrailAnchorResponseTemplate,
		guardrailAnchorHit("a1", "instr-4", "unrelated instruction", distanceFor(guardrailJudgeThreshold-0.05)))
	client := newStubWeaviate(t, body, nil)
	factory := func(map[string]any) (voicepipelinecore.LLMClient, error) {
		t.Fatal("judge must not be called below the judge threshold")
		return nil, nil
	}
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, nil)

	violated := checker.Check(context.Background(), "totally unrelated fragment")
	if violated {
		t.Fatal("expected violated=false below the judge threshold")
	}
	record := box.take()
	if record == nil {
		t.Fatal("expected a record even for a below-threshold check")
	}
	if record.Checks[record.SelectedIndex].Band != "below" {
		t.Fatalf("band = %q, want below", record.Checks[record.SelectedIndex].Band)
	}
}

// -------------------------------------------------------------- boundaries

func TestGuardrailCheckBoundaryAtInterruptThresholdGoesToJudgeBand(t *testing.T) {
	// Exactly 0.85: "> 0.85" is false, so this must land in the judge band
	// (0.85 >= 0.70), NOT interrupt on similarity alone.
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-boundary-90", "instruction", distanceFor(guardrailInterruptThreshold)))
	client := newStubWeaviate(t, body, nil)
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")
	var judgeCalls int32
	factory := func(map[string]any) (voicepipelinecore.LLMClient, error) {
		atomic.AddInt32(&judgeCalls, 1)
		return &stubGuardrailLLMClient{output: `{"violated":false}`}, nil
	}
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	checker.Check(context.Background(), "fragment")
	record := box.take()
	if record == nil || record.Checks[record.SelectedIndex].Band != "judge" {
		t.Fatalf("band at exactly 0.85 = %+v, want judge", record)
	}
	// Only the blocking judge call, no separate audit at this band.
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&judgeCalls); got != 1 {
		t.Fatalf("judge calls = %d, want exactly 1", got)
	}
}

func TestGuardrailCheckBoundaryAtJudgeThresholdGoesToJudgeBand(t *testing.T) {
	// Exactly 0.70: ">= 0.70" is true, so this must be judged, not "below".
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-boundary-75", "instruction", distanceFor(guardrailJudgeThreshold)))
	client := newStubWeaviate(t, body, nil)
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")
	factory := stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":false}`}, nil)
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	checker.Check(context.Background(), "fragment")
	record := box.take()
	if record == nil || record.Checks[record.SelectedIndex].Band != "judge" {
		t.Fatalf("band at exactly 0.70 = %+v, want judge", record)
	}
}

// -------------------------------------------------------------- dedupe/top-1

func TestQueryGuardrailsDedupeKeepsBestSimilarityAndTopOne(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, strings.Join([]string{
		guardrailAnchorHit("a1", "instr-A", "text A", distanceFor(0.60)),
		guardrailAnchorHit("a2", "instr-A", "text A", distanceFor(0.85)), // better score, same instruction
		guardrailAnchorHit("a3", "instr-B", "text B", distanceFor(0.50)),
	}, ","))
	result, err := queryGuardrails(context.Background(), newStubWeaviate(t, body, nil), "q")
	if err != nil {
		t.Fatalf("queryGuardrails: %v", err)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("candidates = %d, want 2 distinct instructions: %+v", len(result.Candidates), result.Candidates)
	}
	if result.Top == nil || result.Top.InstructionID != "instr-A" || result.Top.AnchorID != "a2" {
		t.Fatalf("top = %+v, want instr-A's better anchor a2 (0.85)", result.Top)
	}
	if result.TopInstructionText != "text A" {
		t.Fatalf("top instruction text = %q, want %q", result.TopInstructionText, "text A")
	}
}

func TestQueryGuardrailsSkipsUnusableHits(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, strings.Join([]string{
		// No distance.
		`{"anchorText":"x","answeredBy":[{"instructionText":"t","_additional":{"id":"instr-x"}}],"_additional":{"id":"anchor-x"}}`,
		// No answeredBy cross-reference.
		`{"anchorText":"y","answeredBy":[],"_additional":{"id":"anchor-y","distance":0.1}}`,
		// Empty instructionText.
		`{"anchorText":"z","answeredBy":[{"instructionText":"","_additional":{"id":"instr-z"}}],"_additional":{"id":"anchor-z","distance":0.1}}`,
		// The one usable hit.
		guardrailAnchorHit("a-good", "instr-good", "usable text", distanceFor(0.80)),
	}, ","))
	result, err := queryGuardrails(context.Background(), newStubWeaviate(t, body, nil), "q")
	if err != nil {
		t.Fatalf("queryGuardrails: %v", err)
	}
	if len(result.Candidates) != 1 || result.Top == nil || result.Top.InstructionID != "instr-good" {
		t.Fatalf("expected only the usable hit to survive, got %+v", result)
	}
}

// -------------------------------------------------------------- judge failures

func TestGuardrailRunJudgeFailOpenPathsCaptureOneSentryEach(t *testing.T) {
	workingDocs := newTestGuardrailDocs(t, "judge prompt {{fragment}}")

	tests := []struct {
		name    string
		docs    *DocumentStore
		factory guardrailJudgeClientFactory
	}{
		{
			name:    "missing document store",
			docs:    nil,
			factory: stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":true}`}, nil),
		},
		{
			name:    "missing prompt document",
			docs:    newGuardrailDocsWithNoPrompt(t),
			factory: stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":true}`}, nil),
		},
		{
			name:    "client factory error",
			docs:    workingDocs,
			factory: stubJudgeFactory(nil, errors.New("build boom")),
		},
		{
			name:    "stream error",
			docs:    workingDocs,
			factory: stubJudgeFactory(&stubGuardrailLLMClient{err: errors.New("stream boom")}, nil),
		},
		{
			name:    "empty output",
			docs:    workingDocs,
			factory: stubJudgeFactory(&stubGuardrailLLMClient{output: ""}, nil),
		},
		{
			name:    "malformed output",
			docs:    workingDocs,
			factory: stubJudgeFactory(&stubGuardrailLLMClient{output: "not json"}, nil),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checker, _, _, transport := newTestGuardrailChecker(t, nil, tc.factory, tc.docs)
			ctx := context.Background()
			violated, detail := checker.runJudge(ctx, ctx, "instruction text", "fragment", false)
			if violated {
				t.Fatal("expected fail-open violated=false")
			}
			if detail.Error == "" {
				t.Fatal("expected detail.Error to be set")
			}
			if got := len(transport.Events()); got != 1 {
				t.Fatalf("Sentry events = %d, want exactly 1", got)
			}
		})
	}
}

func TestGuardrailRunJudgeTimeoutWithLiveOuterCtxStillCapturesSentry(t *testing.T) {
	docs := newTestGuardrailDocs(t, "judge prompt")
	factory := stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":true}`, delay: 50 * time.Millisecond}, nil)
	checker, _, _, transport := newTestGuardrailChecker(t, nil, factory, docs)

	judgeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	reportCtx := context.Background() // outer/report ctx stays alive

	violated, detail := checker.runJudge(judgeCtx, reportCtx, "instr", "fragment", false)
	if violated {
		t.Fatal("expected violated=false on timeout")
	}
	if detail.Error == "" {
		t.Fatal("expected an error recorded")
	}
	if got := len(transport.Events()); got != 1 {
		t.Fatalf("Sentry events = %d, want 1 (a judgeCtx timeout with a live reportCtx must still Sentry)", got)
	}
}

func TestGuardrailRunJudgeContextCanceledDoesNotCaptureSentry(t *testing.T) {
	docs := newTestGuardrailDocs(t, "judge prompt")
	// Warm the document cache with a normal call first so the cancelled-ctx
	// call below never touches Redis and only exercises Stream's ctx check.
	if _, _, err := docs.GetDocument(context.Background(), guardrailJudgePromptName, 0, DocumentVariables{}); err != nil {
		t.Fatalf("warm document cache: %v", err)
	}

	factory := stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":true}`, delay: 50 * time.Millisecond}, nil)
	checker, _, _, transport := newTestGuardrailChecker(t, nil, factory, docs)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	violated, _ := checker.runJudge(cancelledCtx, cancelledCtx, "instr", "fragment", false)
	if violated {
		t.Fatal("expected violated=false")
	}
	if got := len(transport.Events()); got != 0 {
		t.Fatalf("Sentry events = %d, want 0 for an already-cancelled ctx", got)
	}
}

// -------------------------------------------------------------- fan-out

func TestGuardrailCheckFanoutSentryFiresOncePastThreshold(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-low", "text", distanceFor(0.50)))
	client := newStubWeaviate(t, body, nil)
	checker, _, _, transport := newTestGuardrailChecker(t, client, nil, nil)

	for i := 0; i < guardrailFanoutSentryThreshold; i++ {
		checker.Check(context.Background(), fmt.Sprintf("fragment %d", i))
	}
	if got := len(transport.Events()); got != 0 {
		t.Fatalf("Sentry events at threshold (%d checks) = %d, want 0", guardrailFanoutSentryThreshold, got)
	}

	checker.Check(context.Background(), "fragment over threshold")
	if got := len(transport.Events()); got != 1 {
		t.Fatalf("Sentry events after exceeding threshold = %d, want 1", got)
	}

	for i := 0; i < 3; i++ {
		checker.Check(context.Background(), "more")
	}
	if got := len(transport.Events()); got != 1 {
		t.Fatalf("Sentry events after further checks in the same turn = %d, want still 1", got)
	}
}

// -------------------------------------------------------------- audit judge

func TestGuardrailAuditJudgeRunsOnlyAboveInterruptThreshold(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-judge-band", "instruction", distanceFor(0.83)))
	client := newStubWeaviate(t, body, nil)
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")
	var judgeCalls int32
	factory := func(map[string]any) (voicepipelinecore.LLMClient, error) {
		atomic.AddInt32(&judgeCalls, 1)
		return &stubGuardrailLLMClient{output: `{"violated":false}`}, nil
	}
	checker, _, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	checker.Check(context.Background(), "fragment")
	time.Sleep(20 * time.Millisecond) // would-be audit goroutine's window
	if got := atomic.LoadInt32(&judgeCalls); got != 1 {
		t.Fatalf("judge calls = %d, want exactly 1 (the blocking judge-band call, no audit)", got)
	}
}

func TestGuardrailAuditJudgeRecordsVerdictOnPendingRecord(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-audit", "instruction", distanceFor(0.94)))
	client := newStubWeaviate(t, body, nil)
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")

	done := make(chan struct{}, 1)
	factory := func(map[string]any) (voicepipelinecore.LLMClient, error) {
		return &notifyingLLMClient{inner: &stubGuardrailLLMClient{output: `{"violated":false}`}, done: done}, nil
	}
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	violated := checker.Check(context.Background(), "fragment")
	if !violated {
		t.Fatal("expected the >0.85 band to interrupt")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("audit judge never completed")
	}
	time.Sleep(10 * time.Millisecond) // setAuditVerdict runs synchronously right after

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	selected := record.Checks[record.SelectedIndex]
	if !selected.Judge.Ran || !selected.Judge.AuditOnly || selected.Judge.Verdict != "no" {
		t.Fatalf("judge detail = %+v, want Ran=true AuditOnly=true Verdict=no", selected.Judge)
	}
}

// syncLogBuffer is a mutex-guarded io.Writer that closes done the first
// time a write leaves the target substring in its accumulated content. It
// exists so tests can wait deterministically for a specific background log
// line instead of racing a plain strings.Builder against the goroutine
// still writing to it (log.Logger serializes its own writes, but a test
// reading the buffer concurrently is a real data race under -race).
type syncLogBuffer struct {
	mu   sync.Mutex
	buf  strings.Builder
	want string
	done chan struct{}
	once sync.Once
}

func newSyncLogBuffer(want string) *syncLogBuffer {
	return &syncLogBuffer{want: want, done: make(chan struct{})}
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	n, err := b.buf.Write(p)
	matched := strings.Contains(b.buf.String(), b.want)
	b.mu.Unlock()
	if matched {
		b.once.Do(func() { close(b.done) })
	}
	return n, err
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestGuardrailAuditVerdictArrivingAfterTakeIsDroppedWithoutPanic(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-late", "instruction", distanceFor(0.94)))
	client := newStubWeaviate(t, body, nil)
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")

	logBuf := newSyncLogBuffer("arrived after the record was taken")
	// Delayed so the main goroutine's box.take() below is guaranteed to run
	// well before the audit goroutine's judge call resolves, deterministically
	// forcing the "arrived too late" path instead of racing goroutine
	// scheduling against an instant stub response.
	factory := stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":false}`, delay: 50 * time.Millisecond}, nil)
	box := &guardrailRecordBox{}
	hub, _ := newGuardrailTestHub(t)
	checker := newGuardrailChecker(
		context.Background(), client, docs, box,
		log.New(logBuf, "", 0), factory,
		"user-1", "conv-1",
	)
	checker.SetSentryHub(hub)

	violated := checker.Check(context.Background(), "fragment")
	if !violated {
		t.Fatal("expected the >0.85 band to interrupt")
	}

	// The record is held for the audit verdict, so reaching the "arrived too
	// late" path now requires the hold to expire first — which is exactly the
	// real scenario it guards: the audit judge overran
	// guardrailAuditVerdictWait, the record was released without a verdict, and
	// the verdict turns up afterwards with nowhere to go.
	box.mu.Lock()
	box.auditDeadline = time.Now().Add(-time.Millisecond)
	box.mu.Unlock()

	if record := box.take(); record == nil {
		t.Fatal("expected a record once the audit hold expired")
	}

	select {
	case <-logBuf.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("expected the late-arrival log line, got: %s", logBuf.String())
	}
}

// -------------------------------------------------------------- the enricher

// A failing check must reach the chunk as status "error" with the message,
// not as a clean "ok". Regression guard: the record's status was previously
// hardcoded to "ok", so a Weaviate outage would have produced chunks claiming
// every guardrail check ran fine for the whole call -- the one thing this
// field must never do.
func TestGuardrailCheckQueryFailurePropagatesErrorStatusToRecord(t *testing.T) {
	client := newStubWeaviate(t, `{"errors":[{"message":"weaviate exploded"}]}`, nil)
	factory := stubJudgeFactory(&stubGuardrailLLMClient{output: `{"violated":true}`}, nil)
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	if violated := checker.Check(context.Background(), "some fragment"); violated {
		t.Fatal("a failed query must fail open to not-violated")
	}

	record := box.take()
	if record == nil {
		t.Fatal("expected a record even for a failed check")
	}
	if record.Status != "error" {
		t.Fatalf("record.Status = %q, want %q", record.Status, "error")
	}
	if record.Err == "" {
		t.Fatal("record.Err must carry the failure message")
	}
	// And the failed check must not masquerade as a below-threshold sample,
	// which would pollute the S3 calibration dataset with a non-observation.
	if band := record.Checks[record.SelectedIndex].Band; band != "error" {
		t.Fatalf("band = %q, want %q", band, "error")
	}
}

// The judge failing is a failure of the check even though it fails open to
// not-violated, so it must surface as status "error" rather than "ok".
func TestGuardrailCheckJudgeFailurePropagatesErrorStatusToRecord(t *testing.T) {
	body := fmt.Sprintf(guardrailAnchorResponseTemplate, guardrailAnchorHit("a1", "instr-9", "borderline", distanceFor(0.83)))
	client := newStubWeaviate(t, body, nil)
	factory := stubJudgeFactory(nil, errors.New("judge unavailable"))
	docs := newTestGuardrailDocs(t, "judge {{fragment}}")
	checker, box, _, _ := newTestGuardrailChecker(t, client, factory, docs)

	if violated := checker.Check(context.Background(), "borderline fragment"); violated {
		t.Fatal("a failed judge must fail open to not-violated")
	}

	record := box.take()
	if record == nil {
		t.Fatal("expected a record")
	}
	if record.Status != "error" {
		t.Fatalf("record.Status = %q, want %q", record.Status, "error")
	}
	if record.Err == "" {
		t.Fatal("record.Err must carry the judge failure message")
	}
}

func TestGuardrailEnrichNoOpWithoutPendingCorrection(t *testing.T) {
	checker, _, _, _ := newTestGuardrailChecker(t, nil, nil, nil)
	messages := []voicepipelinecore.Message{msg("system", "sys"), msg("user", "hi")}

	out := checker.Enrich(context.Background(), messages)

	if len(out) != len(messages) {
		t.Fatalf("expected unchanged messages, got %d want %d", len(out), len(messages))
	}
	for i := range messages {
		if out[i].Role != messages[i].Role || out[i].Content != messages[i].Content {
			t.Fatalf("message %d changed: got %+v want %+v", i, out[i], messages[i])
		}
	}
}

func TestGuardrailEnrichAppendsBlockLastExactlyOnce(t *testing.T) {
	checker, _, _, _ := newTestGuardrailChecker(t, nil, nil, nil)
	checker.pendingCorrection = "do not diagnose"
	messages := []voicepipelinecore.Message{msg("system", "sys"), msg("assistant", "reply"), msg("user", "hi")}
	originalLen := len(messages)

	out := checker.Enrich(context.Background(), messages)

	if len(out) != originalLen+1 {
		t.Fatalf("expected exactly one appended message, got %d", len(out))
	}
	last := out[len(out)-1]
	if last.Role != "user" {
		t.Fatalf("block role = %q, want user", last.Role)
	}
	want := "<system_message>\nYour response violates the following guardrail -\ndo not diagnose\nplease regenerate with correction\n</system_message>"
	if last.Content != want {
		t.Fatalf("block content = %q, want %q", last.Content, want)
	}
	if len(messages) != originalLen {
		t.Fatalf("input slice was mutated: len=%d want %d", len(messages), originalLen)
	}

	// The correction is one-shot: cleared by the Enrich call that consumed
	// it, so a second call with nothing pending must not re-append it.
	out2 := checker.Enrich(context.Background(), messages)
	if len(out2) != originalLen {
		t.Fatalf("expected the correction to be consumed exactly once, got %d messages", len(out2))
	}
}

func TestGuardrailEnrichResetsPerTurnState(t *testing.T) {
	checker, _, _, _ := newTestGuardrailChecker(t, nil, nil, nil)
	checker.turnText = "leftover from a previous turn"
	checker.checkCount = 5
	checker.checks = []guardrailCheck{{Index: 1}}
	checker.fanoutSentryFired = true

	checker.Enrich(context.Background(), []voicepipelinecore.Message{msg("system", "sys")})

	if checker.turnText != "" {
		t.Errorf("turnText = %q, want empty", checker.turnText)
	}
	if checker.checkCount != 0 {
		t.Errorf("checkCount = %d, want 0", checker.checkCount)
	}
	if len(checker.checks) != 0 {
		t.Errorf("checks = %v, want empty", checker.checks)
	}
	if checker.fanoutSentryFired {
		t.Error("fanoutSentryFired = true, want false")
	}
}

// Regression for staging call 891aaa9f. splitSentences hands over
// whitespace-free fragments, so naively concatenating them produced
// "…थी।जैसा कि…" for a turn Disha actually spoke as "…थी। जैसा कि…". That
// string becomes GuardrailLiveQueryAnchor's text backend-side, so the corpus
// would grow run-together sentences.
func TestGuardrailAccumulateRestoresSentenceSpacing(t *testing.T) {
	c := &guardrailChecker{}

	if idx, turn := c.accumulate("हाय जयदीप, हमारी बात अधूरी रह गई थी।"); idx != 1 ||
		turn != "हाय जयदीप, हमारी बात अधूरी रह गई थी।" {
		t.Fatalf("first fragment: index=%d turn=%q", idx, turn)
	}

	idx, turn := c.accumulate("जैसा कि आपने बताया।")
	if idx != 2 {
		t.Fatalf("index = %d, want 2", idx)
	}
	want := "हाय जयदीप, हमारी बात अधूरी रह गई थी। जैसा कि आपने बताया।"
	if turn != want {
		t.Fatalf("turn text = %q, want %q", turn, want)
	}
}

// Whitespace already present on either side must not be doubled.
func TestGuardrailAccumulateDoesNotDoubleSpaces(t *testing.T) {
	c := &guardrailChecker{}
	c.accumulate("One. ")
	_, turn := c.accumulate(" Two.")
	if turn != "One.  Two." && turn != "One. Two." {
		t.Fatalf("turn text = %q, want no inserted third space", turn)
	}
	if turn == "One.   Two." {
		t.Fatal("inserted a space despite both sides already having one")
	}
}

// The judge's raw output arrives in whatever shape the prompt and model
// negotiate. Every case below was either produced live or is explicitly asked
// for by the deployed staging prompt, and each one previously failed to parse
// — which fails OPEN, so the band went quiet with nothing logged as broken.
func TestParseGuardrailJudgeOutput(t *testing.T) {
	for _, tc := range []struct {
		name         string
		raw          string
		wantViolated bool
		wantOK       bool
	}{
		{"bare bool true", `{"violated": true}`, true, true},
		{"bare bool false", `{"violated": false}`, false, true},
		// What followup_call/guardrails/test_prompt actually specifies.
		{"quoted string true", `{"violated": "true"}`, true, true},
		{"quoted string false", `{"violated": "false"}`, false, true},
		{"yes/no", `{"violated": "yes"}`, true, true},
		{"no", `{"violated": "no"}`, false, true},
		// The prompt asks for a ```-fenced block.
		{"fenced json", "```json\n{\"violated\": true}\n```", true, true},
		{"fenced plain", "```\n{\"violated\": false}\n```", false, true},
		{"prose around it", "Sure! Here is the verdict:\n{\"violated\": true}\nHope that helps.", true, true},
		{"think block then fenced", "<think>hmm</think>\n```json\n{\"violated\": true}\n```", true, true},
		{"whitespace", "  \n{\"violated\": true}\n  ", true, true},
		// Still fails open.
		{"empty", "", false, false},
		{"no object", "I think it is fine.", false, false},
		{"unparseable verdict", `{"violated": "maybe"}`, false, false},
		{"malformed json", `{"violated": tru`, false, false},
		{"only think block", "<think>reasoning</think>", false, false},
	} {
		violated, ok := parseGuardrailJudgeOutput(tc.raw)
		if ok != tc.wantOK {
			t.Errorf("%s: ok = %v, want %v (raw=%q)", tc.name, ok, tc.wantOK, tc.raw)
			continue
		}
		if ok && violated != tc.wantViolated {
			t.Errorf("%s: violated = %v, want %v", tc.name, violated, tc.wantViolated)
		}
	}
}
