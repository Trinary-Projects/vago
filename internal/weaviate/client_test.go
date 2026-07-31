package weaviate

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := New(Config{BaseURL: server.URL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

// capturingHandler records the GraphQL query it received and replies with a
// fixed body.
func capturingHandler(t *testing.T, query *string, body string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/graphql" {
			t.Errorf("path = %q, want /v1/graphql", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		raw, _ := io.ReadAll(r.Body)
		var payload struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Errorf("request body is not JSON: %v", err)
		}
		*query = payload.Query
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

const anchorFields = `anchorText
answeredBy { ... on ProtocolInstruction { instructionText _additional { id } } }`

func TestNearTextRequestShape(t *testing.T) {
	var query string
	client := newTestClient(t, capturingHandler(t, &query,
		`{"data":{"Get":{"ProtocolAnchor":[]}}}`))

	_, err := client.NearText(context.Background(), NearTextQuery{
		Class:    "ProtocolAnchor",
		Concepts: []string{"Disha: hi\nUser: acidity"},
		Fields:   anchorFields,
		Where:    EqualBool([]string{"answeredBy", "ProtocolInstruction", "isStaging"}, true),
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("NearText: %v", err)
	}

	for _, want := range []string{
		"Get {",
		"ProtocolAnchor(",
		`nearText: { concepts: ["Disha: hi\nUser: acidity"] }`,
		`where: { path: ["answeredBy","ProtocolInstruction","isStaging"] operator: Equal valueBoolean: true }`,
		"limit: 10",
		"answeredBy { ... on ProtocolInstruction",
		"_additional { id distance }",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q\nfull query:\n%s", want, query)
		}
	}
	// No server-side score cutoff: sub-threshold candidates must come back so
	// callers can calibrate their threshold.
	for _, unwanted := range []string{"certainty:", "distance:"} {
		if strings.Contains(query, unwanted) {
			t.Errorf("query must not carry a server-side cutoff (%q)\n%s", unwanted, query)
		}
	}
}

func TestNearVectorRequestShape(t *testing.T) {
	var query string
	client := newTestClient(t, capturingHandler(t, &query,
		`{"data":{"Get":{"ResponseReviewAnchor":[]}}}`))

	_, err := client.NearVector(context.Background(), NearVectorQuery{
		Class:  "ResponseReviewAnchor",
		Vector: []float32{0.5, -0.25},
		Fields: "anchorText",
		Limit:  3,
	})
	if err != nil {
		t.Fatalf("NearVector: %v", err)
	}
	if !strings.Contains(query, "nearVector: { vector: [0.5,-0.25] }") {
		t.Errorf("unexpected nearVector clause:\n%s", query)
	}
	if strings.Contains(query, "where:") {
		t.Errorf("empty Where must emit no where clause:\n%s", query)
	}
}

func TestNearTextDecodesHits(t *testing.T) {
	body := `{"data":{"Get":{"ProtocolAnchor":[
      {"anchorText":"acidity at night",
       "answeredBy":[{"instructionText":"do not diagnose","_additional":{"id":"instr-1"}}],
       "_additional":{"id":"anchor-1","distance":0.2}},
      {"anchorText":"no distance here",
       "answeredBy":[],
       "_additional":{"id":"anchor-2"}}
    ]}}}`
	var query string
	client := newTestClient(t, capturingHandler(t, &query, body))

	hits, err := client.NearText(context.Background(), NearTextQuery{
		Class: "ProtocolAnchor", Concepts: []string{"q"}, Fields: anchorFields,
	})
	if err != nil {
		t.Fatalf("NearText: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("hits = %d, want 2", len(hits))
	}

	first := hits[0]
	if first.ID != "anchor-1" || !first.DistancePresent {
		t.Fatalf("first hit = %+v", first)
	}
	if got := first.Similarity(); got < 0.79999 || got > 0.80001 {
		t.Errorf("Similarity() = %v, want 0.8", got)
	}
	if got := first.Certainty(); got < 0.89999 || got > 0.90001 {
		t.Errorf("Certainty() = %v, want 0.9", got)
	}
	if got := first.String("anchorText"); got != "acidity at night" {
		t.Errorf("anchorText = %q", got)
	}
	if _, ok := first.Properties["_additional"]; ok {
		t.Error("_additional must not leak into Properties")
	}
	if _, ok := first.Properties["answeredBy"]; !ok {
		t.Error("cross-reference missing from Properties")
	}

	// A hit with no distance can't be score-filtered; callers rely on the flag.
	if hits[1].DistancePresent {
		t.Error("second hit should report DistancePresent=false")
	}
}

// A GraphQL endpoint can return HTTP 200 with an errors array. Treating that as
// success is the classic way to silently retrieve nothing.
func TestGraphQLErrorsPayloadIsAFailure(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":[{"message":"Unknown argument \"nearText\""}]}`))
	})

	_, err := client.NearText(context.Background(), NearTextQuery{
		Class: "ProtocolAnchor", Concepts: []string{"q"}, Fields: "anchorText",
	})
	if err == nil {
		t.Fatal("expected an error for a 200-with-errors response")
	}
	if !strings.Contains(err.Error(), "Unknown argument") {
		t.Errorf("error should carry the server message, got %v", err)
	}
}

func TestGraphQLFailureModes(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", http.StatusUnauthorized, `{"error":"invalid key"}`},
		{"server error", http.StatusInternalServerError, `boom`},
		{"missing data", http.StatusOK, `{}`},
		{"null data", http.StatusOK, `{"data":null}`},
		{"malformed json", http.StatusOK, `{"data":`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			_, err := client.NearText(context.Background(), NearTextQuery{
				Class: "ProtocolAnchor", Concepts: []string{"q"}, Fields: "anchorText",
			})
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestValidationRejectsIncompleteQueries(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request should be sent for an invalid query")
	})
	ctx := context.Background()

	if _, err := client.NearText(ctx, NearTextQuery{Concepts: []string{"q"}, Fields: "a"}); err == nil {
		t.Error("missing class should error")
	}
	if _, err := client.NearText(ctx, NearTextQuery{Class: "C", Concepts: []string{"q"}}); err == nil {
		t.Error("missing fields should error")
	}
	if _, err := client.NearText(ctx, NearTextQuery{Class: "C", Fields: "a"}); err == nil {
		t.Error("missing concepts should error")
	}
	if _, err := client.NearVector(ctx, NearVectorQuery{Class: "C", Fields: "a"}); err == nil {
		t.Error("missing vector should error")
	}
}

func TestNewRejectsEmptyURL(t *testing.T) {
	if _, err := New(Config{BaseURL: "  ", APIKey: "k"}); err != ErrNotConfigured {
		t.Fatalf("err = %v, want ErrNotConfigured", err)
	}
}

func TestNewClientFromEnvRequiresBoth(t *testing.T) {
	for _, tc := range []struct{ url, key string }{
		{"", ""},
		{"http://weaviate", ""},
		{"", "key"},
	} {
		t.Setenv("WEAVIATE_URL", tc.url)
		t.Setenv("WEAVIATE_API_KEY", tc.key)
		if _, err := NewClientFromEnv(nil); err != ErrNotConfigured {
			t.Errorf("url=%q key set=%v: err = %v, want ErrNotConfigured", tc.url, tc.key != "", err)
		}
	}

	t.Setenv("WEAVIATE_URL", "http://weaviate.staging.svc.cluster.local:8080/")
	t.Setenv("WEAVIATE_API_KEY", "key")
	client, err := NewClientFromEnv(nil)
	if err != nil {
		t.Fatalf("NewClientFromEnv: %v", err)
	}
	if client.baseURL != "http://weaviate.staging.svc.cluster.local:8080" {
		t.Errorf("trailing slash not trimmed: %q", client.baseURL)
	}
}

func TestFilterBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			"equal bool through a reference",
			EqualBool([]string{"answeredBy", "ProtocolInstruction", "isStaging"}, true),
			`{ path: ["answeredBy","ProtocolInstruction","isStaging"] operator: Equal valueBoolean: true }`,
		},
		{
			"equal string",
			EqualString([]string{"programType"}, "disha"),
			`{ path: ["programType"] operator: Equal valueString: "disha" }`,
		},
		{
			"equal int",
			EqualInt([]string{"turnsThresholdCount"}, 3),
			`{ path: ["turnsThresholdCount"] operator: Equal valueInt: 3 }`,
		},
		{
			"and of two",
			And(EqualString([]string{"a"}, "1"), EqualString([]string{"b"}, "2")),
			`{ operator: And operands: [ { path: ["a"] operator: Equal valueString: "1" } { path: ["b"] operator: Equal valueString: "2" } ] }`,
		},
		{
			"or of two",
			Or(EqualBool([]string{"a"}, true), EqualBool([]string{"b"}, false)),
			`{ operator: Or operands: [ { path: ["a"] operator: Equal valueBoolean: true } { path: ["b"] operator: Equal valueBoolean: false } ] }`,
		},
		// A single operand needs no wrapper, and zero operands must mean "no
		// filter" rather than an empty operands list that Weaviate rejects.
		{"and of one", And(EqualBool([]string{"a"}, true)), `{ path: ["a"] operator: Equal valueBoolean: true }`},
		{"and of none", And(), ""},
		{"and drops empties", And("", EqualBool([]string{"a"}, true), "   "), `{ path: ["a"] operator: Equal valueBoolean: true }`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("got  %s\nwant %s", tc.got, tc.want)
			}
		})
	}
}

// Values reach GraphQL through json.Marshal, so a quote or backslash in a
// value can't terminate the literal and inject syntax.
func TestFilterBuildersEscapeValues(t *testing.T) {
	got := EqualString([]string{"title"}, `he said "hi" \ then left`)
	if !strings.Contains(got, `valueString: "he said \"hi\" \\ then left"`) {
		t.Fatalf("value not escaped: %s", got)
	}
	if strings.Count(got, `"`)%2 != 0 {
		t.Fatalf("unbalanced quotes suggest injection: %s", got)
	}
}

// Ref is the shared cross-reference decoder; both protocol retrieval and the
// planned guardrail lookup depend on its null/missing handling.
func TestHitCrossRef(t *testing.T) {
	body := `{"data":{"Get":{"C":[
      {"a":"x","ref":[{"text":"body","count":5,"_additional":{"id":"ref-1"}}],"_additional":{"id":"h1","distance":0.1}},
      {"a":"null count","ref":[{"text":"body","count":null,"_additional":{"id":"ref-2"}}],"_additional":{"id":"h2","distance":0.1}},
      {"a":"zero count","ref":[{"text":"body","count":0,"_additional":{"id":"ref-3"}}],"_additional":{"id":"h3","distance":0.1}},
      {"a":"no id","ref":[{"text":"body"}],"_additional":{"id":"h4","distance":0.1}},
      {"a":"empty ref","ref":[],"_additional":{"id":"h5","distance":0.1}},
      {"a":"missing ref","_additional":{"id":"h6","distance":0.1}}
    ]}}}`
	var query string
	client := newTestClient(t, capturingHandler(t, &query, body))
	hits, err := client.NearText(context.Background(), NearTextQuery{
		Class: "C", Concepts: []string{"q"}, Fields: "a ref { text count }",
	})
	if err != nil {
		t.Fatalf("NearText: %v", err)
	}
	if len(hits) != 6 {
		t.Fatalf("hits = %d, want 6", len(hits))
	}

	ref, ok := hits[0].CrossRef("ref")
	if !ok {
		t.Fatal("first hit should have a cross-reference")
	}
	if ref.ID() != "ref-1" || ref.String("text") != "body" {
		t.Errorf("ref = %+v", ref)
	}
	if got := ref.Int("count", 3); got != 5 {
		t.Errorf("Int with a value = %d, want 5", got)
	}
	if got := ref.Int("absent", 3); got != 3 {
		t.Errorf("Int for an absent key = %d, want the fallback 3", got)
	}

	// An unset int property arrives as JSON null, not absent — the case that
	// drives turnsThresholdCount's default.
	nullRef, _ := hits[1].CrossRef("ref")
	if got := nullRef.Int("count", 3); got != 3 {
		t.Errorf("Int for a null value = %d, want the fallback 3", got)
	}
	// Non-positive is treated as unset rather than honoured as a real 0.
	zeroRef, _ := hits[2].CrossRef("ref")
	if got := zeroRef.Int("count", 3); got != 3 {
		t.Errorf("Int for 0 = %d, want the fallback 3", got)
	}

	noIDRef, _ := hits[3].CrossRef("ref")
	if noIDRef.ID() != "" {
		t.Errorf("ID without a selected _additional = %q, want empty", noIDRef.ID())
	}
	if noIDRef.String("missing") != "" {
		t.Error("String for an absent key should be empty")
	}

	if _, ok := hits[4].CrossRef("ref"); ok {
		t.Error("an empty reference list should report absent")
	}
	if _, ok := hits[5].CrossRef("ref"); ok {
		t.Error("a missing reference property should report absent")
	}
	// Zero-value Ref must be safe to use.
	var zero Ref
	if zero.ID() != "" || zero.String("k") != "" || zero.Int("k", 7) != 7 {
		t.Error("zero-value Ref should be safe")
	}
}
