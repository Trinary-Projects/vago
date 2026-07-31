// Package weaviate is a small, collection-agnostic Weaviate client.
//
// It speaks GraphQL over plain net/http with no Weaviate SDK, matching this
// repo's "all outbound services over stdlib HTTP" convention. It owns
// transport, bearer auth, error classification, and score conversion; it does
// NOT own thresholds, dedupe, or domain decoding, so different use cases
// (protocol retrieval, guardrails) can share it without inheriting each
// other's semantics.
//
// Callers supply the GraphQL selection set as a string. That is deliberate:
// cross-reference selections such as
//
//	answeredBy { ... on ProtocolInstruction { instructionText } }
//
// are not expressible in a small typed builder without inventing a schema
// DSL, and each use case selects entirely different properties.
package weaviate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	// defaultTimeout bounds a single GraphQL round trip. Callers that run on
	// a live call's blocking path should also pass a deadline-bearing ctx;
	// this is only a backstop for callers that don't.
	defaultTimeout = 15 * time.Second

	// Connections are kept warm for the lifetime of the client: on a voice
	// call the query runs once per user turn, and paying a TLS handshake per
	// turn would dominate the measured ~20ms query time.
	maxIdleConns        = 4
	maxIdleConnsPerHost = 4
	idleConnTimeout     = 90 * time.Second
)

// ErrNotConfigured is returned by NewClientFromEnv when the environment has
// no Weaviate configured. Callers treat it as "this feature is off" rather
// than a hard failure.
var ErrNotConfigured = errors.New("weaviate: WEAVIATE_URL and WEAVIATE_API_KEY are required")

// Client talks to one Weaviate instance.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	logger  *log.Logger
}

// Config builds a Client explicitly. Tests use this with an httptest URL.
type Config struct {
	BaseURL string
	APIKey  string
	Timeout time.Duration
	Logger  *log.Logger
}

// New builds a Client from an explicit config.
func New(cfg Config) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, ErrNotConfigured
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &Client{
		baseURL: baseURL,
		apiKey:  strings.TrimSpace(cfg.APIKey),
		logger:  cfg.Logger,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				MaxIdleConns:        maxIdleConns,
				MaxIdleConnsPerHost: maxIdleConnsPerHost,
				IdleConnTimeout:     idleConnTimeout,
			},
		},
	}, nil
}

// NewClientFromEnv builds a Client from WEAVIATE_URL / WEAVIATE_API_KEY.
// Returns ErrNotConfigured when either is missing, so a caller can treat an
// unconfigured deployment as "feature disabled" instead of crashing.
//
// In-cluster callers should point WEAVIATE_URL at the ClusterIP Service
// (http://weaviate.<namespace>.svc.cluster.local:8080); the public HTTPS
// ingress is for out-of-cluster tooling and local development.
func NewClientFromEnv(logger *log.Logger) (*Client, error) {
	url := strings.TrimSpace(os.Getenv("WEAVIATE_URL"))
	key := strings.TrimSpace(os.Getenv("WEAVIATE_API_KEY"))
	if url == "" || key == "" {
		return nil, ErrNotConfigured
	}
	return New(Config{BaseURL: url, APIKey: key, Logger: logger})
}

// Hit is one object returned by a vector search.
type Hit struct {
	// ID is the object's Weaviate UUID (_additional.id).
	ID string
	// Distance is the raw _additional.distance. Present reports whether the
	// server actually returned one — a hit without a distance cannot be
	// score-filtered and callers normally drop it.
	Distance        float64
	DistancePresent bool
	// Properties holds whatever the caller's Fields selected, including
	// nested cross-references as []any of map[string]any.
	Properties map[string]any
}

// Similarity is 1 - Distance. Only meaningful on a cosine-distance index.
func (h Hit) Similarity() float64 { return 1 - h.Distance }

// Certainty is Weaviate's normalized cosine score, (2 - Distance) / 2.
func (h Hit) Certainty() float64 { return (2 - h.Distance) / 2 }

// String returns a property as a string, or "" when absent or not a string.
func (h Hit) String(key string) string {
	s, _ := h.Properties[key].(string)
	return s
}

// CrossRef returns the first object of a cross-reference property, e.g. the
// ProtocolInstruction behind a ProtocolAnchor's answeredBy. Weaviate always
// renders cross-references as a list; callers that model one-to-one references
// want its head.
func (h Hit) CrossRef(property string) (Ref, bool) {
	list, ok := h.Properties[property].([]any)
	if !ok || len(list) == 0 {
		return Ref{}, false
	}
	object, ok := list[0].(map[string]any)
	if !ok {
		return Ref{}, false
	}
	return Ref{fields: object}, true
}

// Ref is one referenced object's selected properties.
type Ref struct{ fields map[string]any }

// ID returns the referenced object's Weaviate UUID, which requires the caller's
// selection set to have asked for `_additional { id }` inside the reference.
// Returns "" when it wasn't selected.
func (r Ref) ID() string {
	additional, ok := r.fields["_additional"].(map[string]any)
	if !ok {
		return ""
	}
	id, _ := additional["id"].(string)
	return id
}

// String returns a referenced property as a string, or "" when absent or not a
// string.
func (r Ref) String(key string) string {
	s, _ := r.fields[key].(string)
	return s
}

// Int returns a referenced int property. JSON numbers decode as float64, and an
// unset int property comes back as null rather than being absent, so a missing,
// null, non-numeric, or non-positive value all yield fallback.
func (r Ref) Int(key string, fallback int) int {
	number, ok := r.fields[key].(float64)
	if !ok || number <= 0 {
		return fallback
	}
	return int(number)
}

// NearTextQuery is a server-side-vectorized search. Concepts are sent RAW:
// a collection whose vectorizer applies its own prompt prefix (e.g. TEI with
// --default-prompt) will double-prefix if the caller prefixes too, which
// silently degrades ranking with no error.
type NearTextQuery struct {
	Class    string
	Concepts []string
	// Fields is the GraphQL selection set for the class's own properties and
	// cross-references. _additional{id distance} is appended automatically.
	Fields string
	// Where is a GraphQL where clause, built with the helpers in filters.go.
	// Empty means unfiltered.
	Where string
	Limit int
}

// NearVectorQuery searches with a caller-supplied vector, for collections
// with vectorizer: none.
type NearVectorQuery struct {
	Class  string
	Vector []float32
	Fields string
	Where  string
	Limit  int
}

// NearText runs a nearText search and returns the hits in server order
// (best first).
func (c *Client) NearText(ctx context.Context, q NearTextQuery) ([]Hit, error) {
	if err := validateSearch(q.Class, q.Fields); err != nil {
		return nil, err
	}
	if len(q.Concepts) == 0 {
		return nil, errors.New("weaviate: NearText requires at least one concept")
	}
	concepts, err := json.Marshal(q.Concepts)
	if err != nil {
		return nil, fmt.Errorf("weaviate: encode concepts: %w", err)
	}
	near := fmt.Sprintf("nearText: { concepts: %s }", concepts)
	return c.search(ctx, q.Class, near, q.Fields, q.Where, q.Limit)
}

// NearVector runs a nearVector search and returns the hits in server order.
func (c *Client) NearVector(ctx context.Context, q NearVectorQuery) ([]Hit, error) {
	if err := validateSearch(q.Class, q.Fields); err != nil {
		return nil, err
	}
	if len(q.Vector) == 0 {
		return nil, errors.New("weaviate: NearVector requires a vector")
	}
	vector, err := json.Marshal(q.Vector)
	if err != nil {
		return nil, fmt.Errorf("weaviate: encode vector: %w", err)
	}
	near := fmt.Sprintf("nearVector: { vector: %s }", vector)
	return c.search(ctx, q.Class, near, q.Fields, q.Where, q.Limit)
}

func validateSearch(class, fields string) error {
	if strings.TrimSpace(class) == "" {
		return errors.New("weaviate: class is required")
	}
	if strings.TrimSpace(fields) == "" {
		return errors.New("weaviate: fields selection set is required")
	}
	return nil
}

func (c *Client) search(ctx context.Context, class, near, fields, where string, limit int) ([]Hit, error) {
	args := []string{near}
	if strings.TrimSpace(where) != "" {
		args = append(args, "where: "+where)
	}
	if limit > 0 {
		args = append(args, fmt.Sprintf("limit: %d", limit))
	}
	// No server-side distance/certainty cutoff by design: callers threshold in
	// Go so sub-threshold candidates stay visible for calibration.
	query := fmt.Sprintf(
		"{ Get { %s( %s ) { %s _additional { id distance } } } }",
		class, strings.Join(args, " "), fields,
	)

	var out struct {
		Get map[string][]map[string]any `json:"Get"`
	}
	if err := c.GraphQL(ctx, query, &out); err != nil {
		return nil, err
	}
	return decodeHits(out.Get[class]), nil
}

func decodeHits(raw []map[string]any) []Hit {
	hits := make([]Hit, 0, len(raw))
	for _, item := range raw {
		hit := Hit{Properties: make(map[string]any, len(item))}
		for key, value := range item {
			if key == "_additional" {
				additional, ok := value.(map[string]any)
				if !ok {
					continue
				}
				hit.ID, _ = additional["id"].(string)
				if distance, ok := additional["distance"].(float64); ok {
					hit.Distance = distance
					hit.DistancePresent = true
				}
				continue
			}
			hit.Properties[key] = value
		}
		hits = append(hits, hit)
	}
	return hits
}

// GraphQL posts a raw GraphQL query and decodes the response's "data" object
// into out. It is the escape hatch for queries the typed helpers don't cover
// (Aggregate, hybrid search, generative modules).
//
// A GraphQL endpoint can return HTTP 200 with a non-empty "errors" array; that
// is a failure and is reported as one.
func (c *Client) GraphQL(ctx context.Context, query string, out any) error {
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return fmt.Errorf("weaviate: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/graphql", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("weaviate: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("weaviate: graphql request: %w", err)
	}
	defer resp.Body.Close()

	var payload struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&payload)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("weaviate: graphql HTTP %d", resp.StatusCode)
	}
	if decodeErr != nil {
		return fmt.Errorf("weaviate: decode response: %w", decodeErr)
	}
	if len(payload.Errors) > 0 {
		messages := make([]string, 0, len(payload.Errors))
		for _, e := range payload.Errors {
			messages = append(messages, e.Message)
		}
		return fmt.Errorf("weaviate: graphql errors: %s", strings.Join(messages, "; "))
	}
	if len(payload.Data) == 0 || string(payload.Data) == "null" {
		return errors.New("weaviate: graphql response has no data")
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(payload.Data, out); err != nil {
		return fmt.Errorf("weaviate: decode data: %w", err)
	}
	return nil
}
