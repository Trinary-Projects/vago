package llmrouter

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	vertexScope        = "https://www.googleapis.com/auth/cloud-platform"
	defaultGoogleToken = "https://oauth2.googleapis.com/token"
	// tokenRefreshSkew refreshes the cached token this long before it
	// actually expires so an in-flight request never uses a stale token.
	tokenRefreshSkew = 5 * time.Minute
	// vertexCredsEnv is the env var holding the S3 key of the Vertex
	// service-account JSON, mirroring Python's VERTEX_DISHAAI_CREDS_FILE.
	vertexCredsEnv = "VERTEX_DISHAAI_CREDS_FILE"
)

// serviceAccount is the subset of a Google service-account JSON key we
// need to mint an access token (same file Disha loads via
// VERTEX_DISHAAI_CREDS_FILE).
type serviceAccount struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURI    string `json:"token_uri"`
}

// vertexTokenSource mints and caches a Google OAuth access token from a
// service-account key using only the standard library: it builds an
// RS256-signed JWT assertion and exchanges it at the token endpoint. The
// service-account key is loaded lazily (once) via loadCreds on the first
// token request, so an unused Vertex endpoint never triggers a fetch.
type vertexTokenSource struct {
	loadCreds  func(context.Context) ([]byte, error)
	httpClient *http.Client

	mu        sync.Mutex
	inited    bool
	sa        serviceAccount
	key       *rsa.PrivateKey
	token     string
	expiresAt time.Time
}

func newVertexTokenSource(loadCreds func(context.Context) ([]byte, error), httpClient *http.Client) *vertexTokenSource {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &vertexTokenSource{loadCreds: loadCreds, httpClient: httpClient}
}

var (
	sharedVertexSourcesMu sync.Mutex
	sharedVertexSources   = map[string]*vertexTokenSource{}
)

// sharedVertexTokenSource returns the default process-wide Vertex token
// source used by the original sales Grok endpoint.
func sharedVertexTokenSource() *vertexTokenSource {
	return vertexTokenSourceForEnv(vertexCredsEnv)
}

func vertexTokenSourceForEnv(envKey string) *vertexTokenSource {
	envKey = strings.TrimSpace(envKey)
	if envKey == "" {
		envKey = vertexCredsEnv
	}
	sharedVertexSourcesMu.Lock()
	defer sharedVertexSourcesMu.Unlock()
	if src := sharedVertexSources[envKey]; src != nil {
		return src
	}
	src := newVertexTokenSource(func(ctx context.Context) ([]byte, error) {
		s3Key := strings.TrimSpace(os.Getenv(envKey))
		if s3Key == "" {
			return nil, fmt.Errorf("llmrouter: %s not set; Vertex endpoint disabled", envKey)
		}
		return s3GetObjectFromEnv(ctx, s3Key)
	}, nil)
	sharedVertexSources[envKey] = src
	return src
}

func vertexCredsEnvForConfig(cfg endpointConfig) string {
	if cfg.VertexCredsEnv != "" {
		return cfg.VertexCredsEnv
	}
	switch cfg.VertexProject {
	case "gen-lang-client-0439239631":
		return "VERTEX_DISHAAI_CREDS_FILE"
	case "disha-ai3":
		return "VERTEX_DISHA_AI2_CREDS_FILE"
	default:
		return "VERTEX_AI_CREDS_FILE"
	}
}

// ensureCreds loads + parses the service-account key once. Caller holds s.mu.
func (s *vertexTokenSource) ensureCreds(ctx context.Context) error {
	if s.inited {
		return nil
	}
	if s.loadCreds == nil {
		return errors.New("llmrouter: vertex creds loader not configured")
	}
	raw, err := s.loadCreds(ctx)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return errors.New("llmrouter: empty vertex service-account JSON")
	}
	var sa serviceAccount
	if err := json.Unmarshal(raw, &sa); err != nil {
		return fmt.Errorf("llmrouter: parse vertex service account: %w", err)
	}
	if sa.ClientEmail == "" || sa.PrivateKey == "" {
		return errors.New("llmrouter: vertex service account missing client_email/private_key")
	}
	if sa.TokenURI == "" {
		sa.TokenURI = defaultGoogleToken
	}
	key, err := parseRSAPrivateKey(sa.PrivateKey)
	if err != nil {
		return err
	}
	s.sa = sa
	s.key = key
	s.inited = true
	return nil
}

// Token returns a valid cached access token, loading creds and minting a
// fresh token when the cache is empty or near expiry.
func (s *vertexTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Now().Before(s.expiresAt.Add(-tokenRefreshSkew)) {
		return s.token, nil
	}
	if err := s.ensureCreds(ctx); err != nil {
		return "", err
	}
	tok, expiresIn, err := s.exchange(ctx)
	if err != nil {
		return "", err
	}
	s.token = tok
	s.expiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second)
	return tok, nil
}

func (s *vertexTokenSource) exchange(ctx context.Context) (string, int, error) {
	assertion, err := s.signedJWT(time.Now())
	if err != nil {
		return "", 0, err
	}
	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", assertion)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.sa.TokenURI, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("llmrouter: vertex token exchange: %w", err)
	}
	defer resp.Body.Close()

	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		Error       string `json:"error"`
		ErrorDesc   string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", 0, fmt.Errorf("llmrouter: decode vertex token response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || out.AccessToken == "" {
		return "", 0, fmt.Errorf("llmrouter: vertex token exchange failed (%d): %s %s", resp.StatusCode, out.Error, out.ErrorDesc)
	}
	if out.ExpiresIn <= 0 {
		out.ExpiresIn = 3600
	}
	return out.AccessToken, out.ExpiresIn, nil
}

// signedJWT builds and RS256-signs the JWT assertion for the token
// exchange.
func (s *vertexTokenSource) signedJWT(now time.Time) (string, error) {
	header := map[string]string{"alg": "RS256", "typ": "JWT"}
	claims := map[string]any{
		"iss":   s.sa.ClientEmail,
		"scope": vertexScope,
		"aud":   s.sa.TokenURI,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signingInput := base64URL(headerJSON) + "." + base64URL(claimsJSON)

	digest := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("llmrouter: sign vertex JWT: %w", err)
	}
	return signingInput + "." + base64URL(sig), nil
}

func base64URL(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

// parseRSAPrivateKey decodes a PEM private key (PKCS#8 or PKCS#1), the
// formats Google service-account keys use.
func parseRSAPrivateKey(pemStr string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("llmrouter: vertex private_key is not valid PEM")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rsaKey, ok := key.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("llmrouter: vertex private_key is not RSA")
		}
		return rsaKey, nil
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, errors.New("llmrouter: unable to parse vertex private_key (PKCS#8/PKCS#1)")
}
