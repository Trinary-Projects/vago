package disha

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/jaideep329/talk-go/internal/sentryutil"
)

// idempotencyKeyPrefix marks a key as vago-origin when eyeballing Redis.
// Everything after it is a fixed-width digest, so every key this package
// produces is exactly len(prefix)+32 characters regardless of how long
// the inputs were or what characters they contained (the stage-threshold
// tag name, for instance, has spaces in it).
const (
	idempotencyKeyPrefix = "vago:"
	idempotencyKeyDigest = 32 // hex chars; 128 bits, collision risk negligible
)

// idempotencyKey builds the deterministic dedupe key for one logical
// operation.
//
// Deterministic, not random: the whole mechanism is that a replay
// presents the SAME key so SETNX rejects it the second time. It also has
// to match across independent producers — Disha's own room_finished
// recovery path derives post-call work from its own state, not from
// anything vago sent, and only a recomputable key lets those two collide
// on purpose. Retries live in disha-backend now, which makes this key
// the only thing keeping a backend-side replay safe.
//
// Parts are separated by a NUL byte so that ("ab", "c") and ("a", "bc")
// cannot hash to the same key.
func idempotencyKey(operation string, parts ...string) string {
	h := sha256.New()
	h.Write([]byte(operation))
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	return idempotencyKeyPrefix + hex.EncodeToString(h.Sum(nil))[:idempotencyKeyDigest]
}

// APIStatusError is returned for a non-2xx Disha API response. It keeps
// the status code so a caller can tell a 503 the backend may yet recover
// from apart from a 422 that will never succeed.
type APIStatusError struct {
	Method string
	Path   string
	Status int
	Body   string
}

func (e *APIStatusError) Error() string {
	return fmt.Sprintf("disha: API %s %s returned %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// Status helpers kept for callers that care about the distinction.
func (e *APIStatusError) IsServerSide() bool {
	return e.Status == http.StatusRequestTimeout ||
		e.Status == http.StatusTooManyRequests ||
		e.Status >= 500
}

// captureSentry is a package-var seam so tests can count exactly how
// many events a flow produces.
var captureSentry = sentryutil.Capture
