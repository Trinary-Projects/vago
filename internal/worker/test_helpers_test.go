package worker

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/jaideep329/talk-go/disha"
)

type capturedAPIRequest struct {
	Method string
	Path   string
	Body   map[string]any
}

type workerAPIRecorder struct {
	mu       sync.Mutex
	requests []capturedAPIRequest
}

func newWorkerAPIServer(t *testing.T) (*httptest.Server, *workerAPIRecorder) {
	t.Helper()
	recorder := &workerAPIRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var body map[string]any
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("Unmarshal request: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		recorder.mu.Lock()
		recorder.requests = append(recorder.requests, capturedAPIRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Body:   body,
		})
		recorder.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	t.Cleanup(server.Close)
	return server, recorder
}

func (r *workerAPIRecorder) snapshot() []capturedAPIRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]capturedAPIRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

func newRedisTestClient(t *testing.T) (*miniredis.Miniredis, disha.RedisClient) {
	t.Helper()
	server := miniredis.RunT(t)
	redisClient := disha.NewRedisClient(server.Addr(), "", 0, nil)
	t.Cleanup(func() {
		if err := redisClient.Close(); err != nil {
			t.Fatalf("Close redis: %v", err)
		}
	})
	return server, redisClient
}

func testDeps(redis disha.RedisClient, api *disha.APIClient) disha.Deps {
	return disha.Deps{Redis: redis, API: api}
}

func assertRequest(t *testing.T, got capturedAPIRequest, method, path string) {
	t.Helper()
	if got.Method != method || got.Path != path {
		t.Fatalf("request = %s %s, want %s %s", got.Method, got.Path, method, path)
	}
}
