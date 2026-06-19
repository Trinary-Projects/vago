package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jaideep329/talk-go/disha"
)

func newTestRuntime() *Runtime {
	rt := NewRuntime(disha.Deps{}, nil)
	rt.exitProcess = func(int) {}
	return rt
}

func TestPodRegistrationFromEnv(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-1")
	t.Setenv("POD_UID", "uid-1")
	t.Setenv("GKE_DEPLOYMENT_NAME", "sales-worker")
	t.Setenv("POD_IP", "10.1.2.3")

	reg, ok, err := PodRegistrationFromEnv()
	if err != nil {
		t.Fatalf("PodRegistrationFromEnv: %v", err)
	}
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if reg.PodName != "pod-1" || reg.PodUID != "uid-1" || reg.AppName != "sales-worker" || reg.PodIP != "10.1.2.3" {
		t.Fatalf("registration mismatch: %+v", reg)
	}
}

func TestPodRegistrationFromEnvSkipsLocalWhenIncomplete(t *testing.T) {
	t.Setenv("HOSTNAME", "pod-1")
	t.Setenv("POD_UID", "")
	t.Setenv("GKE_DEPLOYMENT_NAME", "sales-worker")
	t.Setenv("POD_IP", "10.1.2.3")

	_, ok, err := PodRegistrationFromEnv()
	if err != nil {
		t.Fatalf("PodRegistrationFromEnv: %v", err)
	}
	if ok {
		t.Fatal("ok = true, want false")
	}
}

func TestHandleHasActiveSession(t *testing.T) {
	rt := newTestRuntime()
	if outcome, _ := rt.state.claim("conv-active"); outcome != claimGranted {
		t.Fatal("claim did not grant idle worker")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bot/has_active_session", nil)
	rt.handleHasActiveSession(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}
	if body["has_active_session"] != true || body["active_sessions"] != float64(1) {
		t.Fatalf("body mismatch: %+v", body)
	}
}

func TestHandleReadinessCheck(t *testing.T) {
	rt := newTestRuntime()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bot/readiness_check", nil)
	rt.handleReadinessCheck(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want %d", rec.Code, http.StatusOK)
	}

	rt.state.markReserved()
	rec = httptest.NewRecorder()
	rt.handleReadinessCheck(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("reserved status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if !strings.Contains(rec.Body.String(), "Worker is active or reserved") {
		t.Fatalf("body = %q, want reserved detail", rec.Body.String())
	}
}

func TestHandleCreateWorkerRoomRejectsMissingFields(t *testing.T) {
	rt := newTestRuntime()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/bot/create_worker_room", strings.NewReader(`{}`))
	rt.handleCreateWorkerRoom(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	active, _ := rt.state.snapshot()
	if active {
		t.Fatal("worker active = true, want false")
	}
}

func TestHandleCreateWorkerRoomReturnsConflictWhenActive(t *testing.T) {
	rt := newTestRuntime()
	if outcome, _ := rt.state.claim("conv-existing"); outcome != claimGranted {
		t.Fatal("claim did not grant idle worker")
	}

	body := `{"room_url":"https://room.daily.co/test","room_name":"test","token":"user-token","conversation_id":"conv-1","bot_worker_type":"sales_call"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/bot/create_worker_room", strings.NewReader(body))
	rt.handleCreateWorkerRoom(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
	if !strings.Contains(rec.Body.String(), "Worker machine already has an active session") {
		t.Fatalf("body = %q, want conflict detail", rec.Body.String())
	}
	// The conflicting conversation must be reported so the forwarder can tell a
	// genuine conflict apart from a retry of the in-flight conversation.
	if !strings.Contains(rec.Body.String(), "conv-existing") {
		t.Fatalf("body = %q, want active conversation id", rec.Body.String())
	}
}

func TestHandleCreateWorkerRoomTreatsRetriedConversationAsSuccess(t *testing.T) {
	rt := newTestRuntime()
	if outcome, _ := rt.state.claim("conv-1"); outcome != claimGranted {
		t.Fatal("claim did not grant idle worker")
	}

	// A forwarder retry for the conversation already in flight must succeed
	// rather than conflict, so the call is not dropped.
	body := `{"room_url":"https://room.daily.co/test","room_name":"test","token":"user-token","conversation_id":"conv-1","bot_worker_type":"sales_call"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/bot/create_worker_room", strings.NewReader(body))
	rt.handleCreateWorkerRoom(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal response: %v", err)
	}
	if resp["status"] != "success" {
		t.Fatalf("status = %q, want success", resp["status"])
	}
}
