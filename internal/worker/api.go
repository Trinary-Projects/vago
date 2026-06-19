package worker

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jaideep329/talk-go/voicepipelinecore"
)

type workerState struct {
	mu                   sync.Mutex
	active               bool
	reserved             bool
	task                 *voicepipelinecore.PipelineTask
	activeConversationID string
}

// claimOutcome reports the result of attempting to claim the worker for a
// conversation. It distinguishes a fresh claim from a retried request for the
// conversation already in flight (idempotent) and from a genuine conflict with
// a different conversation.
type claimOutcome int

const (
	claimGranted   claimOutcome = iota // worker was idle; the caller now owns it
	claimDuplicate                     // already running this same conversation
	claimConflict                      // busy with a different conversation
)

// claim atomically takes ownership of the worker for conversationID. It mirrors
// the Python CreateWorkerRoom logic that claims the worker before the first
// await and treats a retried request for the in-flight conversation as a
// success rather than spawning a second bot. The returned id is the
// conversation currently holding the worker (useful when the outcome is a
// conflict).
func (s *workerState) claim(conversationID string) (claimOutcome, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active {
		if s.activeConversationID == conversationID {
			return claimDuplicate, s.activeConversationID
		}
		return claimConflict, s.activeConversationID
	}
	s.active = true
	s.activeConversationID = conversationID
	return claimGranted, conversationID
}

func (s *workerState) setTask(task *voicepipelinecore.PipelineTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.task = task
}

func (s *workerState) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = false
	s.reserved = false
	s.task = nil
	s.activeConversationID = ""
}

func (s *workerState) markReserved() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserved = true
}

func (s *workerState) snapshot() (active, reserved bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, s.reserved
}

type workerRoomRequest struct {
	RoomURL        string `json:"room_url"`
	RoomName       string `json:"room_name"`
	BotToken       string `json:"bot_token"`
	Token          string `json:"token"`
	ConversationID string `json:"conversation_id"`
	BotWorkerType  string `json:"bot_worker_type"`
}

func (r *Runtime) RegisterRoutes(mux *http.ServeMux) {
	if mux == nil {
		mux = http.DefaultServeMux
	}
	mux.HandleFunc("/bot/create_worker_room", requireMethod(http.MethodPost, r.handleCreateWorkerRoom))
	mux.HandleFunc("/bot/has_active_session", requireMethod(http.MethodGet, r.handleHasActiveSession))
	mux.HandleFunc("/bot/health_check", requireMethod(http.MethodGet, r.handleHealthCheck))
	mux.HandleFunc("/bot/readiness_check", requireMethod(http.MethodGet, r.handleReadinessCheck))
	mux.HandleFunc("/bot/pre_stop_check", requireMethod(http.MethodGet, r.handleHealthCheck))
	mux.HandleFunc("/bot/mark_machine_reserved", requireMethod(http.MethodPost, r.handleMarkMachineReserved))
	mux.HandleFunc("/bot/trigger_exit", requireMethod(http.MethodPost, r.handleTriggerExit))
}

func (r *Runtime) handleCreateWorkerRoom(w http.ResponseWriter, req *http.Request) {
	var roomReq workerRoomRequest
	if !decodeJSONRequest(w, req, &roomReq) {
		return
	}

	outcome, activeID := r.state.claim(roomReq.ConversationID)
	switch outcome {
	case claimDuplicate:
		// Retried request for the conversation we are already handling - the
		// forwarder re-sent it after a slow response. Treat as success instead
		// of returning a conflict that would drop the call.
		log.Printf("duplicate create_worker_room request for conversation=%s, treating as success\n", roomReq.ConversationID)
		writeJSON(w, http.StatusOK, map[string]string{
			"status":   "success",
			"room_url": roomReq.RoomURL,
		})
		return
	case claimConflict:
		// Busy with a different conversation. Report the active conversation id
		// so the forwarder can tell a genuine conflict apart from a retry. The
		// {"detail": {...}} envelope matches the FastAPI worker's 409 body.
		writeJSON(w, http.StatusConflict, map[string]any{
			"detail": map[string]any{
				"message":                "Worker machine already has an active session",
				"active_conversation_id": activeID,
			},
		})
		return
	}

	// claimGranted: we now own the worker - launch the bot.
	go r.runWorkerRoom(roomReq)
	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "success",
		"room_url": roomReq.RoomURL,
	})
}

func (r *workerRoomRequest) normalize() {
	r.RoomURL = strings.TrimSpace(r.RoomURL)
	r.RoomName = strings.TrimSpace(r.RoomName)
	r.BotToken = strings.TrimSpace(r.BotToken)
	r.Token = strings.TrimSpace(r.Token)
	r.ConversationID = strings.TrimSpace(r.ConversationID)
	r.BotWorkerType = strings.TrimSpace(r.BotWorkerType)
	if r.BotToken == "" {
		r.BotToken = r.Token
	}
}

func (r workerRoomRequest) validate() error {
	fields := []requiredField{
		{Name: "room_url", Value: r.RoomURL},
		{Name: "token", Value: r.Token},
		{Name: "conversation_id", Value: r.ConversationID},
		{Name: "bot_worker_type", Value: r.BotWorkerType},
		{Name: "room_name", Value: r.RoomName},
	}
	return requireFields(fields...)
}

func (r workerRoomRequest) taskLaunchRequest() TaskLaunchRequest {
	return TaskLaunchRequest{
		ConversationID: r.ConversationID,
		BotType:        r.BotWorkerType,
		RoomURL:        r.RoomURL,
		RoomName:       r.RoomName,
		Token:          r.Token,
		BotToken:       r.BotToken,
	}
}

type missingFieldsError struct {
	fields []string
}

func (e *missingFieldsError) Error() string {
	return "Missing required parameters: " + strings.Join(e.fields, ", ")
}

func (r *Runtime) runWorkerRoom(req workerRoomRequest) {
	// Pin the pod against autoscaler eviction for the lifetime of the
	// call. This mirrors Python `BotWorkerManager.create_bot_task`
	// which calls `set_safe_to_evict(pod_name, safe_to_evict=False)`
	// right before launching the bot. It's idempotent with the pin done
	// at reservation time.
	r.pinPodAgainstEviction()

	if r.starter == nil {
		log.Printf("worker task failed to start conversation=%s: task starter is not configured\n", req.ConversationID)
		r.unpinPodAfterCall()
		r.finishWorkerAndQueueCleanup()
		return
	}
	task, err := r.starter(context.Background(), req.taskLaunchRequest(), func(*voicepipelinecore.PipelineTask) {
		r.unpinPodAfterCall()
		r.finishWorkerAndQueueCleanup()
	})
	if err != nil {
		log.Printf("worker task failed to start conversation=%s: %v\n", req.ConversationID, err)
		r.unpinPodAfterCall()
		r.finishWorkerAndQueueCleanup()
		return
	}
	r.state.setTask(task)
	log.Printf("worker task started conversation=%s session=%s\n", req.ConversationID, task.SessionID)
	task.Start()
}

// pinPodAgainstEviction best-effort sets the GKE safe-to-evict annotation to
// "false" so the cluster autoscaler can't evict the pod while it is reserved
// or running a call. Logs failures and otherwise no-ops outside Kubernetes.
func (r *Runtime) pinPodAgainstEviction() {
	if r.deps.GKEPatcher == nil {
		return
	}
	podName := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if podName == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.deps.GKEPatcher.SetSafeToEvict(ctx, podName, false); err != nil {
		log.Printf("failed to set safe-to-evict=false on pod=%s: %v\n", podName, err)
	}
}

// unpinPodAfterCall restores safe-to-evict so the pod can be reaped after
// Disha's cleanup_state job tears it down. Failures are logged but never block
// the cleanup path.
func (r *Runtime) unpinPodAfterCall() {
	if r.deps.GKEPatcher == nil {
		return
	}
	podName := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if podName == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.deps.GKEPatcher.SetSafeToEvict(ctx, podName, true); err != nil {
		log.Printf("failed to set safe-to-evict=true on pod=%s: %v\n", podName, err)
	}
}

func (r *Runtime) finishWorkerAndQueueCleanup() {
	r.state.finish()
	podName := strings.TrimSpace(os.Getenv("HOSTNAME"))
	if podName == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := EnqueueWorkerCleanup(ctx, r.deps, podName); err != nil {
		log.Printf("failed to enqueue worker cleanup for pod=%s: %v\n", podName, err)
	}
}

func (r *Runtime) handleHasActiveSession(w http.ResponseWriter, req *http.Request) {
	active, _ := r.state.snapshot()
	activeSessions := 0
	if active {
		activeSessions = 1
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"has_active_session": active,
		"active_sessions":    activeSessions,
	})
}

func (r *Runtime) handleHealthCheck(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func (r *Runtime) handleReadinessCheck(w http.ResponseWriter, req *http.Request) {
	active, reserved := r.state.snapshot()
	if active || reserved {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready",
			"detail": "Worker is active or reserved",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (r *Runtime) handleMarkMachineReserved(w http.ResponseWriter, req *http.Request) {
	r.state.markReserved()
	// Pin the pod as soon as it is reserved - Disha reserves several
	// seconds before the user joins, and an autoscaler scale-down in
	// that window would kill the pod before the call starts. Mirrors
	// Python `mark_machine_reserved`, which also flips safe-to-evict to
	// false. The matching unpin happens on call cleanup.
	r.pinPodAgainstEviction()
	writeJSON(w, http.StatusOK, map[string]string{"status": "success"})
}

func (r *Runtime) handleTriggerExit(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "success"})
	go func() {
		time.Sleep(50 * time.Millisecond)
		r.exitProcess(0)
	}()
}

type validatedJSONRequest interface {
	normalize()
	validate() error
}

type requiredField struct {
	Name  string
	Value string
}

func requireMethod(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != method {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		next(w, req)
	}
}

func decodeJSONRequest(w http.ResponseWriter, req *http.Request, body validatedJSONRequest) bool {
	if err := json.NewDecoder(req.Body).Decode(body); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return false
	}
	body.normalize()
	if err := body.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func requireFields(fields ...requiredField) error {
	missing := make([]string, 0, len(fields))
	for _, field := range fields {
		if strings.TrimSpace(field.Value) == "" {
			missing = append(missing, field.Name)
		}
	}
	if len(missing) > 0 {
		return &missingFieldsError{fields: missing}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("failed to write JSON response: %v\n", err)
	}
}
