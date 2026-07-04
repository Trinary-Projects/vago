package worker

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/disha"
)

func TestRegisterWorkerPodEnqueuesDBOpsAndSetsRegistrationKey(t *testing.T) {
	_, redisClient := newRedisTestClient(t)
	apiServer, apiRecorder := newWorkerAPIServer(t)
	api := disha.NewAPIClient(apiServer.URL, 10*time.Second, nil)

	err := RegisterWorkerPod(context.Background(), testDeps(redisClient, api), WorkerPodRegistration{
		PodIP:   "10.1.2.3",
		PodName: "pod-1",
		PodUID:  "uid-1",
		AppName: "sales-worker",
	})
	if err != nil {
		t.Fatalf("RegisterWorkerPod: %v", err)
	}

	raw, ok, err := redisClient.GetCache(context.Background(), workerRegistrationKey("pod-1", "uid-1"))
	if err != nil {
		t.Fatalf("GetCache registration key: %v", err)
	}
	if !ok || string(raw) != "true" {
		t.Fatalf("registration cache = %q (present=%v), want JSON true", raw, ok)
	}

	requests := apiRecorder.snapshot()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1: %+v", len(requests), requests)
	}
	assertRequest(t, requests[0], http.MethodPost, "/common/enqueue_job")
	kwargs, ok := requests[0].Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", requests[0].Body["kwargs"])
	}
	if requests[0].Body["module_name"] != "bots.gke_pod_manager" ||
		requests[0].Body["func_name"] != "register_worker_pod_db_ops" ||
		requests[0].Body["sqs_queue"] != "fifo-p0-fast-l1" ||
		requests[0].Body["message_group_id"] != "pod-1" ||
		kwargs["pod_ip"] != "10.1.2.3" ||
		kwargs["pod_name"] != "pod-1" ||
		kwargs["pod_uid"] != "uid-1" ||
		kwargs["app_name"] != "sales-worker" {
		t.Fatalf("enqueue body mismatch: %+v", requests[0].Body)
	}
}

func TestRegisterWorkerPodSkipsAlreadyRegisteredPod(t *testing.T) {
	_, redisClient := newRedisTestClient(t)
	apiServer, apiRecorder := newWorkerAPIServer(t)
	api := disha.NewAPIClient(apiServer.URL, 10*time.Second, nil)
	if err := redisClient.SetCache(context.Background(), workerRegistrationKey("pod-1", "uid-1"), true, time.Hour); err != nil {
		t.Fatalf("SetCache registration key: %v", err)
	}

	err := RegisterWorkerPod(context.Background(), testDeps(redisClient, api), WorkerPodRegistration{
		PodIP:   "10.1.2.3",
		PodName: "pod-1",
		PodUID:  "uid-1",
		AppName: "sales-worker",
	})
	if err != nil {
		t.Fatalf("RegisterWorkerPod duplicate: %v", err)
	}
	if requests := apiRecorder.snapshot(); len(requests) != 0 {
		t.Fatalf("request count = %d, want 0: %+v", len(requests), requests)
	}
}

func TestEnqueueWorkerCleanup(t *testing.T) {
	apiServer, apiRecorder := newWorkerAPIServer(t)
	api := disha.NewAPIClient(apiServer.URL, 10*time.Second, nil)

	if err := EnqueueWorkerCleanup(context.Background(), disha.Deps{API: api}, "pod-1"); err != nil {
		t.Fatalf("EnqueueWorkerCleanup: %v", err)
	}

	requests := apiRecorder.snapshot()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1: %+v", len(requests), requests)
	}
	assertRequest(t, requests[0], http.MethodPost, "/common/enqueue_job")
	kwargs, ok := requests[0].Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", requests[0].Body["kwargs"])
	}
	if requests[0].Body["module_name"] != "bots.signal_handler" ||
		requests[0].Body["func_name"] != "cleanup_state" ||
		requests[0].Body["sqs_queue"] != "p0-fast-l1" ||
		kwargs["pod_name"] != "pod-1" {
		t.Fatalf("enqueue body mismatch: %+v", requests[0].Body)
	}
}

func TestEnqueueWorkerGracefulShutdownSetsSigtermKeyAndEnqueuesJob(t *testing.T) {
	_, redisClient := newRedisTestClient(t)
	apiServer, apiRecorder := newWorkerAPIServer(t)
	api := disha.NewAPIClient(apiServer.URL, 10*time.Second, nil)

	if err := EnqueueWorkerGracefulShutdown(context.Background(), testDeps(redisClient, api), "pod-1", "uid-1"); err != nil {
		t.Fatalf("EnqueueWorkerGracefulShutdown: %v", err)
	}

	raw, ok, err := redisClient.GetCache(context.Background(), workerSigtermKey("pod-1", "uid-1"))
	if err != nil {
		t.Fatalf("GetCache sigterm key: %v", err)
	}
	if !ok || string(raw) != "true" {
		t.Fatalf("sigterm cache = %q (present=%v), want JSON true", raw, ok)
	}

	requests := apiRecorder.snapshot()
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1: %+v", len(requests), requests)
	}
	assertRequest(t, requests[0], http.MethodPost, "/common/enqueue_job")
	kwargs, ok := requests[0].Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", requests[0].Body["kwargs"])
	}
	if requests[0].Body["module_name"] != "bots.signal_handler" ||
		requests[0].Body["func_name"] != "on_graceful_shutdown_initiated" ||
		requests[0].Body["sqs_queue"] != "fifo-p0-fast-l1" ||
		requests[0].Body["message_group_id"] != "pod-1" ||
		kwargs["pod_name"] != "pod-1" ||
		kwargs["pod_uid"] != "uid-1" {
		t.Fatalf("enqueue body mismatch: %+v", requests[0].Body)
	}
}
