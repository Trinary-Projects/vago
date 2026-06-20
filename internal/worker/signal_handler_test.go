package worker

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/jaideep329/talk-go/disha"
)

func TestHandleShutdownSignalEnqueuesGracefulShutdownOnce(t *testing.T) {
	exitCodes := []int{}
	redisServer, redisClient := newRedisTestClient(t)
	defer redisServer.Close()
	apiServer, apiRecorder := newWorkerAPIServer(t)
	rt := NewRuntime(disha.Deps{
		Redis: redisClient,
		API:   disha.NewAPIClient(apiServer.URL, time.Second, nil),
	}, nil)
	rt.exitProcess = func(code int) {
		exitCodes = append(exitCodes, code)
	}
	t.Setenv("HOSTNAME", "pod-1")

	rt.HandleShutdownSignal(syscall.SIGTERM)
	rt.HandleShutdownSignal(syscall.SIGTERM)

	raw, ok, err := redisClient.GetCache(context.Background(), "pod_sigterm:pod-1")
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
	kwargs, ok := requests[0].Body["kwargs"].(map[string]any)
	if !ok {
		t.Fatalf("kwargs = %#v, want object", requests[0].Body["kwargs"])
	}
	if requests[0].Body["module_name"] != "bots.signal_handler" ||
		requests[0].Body["func_name"] != "on_graceful_shutdown_initiated" ||
		requests[0].Body["sqs_queue"] != "fifo-p0-fast-l1" ||
		requests[0].Body["message_group_id"] != "pod-1" ||
		kwargs["pod_name"] != "pod-1" {
		t.Fatalf("enqueue body mismatch: %+v", requests[0].Body)
	}
	if len(exitCodes) != 0 {
		t.Fatalf("exit codes = %+v, want none; idle pod must stay alive until disha-backend deletes it", exitCodes)
	}
}

func TestHandleShutdownSignalExitsWhenHostnameEmpty(t *testing.T) {
	exitCodes := []int{}
	rt := NewRuntime(disha.Deps{}, nil)
	rt.exitProcess = func(code int) {
		exitCodes = append(exitCodes, code)
	}
	t.Setenv("HOSTNAME", "")

	rt.HandleShutdownSignal(syscall.SIGTERM)

	if len(exitCodes) != 1 || exitCodes[0] != 0 {
		t.Fatalf("exit codes = %+v, want [0]", exitCodes)
	}
}

func TestHandleShutdownSignalExitsOnSigint(t *testing.T) {
	exitCodes := []int{}
	apiServer, apiRecorder := newWorkerAPIServer(t)
	rt := NewRuntime(disha.Deps{
		API: disha.NewAPIClient(apiServer.URL, time.Second, nil),
	}, nil)
	rt.exitProcess = func(code int) {
		exitCodes = append(exitCodes, code)
	}
	t.Setenv("HOSTNAME", "pod-1")

	rt.HandleShutdownSignal(syscall.SIGINT)

	if len(exitCodes) != 1 || exitCodes[0] != 0 {
		t.Fatalf("exit codes = %+v, want [0]", exitCodes)
	}
	if len(apiRecorder.snapshot()) != 0 {
		t.Fatalf("request count = %d, want 0 enqueues on SIGINT", len(apiRecorder.snapshot()))
	}
}

func TestHandleShutdownSignalKeepsActiveWorkerAlive(t *testing.T) {
	exitCodes := []int{}
	redisServer, redisClient := newRedisTestClient(t)
	defer redisServer.Close()
	apiServer, apiRecorder := newWorkerAPIServer(t)
	rt := NewRuntime(disha.Deps{
		Redis: redisClient,
		API:   disha.NewAPIClient(apiServer.URL, time.Second, nil),
	}, nil)
	if outcome, _ := rt.state.claim("conv-shutdown"); outcome != claimGranted {
		t.Fatal("worker should start from idle state")
	}
	rt.exitProcess = func(code int) {
		exitCodes = append(exitCodes, code)
	}
	t.Setenv("HOSTNAME", "pod-1")

	rt.HandleShutdownSignal(syscall.SIGTERM)

	if len(apiRecorder.snapshot()) != 1 {
		t.Fatalf("request count = %d, want 1", len(apiRecorder.snapshot()))
	}
	if len(exitCodes) != 0 {
		t.Fatalf("exit codes = %+v, want none for active worker", exitCodes)
	}
}
