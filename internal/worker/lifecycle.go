package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jaideep329/talk-go/disha"
	"github.com/jaideep329/talk-go/internal/sentryutil"
)

const (
	workerRegistrationTTL = 24 * time.Hour
	workerSigtermTTL      = 2 * time.Hour
)

type WorkerPodRegistration struct {
	PodIP   string
	PodName string
	PodUID  string
	AppName string
}

func RegisterWorkerPod(ctx context.Context, deps disha.Deps, reg WorkerPodRegistration) error {
	if deps.Redis == nil {
		return errors.New("disha: Redis dependency is required")
	}
	if deps.API == nil {
		return errors.New("disha: API dependency is required")
	}
	if reg.PodIP == "" || reg.PodName == "" || reg.PodUID == "" || reg.AppName == "" {
		return fmt.Errorf("disha: incomplete worker registration: %+v", reg)
	}

	key := workerRegistrationKey(reg.PodName, reg.PodUID)
	if _, ok, err := deps.Redis.GetCache(ctx, key); err != nil {
		return err
	} else if ok {
		if deps.Logger != nil {
			deps.Logger.Printf("disha: worker pod already registered, skipping key=%s\n", key)
		}
		return nil
	}

	// Match Disha's worker registration order: enqueue the DB work first,
	// then write the Redis idempotency key so a failed enqueue can retry.
	if err := deps.API.EnqueueJob(ctx, disha.EnqueueJobRequest{
		ModuleName: "bots.gke_pod_manager",
		FuncName:   "register_worker_pod_db_ops",
		Kwargs: map[string]any{
			"pod_ip":   reg.PodIP,
			"pod_name": reg.PodName,
			"pod_uid":  reg.PodUID,
			"app_name": reg.AppName,
		},
		SQSQueue:       "fifo-p0-fast-l1",
		MessageGroupID: reg.PodName,
	}); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "worker_lifecycle",
				"operation": "register_worker_pod_db_ops",
			},
			Details: map[string]any{
				"pod_name": reg.PodName,
				"pod_uid":  reg.PodUID,
			},
		})
		return err
	}

	if err := deps.Redis.SetCache(ctx, key, true, workerRegistrationTTL); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "worker_lifecycle",
				"operation": "set_worker_registration_key",
			},
			Details: map[string]any{
				"pod_name": reg.PodName,
				"pod_uid":  reg.PodUID,
			},
		})
		return err
	}
	return nil
}

func EnqueueWorkerCleanup(ctx context.Context, deps disha.Deps, podName string) error {
	if deps.API == nil {
		return errors.New("disha: API dependency is required")
	}
	if podName == "" {
		return errors.New("disha: pod_name is required")
	}
	if err := deps.API.EnqueueJob(ctx, disha.EnqueueJobRequest{
		ModuleName: "bots.signal_handler",
		FuncName:   "cleanup_state",
		Kwargs: map[string]any{
			"pod_name": podName,
		},
		SQSQueue: "p0-fast-l1",
	}); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "worker_lifecycle",
				"operation": "cleanup_state",
			},
			Details: map[string]any{
				"pod_name": podName,
			},
		})
		return err
	}
	return nil
}

func EnqueueWorkerGracefulShutdown(ctx context.Context, deps disha.Deps, podName string) error {
	if deps.Redis == nil {
		return errors.New("disha: Redis dependency is required")
	}
	if deps.API == nil {
		return errors.New("disha: API dependency is required")
	}
	if podName == "" {
		return errors.New("disha: pod_name is required")
	}
	if err := deps.Redis.SetCache(ctx, workerSigtermKey(podName), true, workerSigtermTTL); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "worker_lifecycle",
				"operation": "set_sigterm_key",
			},
			Details: map[string]any{
				"pod_name": podName,
			},
		})
		return err
	}
	if err := deps.API.EnqueueJob(ctx, disha.EnqueueJobRequest{
		ModuleName: "bots.signal_handler",
		FuncName:   "on_graceful_shutdown_initiated",
		Kwargs: map[string]any{
			"pod_name": podName,
		},
		SQSQueue:       "fifo-p0-fast-l1",
		MessageGroupID: podName,
	}); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err: err,
			Tags: map[string]string{
				"component": "worker_lifecycle",
				"operation": "on_graceful_shutdown_initiated",
			},
			Details: map[string]any{
				"pod_name": podName,
			},
		})
		return err
	}
	return nil
}

func workerRegistrationKey(podName, podUID string) string {
	return fmt.Sprintf("registered_pod:%s:%s", podName, podUID)
}

func workerSigtermKey(podName string) string {
	return fmt.Sprintf("pod_sigterm:%s", podName)
}

func (r *Runtime) RegisterWorkerPodIfConfigured() {
	reg, ok, err := PodRegistrationFromEnv()
	if err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "worker_registration"},
		})
		log.Fatal("worker registration config error:", err)
	}
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := RegisterWorkerPod(ctx, r.deps, reg); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "worker_registration"},
			Details: map[string]any{
				"pod_name": reg.PodName,
				"pod_uid":  reg.PodUID,
			},
		})
		log.Fatal("worker pod registration failed:", err)
	}
	log.Printf("registered worker pod: pod_name=%s app_name=%s\n", reg.PodName, reg.AppName)
}

func PodRegistrationFromEnv() (WorkerPodRegistration, bool, error) {
	podName := strings.TrimSpace(os.Getenv("HOSTNAME"))
	podUID := strings.TrimSpace(os.Getenv("POD_UID"))
	appName := strings.TrimSpace(os.Getenv("GKE_DEPLOYMENT_NAME"))
	if podName == "" || podUID == "" || appName == "" {
		return WorkerPodRegistration{}, false, nil
	}
	podIP := strings.TrimSpace(os.Getenv("POD_IP"))
	if podIP == "" {
		var err error
		podIP, err = detectPodIP()
		if err != nil {
			return WorkerPodRegistration{}, false, err
		}
	}
	return WorkerPodRegistration{
		PodIP:   podIP,
		PodName: podName,
		PodUID:  podUID,
		AppName: appName,
	}, true, nil
}

func detectPodIP() (string, error) {
	hostname, err := os.Hostname()
	if err == nil && hostname != "" {
		if ips, lookupErr := net.LookupIP(hostname); lookupErr == nil {
			for _, ip := range ips {
				if v4 := ip.To4(); v4 != nil && !v4.IsLoopback() {
					return v4.String(), nil
				}
			}
		}
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		if v4 := ipNet.IP.To4(); v4 != nil && !v4.IsLoopback() {
			return v4.String(), nil
		}
	}
	return "", errors.New("no non-loopback pod IP found")
}
