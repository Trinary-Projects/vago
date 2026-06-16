package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/disha"
	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

var (
	sessions   = map[string]*voicepipelinecore.PipelineTask{}
	sessionsMu sync.Mutex
	dishaDeps  disha.Deps
	worker     workerRuntime
)

func main() {
	exitCode := 0
	defer func() {
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()
	loadEnv(".env")
	appLog, _ := os.Create("app.log")
	log.SetOutput(io.MultiWriter(os.Stderr, appLog))
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	stopPyroscope := startPyroscopeIfEnabled()
	defer stopPyroscope()
	if dsn := strings.TrimSpace(os.Getenv("SENTRY_DSN")); dsn != "" {
		if err := sentry.Init(sentry.ClientOptions{
			Dsn:              dsn,
			Environment:      firstNonEmpty(os.Getenv("SENTRY_ENVIRONMENT"), os.Getenv("ENVIRONMENT")),
			Release:          strings.TrimSpace(os.Getenv("SENTRY_RELEASE")),
			AttachStacktrace: true,
		}); err != nil {
			log.Printf("sentry init failed: %v\n", err)
		} else {
			log.Println("sentry enabled")
		}
	} else {
		log.Println("sentry disabled: SENTRY_DSN is empty")
	}
	defer reportAbruptShutdownOnExit()
	defer sentry.Flush(2 * time.Second)
	dishaDeps = newDishaDeps()
	defer closeDishaDeps(dishaDeps)
	registerCleanupHandlers()
	registerWorkerPodIfConfigured()
	http.HandleFunc("/bot/create_worker_room", requireMethod(http.MethodPost, handleCreateWorkerRoom))
	http.HandleFunc("/bot/has_active_session", requireMethod(http.MethodGet, handleHasActiveSession))
	http.HandleFunc("/bot/health_check", requireMethod(http.MethodGet, handleHealthCheck))
	http.HandleFunc("/bot/readiness_check", requireMethod(http.MethodGet, handleReadinessCheck))
	http.HandleFunc("/bot/pre_stop_check", requireMethod(http.MethodGet, handleHealthCheck))
	http.HandleFunc("/bot/mark_machine_reserved", requireMethod(http.MethodPost, handleMarkMachineReserved))
	http.HandleFunc("/bot/trigger_exit", requireMethod(http.MethodPost, handleTriggerExit))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/", "/daily-client.html":
			http.ServeFile(w, r, "clients/daily-client.html")
		case "/livekit-client.html", "/LiveKitClient.html", "/LifeKitClient.html":
			http.ServeFile(w, r, "clients/livekit-client.html")
		default:
			http.NotFound(w, r)
		}
	})
	addr := serverAddr()
	log.Println("HTTP server listening on", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		sentryutil.Capture(sentryutil.Event{
			Err:  err,
			Tags: map[string]string{"component": "http_server"},
			Details: map[string]any{
				"addr": addr,
			},
		})
		log.Println("HTTP server error:", err)
		exitCode = 1
		return
	}
	markGracefulShutdownCompleted()
}

type botTaskLaunchRequest struct {
	ConversationID string `json:"conversation_id"`
	BotType        string `json:"bot_type"`
	RoomURL        string `json:"room_url"`
	RoomName       string `json:"room_name"`
	Token          string `json:"token"`
	BotToken       string `json:"bot_token"`
}

func buildBotTask(ctx context.Context, req botTaskLaunchRequest) (*voicepipelinecore.PipelineTask, error) {
	if req.RoomURL == "" {
		return nil, errors.New("room_url is required")
	}
	if req.ConversationID == "" {
		return nil, errors.New("conversation_id is required")
	}
	botToken := req.BotToken
	if botToken == "" {
		botToken = req.Token
	}
	botType := req.BotType
	if botType == "" {
		botType = disha.SalesCallBotType
	}
	bot, err := disha.NewBot(botType)
	if err != nil {
		return nil, err
	}
	return disha.NewBotTask(ctx, bot, disha.BotTaskRequest{
		ConversationID: req.ConversationID,
		RoomURL:        req.RoomURL,
		RoomName:       req.RoomName,
		RoomToken:      botToken,
	}, dishaDeps)
}

func prepareTask(ctx context.Context, req botTaskLaunchRequest, onCleanup func(*voicepipelinecore.PipelineTask)) (*voicepipelinecore.PipelineTask, error) {
	task, err := buildBotTask(ctx, req)
	if err != nil {
		return nil, err
	}
	previousCleanup := task.OnCleanup
	task.OnCleanup = func() {
		unregisterTask(task)
		if previousCleanup != nil {
			previousCleanup()
		}
		if onCleanup != nil {
			onCleanup(task)
		}
	}
	registerTask(task)
	return task, nil
}

func registerTask(task *voicepipelinecore.PipelineTask) {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	sessions[task.SessionID] = task
}

func unregisterTask(task *voicepipelinecore.PipelineTask) {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()
	delete(sessions, task.SessionID)
}

func newDishaDeps() disha.Deps {
	logger := log.New(log.Writer(), "[disha] ", log.Flags())
	redis := disha.NewRedisClient(
		os.Getenv("DISHA_REDIS_URL"),
		os.Getenv("DISHA_REDIS_PASSWORD"),
		redisDBFromEnv(),
		logger,
	)
	phonetic := disha.NewPhoneticDictFromEnv(logger)
	// Eagerly preload the phonetic dict in the background so the first
	// Cartesia turn doesn't pay an S3 round trip. Failures here are
	// non-fatal: TTS still works without the dictionary.
	if phonetic != nil {
		go func() {
			if err := phonetic.Preload(context.Background()); err != nil {
				logger.Printf("disha: phonetic dict preload failed: %v\n", err)
			}
		}()
	}
	return disha.Deps{
		Logger:       logger,
		Redis:        redis,
		API:          disha.NewAPIClient(firstNonEmpty(os.Getenv("DISHA_API_URL"), os.Getenv("API_BASE_URL")), 10*time.Second, logger),
		Documents:    disha.NewDocumentStore(redis, logger),
		PhoneticDict: phonetic,
		S3:           disha.NewS3GetClientFromEnv(logger, "AWS_BUCKET_NAME", "AWS_MAIN_REGION"),
		GKEPatcher:   disha.NewGKEPodPatcher(logger),
	}
}

func closeDishaDeps(deps disha.Deps) {
	if deps.Documents != nil {
		if err := deps.Documents.Close(); err != nil {
			log.Printf("failed to close Disha document store: %v", err)
		}
	}
	if deps.Redis != nil {
		if err := deps.Redis.Close(); err != nil {
			log.Printf("failed to close Disha Redis client: %v", err)
		}
	}
}

func redisDBFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("REDIS_DB"))
	if raw == "" {
		return 0
	}
	db, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid REDIS_DB=%q, using 0", raw)
		return 0
	}
	return db
}

func serverAddr() string {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = strings.TrimSpace(os.Getenv("FAST_API_PORT"))
	}
	if port == "" {
		port = "3000"
	}
	if strings.HasPrefix(port, ":") {
		return port
	}
	return ":" + port
}

func loadEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatal("failed to read .env:", err)
		}
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			if err := os.Setenv(parts[0], parts[1]); err != nil {
				return
			}
		}
	}
}

func registerWorkerPodIfConfigured() {
	reg, ok, err := workerPodRegistrationFromEnv()
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
	if err := disha.RegisterWorkerPod(ctx, dishaDeps, reg); err != nil {
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

func workerPodRegistrationFromEnv() (disha.WorkerPodRegistration, bool, error) {
	podName := strings.TrimSpace(os.Getenv("HOSTNAME"))
	podUID := strings.TrimSpace(os.Getenv("POD_UID"))
	appName := strings.TrimSpace(os.Getenv("GKE_DEPLOYMENT_NAME"))
	if podName == "" || podUID == "" || appName == "" {
		return disha.WorkerPodRegistration{}, false, nil
	}
	podIP := strings.TrimSpace(os.Getenv("POD_IP"))
	if podIP == "" {
		var err error
		podIP, err = detectPodIP()
		if err != nil {
			return disha.WorkerPodRegistration{}, false, err
		}
	}
	return disha.WorkerPodRegistration{
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
