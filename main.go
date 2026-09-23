package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jaideep329/talk-go/disha"
	"github.com/jaideep329/talk-go/internal/perf"
	"github.com/jaideep329/talk-go/internal/sentryutil"
	"github.com/jaideep329/talk-go/internal/worker"
	"github.com/jaideep329/talk-go/voicepipelinecore"
)

var (
	sessions   = map[string]*voicepipelinecore.PipelineTask{}
	sessionsMu sync.Mutex
	dishaDeps  disha.Deps
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
	stopPyroscope := perf.StartPyroscopeIfEnabled()
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
	defer sentry.Flush(2 * time.Second)
	dishaDeps = newDishaDeps()
	defer closeDishaDeps(dishaDeps)
	workerRuntime := worker.NewRuntime(dishaDeps, prepareTask)
	defer workerRuntime.ReportAbruptShutdownOnExit()
	workerRuntime.RegisterSignalHandlers()
	workerRuntime.RegisterWorkerPodIfConfigured()
	workerRuntime.RegisterRoutes(http.DefaultServeMux)
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
	workerRuntime.MarkGracefulShutdownCompleted()
}

func buildBotTask(ctx context.Context, req worker.TaskLaunchRequest) (*voicepipelinecore.PipelineTask, error) {
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

func prepareTask(ctx context.Context, req worker.TaskLaunchRequest, onCleanup func(*voicepipelinecore.PipelineTask)) (*voicepipelinecore.PipelineTask, error) {
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
		GKEPatcher:   worker.NewGKEPodPatcher(logger),
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
