package perf

import (
	"log"
	"os"
	"strings"

	"github.com/grafana/pyroscope-go"
)

func StartPyroscopeIfEnabled() func() {
	if !Enabled() {
		log.Println("performance diagnostics disabled")
		return func() {}
	}
	serverAddress := strings.TrimSpace(os.Getenv("PYROSCOPE_SERVER_ADDRESS"))
	basicAuthUser := strings.TrimSpace(os.Getenv("PYROSCOPE_BASIC_AUTH_USER"))
	basicAuthPassword := strings.TrimSpace(os.Getenv("PYROSCOPE_BASIC_AUTH_PASSWORD"))
	if serverAddress == "" || basicAuthUser == "" || basicAuthPassword == "" {
		log.Println("pyroscope disabled: missing server address or basic auth credentials")
		return func() {}
	}

	applicationName := strings.TrimSpace(os.Getenv("PYROSCOPE_GO_APPLICATION_NAME"))
	if applicationName == "" {
		applicationName = "talk-go.worker.go"
	}
	tags := map[string]string{
		"environment": pyroscopeEnv(),
		"deployment":  strings.TrimSpace(os.Getenv("GKE_DEPLOYMENT_NAME")),
		"pod":         strings.TrimSpace(os.Getenv("HOSTNAME")),
		"node":        strings.TrimSpace(os.Getenv("NODE_NAME")),
		"process":     "go-worker",
		"language":    "go",
	}
	for k, v := range tags {
		if strings.TrimSpace(v) == "" {
			delete(tags, k)
		}
	}

	cfg := pyroscope.Config{
		ApplicationName:   applicationName,
		ServerAddress:     serverAddress,
		BasicAuthUser:     basicAuthUser,
		BasicAuthPassword: basicAuthPassword,
		TenantID:          strings.TrimSpace(os.Getenv("PYROSCOPE_TENANT_ID")),
		Logger:            pyroscope.StandardLogger,
		Tags:              tags,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
		},
	}
	profiler, err := pyroscope.Start(cfg)
	if err != nil {
		log.Printf("pyroscope start failed: %v\n", err)
		return func() {}
	}
	log.Printf("pyroscope enabled: application=%s environment=%s\n", applicationName, pyroscopeEnv())
	return func() {
		if err := profiler.Stop(); err != nil {
			log.Printf("pyroscope stop failed: %v\n", err)
		}
	}
}

func pyroscopeEnv() string {
	return firstNonEmpty(
		os.Getenv("PYROSCOPE_ENVIRONMENT"),
		os.Getenv("ENVIRONMENT"),
		os.Getenv("SENTRY_ENVIRONMENT"),
	)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
