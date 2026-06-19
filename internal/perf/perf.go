package perf

import (
	"os"
	"strings"
)

const (
	EnvName       = "PERF_DIAGNOSTICS_ENABLED"
	LegacyEnvName = "PYROSCOPE_ENABLED"
)

func Enabled() bool {
	if value, ok := os.LookupEnv(EnvName); ok {
		return strings.TrimSpace(value) == "1"
	}
	return strings.TrimSpace(os.Getenv(LegacyEnvName)) == "1"
}
