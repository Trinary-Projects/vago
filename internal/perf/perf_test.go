package perf

import (
	"os"
	"testing"
)

func TestEnabledUsesCanonicalFlag(t *testing.T) {
	t.Setenv(EnvName, "1")
	t.Setenv(LegacyEnvName, "0")

	if !Enabled() {
		t.Fatal("Enabled() = false, want true from canonical flag")
	}
}

func TestEnabledCanonicalFlagOverridesLegacy(t *testing.T) {
	t.Setenv(EnvName, "0")
	t.Setenv(LegacyEnvName, "1")

	if Enabled() {
		t.Fatal("Enabled() = true, want false because canonical flag overrides legacy")
	}
}

func TestEnabledFallsBackToLegacyFlag(t *testing.T) {
	unsetEnv(t, EnvName)
	t.Setenv(LegacyEnvName, "1")

	if !Enabled() {
		t.Fatal("Enabled() = false, want true from legacy flag")
	}
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()
	old, ok := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatalf("Unsetenv(%s): %v", key, err)
	}
	t.Cleanup(func() {
		if ok {
			_ = os.Setenv(key, old)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}
