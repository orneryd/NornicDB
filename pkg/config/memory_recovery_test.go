package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryRecoveryTimingsConfiguration(t *testing.T) {
	const intervalEnv = "NORNICDB_MEMORY_RECOVERY_INTERVAL_MS"
	const rearmEnv = "NORNICDB_MEMORY_RECOVERY_REARM_MS"
	t.Setenv(intervalEnv, "")
	t.Setenv(rearmEnv, "")
	load := func(body string) (*Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0600))
		return LoadFromFile(path)
	}
	t.Run("defaults", func(t *testing.T) {
		cfg := LoadDefaults()
		require.EqualValues(t, 5000, cfg.Memory.RecoveryIntervalMS)
		require.EqualValues(t, 30000, cfg.Memory.RecoveryRearmMS)
		cfg, err := load("memory: {}\n")
		require.NoError(t, err)
		require.EqualValues(t, 5000, cfg.Memory.RecoveryIntervalMS)
		require.EqualValues(t, 30000, cfg.Memory.RecoveryRearmMS)
	})
	t.Run("yaml and environment precedence", func(t *testing.T) {
		body := "memory:\n  recovery_interval_ms: 7\n  recovery_rearm_ms: 11\n"
		cfg, err := load(body)
		require.NoError(t, err)
		require.EqualValues(t, 7, cfg.Memory.RecoveryIntervalMS)
		require.EqualValues(t, 11, cfg.Memory.RecoveryRearmMS)
		t.Setenv(intervalEnv, "1")
		t.Setenv(rearmEnv, "1")
		cfg, err = load(body)
		require.NoError(t, err)
		require.EqualValues(t, 1, cfg.Memory.RecoveryIntervalMS)
		require.EqualValues(t, 1, cfg.Memory.RecoveryRearmMS)
	})
	for _, value := range []string{"0", "-1", "1.5", "5s", "9223372036854775807"} {
		for _, name := range []string{intervalEnv, rearmEnv} {
			t.Run(name+"="+value, func(t *testing.T) {
				t.Setenv(name, value)
				_, err := load("memory: {}\n")
				require.ErrorContains(t, err, name)
			})
		}
		for _, key := range []string{"recovery_interval_ms", "recovery_rearm_ms"} {
			t.Run(key+"="+value, func(t *testing.T) {
				_, err := load("memory:\n  " + key + ": " + value + "\n")
				require.Error(t, err)
			})
		}
	}
	t.Run("ordering", func(t *testing.T) {
		_, err := load("memory:\n  recovery_interval_ms: 11\n  recovery_rearm_ms: 7\n")
		require.ErrorContains(t, err, "recovery_rearm_ms")
		t.Setenv(intervalEnv, "30001")
		_, err = load("memory: {}\n")
		require.ErrorContains(t, err, "NORNICDB_MEMORY_RECOVERY_REARM_MS")
		cfg := LoadDefaults()
		cfg.Memory.RecoveryRearmMS = 0
		require.Error(t, cfg.Validate())
	})
	t.Run("startup environment error", func(t *testing.T) {
		t.Setenv(intervalEnv, "bad")
		defer func() {
			got := recover()
			require.NotNil(t, got)
			require.True(t, strings.Contains(got.(string), intervalEnv))
		}()
		LoadFromEnv()
	})
}
