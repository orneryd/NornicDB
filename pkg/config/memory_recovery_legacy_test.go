package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemoryRecoveryLegacyConfigValidation(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Memory.RecoveryIntervalMS, cfg.Memory.RecoveryRearmMS = 0, 0
	require.NoError(t, cfg.Validate(), "legacy embedded config omits both timing fields")
	require.Zero(t, cfg.Memory.RecoveryIntervalMS, "validation does not mutate the caller")
	require.Zero(t, cfg.Memory.RecoveryRearmMS)
	for _, pair := range [][2]int64{{0, 1}, {1, 0}, {-1, -1}, {2, 1}} {
		cfg.Memory.RecoveryIntervalMS, cfg.Memory.RecoveryRearmMS = pair[0], pair[1]
		require.Error(t, cfg.Validate())
	}
}

func TestMemoryRecoveryExplicitZeroPair(t *testing.T) {
	t.Setenv("NORNICDB_MEMORY_RECOVERY_INTERVAL_MS", "")
	t.Setenv("NORNICDB_MEMORY_RECOVERY_REARM_MS", "")
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("memory:\n  recovery_interval_ms: 0\n  recovery_rearm_ms: 0\n"), 0600))
	_, err := LoadFromFile(path)
	require.ErrorContains(t, err, "recovery_interval_ms")
	require.NoError(t, os.WriteFile(path, []byte("memory: {}\n"), 0600))
	t.Setenv("NORNICDB_MEMORY_RECOVERY_INTERVAL_MS", "0")
	t.Setenv("NORNICDB_MEMORY_RECOVERY_REARM_MS", "0")
	_, err = LoadFromFile(path)
	require.ErrorContains(t, err, "NORNICDB_MEMORY_RECOVERY_INTERVAL_MS")
}
