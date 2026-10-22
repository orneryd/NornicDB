package nornicdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenRecoveryTimingsConfiguration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.RecoveryIntervalMS = 12
	cfg.Memory.RecoveryRearmMS = 11
	db, err := Open(t.TempDir(), cfg)
	require.Nil(t, db)
	require.ErrorContains(t, err, "recovery_rearm_ms")
	for _, pair := range [][2]int64{{0, 0}, {7, 11}} {
		cfg = DefaultConfig()
		cfg.Memory.DecayEnabled = false
		cfg.Memory.AutoLinksEnabled = false
		cfg.Memory.ApplicationSoftMax = 256 << 20
		cfg.Memory.SearchBM25Enabled = false
		cfg.Memory.SearchVectorEnabled = false
		cfg.Memory.SearchBM25Warming = "lazy"
		cfg.Memory.SearchVectorWarming = "lazy"
		cfg.Memory.RecoveryIntervalMS, cfg.Memory.RecoveryRearmMS = pair[0], pair[1]
		db, err = Open(t.TempDir(), cfg)
		require.NoError(t, err)
		require.NotNil(t, db.MemoryPolicy())
		require.NoError(t, db.Close())
	}
}
