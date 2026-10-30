package nornicdb

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResolveBadgerOptionsStorageMode(t *testing.T) {
	cfg := config.LoadDefaults()
	cfg.Database.BadgerNodeCacheMaxEntries = 321
	cfg.Database.BadgerEdgeTypeCacheMaxTypes = 12

	t.Run("default preserves current behavior", func(t *testing.T) {
		options := resolveBadgerOptions("/data", cfg)

		require.True(t, options.HighPerformance)
		require.False(t, options.LowMemory)
		require.Equal(t, 321, options.NodeCacheMaxEntries)
		require.Equal(t, 12, options.EdgeTypeCacheMaxTypes)
	})

	t.Run("low selects memory constrained representation", func(t *testing.T) {
		cfg.Storage.Mode = "low"
		options := resolveBadgerOptions("/data", cfg)

		require.False(t, options.HighPerformance)
		require.True(t, options.LowMemory)
		require.Equal(t, 321, options.NodeCacheMaxEntries)
		require.Equal(t, 12, options.EdgeTypeCacheMaxTypes)
	})
}

func TestResolveDurabilityOptions(t *testing.T) {
	tests := []struct {
		name            string
		configure       func(*config.Config)
		wantWALMode     string
		wantWALInterval time.Duration
		wantBadgerSync  bool
	}{
		{
			name:            "defaults",
			configure:       func(*config.Config) {},
			wantWALMode:     "batch",
			wantWALInterval: 100 * time.Millisecond, // config default WALSyncInterval
		},
		{
			name: "explicit none",
			configure: func(cfg *config.Config) {
				cfg.Database.WALSyncMode = "none"
				cfg.Database.WALSyncInterval = 250 * time.Millisecond
			},
			wantWALMode:     "none",
			wantWALInterval: 0,
		},
		{
			name: "async disabled uses WAL interval",
			configure: func(cfg *config.Config) {
				cfg.Database.AsyncWritesEnabled = false
				cfg.Database.WALSyncInterval = 250 * time.Millisecond
			},
			wantWALMode:     "batch",
			wantWALInterval: 250 * time.Millisecond,
		},
		{
			name: "strict overrides",
			configure: func(cfg *config.Config) {
				cfg.Database.StrictDurability = true
				cfg.Database.WALSyncMode = "batch"
				cfg.Database.WALSyncInterval = time.Second
			},
			wantWALMode:     "immediate",
			wantWALInterval: 0,
			wantBadgerSync:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.LoadDefaults()
			test.configure(cfg)

			badgerOptions, walConfig := resolveDurabilityOptions("/data", cfg)
			require.Equal(t, test.wantBadgerSync, badgerOptions.SyncWrites)
			require.Equal(t, test.wantWALMode, walConfig.SyncMode)
			require.Equal(t, test.wantWALInterval, walConfig.BatchSyncInterval)
			require.Equal(t, cfg.Database.WALSnapshotInterval, walConfig.SnapshotInterval)
		})
	}
}

func TestResolveDurabilityOptions_WriteBehindMaxOps(t *testing.T) {
	tests := []struct {
		name    string
		config  func(*config.Config)
		wantOps int
	}{
		{
			name:    "auto default (no cap configured)",
			config:  func(cfg *config.Config) {},
			wantOps: 0, // 0 = engine researched default cap
		},
		{
			name: "primary cap wired",
			config: func(cfg *config.Config) {
				cfg.Database.AsyncMaxOps = 120000
			},
			wantOps: 120000,
		},
		{
			name: "legacy node+edge caps fall back to sum",
			config: func(cfg *config.Config) {
				cfg.Database.AsyncMaxNodeCacheSize = 50000
				cfg.Database.AsyncMaxEdgeCacheSize = 100000
			},
			wantOps: 150000,
		},
		{
			name: "primary cap wins over legacy",
			config: func(cfg *config.Config) {
				cfg.Database.AsyncMaxOps = 30000
				cfg.Database.AsyncMaxNodeCacheSize = 50000
				cfg.Database.AsyncMaxEdgeCacheSize = 100000
			},
			wantOps: 30000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.LoadDefaults()
			cfg.Database.AsyncWritesEnabled = true
			cfg.Database.StrictDurability = false
			tt.config(cfg)

			badgerOptions, _ := resolveDurabilityOptions("/data", cfg)
			require.True(t, badgerOptions.WriteBehind)
			require.Equal(t, tt.wantOps, badgerOptions.WriteBehindMaxOps)
		})
	}
}

func TestOpenStrictDurabilityUsesWALStack(t *testing.T) {
	cfg := config.LoadDefaults()
	cfg.Database.StrictDurability = true
	cfg.Database.AsyncWritesEnabled = true
	cfg.Database.WALAutoCompactionEnabled = false

	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, isWAL := db.baseStorage.(*storage.WALEngine)
	require.True(t, isWAL, "base storage is %T", db.baseStorage)
}
