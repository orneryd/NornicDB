package nornicdb

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
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
		name              string
		configure         func(*config.Config)
		wantWALMode       string
		wantWALInterval   time.Duration
		wantBadgerSync    bool
		wantAsyncInterval time.Duration
	}{
		{
			name:              "defaults",
			configure:         func(*config.Config) {},
			wantWALMode:       "batch",
			wantWALInterval:   100 * time.Millisecond,
			wantAsyncInterval: 50 * time.Millisecond,
		},
		{
			name: "explicit none",
			configure: func(cfg *config.Config) {
				cfg.Database.WALSyncMode = "none"
				cfg.Database.WALSyncInterval = 250 * time.Millisecond
			},
			wantWALMode:       "none",
			wantWALInterval:   0,
			wantAsyncInterval: 50 * time.Millisecond,
		},
		{
			name: "strict overrides",
			configure: func(cfg *config.Config) {
				cfg.Database.StrictDurability = true
				cfg.Database.WALSyncMode = "batch"
				cfg.Database.WALSyncInterval = time.Second
				cfg.Database.AsyncFlushInterval = 200 * time.Millisecond
			},
			wantWALMode:       "immediate",
			wantWALInterval:   0,
			wantBadgerSync:    true,
			wantAsyncInterval: 10 * time.Millisecond,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.LoadDefaults()
			test.configure(cfg)

			badgerOptions, walConfig, asyncConfig := resolveDurabilityOptions("/data", cfg)
			require.Equal(t, test.wantBadgerSync, badgerOptions.SyncWrites)
			require.Equal(t, test.wantWALMode, walConfig.SyncMode)
			require.Equal(t, test.wantWALInterval, walConfig.BatchSyncInterval)
			require.Equal(t, test.wantAsyncInterval, asyncConfig.FlushInterval)
			require.Equal(t, cfg.Database.AsyncMaxNodeCacheSize, asyncConfig.MaxNodeCacheSize)
			require.Equal(t, cfg.Database.AsyncMaxEdgeCacheSize, asyncConfig.MaxEdgeCacheSize)
		})
	}
}
