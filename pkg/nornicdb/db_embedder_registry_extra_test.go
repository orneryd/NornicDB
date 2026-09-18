package nornicdb

import (
	"context"
	"sync"
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedderRegistrySeparatesEmbeddingModes(t *testing.T) {
	contextual := &embed.Config{Provider: "voyage", Mode: embed.VoyageModeContextualized, Model: "shared-name", Dimensions: 1024}
	multimodal := &embed.Config{Provider: "voyage", Mode: embed.VoyageModeMultimodal, Model: "shared-name", Dimensions: 1024}
	require.NotEqual(t, embedConfigKey(contextual), embedConfigKey(multimodal))
}

func TestDB_GetOrCreateEmbedderForDB_ExtraBranches(t *testing.T) {
	t.Run("inflight create channel without registry entry falls back to global embedder", func(t *testing.T) {
		global := &factoryTestEmbedder{dims: 4}
		cfg := &embed.Config{Provider: "openai", Model: "tenant-model", Dimensions: 4}
		key := embedConfigKey(cfg)
		ch := make(chan struct{})
		close(ch)

		db := &DB{
			embedQueue: &EmbedQueue{embedder: global},
			embedConfigForDB: func(dbName string) (*embed.Config, error) {
				require.Equal(t, "tenant", dbName)
				return cfg, nil
			},
			embedderRegistry: map[string]embed.Embedder{},
			embedderCreate:   map[string]chan struct{}{key: ch},
		}

		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, global, got)
	})

	t.Run("create path prefers concurrently inserted existing registry embedder", func(t *testing.T) {
		global := &factoryTestEmbedder{dims: 5}
		existing := &factoryTestEmbedder{dims: 5}
		created := &factoryTestEmbedder{dims: 5}
		cfg := &embed.Config{Provider: "openai", Model: "tenant-model-2", Dimensions: 5}
		key := embedConfigKey(cfg)

		db := &DB{
			embedQueue:       &EmbedQueue{embedder: global},
			embedderRegistry: map[string]embed.Embedder{},
			embedConfigForDB: func(dbName string) (*embed.Config, error) { return cfg, nil },
		}
		db.embedderFactory = func(resolved *embed.Config) (embed.Embedder, error) {
			require.Equal(t, key, embedConfigKey(resolved))
			db.embedderRegistryMu.Lock()
			db.embedderRegistry[key] = existing
			db.embedderRegistryMu.Unlock()
			return created, nil
		}

		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, existing, got)
	})

	t.Run("equivalent local config reuses active global embedder", func(t *testing.T) {
		global := &mockEmbedder{dims: 7, model: "equivalent-local"}
		cfg := &embed.Config{Provider: "local", Model: "equivalent-local", Dimensions: 7, GPULayers: -1}
		key := embedConfigKey(cfg)

		db := &DB{
			embedQueue:       &EmbedQueue{embedder: global},
			embedderRegistry: map[string]embed.Embedder{},
			embedConfigForDB: func(dbName string) (*embed.Config, error) { return cfg, nil },
		}
		db.SetDefaultEmbedConfig(cfg)

		got, err := db.getOrCreateEmbedderForDB("tenant")
		require.NoError(t, err)
		require.Same(t, global, got)

		db.embedderRegistryMu.RLock()
		require.Same(t, global, db.embedderRegistry[key])
		db.embedderRegistryMu.RUnlock()
	})
}

func TestDB_SetEmbedder_ExistingQueueAppliesYieldFn(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	queue := NewEmbedQueue(nil, base, &EmbedQueueConfig{DeferWorkerStart: true})
	t.Cleanup(func() { queue.Close() })

	db := &DB{
		baseStorage:       base,
		embedQueue:        queue,
		embedQueueYieldFn: func() bool { return true },
		config:            DefaultConfig(),
	}

	m := &mockEmbedder{dims: 8, model: "set-embedder"}
	db.SetEmbedder(m)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Same(t, m, queue.embedder)
	require.NotNil(t, queue.shouldYield)
	require.True(t, queue.shouldYield())
}

func TestDB_SetDefaultEmbedConfig_ReturnsWhenQueueUnavailable(t *testing.T) {
	db := &DB{}
	db.SetDefaultEmbedConfig(&embed.Config{Provider: "openai", Model: "m", Dimensions: 3})
	require.Nil(t, db.embedderRegistry)

	db.embedQueue = &EmbedQueue{}
	db.SetDefaultEmbedConfig(&embed.Config{Provider: "openai", Model: "m", Dimensions: 3})
	require.Nil(t, db.embedderRegistry)
}

func TestDB_BuildSearchIndexes_PropagatesGetOrCreateError(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	db := &DB{
		storage:        storage.NewNamespacedEngine(base, "system"),
		searchServices: map[string]*dbSearchService{},
		config:         DefaultConfig(),
	}

	err := db.BuildSearchIndexes(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "system database")
}

func TestDB_SetEmbedder_PanicsWhenBaseStorageNil(t *testing.T) {
	db := &DB{config: DefaultConfig()}
	require.Panics(t, func() {
		db.SetEmbedder(&mockEmbedder{dims: 2, model: "panic-embedder"})
	})
}

func TestDB_GetOrCreateEmbedderForDB_SingleFlightWaitPath(t *testing.T) {
	global := &factoryTestEmbedder{dims: 6}
	cfg := &embed.Config{Provider: "openai", Model: "sf", Dimensions: 6}
	key := embedConfigKey(cfg)

	db := &DB{
		embedQueue:       &EmbedQueue{embedder: global},
		embedderRegistry: map[string]embed.Embedder{},
		embedConfigForDB: func(dbName string) (*embed.Config, error) { return cfg, nil },
		embedderCreate:   map[string]chan struct{}{key: make(chan struct{})},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		db.embedderRegistryMu.Lock()
		db.embedderRegistry[key] = global
		db.embedderRegistryMu.Unlock()
		db.embedderCreateMu.Lock()
		close(db.embedderCreate[key])
		db.embedderCreateMu.Unlock()
	}()

	got, err := db.getOrCreateEmbedderForDB("tenant")
	wg.Wait()
	require.NoError(t, err)
	require.Same(t, global, got)
}

func TestDB_SetEmbedder_ClusteringFlagBranches(t *testing.T) {
	t.Run("enabled with zero interval stays manual", func(t *testing.T) {
		cleanup := featureflags.WithGPUClusteringEnabled()
		t.Cleanup(cleanup)

		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		cfg := DefaultConfig()
		cfg.Memory.KmeansClusterInterval = 0

		db := &DB{baseStorage: base, config: cfg}
		db.storage = storage.NewNamespacedEngine(base, "nornic")
		db.searchServices = map[string]*dbSearchService{}
		db.SetEmbedder(&mockEmbedder{dims: 4, model: "cluster-manual"})
		t.Cleanup(func() {
			if db.embedQueue != nil {
				db.embedQueue.Close()
			}
		})
		require.NotNil(t, db.embedQueue)
		require.Nil(t, db.clusterTicker)
	})

	t.Run("enabled with positive interval starts timer", func(t *testing.T) {
		cleanup := featureflags.WithGPUClusteringEnabled()
		t.Cleanup(cleanup)

		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		cfg := DefaultConfig()
		cfg.Memory.KmeansClusterInterval = time.Hour

		db := &DB{baseStorage: base, config: cfg}
		db.storage = storage.NewNamespacedEngine(base, "nornic")
		db.searchServices = map[string]*dbSearchService{}
		db.SetEmbedder(&mockEmbedder{dims: 4, model: "cluster-timer"})
		t.Cleanup(func() {
			if db.embedQueue != nil {
				db.embedQueue.Close()
			}
		})
		require.NotNil(t, db.embedQueue)
		require.NotNil(t, db.clusterTicker)
		db.stopClusteringTimer()
	})
}
