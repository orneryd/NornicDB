package nornicdb

import (
	"context"
	"os"
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type pendingCountEngine struct {
	storage.Engine
	count int
}

func (p *pendingCountEngine) PendingEmbeddingsCount() int { return p.count }

func TestDBWrapperHelpers_StorageAccess(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	namespaced := storage.NewNamespacedEngine(base, "nornic")
	db := &DB{storage: namespaced}

	require.Same(t, namespaced, db.GetStorage())
	require.Same(t, base, db.GetBaseStorageForManager())

	db.storage = base
	require.Panics(t, func() {
		_ = db.GetBaseStorageForManager()
	})
}

func TestDBWrapperHelpers_EmbedConfigRegistration(t *testing.T) {
	mock := newMockEmbedder()
	db := &DB{}

	db.SetDefaultEmbedConfig(nil)
	require.Nil(t, db.embedderRegistry)

	db.embedQueue = &EmbedQueue{embedder: mock}
	cfg := &embed.Config{
		Provider:   "local",
		Model:      "test-model",
		Dimensions: mock.Dimensions(),
		GPULayers:  0, // Explicit CPU-only selection is part of the registry key.
	}

	db.SetDefaultEmbedConfig(cfg)
	key := embedConfigKey(cfg)
	require.Equal(t, key, db.defaultEmbedKey)
	require.NotNil(t, db.embedderRegistry)
	require.Same(t, mock, db.embedderRegistry[key])

	var calledDB string
	resolver := func(dbName string) (*embed.Config, error) {
		calledDB = dbName
		return cfg, nil
	}
	db.SetEmbedConfigForDB(resolver)

	embedder, err := db.getOrCreateEmbedderForDB("tenant_a")
	require.NoError(t, err)
	require.Same(t, mock, embedder)
	require.Equal(t, "tenant_a", calledDB)
}

func TestDBWrapperHelpers_QueueAndExecutorAccess(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	queueCfg := DefaultEmbedQueueConfig()
	queueCfg.DeferWorkerStart = true
	queue := NewEmbedQueue(newMockEmbedder(), engine, queueCfg)
	t.Cleanup(queue.Close)

	exec := &cypher.StorageExecutor{}
	db := &DB{
		embedQueue:           queue,
		cypherExecutor:       exec,
		allDatabasesProvider: nil,
	}

	require.Same(t, exec, db.GetCypherExecutor())
	require.Same(t, queue, db.GetEmbedQueue())

	stats := db.EmbedQueueStats()
	require.NotNil(t, stats)
	require.False(t, stats.Running)
	require.Equal(t, 0, stats.Processed)
	require.Equal(t, 0, stats.Failed)

	db.SetAllDatabasesProvider(func() []DatabaseAndStorage {
		return []DatabaseAndStorage{{Name: "nornic", Storage: engine}}
	})
	require.NotNil(t, db.allDatabasesProvider)

	db.SetDbConfigResolver(func(dbName string) (int, float64, string) {
		if dbName == "tenant_a" {
			return 768, 0.42, "v2"
		}
		return 0, 0, ""
	})
	require.NotNil(t, db.dbConfigResolver)

	count, err := db.EmbedExisting(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, count)

	require.NoError(t, db.ResetEmbedWorker())

	db.StopEmbedQueue()
	require.Nil(t, db.embedQueue)
	require.Nil(t, db.EmbedQueueStats())

	_, err = db.EmbedExisting(context.Background())
	require.Error(t, err)
	require.Error(t, db.ResetEmbedWorker())
}

func TestDBWrapperHelpers_VectorDimensionsHelpers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 7
	db, err := Open("", cfg)
	require.NoError(t, err)

	require.Equal(t, 7, db.VectorIndexDimensions())
	require.Equal(t, 7, db.VectorIndexDimensionsCached())

	require.NoError(t, db.Close())
	require.Equal(t, 0, db.VectorIndexDimensions())

	// Cached helper falls back to configured dims when no service exists, then to 0.
	db2 := &DB{embeddingDims: 9, searchServices: map[string]*dbSearchService{}}
	namespaced := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	db2.storage = namespaced
	require.Equal(t, 9, db2.VectorIndexDimensionsCached())
	db2.embeddingDims = 0
	require.Equal(t, 0, db2.VectorIndexDimensionsCached())
}

func TestDBWrapperHelpers_EmbeddingAndPendingCounts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// No provider: only default DB count is returned.
	defaultSvc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NoError(t, defaultSvc.IndexNode(&storage.Node{
		ID:              "n1",
		Properties:      map[string]interface{}{"content": "alpha"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	}))
	require.Equal(t, 1, db.EmbeddingCount())

	// Multi-db provider: system is skipped and other DBs are included.
	db2Svc, err := db.GetOrCreateSearchService("tenant_cov", nil)
	require.NoError(t, err)
	require.NoError(t, db2Svc.IndexNode(&storage.Node{
		ID:              "n2",
		Properties:      map[string]interface{}{"content": "beta"},
		ChunkEmbeddings: [][]float32{{0.3, 0.2, 0.1}},
	}))
	compositeSvc, err := db.GetOrCreateSearchService("cmp_cov", nil)
	require.NoError(t, err)
	require.NoError(t, compositeSvc.IndexNode(&storage.Node{
		ID:              "n3",
		Properties:      map[string]interface{}{"content": "gamma"},
		ChunkEmbeddings: [][]float32{{0.5, 0.1, 0.9}},
	}))
	db.SetAllDatabasesProvider(func() []DatabaseAndStorage {
		return []DatabaseAndStorage{
			{Name: db.defaultDatabaseName(), Storage: db.storage},
			{Name: "tenant_cov", Storage: nil},
			{Name: "cmp_cov", Storage: nil, IsComposite: true},
			{Name: "system", Storage: nil},
		}
	})
	require.Equal(t, 2, db.EmbeddingCount())

	// Pending count uses optional storage fast-path interface.
	engine := &pendingCountEngine{Engine: storage.NewMemoryEngine(), count: 7}
	db.baseStorage = engine
	require.Equal(t, 7, db.PendingEmbeddingsCount())
	db.baseStorage = storage.NewMemoryEngine()
	require.Equal(t, 0, db.PendingEmbeddingsCount())
}

func TestDBWrapperHelpers_MaybeEnableReplicationPaths(t *testing.T) {
	origMode, hadMode := os.LookupEnv("NORNICDB_CLUSTER_MODE")
	origDataDir, hadData := os.LookupEnv("NORNICDB_CLUSTER_DATA_DIR")
	origRole, hadRole := os.LookupEnv("NORNICDB_CLUSTER_HA_ROLE")
	origPeer, hadPeer := os.LookupEnv("NORNICDB_CLUSTER_HA_PEER_ADDR")
	origBind, hadBind := os.LookupEnv("NORNICDB_CLUSTER_BIND_ADDR")
	t.Cleanup(func() {
		if hadMode {
			_ = os.Setenv("NORNICDB_CLUSTER_MODE", origMode)
		} else {
			_ = os.Unsetenv("NORNICDB_CLUSTER_MODE")
		}
		if hadData {
			_ = os.Setenv("NORNICDB_CLUSTER_DATA_DIR", origDataDir)
		} else {
			_ = os.Unsetenv("NORNICDB_CLUSTER_DATA_DIR")
		}
		if hadRole {
			_ = os.Setenv("NORNICDB_CLUSTER_HA_ROLE", origRole)
		} else {
			_ = os.Unsetenv("NORNICDB_CLUSTER_HA_ROLE")
		}
		if hadPeer {
			_ = os.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", origPeer)
		} else {
			_ = os.Unsetenv("NORNICDB_CLUSTER_HA_PEER_ADDR")
		}
		if hadBind {
			_ = os.Setenv("NORNICDB_CLUSTER_BIND_ADDR", origBind)
		} else {
			_ = os.Unsetenv("NORNICDB_CLUSTER_BIND_ADDR")
		}
	})

	base := storage.NewMemoryEngine()

	db := &DB{config: DefaultConfig()}

	_ = os.Unsetenv("NORNICDB_CLUSTER_MODE")
	got, err := db.maybeEnableReplication(base)
	require.NoError(t, err)
	require.Same(t, base, got)

	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_MODE", "standalone"))
	got, err = db.maybeEnableReplication(base)
	require.NoError(t, err)
	require.Same(t, base, got)

	// Non-standalone mode should attempt replication setup. Force adapter creation failure
	// by pointing DataDir at a file path, so mkdir for replication/wal fails deterministically.
	badPath := t.TempDir() + "/not-a-dir"
	require.NoError(t, os.WriteFile(badPath, []byte("x"), 0644))
	cfg := DefaultConfig()
	cfg.Database.DataDir = badPath
	db.config = cfg

	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_MODE", "raft"))
	_, err = db.maybeEnableReplication(base)
	require.Error(t, err)
	require.Contains(t, err.Error(), "replication: create storage adapter")

	// Invalid mode reaches replicator-construction error branch.
	goodDataDir := t.TempDir()
	cfg.Database.DataDir = goodDataDir
	db.config = cfg
	require.NoError(t, os.Unsetenv("NORNICDB_CLUSTER_DATA_DIR"))
	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_MODE", "invalid_mode"))
	_, err = db.maybeEnableReplication(base)
	require.Error(t, err)
	require.Contains(t, err.Error(), "replication: create replicator")
	require.Contains(t, err.Error(), "unknown replication mode")

	// Explicit cluster data dir override path is honored when set.
	overridePath := t.TempDir() + "/not-a-dir-override"
	require.NoError(t, os.WriteFile(overridePath, []byte("x"), 0644))
	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_DATA_DIR", overridePath))
	_, err = db.maybeEnableReplication(base)
	require.Error(t, err)
	require.Contains(t, err.Error(), "replication: create storage adapter")

	// Invalid HA role reaches a different replicator-construction failure path.
	validDataDir := t.TempDir()
	cfg.Database.DataDir = validDataDir
	db.config = cfg
	require.NoError(t, os.Unsetenv("NORNICDB_CLUSTER_DATA_DIR"))
	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_MODE", "ha_standby"))
	t.Setenv("NORNICDB_CLUSTER_HA_ROLE", "invalid-role")
	_, err = db.maybeEnableReplication(base)
	require.Error(t, err)
	require.Contains(t, err.Error(), "replication: create replicator")
	require.Contains(t, err.Error(), "invalid HA role")

	// Valid HA primary config exercises successful replication wiring/start path.
	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_HA_ROLE", "primary"))
	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", "127.0.0.1:65534"))
	require.NoError(t, os.Setenv("NORNICDB_CLUSTER_BIND_ADDR", "127.0.0.1:0"))
	replicated, err := db.maybeEnableReplication(base)
	require.NoError(t, err)
	require.NotSame(t, base, replicated)
	require.NotNil(t, db.replicator)
	require.NotNil(t, db.replicationAdapter)
	require.NotNil(t, db.replicationTrans)

	if db.replicator != nil {
		_ = db.replicator.Shutdown()
	}
	if db.replicationTrans != nil {
		_ = db.replicationTrans.Close()
	}
	if db.replicationAdapter != nil {
		_ = db.replicationAdapter.Close()
	}
	db.replicator = nil
	db.replicationTrans = nil
	db.replicationAdapter = nil
}

func TestDBWrapperHelpers_SetEmbedderBranches(t *testing.T) {
	t.Run("nil embedder is a no-op", func(t *testing.T) {
		db := &DB{}
		closeTestDBOnCleanup(t, db)
		db.SetEmbedder(nil)
		require.Nil(t, db.embedQueue)
	})

	t.Run("nil base storage panics", func(t *testing.T) {
		db := &DB{}
		closeTestDBOnCleanup(t, db)
		require.PanicsWithValue(t, "nornicdb: baseStorage is nil in SetEmbedder", func() {
			db.SetEmbedder(newMockEmbedder())
		})
	})

	t.Run("creates embed queue when not present", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		cfg := DefaultConfig()
		cfg.Memory.KmeansClusterInterval = 0 // avoid timer side effects
		calledYield := false
		db := &DB{
			config:            cfg,
			baseStorage:       base,
			storage:           storage.NewNamespacedEngine(base, "nornic"),
			embedWorkerConfig: DefaultEmbedQueueConfig(),
			embedQueueYieldFn: func() bool { calledYield = true; return true },
		}
		closeTestDBOnCleanup(t, db)
		db.embedWorkerConfig.DeferWorkerStart = true

		db.SetEmbedder(newMockEmbedder())
		require.NotNil(t, db.embedQueue)
		db.embedQueue.mu.Lock()
		gotYield := db.embedQueue.shouldYield
		db.embedQueue.mu.Unlock()
		require.NotNil(t, gotYield)
		require.True(t, gotYield())
		require.True(t, calledYield)
		db.embedQueue.Close()
	})

	t.Run("reuses existing queue and swaps embedder", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		cfg := DefaultConfig()
		cfg.Memory.KmeansClusterInterval = 0

		queueCfg := DefaultEmbedQueueConfig()
		queueCfg.DeferWorkerStart = true
		initial := newMockEmbedder()
		q := NewEmbedQueue(initial, base, queueCfg)
		t.Cleanup(q.Close)

		db := &DB{
			config:      cfg,
			baseStorage: base,
			storage:     storage.NewNamespacedEngine(base, "nornic"),
			embedQueue:  q,
		}
		closeTestDBOnCleanup(t, db)

		next := newMockEmbedder()
		db.SetEmbedder(next)
		require.Same(t, q, db.embedQueue, "existing queue should be reused")
		require.Same(t, next, db.embedQueue.embedder, "existing queue should receive new embedder")
	})

	t.Run("reuses existing queue and starts clustering timer when enabled", func(t *testing.T) {
		cleanup := featureflags.WithGPUClusteringEnabled()
		t.Cleanup(cleanup)

		base := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		cfg := DefaultConfig()
		cfg.Memory.KmeansClusterInterval = 5 * time.Millisecond

		queueCfg := DefaultEmbedQueueConfig()
		queueCfg.DeferWorkerStart = true
		q := NewEmbedQueue(newMockEmbedder(), base, queueCfg)
		t.Cleanup(q.Close)

		db := &DB{
			config:            cfg,
			baseStorage:       base,
			storage:           storage.NewNamespacedEngine(base, "nornic"),
			embedQueue:        q,
			searchServices:    map[string]*dbSearchService{},
			buildCtx:          context.Background(),
			embedWorkerConfig: DefaultEmbedQueueConfig(),
		}
		closeTestDBOnCleanup(t, db)

		db.SetEmbedder(newMockEmbedder())
		require.NotNil(t, db.clusterTicker, "clustering timer should start when GPU clustering is enabled")

		time.Sleep(15 * time.Millisecond)
		db.stopClusteringTimer()
		require.Nil(t, db.clusterTicker)
	})
}

func TestDBWrapperHelpers_CountAndBuildBranches(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	db := &DB{
		storage:             storage.NewNamespacedEngine(base, "nornic"),
		baseStorage:         base,
		config:              DefaultConfig(),
		searchServices:      make(map[string]*dbSearchService),
		embeddingDims:       3,
		searchMinSimilarity: 0.1,
		buildCtx:            context.Background(),
	}

	_, err := db.storage.CreateNode(&storage.Node{ID: "default", Properties: map[string]any{"content": "default"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}})
	require.NoError(t, err)
	require.NoError(t, db.BuildSearchIndexes(context.Background()))
	require.Equal(t, 1, db.EmbeddingCount())
	require.Equal(t, 1, db.EmbeddingCountCached())
	require.Equal(t, 3, db.VectorIndexDimensionsCached())

	tenantStorage := storage.NewNamespacedEngine(base, "tenant_count")
	_, err = tenantStorage.CreateNode(&storage.Node{ID: "tenant", Properties: map[string]any{"content": "tenant"}, ChunkEmbeddings: [][]float32{{0, 1, 0}}})
	require.NoError(t, err)
	tenantSvc, err := db.EnsureSearchIndexesBuilt(context.Background(), "tenant_count", tenantStorage)
	require.NoError(t, err)
	require.Equal(t, 1, tenantSvc.EmbeddingCount())

	db.SetAllDatabasesProvider(func() []DatabaseAndStorage {
		return []DatabaseAndStorage{
			{Name: "system", Storage: storage.NewNamespacedEngine(base, "system")},
			{Name: "composite", Storage: tenantStorage, IsComposite: true},
			{Name: "nornic", Storage: db.storage},
			{Name: "tenant_count", Storage: tenantStorage},
			{Name: "missing_base"},
		}
	})
	require.Equal(t, 2, db.EmbeddingCount())

	db.closed = true
	require.ErrorIs(t, db.BuildSearchIndexes(context.Background()), ErrClosed)
}
