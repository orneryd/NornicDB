package nornicdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type blockingIterEngine struct {
	storage.Engine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type namespaceListEngine struct {
	storage.Engine
	namespaces []string
}

func (e *namespaceListEngine) ListNamespaces() []string {
	return append([]string(nil), e.namespaces...)
}

func (e *blockingIterEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	nodes, err := e.AllNodes()
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if !fn(n) {
			return nil
		}
	}
	return nil
}

func TestSearchServices_HelperBranches(t *testing.T) {
	t.Run("splitQualifiedID validity", func(t *testing.T) {
		dbName, local, ok := splitQualifiedID("tenant:node1")
		require.True(t, ok)
		require.Equal(t, "tenant", dbName)
		require.Equal(t, "node1", local)

		_, _, ok = splitQualifiedID("tenant:")
		require.False(t, ok)
		_, _, ok = splitQualifiedID(":node")
		require.False(t, ok)
		_, _, ok = splitQualifiedID("not-qualified")
		require.False(t, ok)
	})

	t.Run("defaultDatabaseName panics when storage is not namespaced", func(t *testing.T) {
		db := &DB{storage: storage.NewMemoryEngine()}
		require.Panics(t, func() {
			_ = db.defaultDatabaseName()
		})
	})

	t.Run("kmeansNumClusters defaults to zero with nil config", func(t *testing.T) {
		db := &DB{}
		require.Equal(t, 0, db.kmeansNumClusters())
	})

	t.Run("pending queue helpers guard nil/empty inputs", func(t *testing.T) {
		var nilEntry *dbSearchService
		nilEntry.queueIndex(&storage.Node{ID: "x"})
		nilEntry.queueRemove("x")
		require.Nil(t, nilEntry.drainPending())

		entry := &dbSearchService{}
		entry.queueIndex(nil)
		entry.queueRemove("")
		require.Nil(t, entry.drainPending())

		entry.queueIndex(&storage.Node{ID: "a"})
		entry.queueRemove("a")
		drained := entry.drainPending()
		require.Len(t, drained, 1)
		require.True(t, drained["a"].remove)
	})

	t.Run("GetDatabaseManagedEmbeddingStats reports bytes per database", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(&storage.Node{
			ID:              storage.NodeID("embed-stats-node"),
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"title": "x"},
			ChunkEmbeddings: [][]float32{{1, 0, 0}},
		}))

		count, dims, bytes := db.GetDatabaseManagedEmbeddingStats(db.defaultDatabaseName())
		require.Equal(t, 1, count)
		require.Equal(t, 3, dims)
		require.Equal(t, int64(12), bytes)
	})

	t.Run("shouldIgnoreSearchIndexingError recognizes shutdown errors", func(t *testing.T) {
		db := &DB{}
		require.True(t, db.shouldIgnoreSearchIndexingError(context.Canceled))
		require.True(t, db.shouldIgnoreSearchIndexingError(storage.ErrStorageClosed))
		require.True(t, db.shouldIgnoreSearchIndexingError(ErrClosed))
		require.False(t, db.shouldIgnoreSearchIndexingError(nil))
		require.False(t, db.shouldIgnoreSearchIndexingError(errors.New("boom")))

		db.closed = true
		require.True(t, db.shouldIgnoreSearchIndexingError(errors.New("after close")))
	})

	t.Run("pending flush debounces ready-service mutations", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		db := &DB{buildCtx: context.Background()}
		svc := search.NewServiceWithDimensionsAndBM25Engine(engine, 3, search.DefaultBM25Engine())
		entry := &dbSearchService{
			dbName:            "nornic",
			svc:               svc,
			buildDone:         make(chan struct{}),
			pendingFlushDelay: 20 * time.Millisecond,
		}
		close(entry.buildDone)

		entry.queueIndex(&storage.Node{ID: "debounced", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
		db.ensurePendingFlush(entry)

		time.Sleep(5 * time.Millisecond)
		require.Equal(t, 0, svc.EmbeddingCount())
		require.Eventually(t, func() bool {
			return svc.EmbeddingCount() == 1
		}, time.Second, 10*time.Millisecond)
		db.bgWg.Wait()
	})
}

func TestSearchServices_PerDatabaseIsolation_EventRouting(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringDisabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelBuild()

	// Wait for the per-database search services to finish their initial startup build
	// before injecting event-driven index updates. This test is verifying namespace
	// routing, not the startup warmup path, and a concurrent startup rebuild can
	// otherwise race with IndexNode() and make the expected counts nondeterministic.
	defaultSvc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NoError(t, db.ensureSearchIndexesBuilt(buildCtx, db.defaultDatabaseName()))
	require.Equal(t, 0, defaultSvc.EmbeddingCount())

	db2Svc, err := db.GetOrCreateSearchService("db2", nil)
	require.NoError(t, err)
	require.NoError(t, db.ensureSearchIndexesBuilt(buildCtx, "db2"))
	require.Equal(t, 0, db2Svc.EmbeddingCount())

	// Create and index a node in the default database (nornic).
	alpha := &storage.Node{
		ID:     storage.NodeID("alpha"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "hello alpha",
		},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	}
	_, err = db.storage.CreateNode(alpha)
	require.NoError(t, err)
	db.indexNodeFromEvent(&storage.Node{
		ID:              storage.NodeID("nornic:alpha"),
		Labels:          alpha.Labels,
		Properties:      alpha.Properties,
		ChunkEmbeddings: alpha.ChunkEmbeddings,
	})

	// Create and index a node in another database.
	db2Storage := storage.NewNamespacedEngine(db.baseStorage, "db2")
	beta := &storage.Node{
		ID:     storage.NodeID("beta"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "world beta",
		},
		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
	}
	_, err = db2Storage.CreateNode(beta)
	require.NoError(t, err)
	db.indexNodeFromEvent(&storage.Node{
		ID:              storage.NodeID("db2:beta"),
		Labels:          beta.Labels,
		Properties:      beta.Properties,
		ChunkEmbeddings: beta.ChunkEmbeddings,
	})

	// Default DB service should only contain default DB embedding.
	require.Eventually(t, func() bool {
		return defaultSvc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond)

	// db2 service should exist and contain only db2 embedding.
	require.Eventually(t, func() bool {
		return db2Svc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond)

	// Keep assertion scope focused on event routing/isolation only.
	// Cleanup/removal behavior is covered by dedicated event-removal tests.
}

func TestSearchServices_ResetDropsCache(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.GetOrCreateSearchService("db2", nil)
	require.NoError(t, err)

	db.searchServicesMu.RLock()
	_, exists := db.searchServices["db2"]
	db.searchServicesMu.RUnlock()
	require.True(t, exists)

	db.ResetSearchService("db2")

	db.searchServicesMu.RLock()
	_, exists = db.searchServices["db2"]
	db.searchServicesMu.RUnlock()
	require.False(t, exists)

	// Empty dbName should reset default database service.
	_, err = db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	db.ResetSearchService("")
	db.searchServicesMu.RLock()
	_, exists = db.searchServices[db.defaultDatabaseName()]
	db.searchServicesMu.RUnlock()
	require.False(t, exists)
}

func TestSearchServices_DropSearchServiceState_RemovesPersistedArtifacts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Database.DataDir = t.TempDir()
	cfg.Database.PersistSearchIndexes = true
	db, err := Open(cfg.Database.DataDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	svc, err := db.GetOrCreateSearchService("db_drop_cov", nil)
	require.NoError(t, err)
	require.NotNil(t, svc)

	persistDir := filepath.Join(cfg.Database.DataDir, "search", "db_drop_cov")
	require.NoError(t, os.MkdirAll(persistDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(persistDir, "bm25"), []byte("stale"), 0o644))

	db.DropSearchServiceState("db_drop_cov")

	db.searchServicesMu.RLock()
	_, exists := db.searchServices["db_drop_cov"]
	db.searchServicesMu.RUnlock()
	require.False(t, exists)

	_, statErr := os.Stat(persistDir)
	require.True(t, os.IsNotExist(statErr), "expected persisted search directory to be removed")
}

func TestSearchServices_ClusteringRunnerInitializesKnownNamespaces(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Create a node in a second database without touching the search service cache.
	db2Storage := storage.NewNamespacedEngine(db.baseStorage, "db2")
	_, err = db2Storage.CreateNode(&storage.Node{
		ID:     storage.NodeID("beta"),
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"content": "world beta",
		},
		ChunkEmbeddings: [][]float32{{0.4, 0.5, 0.6}},
	})
	require.NoError(t, err)

	// The clustering runner should discover db2 and initialize a search service for it.
	db.runClusteringOnceAllDatabases(context.Background())

	db.searchServicesMu.RLock()
	_, db2Exists := db.searchServices["db2"]
	_, systemExists := db.searchServices["system"]
	db.searchServicesMu.RUnlock()

	require.True(t, db2Exists)
	require.False(t, systemExists)
}

func TestSearchServices_ClusteringFlagUpgradesCachedService(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringDisabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Create service while clustering is disabled.
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.False(t, svc.IsClusteringEnabled())

	// Enable clustering and run the clustering runner; it should upgrade the cached service.
	enable := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(enable)

	db.runClusteringOnceAllDatabases(context.Background())

	svc, err = db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.True(t, svc.IsClusteringEnabled())
}

func TestSearchServices_RunClusteringOnceAllDatabases_Guards(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Default DB with one embedding and completed build.
	defaultStorage := db.storage
	_, err = defaultStorage.CreateNode(&storage.Node{
		ID:              storage.NodeID("alpha"),
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		Properties: map[string]any{
			"content":   "alpha",
			"embedding": []float32{0.1, 0.2, 0.3},
		},
	})
	require.NoError(t, err)

	buildCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defaultSvc, err := db.EnsureSearchIndexesBuilt(buildCtx, db.defaultDatabaseName(), defaultStorage)
	require.NoError(t, err)
	require.NotNil(t, defaultSvc)
	require.Eventually(t, func() bool { return defaultSvc.EmbeddingCount() >= 1 }, 3*time.Second, 25*time.Millisecond)

	// db2 service exists but intentionally remains unbuilt (not ready).
	db2Svc, err := db.GetOrCreateSearchService("db2", nil)
	require.NoError(t, err)
	require.NotNil(t, db2Svc)
	require.False(t, db2Svc.GetBuildProgress().Ready)

	db.searchServicesMu.RLock()
	defaultEntry := db.searchServices[db.defaultDatabaseName()]
	db2Entry := db.searchServices["db2"]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, defaultEntry)
	require.NotNil(t, db2Entry)

	// Seed non-zero state and ensure the function updates only the built/ready service.
	defaultEntry.clusterMu.Lock()
	defaultEntry.lastClusteredEmbedCount = 0
	defaultEntry.clusterMu.Unlock()
	db2Entry.clusterMu.Lock()
	db2Entry.lastClusteredEmbedCount = 123
	db2Entry.clusterMu.Unlock()

	db.runClusteringOnceAllDatabases(context.Background())
	expectedCount := defaultSvc.EmbeddingCount()

	defaultEntry.clusterMu.Lock()
	defaultAfter := defaultEntry.lastClusteredEmbedCount
	defaultEntry.clusterMu.Unlock()
	db2Entry.clusterMu.Lock()
	db2After := db2Entry.lastClusteredEmbedCount
	db2Entry.clusterMu.Unlock()

	require.Equal(t, expectedCount, defaultAfter, "ready service should update clustered count")
	require.Equal(t, 123, db2After, "not-ready service should be skipped")

	// Canceled context should return immediately without mutating counters.
	defaultEntry.clusterMu.Lock()
	defaultEntry.lastClusteredEmbedCount = 77
	defaultEntry.clusterMu.Unlock()
	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	db.runClusteringOnceAllDatabases(canceledCtx)
	defaultEntry.clusterMu.Lock()
	defer defaultEntry.clusterMu.Unlock()
	require.Equal(t, 77, defaultEntry.lastClusteredEmbedCount)
}

func TestSearchServices_RunClusteringOnceAllDatabases_BranchMatrix(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	if db.buildCancel != nil {
		db.buildCancel()
	}
	db.bgWg.Wait()

	// Add namespace lister branch coverage (skips empty/system namespaces).
	db.baseStorage = &namespaceListEngine{
		Engine:     db.baseStorage,
		namespaces: []string{"", "system", "tenant_from_lister"},
	}

	readyEngine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "ready_cov")
	readySvc := search.NewServiceWithDimensions(readyEngine, 3)
	readySvc.EnableClustering(nil, 2)
	_, err = readyEngine.CreateNode(&storage.Node{
		ID:              "r1",
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		Properties:      map[string]any{"content": "ready"},
	})
	require.NoError(t, err)
	require.NoError(t, readySvc.BuildIndexes(context.Background()))

	disabledSvc := search.NewServiceWithDimensions(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "disabled_cov"), 3)

	notReadySvc := search.NewServiceWithDimensions(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "not_ready_cov"), 3)
	notReadySvc.EnableClustering(nil, 2)

	db.searchServicesMu.Lock()
	db.searchServices["nil_entry_cov"] = nil
	db.searchServices["nil_svc_cov"] = &dbSearchService{dbName: "nil_svc_cov"}
	db.searchServices["system"] = &dbSearchService{dbName: "system", svc: disabledSvc}
	db.searchServices["disabled_cov"] = &dbSearchService{dbName: "disabled_cov", svc: disabledSvc}
	db.searchServices["not_ready_cov"] = &dbSearchService{dbName: "not_ready_cov", svc: notReadySvc, lastClusteredEmbedCount: 11}
	db.searchServices["ready_cov"] = &dbSearchService{
		dbName:                  "ready_cov",
		svc:                     readySvc,
		lastClusteredEmbedCount: readySvc.EmbeddingCount(), // shouldRun=false branch
	}
	db.searchServicesMu.Unlock()

	db.runClusteringOnceAllDatabases(context.Background())

	// not_ready entry should be unchanged because clustering is deferred before ready.
	db.searchServicesMu.RLock()
	notReadyEntry := db.searchServices["not_ready_cov"]
	_, listerCreated := db.searchServices["tenant_from_lister"]
	_, systemCreated := db.searchServices["system"]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, notReadyEntry)
	require.Equal(t, 11, notReadyEntry.lastClusteredEmbedCount)
	require.True(t, listerCreated)
	require.True(t, systemCreated)
}

func TestSearchServices_RunClusteringOnceAllDatabases_NilContext(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NotPanics(t, func() {
		db.runClusteringOnceAllDatabases(context.TODO())
	})
}

func TestSearchServices_SkipsQdrantNamespaceNodes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)

	before := svc.EmbeddingCount()
	db.indexNodeFromEvent(&storage.Node{
		ID: storage.NodeID("nornic:qdrant:bench_col:1"),
		NamedEmbeddings: map[string][]float32{
			"default": {1, 0, 0},
		},
	})
	after := svc.EmbeddingCount()
	require.Equal(t, before, after)
}

func TestSearchServices_EventRemovalAndCreationErrorBranches(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	t.Run("removeNodeFromEvent unprefixed ID falls back to default db", func(t *testing.T) {
		svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)

		node := &storage.Node{
			ID:              "n-local",
			Properties:      map[string]any{"content": "remove me"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		}
		require.NoError(t, svc.IndexNode(node))
		require.Equal(t, 1, svc.EmbeddingCount())

		db.removeNodeFromEvent("n-local")
		require.Eventually(t, func() bool {
			return svc.EmbeddingCount() == 0
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("system database creation is rejected", func(t *testing.T) {
		_, err := db.getOrCreateSearchService("system", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "system database")
	})

	t.Run("indexNodeFromEvent ignores unqualified IDs", func(t *testing.T) {
		svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
		require.NoError(t, err)
		before := svc.EmbeddingCount()

		db.indexNodeFromEvent(&storage.Node{
			ID:              "unqualified",
			Properties:      map[string]any{"content": "ignored"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		})

		require.Equal(t, before, svc.EmbeddingCount())
	})

	t.Run("indexNodeFromEvent tolerates service creation failure", func(t *testing.T) {
		minimal := &DB{
			embeddingDims:       3,
			searchServices:      make(map[string]*dbSearchService),
			searchMinSimilarity: 0.1,
		}
		minimal.indexNodeFromEvent(&storage.Node{
			ID:              "tenant:n1",
			Properties:      map[string]any{"content": "noop"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		})
	})

	t.Run("nil base storage returns deterministic error", func(t *testing.T) {
		minimal := &DB{
			embeddingDims:       3,
			searchServices:      make(map[string]*dbSearchService),
			searchMinSimilarity: 0.1,
		}
		_, err := minimal.getOrCreateSearchService("tenant_cov", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "base storage is nil")
	})
}

// TestRunClusteringOnceAllDatabases_RespectsContextCancellation verifies that
// runClusteringOnceAllDatabases returns promptly when the context is cancelled
// (e.g. on server shutdown). The goroutine must exit so Close() can complete.
func TestRunClusteringOnceAllDatabases_RespectsContextCancellation(t *testing.T) {
	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately so runClusteringOnceAllDatabases exits right away.

	done := make(chan struct{})
	go func() {
		defer close(done)
		db.runClusteringOnceAllDatabases(ctx)
	}()

	select {
	case <-done:
		// Goroutine exited; cancellation was respected.
	case <-time.After(2 * time.Second):
		t.Fatal("runClusteringOnceAllDatabases did not return after context cancellation within 2s")
	}
}

// TestTriggerSearchClustering_DoesNotPanic verifies TriggerSearchClustering
// runs without panicking when buildCtx is set (normal Open path) and when
// clustering is disabled (returns early). Also ensures nil buildCtx is handled
// defensively in code paths that may call TriggerSearchClustering.
func TestTriggerSearchClustering_DoesNotPanic(t *testing.T) {
	t.Run("clustering_disabled_returns_early", func(t *testing.T) {
		cleanup := featureflags.WithGPUClusteringDisabled()
		t.Cleanup(cleanup)

		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		err = db.TriggerSearchClustering()
		require.NoError(t, err)
	})

	t.Run("clustering_enabled_uses_buildCtx", func(t *testing.T) {
		cleanup := featureflags.WithGPUClusteringEnabled()
		t.Cleanup(cleanup)

		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		require.NotNil(t, db.buildCtx, "Open() should set buildCtx so clustering can be cancelled on Close()")
		err = db.TriggerSearchClustering()
		require.NoError(t, err)
	})
}

func TestSearchServices_RerankerStatusAndBuildStartHelpers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Explicitly store odd cache entries so setter loop skips nils safely.
	db.searchServicesMu.Lock()
	db.searchServices["nil_entry"] = nil
	db.searchServices["nil_svc"] = &dbSearchService{}
	db.searchServicesMu.Unlock()

	// Global reranker setter should tolerate nil/empty entries.
	db.SetSearchReranker(nil)

	// Resolver should be consulted for new DB services.
	var calledMu sync.Mutex
	called := make(map[string]int)
	db.SetRerankerResolver(func(dbName string) search.Reranker {
		calledMu.Lock()
		called[dbName]++
		calledMu.Unlock()
		return nil
	})

	svc, err := db.GetOrCreateSearchService("tenant_cov", nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
	calledMu.Lock()
	tenantCalls := called["tenant_cov"]
	calledMu.Unlock()
	require.GreaterOrEqual(t, tenantCalls, 1, "reranker resolver must be consulted for tenant_cov")

	// Not initialized path: missing entry and nil-svc entry both report not_initialized.
	missing := db.GetDatabaseSearchStatus("missing_cov")
	require.False(t, missing.Ready)
	require.False(t, missing.Building)
	require.False(t, missing.Initialized)
	require.Equal(t, "not_initialized", missing.Phase)
	require.Equal(t, int64(-1), missing.ETASeconds)

	nilSvc := db.GetDatabaseSearchStatus("nil_svc")
	require.False(t, nilSvc.Initialized)
	require.Equal(t, "not_initialized", nilSvc.Phase)

	// Start build without waiting; helper should return service and initialize status.
	startedSvc, err := db.EnsureSearchIndexesBuildStarted("tenant_cov", nil)
	require.NoError(t, err)
	require.Same(t, svc, startedSvc)

	// Ensure completion so this test does not leak in-flight builders.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, "tenant_cov"))

	ready := db.GetDatabaseSearchStatus("tenant_cov")
	require.True(t, ready.Initialized)
}

func TestSearchServices_StartBuildDefaultAndClosedBranches(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Empty dbName should resolve to default database.
	svc, err := db.EnsureSearchIndexesBuildStarted("", db.storage)
	require.NoError(t, err)
	require.NotNil(t, svc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, db.defaultDatabaseName()))

	// Closed DB should reject public getter.
	require.NoError(t, db.Close())
	_, err = db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.ErrorIs(t, err, ErrClosed)
}

func TestSearchServices_EnsureBuilt_ContextDonePath(t *testing.T) {
	db := &DB{
		storage:             storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"),
		searchServices:      make(map[string]*dbSearchService),
		embeddingDims:       3,
		searchMinSimilarity: 0.1,
	}
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant_timeout")
	svc := search.NewServiceWithDimensions(engine, 3)

	entry := &dbSearchService{
		dbName:    "tenant_timeout",
		svc:       svc,
		buildDone: make(chan struct{}),
	}
	// Pre-consume buildOnce so startSearchIndexBuild does not launch a goroutine.
	entry.buildOnce.Do(func() {})
	db.searchServices["tenant_timeout"] = entry

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := db.ensureSearchIndexesBuilt(ctx, "tenant_timeout")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestSearchServices_EnsureBuiltAndStart_ErrorBranches(t *testing.T) {
	t.Run("ensureSearchIndexesBuilt returns not initialized error for missing service", func(t *testing.T) {
		db := &DB{
			storage:        storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic"),
			searchServices: map[string]*dbSearchService{},
		}

		err := db.ensureSearchIndexesBuilt(context.Background(), "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "search service not initialized")
	})

	t.Run("EnsureSearchIndexesBuildStarted rejects system database", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		_, err = db.EnsureSearchIndexesBuildStarted("system", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "system database")
	})
}

func TestSearchServices_CloseBuildDone_IsIdempotent(t *testing.T) {
	entry := &dbSearchService{buildDone: make(chan struct{})}
	entry.closeBuildDone()
	require.NotPanics(t, func() {
		entry.closeBuildDone()
	})
}

func TestSearchServices_EnsurePendingFlush_ReplaysQueuedOps(t *testing.T) {
	db := &DB{}
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant_flush")
	svc := search.NewServiceWithDimensions(engine, 3)

	entry := &dbSearchService{
		dbName:    "tenant_flush",
		svc:       svc,
		buildDone: make(chan struct{}),
	}

	// Queue operations before build completes:
	// - remove op (should be tolerated even when missing)
	// - nil node op (should be skipped)
	// - valid index op (should be applied)
	entry.pendingMu.Lock()
	entry.pendingOps = map[string]pendingSearchMutation{
		"missing": {remove: true},
		"nil-op":  {node: nil, remove: false},
		"n1": {
			node: &storage.Node{
				ID:              "n1",
				Properties:      map[string]any{"content": "flush"},
				ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
			},
		},
	}
	entry.pendingMu.Unlock()

	db.ensurePendingFlush(entry)
	close(entry.buildDone)

	require.Eventually(t, func() bool {
		return svc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSearchServices_GetOrCreate_ResolverAndPersistPaths(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Database.DataDir = t.TempDir()
	cfg.Database.PersistSearchIndexes = true
	db, err := Open(cfg.Database.DataDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Resolver should override dimensions and BM25 engine selection.
	db.SetDbConfigResolver(func(dbName string) (embeddingDims int, searchMinSimilarity float64, bm25Engine string) {
		if dbName == "tenant_cfg" {
			return 7, 0.42, search.BM25EngineV1
		}
		return 0, 0, ""
	})

	svc, err := db.getOrCreateSearchService("", db.storage) // empty name -> default db branch
	require.NoError(t, err)
	require.NotNil(t, svc)

	tenantSvc, err := db.getOrCreateSearchService("tenant_cfg", nil)
	require.NoError(t, err)
	require.NotNil(t, tenantSvc)
	require.Equal(t, 7, tenantSvc.VectorIndexDimensions())

	// Persist path branch: BM25 v1 should use "bm25" filename.
	tenantStorage := storage.NewNamespacedEngine(db.baseStorage, "tenant_cfg")
	_, err = tenantStorage.CreateNode(&storage.Node{
		ID:              "persist-1",
		Properties:      map[string]any{"content": "persist me"},
		ChunkEmbeddings: [][]float32{{1, 0, 0, 0, 0, 0, 0}},
	})
	require.NoError(t, err)
	_, err = db.EnsureSearchIndexesBuilt(context.Background(), "tenant_cfg", tenantStorage)
	require.NoError(t, err)

	bm25v1Path := filepath.Join(cfg.Database.DataDir, "search", "tenant_cfg", "bm25")
	_, err = os.Stat(bm25v1Path)
	require.NoError(t, err)
}

func TestSearchServices_EventDuringBuild_IsDeterministicallyReplayed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Use isolated storage for deterministic replay assertions. Using db.baseStorage
	// can introduce unrelated async mutation callbacks that race this test.
	tenantStorage := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant_race")
	blocking := &blockingIterEngine{
		Engine:  tenantStorage,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	_, err = db.EnsureSearchIndexesBuildStarted("tenant_race", blocking)
	require.NoError(t, err)

	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("build did not enter iterator in time")
	}

	// Create node while build is still blocked and emit event.
	_, err = tenantStorage.CreateNode(&storage.Node{
		ID:              "late",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "late"},
		ChunkEmbeddings: [][]float32{{0.2, 0.3, 0.4}},
	})
	require.NoError(t, err)
	db.indexNodeFromEvent(&storage.Node{
		ID:              "tenant_race:late",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "late"},
		ChunkEmbeddings: [][]float32{{0.2, 0.3, 0.4}},
	})

	// Build still blocked, event should be deferred.
	svc, err := db.GetOrCreateSearchService("tenant_race", blocking)
	require.NoError(t, err)
	require.Equal(t, 0, svc.EmbeddingCount())

	close(blocking.release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, "tenant_race"))

	// Deferred event mutation must be replayed after build completion.
	require.Eventually(t, func() bool {
		return svc.EmbeddingCount() >= 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSearchServices_RemoveEventDuringBuild_IsDeterministicallyReplayed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Use isolated storage for deterministic replay assertions. Using db.baseStorage
	// can introduce unrelated async mutation callbacks that race this test.
	tenantStorage := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant_remove_race")
	blocking := &blockingIterEngine{
		Engine:  tenantStorage,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}

	// Seed a node and start a blocked build so remove event is queued.
	_, err = tenantStorage.CreateNode(&storage.Node{
		ID:              "gone",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "gone"},
		ChunkEmbeddings: [][]float32{{0.2, 0.3, 0.4}},
	})
	require.NoError(t, err)
	_, err = db.EnsureSearchIndexesBuildStarted("tenant_remove_race", blocking)
	require.NoError(t, err)

	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("build did not enter iterator in time")
	}

	svc, err := db.GetOrCreateSearchService("tenant_remove_race", blocking)
	require.NoError(t, err)

	// Queue remove while build is blocked.
	db.removeNodeFromEvent("tenant_remove_race:gone")
	require.Equal(t, 0, svc.EmbeddingCount())

	close(blocking.release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, "tenant_remove_race"))

	// Deferred remove must be replayed after build completion.
	require.Eventually(t, func() bool {
		return svc.EmbeddingCount() == 0
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSearchServices_ManualModeSkipsEventTriggeredBuildUntilExplicitStart(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Database.SearchIndexBuildMode = featureflags.SearchIndexBuildModeManual
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tenantStorage := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant_manual")
	blocking := &blockingIterEngine{
		Engine:  tenantStorage,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	releaseOnce := sync.Once{}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(blocking.release) })
	})

	svc, err := db.GetOrCreateSearchService("tenant_manual", blocking)
	require.NoError(t, err)
	_, err = tenantStorage.CreateNode(&storage.Node{
		ID:              "late",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "late manual"},
		ChunkEmbeddings: [][]float32{{0.2, 0.3, 0.4}},
	})
	require.NoError(t, err)

	db.indexNodeFromEvent(&storage.Node{
		ID:              "tenant_manual:late",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "late manual"},
		ChunkEmbeddings: [][]float32{{0.2, 0.3, 0.4}},
	})

	select {
	case <-blocking.entered:
		t.Fatal("manual mode must not start search index build from mutation events")
	case <-time.After(350 * time.Millisecond):
	}
	require.Equal(t, 0, svc.EmbeddingCount())

	_, err = db.EnsureSearchIndexesBuildStarted("tenant_manual", blocking)
	require.NoError(t, err)
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("explicit manual build did not enter iterator in time")
	}
	releaseOnce.Do(func() { close(blocking.release) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, "tenant_manual"))
	require.Eventually(t, func() bool {
		return svc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSearchServices_DisabledModeRejectsExplicitBuilds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Database.SearchIndexBuildMode = featureflags.SearchIndexBuildModeDisabled
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.EnsureSearchIndexesBuildStarted("", db.storage)
	require.ErrorIs(t, err, ErrSearchIndexBuildDisabled)

	_, err = db.EnsureSearchIndexesBuilt(context.Background(), "", db.storage)
	require.ErrorIs(t, err, ErrSearchIndexBuildDisabled)

	err = db.BuildSearchIndexes(context.Background())
	require.ErrorIs(t, err, ErrSearchIndexBuildDisabled)
}
