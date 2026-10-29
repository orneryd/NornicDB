package nornicdb

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// seedReservedSearchRefresh supplies already-reserved work to tests of the
// pending queue and drain, without scheduling a separate event flush.
func seedReservedSearchRefresh(entry *dbSearchService, id string, work search.RefreshRetry) {
	entry.pendingMu.Lock()
	defer entry.pendingMu.Unlock()
	entry.queueReservedRefreshLocked(id, work)
}

func TestMissingStemmerDisablesOnlyBM25(t *testing.T) {
	db, err := Open(t.TempDir(), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetDbSearchOptionsResolver(func(string) search.ServiceOptions {
		return search.ServiceOptions{BM25StemmerID: "snowball.missing"}
	})
	db.SetDbSearchFlagsResolver(func(string) (bool, bool, string, string) {
		return true, true, "startup", "startup"
	})
	db.ResetSearchService(db.defaultDatabaseName())

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
	require.NoError(t, err)
	require.False(t, svc.BM25Enabled())
	require.True(t, svc.VectorEnabled())
	_, found := stemmer.Lookup("snowball.missing")
	require.False(t, found)
}

func TestManagedSearchServiceBindsContinuationToItsDatabaseWhenRequestOmitsDatabase(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.SearchCursorMax = 8
	cfg.Database.AsyncWritesEnabled = false
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for _, id := range []storage.NodeID{"continuation-a", "continuation-b"} {
		_, err := db.storage.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	service, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	page, err := service.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(),
		search.SearchContinuationRequest{Owner: "anonymous", Mode: search.SearchContinuationID, N: 1},
		nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.NotEmpty(t, page.QID)

	boundDatabase, err := db.ResolveSearchContinuationDatabase("anonymous", page.QID, "")
	require.NoError(t, err)
	require.Equal(t, db.defaultDatabaseName(), boundDatabase)
}

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

func TestSearchPersistenceBasePathRejectsInvalidDatabaseNames(t *testing.T) {
	dataDir := t.TempDir()
	for _, name := range []string{"", ".", "..", "../escape", "nested/name", `nested\name`, "/absolute", "nul\x00name"} {
		t.Run(name, func(t *testing.T) {
			_, err := searchPersistenceBasePath(dataDir, name)
			require.Error(t, err)
		})
	}

	path, err := searchPersistenceBasePath(dataDir, "tenant-1")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dataDir, "search", "tenant-1"), path)
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
		db := &DB{storage: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))}
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
		nodes, edges := nilEntry.drainPending()
		require.Nil(t, nodes)
		require.Nil(t, edges)

		entry := &dbSearchService{svc: search.NewService(storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "test"), 3, nil)}
		nodes, edges = entry.drainPending()
		require.Nil(t, nodes)
		require.Nil(t, edges)

		seedReservedSearchRefresh(entry, "a", search.RefreshRetry{Node: entry.svc.ReserveNodeMutation("a")})
		newest := entry.svc.ReserveNodeMutation("a")
		seedReservedSearchRefresh(entry, "a", search.RefreshRetry{Node: newest})
		nodes, _ = entry.drainPending()
		require.Equal(t, map[string]search.NodeMutation{"a": newest}, nodes)
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
		engine := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { _ = engine.Close() })
		store := storage.NewNamespacedEngine(engine, "nornic")
		db := &DB{buildCtx: context.Background()}
		svc := search.NewService(store, 3, nil)
		entry := &dbSearchService{
			dbName:            "nornic",
			svc:               svc,
			buildDone:         make(chan struct{}),
			pendingFlushDelay: 20 * time.Millisecond,
		}
		close(entry.buildDone)

		_, err := store.CreateNode(&storage.Node{ID: "debounced", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
		require.NoError(t, err)
		seedReservedSearchRefresh(entry, "debounced", search.RefreshRetry{Node: entry.svc.ReserveNodeMutation("debounced")})
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

func TestSearchStatusReportsFailedBuild(t *testing.T) {
	db, err := Open("", DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dbName := db.defaultDatabaseName()
	_, err = db.GetOrCreateSearchService(dbName, db.storage)
	require.NoError(t, err)
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	entry.buildErrMu.Lock()
	entry.buildErr = errors.New("index rebuild failed")
	entry.buildErrMu.Unlock()
	status := db.GetDatabaseSearchStatus(dbName)
	require.Equal(t, "failed", status.Phase)
	require.Equal(t, "index rebuild failed", status.Error)
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

func openClusteringTestCache(t *testing.T) *DB {
	t.Helper()

	cleanup := featureflags.WithGPUClusteringEnabled()
	t.Cleanup(cleanup)

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSearchServices_RunClusteringOnceAllDatabases(t *testing.T) {
	t.Run("guards", func(t *testing.T) {
		db := openClusteringTestCache(t)

		// Default DB with one embedding and completed build.
		defaultStorage := db.storage
		_, err := defaultStorage.CreateNode(&storage.Node{
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
		defaultSvc, err := db.EnsureSearchIndexes(buildCtx, db.defaultDatabaseName(), defaultStorage, SearchIndexBuildWait)
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
	})
	t.Run("branch matrix", func(t *testing.T) {
		db := openClusteringTestCache(t)
		if db.buildCancel != nil {
			db.buildCancel()
		}
		db.bgWg.Wait()

		// Add namespace lister branch coverage (skips empty/system namespaces).
		db.baseStorage = &namespaceListEngine{
			Engine:     db.baseStorage,
			namespaces: []string{"", "system", "tenant_from_lister"},
		}

		readyEngine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "ready_cov")
		readySvc := search.NewService(readyEngine, 3, nil)
		readySvc.EnableClustering(nil, 2)
		_, err := readyEngine.CreateNode(&storage.Node{
			ID:              "r1",
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
			Properties:      map[string]any{"content": "ready"},
		})
		require.NoError(t, err)
		require.NoError(t, readySvc.BuildIndexes(context.Background()))

		disabledSvc := search.NewService(storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "disabled_cov"), 3, nil)

		notReadySvc := search.NewService(storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "not_ready_cov"), 3, nil)
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
	})
	t.Run("nil and TODO context", func(t *testing.T) {
		db := openClusteringTestCache(t)

		require.NotPanics(t, func() {
			db.runClusteringOnceAllDatabases(nil)
		})
		require.NotPanics(t, func() {
			db.runClusteringOnceAllDatabases(context.TODO())
		})
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
		// A write during the startup build is applied by the build; this
		// case counts entries right after the write.
		_, err = db.EnsureSearchIndexes(context.Background(), db.defaultDatabaseName(), nil, SearchIndexBuildWait)
		require.NoError(t, err)

		node := &storage.Node{
			ID:              "n-local",
			Properties:      map[string]any{"content": "remove me"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		}
		require.NoError(t, svc.IndexNode(node))
		require.Equal(t, 1, svc.EmbeddingCount())

		db.queueSearchRefreshEvent(searchNodeDelete, "n-local")
		require.Eventually(t, func() bool {
			return svc.EmbeddingCount() == 0
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("system database creation is rejected", func(t *testing.T) {
		_, err := db.GetOrCreateSearchService("system", nil)
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
		_, err := minimal.GetOrCreateSearchService("tenant_cov", nil)
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
	startedSvc, err := db.EnsureSearchIndexes(nil, "tenant_cov", nil, SearchIndexBuildStart)
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
	svc, err := db.EnsureSearchIndexes(nil, "", db.storage, SearchIndexBuildStart)
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

// TestSearchServices_ClosedDBCannotCreateService pins the public creation
// fence: once Close has begun, GetOrCreateSearchService rejects creation of
// both the default database's service and a tenant database's service instead
// of building a new service against closing storage.
func TestSearchServices_ClosedDBCannotCreateService(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Memory.EmbeddingEnabled = false
	db, err := Open("", cfg)
	require.NoError(t, err)

	defaultName := db.defaultDatabaseName()
	if db.buildCancel != nil {
		db.buildCancel()
	}
	db.bgWg.Wait()
	db.ResetSearchService(defaultName)
	db.ResetSearchService("tenant_closed")
	require.NoError(t, db.Close())

	_, err = db.GetOrCreateSearchService(defaultName, db.storage)
	require.ErrorIs(t, err, ErrClosed, "a closed database must not create the default search service")

	_, err = db.GetOrCreateSearchService("tenant_closed", nil)
	require.ErrorIs(t, err, ErrClosed, "a closed database must not create a tenant search service")

	db.searchServicesMu.RLock()
	_, defaultEntry := db.searchServices[defaultName]
	_, tenantEntry := db.searchServices["tenant_closed"]
	db.searchServicesMu.RUnlock()
	require.False(t, defaultEntry, "the rejected default lookup must not cache a service")
	require.False(t, tenantEntry, "the rejected tenant lookup must not cache a service")
}

func TestSearchServices_EnsureBuilt_ContextDonePath(t *testing.T) {
	db := &DB{
		storage:             storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic"),
		searchServices:      make(map[string]*dbSearchService),
		embeddingDims:       3,
		searchMinSimilarity: 0.1,
	}
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "tenant_timeout")
	svc := search.NewService(engine, 3, nil)

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
			storage:        storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic"),
			searchServices: map[string]*dbSearchService{},
		}

		err := db.ensureSearchIndexesBuilt(context.Background(), "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "search service not initialized")
	})

	t.Run("EnsureSearchIndexes rejects system database", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Memory.EmbeddingDimensions = 3
		db, err := Open("", cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		_, err = db.EnsureSearchIndexes(nil, "system", nil, SearchIndexBuildStart)
		require.Error(t, err)
		require.Contains(t, err.Error(), "system database")
	})
}

// TestSearchServices_EnsureIndexesModes pins the typed build mode shared by
// every search-index build entry point: SearchIndexBuildStart returns once the
// build has started, and SearchIndexBuildWait waits for the build that the same
// service completes and reports its error.
func TestSearchServices_EnsureIndexesModes(t *testing.T) {
	start := func(t *testing.T) (*DB, *dbSearchService, string) {
		t.Helper()
		db := &DB{
			storage:             storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic"),
			searchServices:      make(map[string]*dbSearchService),
			embeddingDims:       3,
			searchMinSimilarity: 0.1,
			buildCtx:            context.Background(),
			config:              DefaultConfig(),
		}
		db.SetDbSearchFlagsResolver(func(string) (bool, bool, string, string) {
			return true, false, "startup", "startup"
		})
		const dbName = "tenant_modes"
		engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), dbName)
		entry := &dbSearchService{
			dbName:    dbName,
			svc:       search.NewService(engine, 3, nil),
			buildDone: make(chan struct{}),
		}
		db.searchServices[dbName] = entry
		return db, entry, dbName
	}

	t.Run("start returns without waiting for the build", func(t *testing.T) {
		db, entry, dbName := start(t)
		// A pre-consumed Once keeps the build from running, so the buildDone
		// signal stays open: only a mode that does not wait can return.
		entry.buildOnce.Do(func() {})
		svc, err := db.EnsureSearchIndexes(nil, dbName, nil, SearchIndexBuildStart)
		require.NoError(t, err)
		require.Same(t, entry.svc, svc)
		require.False(t, svc.GetBuildProgress().Ready)
	})

	t.Run("wait reports the build that a prior start initiated", func(t *testing.T) {
		db, entry, dbName := start(t)
		_, err := db.EnsureSearchIndexes(nil, dbName, nil, SearchIndexBuildStart)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		svc, err := db.EnsureSearchIndexes(ctx, dbName, nil, SearchIndexBuildWait)
		require.NoError(t, err)
		require.Same(t, entry.svc, svc)
		require.True(t, svc.GetBuildProgress().Ready)

		entry.buildErrMu.RLock()
		buildErr := entry.buildErr
		entry.buildErrMu.RUnlock()
		require.NoError(t, buildErr)
	})

	t.Run("wait reports the failed build's error", func(t *testing.T) {
		db, entry, dbName := start(t)
		buildErr := errors.New("index build failed")
		entry.buildOnce.Do(func() {
			entry.buildErrMu.Lock()
			entry.buildErr = buildErr
			entry.buildErrMu.Unlock()
			entry.closeBuildDone()
		})

		svc, err := db.EnsureSearchIndexes(context.Background(), dbName, nil, SearchIndexBuildWait)
		require.ErrorIs(t, err, buildErr)
		require.Same(t, entry.svc, svc)
	})

	t.Run("wait returns the caller context error while the build runs", func(t *testing.T) {
		db, entry, dbName := start(t)
		entry.buildOnce.Do(func() {})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		svc, err := db.EnsureSearchIndexes(ctx, dbName, nil, SearchIndexBuildWait)
		require.ErrorIs(t, err, context.Canceled)
		require.Same(t, entry.svc, svc)
	})

	t.Run("unknown mode is rejected without starting a build", func(t *testing.T) {
		for _, state := range []string{"startup", "disabled", "lazy"} {
			t.Run(state, func(t *testing.T) {
				db, entry, dbName := start(t)
				db.SetDbSearchFlagsResolver(func(string) (bool, bool, string, string) {
					return state != "disabled", false, state, "startup"
				})
				svc, err := db.EnsureSearchIndexes(context.Background(), dbName, nil, SearchIndexBuildMode(99))
				require.ErrorContains(t, err, "unsupported search index build mode 99")
				require.Nil(t, svc)
				require.False(t, entry.svc.GetBuildProgress().Ready)
				require.False(t, entry.svc.GetBuildProgress().Building)
			})
		}
	})
}

func TestSearchServices_OffLazyStatsAndRemoveBranches(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	db := &DB{
		storage:             storage.NewNamespacedEngine(base, "nornic"),
		baseStorage:         base,
		config:              DefaultConfig(),
		searchServices:      make(map[string]*dbSearchService),
		embeddingDims:       3,
		searchMinSimilarity: 0.2,
		buildCtx:            context.Background(),
	}

	db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) {
		if dbName == "off_cov" {
			return false, false, "startup", "startup"
		}
		return true, true, "lazy", "lazy"
	})

	svc, err := db.EnsureSearchIndexes(context.Background(), "off_cov", nil, SearchIndexBuildWait)
	require.NoError(t, err)
	require.True(t, svc.GetBuildProgress().Ready)
	status := db.GetDatabaseSearchStatus("off_cov")
	require.True(t, status.Ready)
	require.False(t, status.BM25Enabled)
	require.False(t, status.VectorEnabled)
	require.False(t, status.LazyTriggerNeeded)

	lazySvc, err := db.EnsureSearchIndexes(context.Background(), "lazy_cov", nil, SearchIndexBuildWait)
	require.NoError(t, err)
	require.NotNil(t, lazySvc)
	lazyStatus := db.GetDatabaseSearchStatus("lazy_cov")
	require.True(t, lazyStatus.Initialized)
	require.False(t, lazyStatus.Ready)
	require.True(t, lazyStatus.LazyTriggerNeeded)

	startedLazy, err := db.EnsureSearchIndexes(nil, "lazy_started_cov", nil, SearchIndexBuildStart)
	require.NoError(t, err)
	require.NotNil(t, startedLazy)
	require.False(t, startedLazy.GetBuildProgress().Ready)

	require.NoError(t, search.RefreshIndexes(context.Background(), dbRefreshScope{db: db, dbName: "lazy_cov"}, search.RefreshRequest{}))
	err = search.RefreshIndexes(context.Background(), dbRefreshScope{db: db, dbName: "system"}, search.RefreshRequest{Node: "node"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "system database")

	count, dims, bytes := db.GetDatabaseManagedEmbeddingStats("")
	require.Zero(t, count)
	require.Equal(t, 3, dims)
	require.Zero(t, bytes)
	count, dims, bytes = db.GetDatabaseManagedEmbeddingStats("system")
	require.Zero(t, count)
	require.Zero(t, dims)
	require.Zero(t, bytes)

	broken := &DB{storage: storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic"), searchServices: make(map[string]*dbSearchService)}
	count, dims, bytes = broken.GetDatabaseManagedEmbeddingStats("tenant_without_base")
	require.Zero(t, count)
	require.Zero(t, dims)
	require.Zero(t, bytes)
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
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "tenant_flush")
	svc := search.NewService(engine, 3, nil)

	entry := &dbSearchService{
		dbName:    "tenant_flush",
		svc:       svc,
		buildDone: make(chan struct{}),
	}

	// Queue refreshes before the build completes: one for a node that is
	// missing (tolerated) and one for a stored node (applied).
	_, err := engine.CreateNode(&storage.Node{ID: "n1", Properties: map[string]any{"content": "flush"}, ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}}})
	require.NoError(t, err)
	seedReservedSearchRefresh(entry, "missing", search.RefreshRetry{Node: entry.svc.ReserveNodeMutation("missing")})
	seedReservedSearchRefresh(entry, "n1", search.RefreshRetry{Node: entry.svc.ReserveNodeMutation("n1")})

	db.ensurePendingFlush(entry)
	close(entry.buildDone)

	require.Eventually(t, func() bool {
		return svc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond)
}

func TestSearchServices_LazyWriteEventDoesNotStartIndexBuild(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchVectorEnabled = true
	cfg.Memory.SearchVectorWarming = "lazy"
	cfg.Database.AsyncWritesEnabled = false

	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const dbName = "tenant_lazy_ingest"
	// The write and the event storage publishes for it.
	_, err = storage.NewNamespacedEngine(db.baseStorage, dbName).CreateNode(&storage.Node{
		ID:              "n1",
		Labels:          []string{"Entity"},
		Properties:      map[string]any{"name": "alice"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	})
	require.NoError(t, err)
	db.indexNodeFromEvent(&storage.Node{ID: storage.NodeID(dbName + ":n1")})

	svc, err := db.GetOrCreateSearchService(dbName, nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, entry)
	entry.pendingFlushMu.Lock()
	require.False(t, entry.pendingFlushRunning, "lazy-cold writes should not start a flush worker")
	require.Nil(t, entry.pendingFlushTimer, "lazy-cold writes should not allocate a flush timer")
	entry.pendingFlushMu.Unlock()

	progress := svc.GetBuildProgress()
	require.False(t, progress.Building, "lazy write events must not start BuildIndexes")
	require.False(t, progress.Ready, "lazy vector search stays cold until the first read")
	require.Equal(t, 0, svc.EmbeddingCount(), "queued lazy writes must not populate ANN indexes before warmup")

	status := db.GetDatabaseSearchStatus(dbName)
	require.True(t, status.LazyTriggerNeeded, "first read should remain the lazy warm trigger")

	require.True(t, entry.hasPending(), "write mutation should remain queued for post-build replay")
	entry.pendingFlushMu.Lock()
	require.False(t, entry.pendingFlushRunning, "lazy-cold writes should not keep a flush worker running")
	require.Nil(t, entry.pendingFlushTimer, "lazy-cold writes should not reschedule flush timers until warmup starts")
	entry.pendingFlushMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, dbName))
	require.Eventually(t, func() bool {
		return svc.EmbeddingCount() == 1
	}, 5*time.Second, 10*time.Millisecond, "lazy build should replay queued write mutations after warmup")
}

func TestSearchServiceReceivesDatabaseIndexCapacityPolicy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	db.SetDbSearchOptionsResolver(func(dbName string) search.ServiceOptions {
		return search.ServiceOptions{
			DatabaseID:             dbName,
			BM25MemoryMaxBytes:     1 << 20,
			VectorMemoryMaxBytes:   2 << 20,
			MetadataMemoryMaxBytes: 3 << 20,
			BM25StorageMode:        "memory",
			VectorStorageMode:      "memory",
		}
	})

	service, err := db.GetOrCreateSearchService("capacity", nil)
	require.NoError(t, err)
	bm25Max, vectorMax, metadataMax, bm25Storage, vectorStorage := service.IndexCapacityPolicy()
	require.Equal(t, int64(1<<20), bm25Max)
	require.Equal(t, int64(2<<20), vectorMax)
	require.Equal(t, int64(3<<20), metadataMax)
	require.Equal(t, "memory", bm25Storage)
	require.Equal(t, "memory", vectorStorage)
}

func TestSearchServices_GetOrCreate_ResolverAndPersistPaths(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Database.DataDir = t.TempDir()
	cfg.Database.PersistSearchIndexes = true
	db, err := Open(cfg.Database.DataDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Resolver should override dimensions and minimum similarity.
	db.SetDbConfigResolver(func(dbName string) (embeddingDims int, searchMinSimilarity float64) {
		if dbName == "tenant_cfg" {
			return 7, 0.42
		}
		return 0, 0
	})

	svc, err := db.GetOrCreateSearchService("", db.storage) // empty name -> default db branch
	require.NoError(t, err)
	require.NotNil(t, svc)

	tenantSvc, err := db.GetOrCreateSearchService("tenant_cfg", nil)
	require.NoError(t, err)
	require.NotNil(t, tenantSvc)
	require.Equal(t, 7, tenantSvc.VectorIndexDimensions())

	// The full-text index persists under its current filename.
	tenantStorage := storage.NewNamespacedEngine(db.baseStorage, "tenant_cfg")
	_, err = tenantStorage.CreateNode(&storage.Node{
		ID:              "persist-1",
		Properties:      map[string]any{"content": "persist me"},
		ChunkEmbeddings: [][]float32{{1, 0, 0, 0, 0, 0, 0}},
	})
	require.NoError(t, err)
	_, err = db.EnsureSearchIndexes(context.Background(), "tenant_cfg", tenantStorage, SearchIndexBuildWait)
	require.NoError(t, err)

	// The current generation persists under the directory CURRENT names.
	base := filepath.Join(cfg.Database.DataDir, "search", "tenant_cfg")
	token, err := readSearchStateToken(filepath.Join(base, searchStateCurrentFile))
	require.NoError(t, err)
	fulltextPath := filepath.Join(base, token, "bm25.v2")
	_, err = os.Stat(fulltextPath)
	require.NoError(t, err)
}

// TestSearchServices_EventDuringBuild_IsDeterministicallyReplay drives one
// isolated build-blocked replay per event kind through a single harness, so the
// deterministic start/hold/release protocol is written once while each kind
// keeps its own seeding, event, and post-replay assertion.
func TestSearchServices_EventDuringBuild_IsDeterministicallyReplay(t *testing.T) {
	testCases := []struct {
		name                string
		dbName              string
		seed                bool
		mutateBeforeService bool
		mutate              func(t *testing.T, db *DB, tenantStorage storage.Engine)
		check               func(t *testing.T, svc *search.Service)
	}{
		{
			name:                "index event is deferred then replayed",
			dbName:              "tenant_race",
			mutateBeforeService: true,
			mutate: func(t *testing.T, db *DB, tenantStorage storage.Engine) {
				// Create node while build is still blocked and emit event.
				_, err := tenantStorage.CreateNode(&storage.Node{
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
			},
			check: func(t *testing.T, svc *search.Service) {
				// Deferred event mutation must be replayed after build completion.
				require.Eventually(t, func() bool {
					return svc.EmbeddingCount() >= 1
				}, 5*time.Second, 10*time.Millisecond)
			},
		},
		{
			name:   "remove event is deferred then replayed",
			dbName: "tenant_remove_race",
			seed:   true,
			mutate: func(t *testing.T, db *DB, tenantStorage storage.Engine) {
				// Delete and queue its remove event while build is blocked.
				require.NoError(t, tenantStorage.DeleteNode("gone"))
				db.queueSearchRefreshEvent(searchNodeDelete, "tenant_remove_race:gone")
			},
			check: func(t *testing.T, svc *search.Service) {
				// Deferred remove must be replayed after build completion.
				require.Eventually(t, func() bool {
					return svc.EmbeddingCount() == 0
				}, 5*time.Second, 10*time.Millisecond)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			cfg := DefaultConfig()
			cfg.Memory.EmbeddingDimensions = 3
			db, err := Open("", cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			// Use isolated storage for deterministic replay assertions. Using db.baseStorage
			// can introduce unrelated async mutation callbacks that race this test.
			tenantStorage := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), tc.dbName)
			blocking := &blockingIterEngine{
				Engine:  tenantStorage,
				entered: make(chan struct{}),
				release: make(chan struct{}),
			}

			if tc.seed {
				// Seed a node and start a blocked build so remove event is queued.
				_, err = tenantStorage.CreateNode(&storage.Node{
					ID:              "gone",
					Labels:          []string{"Doc"},
					Properties:      map[string]any{"content": "gone"},
					ChunkEmbeddings: [][]float32{{0.2, 0.3, 0.4}},
				})
				require.NoError(t, err)
			}
			_, err = db.EnsureSearchIndexes(nil, tc.dbName, blocking, SearchIndexBuildStart)
			require.NoError(t, err)

			select {
			case <-blocking.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("build did not enter iterator in time")
			}

			if tc.mutateBeforeService {
				tc.mutate(t, db, tenantStorage)
			}
			svc, err := db.GetOrCreateSearchService(tc.dbName, blocking)
			require.NoError(t, err)

			if !tc.mutateBeforeService {
				tc.mutate(t, db, tenantStorage)
			}

			// Build still blocked, event should be deferred.
			require.Equal(t, 0, svc.EmbeddingCount())

			close(blocking.release)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, db.ensureSearchIndexesBuilt(ctx, tc.dbName))

			tc.check(t, svc)
		})
	}
}
