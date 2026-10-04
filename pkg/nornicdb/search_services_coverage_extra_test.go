package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newCoverageSearchDB() *DB {
	base := storage.NewMemoryEngine()
	return &DB{
		storage:             storage.NewNamespacedEngine(base, "nornic"),
		baseStorage:         base,
		config:              DefaultConfig(),
		embeddingDims:       3,
		searchMinSimilarity: 0.1,
		searchServices:      make(map[string]*dbSearchService),
	}
}

func TestSearchServices_Coverage_EmptyDBNameHelpers(t *testing.T) {
	db := newCoverageSearchDB()
	t.Cleanup(func() { _ = db.baseStorage.Close() })

	status := db.GetDatabaseSearchStatus("")
	require.Equal(t, "not_initialized", status.Phase)

	db.DropSearchServiceState("")
}

func TestSearchServices_Coverage_StartBuildGuardsAndNilContext(t *testing.T) {
	db := newCoverageSearchDB()
	t.Cleanup(func() { _ = db.baseStorage.Close() })

	db.startSearchIndexBuild(nil, nil)
	db.startSearchIndexBuild(&dbSearchService{}, nil)

	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant-build")
	svc := search.NewServiceWithDimensions(engine, 3)
	entry := &dbSearchService{dbName: "tenant-build", svc: svc, buildDone: make(chan struct{})}

	db.startSearchIndexBuild(entry, nil)
	select {
	case <-entry.buildDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected build to complete")
	}
}

func TestSearchServices_Coverage_EnsurePendingFlushBranches(t *testing.T) {
	t.Run("guards nil entry", func(t *testing.T) {
		db := newCoverageSearchDB()
		t.Cleanup(func() { _ = db.baseStorage.Close() })
		db.ensurePendingFlush(nil)
	})

	t.Run("returns early when flush already running", func(t *testing.T) {
		db := newCoverageSearchDB()
		t.Cleanup(func() { _ = db.baseStorage.Close() })
		svc := search.NewServiceWithDimensions(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant"), 3)
		entry := &dbSearchService{
			dbName:              "tenant",
			svc:                 svc,
			pendingFlushDelay:   5 * time.Millisecond,
			pendingFlushRunning: true,
		}

		db.ensurePendingFlush(entry)
		// The timer's callback clears pendingFlushTimer when it runs (#880).
		require.Eventually(t, func() bool {
			entry.pendingFlushMu.Lock()
			defer entry.pendingFlushMu.Unlock()
			return entry.pendingFlushTimer == nil
		}, 10*time.Second, time.Millisecond)
		entry.pendingFlushMu.Lock()
		require.True(t, entry.pendingFlushRunning)
		entry.pendingFlushMu.Unlock()
	})

	t.Run("startBackgroundTask false resets pendingFlushRunning", func(t *testing.T) {
		db := newCoverageSearchDB()
		t.Cleanup(func() { _ = db.baseStorage.Close() })
		db.closed = true
		svc := search.NewServiceWithDimensions(storage.NewNamespacedEngine(storage.NewMemoryEngine(), "tenant"), 3)
		entry := &dbSearchService{
			dbName:            "tenant",
			svc:               svc,
			pendingFlushDelay: 5 * time.Millisecond,
			pendingOps: map[string]pendingSearchMutation{
				"n1": {remove: true},
			},
		}

		db.ensurePendingFlush(entry)
		// Wait until the timer has fired and the refused task has reset
		// pendingFlushRunning; before the timer fires it is false too (#880).
		require.Eventually(t, func() bool {
			entry.pendingFlushMu.Lock()
			defer entry.pendingFlushMu.Unlock()
			return entry.pendingFlushTimer == nil && !entry.pendingFlushRunning
		}, 10*time.Second, time.Millisecond)
	})
}

func TestSearchServices_Coverage_EnsureBuiltAndStartedDefaultBranches(t *testing.T) {
	db := newCoverageSearchDB()
	t.Cleanup(func() { _ = db.baseStorage.Close() })

	db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) {
		if dbName == db.defaultDatabaseName() {
			return false, false, "startup", "startup"
		}
		return true, false, "startup", "startup"
	})

	svc, err := db.EnsureSearchIndexesBuilt(context.Background(), "", nil)
	require.NoError(t, err)
	require.True(t, svc.GetBuildProgress().Ready)

	startedSvc, err := db.EnsureSearchIndexesBuildStarted("", nil)
	require.NoError(t, err)
	require.NotNil(t, startedSvc)
}

func TestSearchServices_Coverage_EnsureBuildStartedUsesBackgroundWhenBuildCtxNil(t *testing.T) {
	db := newCoverageSearchDB()
	t.Cleanup(func() { _ = db.baseStorage.Close() })

	db.SetDbSearchFlagsResolver(func(dbName string) (bool, bool, string, string) {
		return true, false, "startup", "startup"
	})

	svc, err := db.EnsureSearchIndexesBuildStarted("tenant-startup", nil)
	require.NoError(t, err)
	require.NotNil(t, svc)

	require.NoError(t, db.ensureSearchIndexesBuilt(context.Background(), "tenant-startup"))
}

func TestSearchServices_Coverage_EventAndClusteringNilContextBranches(t *testing.T) {
	db := newCoverageSearchDB()
	t.Cleanup(func() { _ = db.baseStorage.Close() })

	db.indexNodeFromEvent(nil)
	db.runClusteringOnceAllDatabases(context.Background())
}
