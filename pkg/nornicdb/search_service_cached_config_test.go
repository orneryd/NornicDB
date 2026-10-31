package nornicdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingSearchServiceResolver installs a DbConfigResolver that counts the
// resolutions it serves, so a test can show whether a cached lookup re-entered
// the resolver for a service that already exists.
type countingSearchServiceResolver struct {
	mu       sync.Mutex
	calls    int
	dims     int            // >0 overrides the embedding dimensions of every database
	override map[string]int // per-database dims overrides (phase 2)
	minSim   float64        // min similarity reported for every database
}

func (c *countingSearchServiceResolver) resolver(dbName string) (int, float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if dims, ok := c.override[dbName]; ok {
		return dims, c.minSim
	}
	return c.dims, c.minSim
}

func (c *countingSearchServiceResolver) setOverride(dbName string, dims int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.override == nil {
		c.override = make(map[string]int)
	}
	c.override[dbName] = dims
}

func (c *countingSearchServiceResolver) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// newCachedConfigSearchDB builds a DB directly (no storage event pipeline, so
// no unrelated create-path lookups) whose default database service is not yet
// created.
func newCachedConfigSearchDB(t *testing.T) *DB {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	cfg := DefaultConfig()
	cfg.Database.DataDir = t.TempDir()
	cfg.Database.PersistSearchIndexes = false
	cfg.Memory.EmbeddingDimensions = 3
	cfg.Memory.EmbeddingEnabled = false
	return &DB{
		storage:             storage.NewNamespacedEngine(base, "nornic"),
		baseStorage:         base,
		config:              cfg,
		embeddingDims:       3,
		searchMinSimilarity: 0.1,
		searchServices:      make(map[string]*dbSearchService),
	}
}

// TestSearchServiceCachedLookupSkipsConfigResolution pins the creation contract
// of GetOrCreateSearchService: the per-database config resolution (dims,
// minSimilarity) and the per-database search options are inputs to
// building a service, so they belong to the create path only. A service that is
// already cached is returned exactly as built, without re-entering either
// resolver, and a later resolution that reports different dimensions does not
// retune it. Reset drops the cache entry, so the next lookup creates again and
// consumes a fresh resolution.
func TestSearchServiceCachedLookupSkipsConfigResolution(t *testing.T) {
	db := newCachedConfigSearchDB(t)

	dimsResolver := &countingSearchServiceResolver{dims: 5, minSim: 0.25}
	db.SetDbConfigResolver(dimsResolver.resolver)

	var optionsCalls int
	db.SetDbSearchOptionsResolver(func(dbName string) search.ServiceOptions {
		optionsCalls++
		return search.DefaultServiceOptions(dbName)
	})

	const dbName = "nornic"
	t.Cleanup(func() { db.ResetSearchService(dbName) })
	created, err := db.GetOrCreateSearchService(dbName, nil)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.Equal(t, 5, created.VectorIndexDimensions(), "the create path resolves the per-DB dimensions")
	require.Equal(t, 1, dimsResolver.callCount(), "creating the service resolves the config exactly once")
	require.Equal(t, 1, optionsCalls, "creating the service resolves the search options exactly once")

	// Repeated cached lookups, through both the public entry point and the
	// internal one used by indexNodeFromEvent, return the same service and
	// resolve nothing.
	for i := 0; i < 3; i++ {
		cached, cachedErr := db.GetOrCreateSearchService(dbName, nil)
		require.NoError(t, cachedErr)
		require.Same(t, created, cached)

		internal, internalErr := db.GetOrCreateSearchService(dbName, nil)
		require.NoError(t, internalErr)
		require.Same(t, created, internal)
	}
	require.Equal(t, 1, dimsResolver.callCount(),
		"cached lookups must not re-enter the per-DB config resolver; only service creation consumes its result")
	require.Equal(t, 1, optionsCalls,
		"cached lookups must not re-enter the per-DB search options resolver")

	// Changing what the resolver reports does not retune the cached service:
	// dims/minSim/options are creation inputs.
	dimsResolver.setOverride(dbName, 9)
	cached, err := db.GetOrCreateSearchService(dbName, nil)
	require.NoError(t, err)
	require.Same(t, created, cached)
	require.Equal(t, 5, cached.VectorIndexDimensions(), "a cached lookup keeps the service's original dimensions")
	require.Equal(t, 1, dimsResolver.callCount(), "a cached lookup resolves nothing")

	// Reset ends the cache entry, so the next lookup is a fresh creation and
	// consumes a fresh resolution (now 9 dimensions).
	db.ResetSearchService(dbName)
	recreated, err := db.GetOrCreateSearchService(dbName, nil)
	require.NoError(t, err)
	require.NotNil(t, recreated)
	require.NotSame(t, created, recreated)
	require.Equal(t, 9, recreated.VectorIndexDimensions(), "a recreated service resolves the config again")
	require.Equal(t, 2, dimsResolver.callCount(), "creation after reset resolves exactly once more")
	require.Equal(t, 2, optionsCalls, "creation after reset resolves the search options exactly once more")
}

func TestDisabledSearchEventsDoNotQueueAndRebuildAfterEnable(t *testing.T) {
	db := newCachedConfigSearchDB(t)
	const name = "nornic"
	t.Cleanup(func() { db.ResetSearchService(name) })
	db.config.Memory.SearchBM25Enabled = false
	db.config.Memory.SearchVectorEnabled = false
	_, err := db.storage.CreateNode(&storage.Node{ID: "stored", Labels: []string{"Doc"}, Properties: map[string]any{"title": "retained quartz"}})
	require.NoError(t, err)
	var resolutions atomic.Int64
	db.SetRerankerResolver(func(string) search.Reranker { resolutions.Add(1); return nil })
	db.queueSearchRefreshEvent(searchNodeWrite, "nornic:stored", storage.CaptureVectorWorkOrigin())
	require.True(t, db.GetDatabaseSearchStatus(name).Initialized, "disabled service still exists for readiness reporting")
	require.EqualValues(t, 1, resolutions.Load(), "the first event creates one service")
	for range 10 {
		db.queueSearchRefreshEvent(searchNodeWrite, "nornic:stored", storage.CaptureVectorWorkOrigin())
	}
	require.EqualValues(t, 1, resolutions.Load(), "disabled cached events do not resolve or queue search work")
	db.searchServicesMu.RLock()
	entry := db.searchServices[name]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, entry)
	entry.pendingMu.Lock()
	queued := len(entry.pendingOps)
	entry.pendingMu.Unlock()
	require.Zero(t, queued)
	entry.pendingFlushMu.Lock()
	timer, running := entry.pendingFlushTimer, entry.pendingFlushRunning
	entry.pendingFlushMu.Unlock()
	require.Nil(t, timer)
	require.False(t, running)
	require.Zero(t, entry.svc.FulltextDocCount())
	db.config.Memory.SearchBM25Enabled = true
	db.ResetSearchService(name)
	rebuilt, err := db.EnsureSearchIndexes(context.Background(), name, nil, SearchIndexBuildWait)
	require.NoError(t, err)
	require.NotSame(t, entry.svc, rebuilt)
	require.True(t, rebuilt.BM25Enabled())
	require.Equal(t, 1, rebuilt.FulltextDocCount(), "re-enabling rebuilds records written while search was disabled")
}

func TestSearchEventUsesCachedServiceAndQueryRefreshesReranker(t *testing.T) {
	db := newCachedConfigSearchDB(t)
	const name = "nornic"
	t.Cleanup(func() { db.ResetSearchService(name) })
	db.config.Memory.SearchBM25Enabled = true
	db.config.Memory.SearchVectorEnabled = false
	var resolutions atomic.Int64
	db.SetRerankerResolver(func(string) search.Reranker { resolutions.Add(1); return nil })
	svc, err := db.EnsureSearchIndexes(context.Background(), name, nil, SearchIndexBuildWait)
	require.NoError(t, err)
	before := resolutions.Load()
	_, err = db.storage.CreateNode(&storage.Node{ID: "stored", Labels: []string{"Doc"}, Properties: map[string]any{"title": "indexed quartz"}})
	require.NoError(t, err)
	for range 5 {
		db.queueSearchRefreshEvent(searchNodeWrite, "nornic:stored", storage.CaptureVectorWorkOrigin())
	}
	require.Equal(t, before, resolutions.Load(), "indexing events must not resolve query-only reranker configuration")
	require.Eventually(t, func() bool { return svc.FulltextDocCount() == 1 }, 3*time.Second, 10*time.Millisecond, "cached service still indexes the latest stored document")
	_, err = db.GetOrCreateSearchService(name, nil)
	require.NoError(t, err)
	require.Equal(t, before+1, resolutions.Load(), "query-facing lookup resolves the current reranker")
}
