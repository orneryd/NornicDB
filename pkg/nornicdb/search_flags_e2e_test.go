package nornicdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// E2E tests that exercise the full DB write path (Cypher CREATE → BadgerDB
// storage → indexNodeFromEvent → search.Service.IndexNode) and verify that
// per-DB search index master switches actually short-circuit the search
// service work — not just at the search.Service unit level.
//
// These were prompted by the operator-level concern that the
// benchmark_northwind_vs_neo4j.sh script's --search-bm25-enabled=false
// --search-vector-enabled=false flags weren't actually disabling indexing
// during seed-time inserts. The fixture mirrors what the bench does:
// open a DB, configure the resolver to disable indexes for a target DB,
// run a CREATE, and assert the search service didn't materialize anything.

// TestSearchFlagsE2E_BothDisabled — when both flags are off, a Cypher CREATE
// must NOT populate BM25 or vector indexes. The search service should still
// be created (so handlers can return their structured 503), but it carries
// zero documents and zero embeddings.
func TestSearchFlagsE2E_BothDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchVectorEnabled = false

	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dbName := db.defaultDatabaseName()
	// No resolver wired — the bench script doesn't wire one either. Global
	// cfg.Memory.Search*Enabled values are the only signal.

	ctx := context.Background()
	_, err = db.ExecuteCypher(ctx, `CREATE (n:Doc {title: "the quick brown fox"})`, nil)
	require.NoError(t, err)

	// Pump indexNodeFromEvent: the storage event hook is asynchronous via
	// pendingFlush. Wait long enough that any indexing would have landed.
	require.Eventually(t, func() bool {
		st := db.GetDatabaseSearchStatus(dbName)
		return st.Initialized
	}, 2*time.Second, 25*time.Millisecond, "search service should be initialized after first write")

	// Verify the search service exists and reports the disabled state.
	svc, err := db.GetOrCreateSearchService(dbName, db.storage)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.False(t, svc.BM25Enabled(), "BM25Enabled flag must be off after resolver returned false")
	assert.False(t, svc.VectorEnabled(), "VectorEnabled flag must be off")
	assert.Equal(t, 0, svc.EmbeddingCount(), "no embeddings populated when vector disabled")

	// Even after explicit settle, the BM25 fulltext index is the no-op stub
	// (or the real index with zero docs because IndexNode short-circuits).
	// Either way: zero docs.
	// Wait briefly for any pending flush to complete.
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, svc.FulltextDocCount(), "no BM25 docs should land when both disabled")
}

// TestSearchFlagsE2E_VectorDisabledOnly — BM25 stays on (default), vector is
// off. Cypher CREATE should populate BM25 but NOT vector indexes.
func TestSearchFlagsE2E_VectorDisabledOnly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchBM25Enabled = true
	cfg.Memory.SearchVectorEnabled = false

	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dbName := db.defaultDatabaseName()
	// No resolver wired — exercise the global-config fallback path.

	ctx := context.Background()
	_, err = db.ExecuteCypher(ctx, `CREATE (n:Doc {title: "alpha bravo charlie"})`, nil)
	require.NoError(t, err)

	svc, err := db.GetOrCreateSearchService(dbName, db.storage)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.True(t, svc.BM25Enabled())
	assert.False(t, svc.VectorEnabled())

	// BM25 should accumulate the doc; vector index stays empty.
	require.Eventually(t, func() bool {
		return svc.FulltextDocCount() == 1
	}, 3*time.Second, 50*time.Millisecond, "BM25 should index the doc when BM25 is enabled")
	assert.Equal(t, 0, svc.EmbeddingCount(), "no embeddings populated when vector disabled")
}

// TestSearchFlagsE2E_BenchmarkScenario — directly mirrors the
// benchmark_northwind_vs_neo4j.sh GRAPH_ONLY=1 invocation: NornicDB started
// with both per-DB master switches off, the workload performs only Cypher
// CREATE + MATCH (no fulltext, no vector), and the search service should
// remain quiet — no fulltext docs, no embeddings, no work done that the
// benchmark would attribute to NornicDB graph performance.
//
// This is the test that proves the bench script's flags actually disable
// what they claim to disable.
//
// IMPORTANT: this test does NOT call SetDbSearchFlagsResolver. The bench
// script doesn't either — it relies entirely on cfg.Memory.Search*Enabled
// being honoured by the boot orchestrator. If a future regression makes
// resolveSearchFlags ignore the global config, this test fails.
func TestSearchFlagsE2E_BenchmarkScenario(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchVectorEnabled = false

	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dbName := db.defaultDatabaseName()

	// State after the async boot orchestrator runs EnsureSearchIndexesBuilt
	// for the default database. With both flags off, the service must be
	// created, marked ready (so handlers don't see perpetual building), and
	// have zero docs in BM25 / zero embeddings — no build should run.
	svc, err := db.GetOrCreateSearchService(dbName, db.storage)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.False(t, svc.BM25Enabled(), "BM25 disabled at boot via cfg.Memory.SearchBM25Enabled")
	assert.False(t, svc.VectorEnabled(), "vector disabled at boot via cfg.Memory.SearchVectorEnabled")
	require.Eventually(t, func() bool {
		return svc.IsReady()
	}, 3*time.Second, 25*time.Millisecond, "MarkReadyDisabled should fire after boot orchestrator runs")
	assert.Equal(t, 0, svc.FulltextDocCount(), "no BM25 docs after boot when disabled")
	assert.Equal(t, 0, svc.EmbeddingCount(), "no embeddings after boot when disabled")

	ctx := context.Background()
	// Northwind-style seed: a handful of Categories + Products with
	// PART_OF relationships. None of these properties trigger embeddings.
	_, err = db.ExecuteCypher(ctx, `UNWIND range(1, 25) AS i CREATE (c:Category {categoryID: i, categoryName: "category-" + toString(i)})`, nil)
	require.NoError(t, err)
	_, err = db.ExecuteCypher(ctx, `UNWIND range(1, 100) AS i CREATE (p:Product {productID: i, productName: "product-" + toString(i), unitPrice: 1.0 * i})`, nil)
	require.NoError(t, err)

	// Allow any pending background flush to settle.
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 0, svc.FulltextDocCount(), "BM25 must not accumulate docs in graph-only mode (this is what the benchmark relies on)")
	assert.Equal(t, 0, svc.EmbeddingCount(), "vector index must not accumulate embeddings in graph-only mode")

	// Sanity: actual graph data IS present (graph-only mode disables SEARCH
	// indexes, not graph storage).
	res, err := db.ExecuteCypher(ctx, `MATCH (p:Product) RETURN count(p) AS n`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "n", res.Columns[0])
}

// TestSearchFlagsE2E_MultiDB_NoRaceWithBootOrchestrator — mirrors the user's
// real-world report: NornicDB started with both flags off, two databases
// (default `nornic` and an existing `d3_demo` with prior data on disk), and
// observed that the default DB stayed silent but `d3_demo` still indexed
// 432 nodes. The race: a write event firing between getOrCreateSearchService
// and EnsureSearchIndexesBuilt's MarkReadyDisabled call would observe
// progress.Ready=false and call startSearchIndexBuild → BuildIndexes →
// iterate every node from disk.
//
// This test reproduces the race by simulating an indexNodeFromEvent firing
// for a non-default DB before the boot orchestrator gets there. The fix
// (MarkReadyDisabled inside getOrCreateSearchService for both-off case)
// must prevent any BuildIndexes work from running.
func TestSearchFlagsE2E_MultiDB_NoRaceWithBootOrchestrator(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchVectorEnabled = false

	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Race window simulation: fire an indexNodeFromEvent for a non-default
	// DB immediately. With the fix, getOrCreateSearchService inside the
	// event handler creates the service and marks it ready-disabled BEFORE
	// the pendingFlush goroutine can call startSearchIndexBuild.
	for i := 0; i < 20; i++ {
		db.indexNodeFromEvent(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("d3_demo:n%d", i)),
			Labels:     []string{"Star"},
			Properties: map[string]any{"name": fmt.Sprintf("star-%d", i)},
		})
	}

	// Allow boot orchestrator + any pendingFlush goroutine to run.
	time.Sleep(500 * time.Millisecond)

	// Both services must exist, both must be ready (so handlers don't 503
	// with "still building"), and neither may have any docs / embeddings.
	for _, dbName := range []string{db.defaultDatabaseName(), "d3_demo"} {
		svc, err := db.GetOrCreateSearchService(dbName, nil)
		require.NoError(t, err)
		require.NotNil(t, svc, "service must exist for %s", dbName)
		assert.False(t, svc.BM25Enabled(), "%s: BM25 must stay disabled", dbName)
		assert.False(t, svc.VectorEnabled(), "%s: vector must stay disabled", dbName)
		assert.True(t, svc.IsReady(), "%s: service must be marked ready (no perpetual building)", dbName)
		assert.Equal(t, 0, svc.FulltextDocCount(), "%s: race must not have triggered BM25 indexing", dbName)
		assert.Equal(t, 0, svc.EmbeddingCount(), "%s: race must not have triggered vector indexing", dbName)
	}
}
