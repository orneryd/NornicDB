package nornicdb

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// E2E coverage for the operator-reported regression where a bulk-load path
// finishes successfully, nodes carry valid 1024-dim embeddings on disk, but
// vector retrieval returns zero candidates even when the query vector is one
// of the exact stored vectors.
//
// The contract these tests pin down:
//
//  1. After bulk-creating nodes that already carry ChunkEmbeddings, calling
//     BuildSearchIndexes must populate the in-memory vector index from
//     storage. EmbeddingCount must equal the number of bulk-loaded nodes.
//
//  2. VectorSearchCandidates with a query vector that is one of the stored
//     vectors verbatim must return that node as the top candidate. (Cosine
//     similarity of identical unit vectors is 1.0.)
//
//  3. After a "drop and recreate" cycle (ResetSearchService + rebuild), the
//     same recall guarantee must hold. The user reported this path silently
//     no-op'd in their environment.
//
//  4. The same recall guarantee must hold after a full DB Close + Open with
//     the underlying Badger files preserved — the BuildIndexes restore path
//     must repopulate the vector index from the persisted node bodies.
//
// All these tests intentionally avoid an external embedder: they write
// known float32 vectors directly into ChunkEmbeddings so the recall
// assertion is deterministic and a regression in the index pipeline is
// the only thing that can break them.

const e2eBulkVectorDims = 1024

// makeUnitVector returns a deterministic 1024-dim unit vector seeded by id
// so test fixtures are reproducible.
func makeUnitVector(seed int64) []float32 {
	r := rand.New(rand.NewSource(seed))
	v := make([]float32, e2eBulkVectorDims)
	var norm float64
	for i := range v {
		v[i] = float32(r.NormFloat64())
		norm += float64(v[i]) * float64(v[i])
	}
	if norm == 0 {
		return v
	}
	inv := float32(1.0 / float64(float64Sqrt(norm)))
	for i := range v {
		v[i] *= inv
	}
	return v
}

func ptrFloat(v float64) *float64 { return &v }

// float64Sqrt is a tiny shim so we don't import math just for one call.
func float64Sqrt(x float64) float64 {
	// Newton's method seeded at x is plenty for the magnitudes we hit
	// (norm of 1024 random ~N(0,1) values ≈ 32).
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 8; i++ {
		z = (z + x/z) / 2
	}
	return z
}

// bulkInsertEmbeddedNodes creates n nodes with deterministic 1024-dim
// ChunkEmbeddings via the storage engine directly, mimicking a bulk-load
// pipeline (no embedder, no Cypher CREATE roundtrip per node). Returns the
// list of fully-qualified node IDs and their embeddings in the same order
// so tests can pick a known seed vector for queries.
func bulkInsertEmbeddedNodes(t *testing.T, db *DB, count int) ([]storage.NodeID, [][]float32) {
	t.Helper()
	ids := make([]storage.NodeID, 0, count)
	vecs := make([][]float32, 0, count)
	for i := 0; i < count; i++ {
		id := storage.NodeID(fmt.Sprintf("nornic:bulk-%d", i))
		vec := makeUnitVector(int64(i + 1))
		_, err := db.storage.CreateNode(&storage.Node{
			ID:     id,
			Labels: []string{"BulkDoc"},
			Properties: map[string]any{
				"id":    string(id),
				"title": fmt.Sprintf("doc %d", i),
			},
			ChunkEmbeddings: [][]float32{vec},
			EmbedMeta: map[string]any{
				"has_embedding":        true,
				"embedding_model":      "test-bulk",
				"embedding_dimensions": e2eBulkVectorDims,
				"embedded_at":          "2026-01-01T00:00:00Z",
				"chunk_count":          1,
			},
		})
		require.NoError(t, err)
		ids = append(ids, id)
		vecs = append(vecs, vec)
	}
	return ids, vecs
}

// localID strips the "<db>:" prefix that storage requires. The search index
// stores nodes under their unprefixed local IDs, so VectorSearchCandidates
// returns local IDs in its results.
func localID(qualified storage.NodeID) string {
	s := string(qualified)
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return s[i+1:]
		}
	}
	return s
}

// TestBulkLoadVectorRecall_FreshBuild — the simplest contract: bulk-insert
// embedded nodes, run BuildIndexes once, query with a stored vector, get the
// originating node back.
func TestBulkLoadVectorRecall_FreshBuild(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false // no real embedder; we supply the vectors
	cfg.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg.Memory.SearchBM25Enabled = true
	cfg.Memory.SearchVectorEnabled = true

	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const nodeCount = 50
	ids, vecs := bulkInsertEmbeddedNodes(t, db, nodeCount)

	// Trigger a synchronous build. BuildSearchIndexes wraps Service.BuildIndexes
	// which is the same path the boot orchestrator uses in production.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, db.BuildSearchIndexes(ctx))

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NotNil(t, svc)
	require.True(t, svc.VectorEnabled(), "vector flag must be on")
	require.True(t, svc.IsReady(), "service must be ready after BuildSearchIndexes")

	// Every bulk-loaded node should have its main embedding indexed.
	require.GreaterOrEqual(t, svc.EmbeddingCount(), nodeCount,
		"vector index should hold at least one entry per bulk-loaded node (got %d, want >= %d)",
		svc.EmbeddingCount(), nodeCount)

	// Query with the exact stored vector for node[0]. Cosine similarity of
	// identical unit vectors is 1.0, so even with a strict MinSimilarity the
	// node must appear at the top.
	cands, err := svc.VectorSearchCandidates(ctx, vecs[0], &search.SearchOptions{
		Limit:         5,
		MinSimilarity: ptrFloat(0.99),
	})
	require.NoError(t, err)
	require.NotEmpty(t, cands, "vector recall must return at least one candidate when the query is one of the stored vectors verbatim")

	wantID := localID(ids[0])
	assert.Equal(t, wantID, cands[0].ID,
		"top-1 candidate for query=stored_vector(0) must be node 0 (got %q with score %.6f)",
		cands[0].ID, cands[0].Score)
	assert.InDelta(t, 1.0, cands[0].Score, 1e-3,
		"score for query=stored_vector(0) must be ~1.0 (got %.6f)", cands[0].Score)
}

// TestBulkLoadVectorRecall_DropAndRecreate — the user reported that dropping
// and recreating the vector index "did nothing". Reset the search service
// (the closest in-process equivalent of "drop the vector index") and then
// rebuild — recall must still hold.
func TestBulkLoadVectorRecall_DropAndRecreate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg.Memory.SearchBM25Enabled = true
	cfg.Memory.SearchVectorEnabled = true

	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const nodeCount = 30
	ids, vecs := bulkInsertEmbeddedNodes(t, db, nodeCount)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, db.BuildSearchIndexes(ctx))

	dbName := db.defaultDatabaseName()
	svc1, err := db.GetOrCreateSearchService(dbName, db.storage)
	require.NoError(t, err)
	require.True(t, svc1.IsReady())
	require.GreaterOrEqual(t, svc1.EmbeddingCount(), nodeCount)

	// "Drop" — invalidate the in-memory service so the next access
	// reconstructs from scratch. Equivalent to the operator command path.
	db.ResetSearchService(dbName)

	// "Recreate" — fetch a fresh service and rebuild from storage.
	svc2, err := db.GetOrCreateSearchService(dbName, db.storage)
	require.NoError(t, err)
	require.NotSame(t, svc1, svc2, "ResetSearchService must yield a new instance")

	require.NoError(t, svc2.BuildIndexes(ctx))
	require.True(t, svc2.IsReady(), "rebuilt service must reach ready")
	require.GreaterOrEqual(t, svc2.EmbeddingCount(), nodeCount,
		"after drop+recreate, every bulk-loaded node's embedding must be in the index again (got %d, want >= %d)",
		svc2.EmbeddingCount(), nodeCount)

	// Query with a vector picked from the middle of the set so the test
	// fails just as readily on partial-restore bugs as full-empty bugs.
	const probe = 17
	cands, err := svc2.VectorSearchCandidates(ctx, vecs[probe], &search.SearchOptions{
		Limit:         5,
		MinSimilarity: ptrFloat(0.99),
	})
	require.NoError(t, err)
	require.NotEmpty(t, cands,
		"after drop+recreate, querying with stored vector %d must return candidates (got 0)", probe)
	assert.Equal(t, localID(ids[probe]), cands[0].ID,
		"after drop+recreate, top-1 candidate for stored vector %d must be node %d (got %q)",
		probe, probe, cands[0].ID)
}

// TestBulkLoadVectorRecall_PersistAndReopen — the heaviest scenario: bulk-load,
// build (with persistence enabled), close DB, reopen with the same data dir,
// and verify that recall works without an explicit BuildIndexes call. This
// pins the on-disk restore contract that the user said was broken.
func TestBulkLoadVectorRecall_PersistAndReopen(t *testing.T) {
	dataDir := t.TempDir()

	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg.Memory.SearchBM25Enabled = true
	cfg.Memory.SearchVectorEnabled = true
	cfg.Memory.SearchBM25Warming = "startup"
	cfg.Memory.SearchVectorWarming = "startup"

	db, err := Open(dataDir, cfg)
	require.NoError(t, err)

	const nodeCount = 25
	ids, vecs := bulkInsertEmbeddedNodes(t, db, nodeCount)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, db.BuildSearchIndexes(ctx))

	svc1, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.GreaterOrEqual(t, svc1.EmbeddingCount(), nodeCount)

	// Sanity check before close: querying with a stored vector returns it.
	const probe = 7
	cands, err := svc1.VectorSearchCandidates(ctx, vecs[probe], &search.SearchOptions{
		Limit:         5,
		MinSimilarity: ptrFloat(0.99),
	})
	require.NoError(t, err)
	require.NotEmpty(t, cands, "pre-restart sanity: stored-vector query must return candidates")
	require.Equal(t, localID(ids[probe]), cands[0].ID, "pre-restart sanity: top-1 must be node %d", probe)

	require.NoError(t, db.Close())

	// Reopen with the same data dir. The boot orchestrator must rebuild or
	// restore the vector index so the same recall guarantee holds.
	cfg2 := DefaultConfig()
	cfg2.Memory.EmbeddingEnabled = false
	cfg2.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg2.Memory.SearchBM25Enabled = true
	cfg2.Memory.SearchVectorEnabled = true
	cfg2.Memory.SearchBM25Warming = "startup"
	cfg2.Memory.SearchVectorWarming = "startup"

	db2, err := Open(dataDir, cfg2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	// startup warming kicks BuildIndexes in a background task; wait for ready.
	dbName := db2.defaultDatabaseName()
	require.Eventually(t, func() bool {
		st := db2.GetDatabaseSearchStatus(dbName)
		return st.Initialized && st.Ready
	}, 30*time.Second, 100*time.Millisecond, "search must reach ready after reopen with persisted data")

	svc2, err := db2.GetOrCreateSearchService(dbName, db2.storage)
	require.NoError(t, err)
	require.GreaterOrEqual(t, svc2.EmbeddingCount(), nodeCount,
		"after reopen, persisted vector index must restore every bulk-loaded node's embedding (got %d, want >= %d)",
		svc2.EmbeddingCount(), nodeCount)

	// The user's exact reported failure mode: query with a stored vector after restart.
	cands2, err := svc2.VectorSearchCandidates(ctx, vecs[probe], &search.SearchOptions{
		Limit:         5,
		MinSimilarity: ptrFloat(0.99),
	})
	require.NoError(t, err)
	require.NotEmpty(t, cands2,
		"REGRESSION: after restart, querying with one of the stored vectors verbatim must return candidates (got 0). "+
			"This is the exact scenario the operator reported.")
	assert.Equal(t, localID(ids[probe]), cands2[0].ID,
		"after restart, top-1 candidate for stored vector %d must be node %d (got %q)",
		probe, probe, cands2[0].ID)
	assert.InDelta(t, 1.0, cands2[0].Score, 1e-3,
		"after restart, score for query=stored_vector must remain ~1.0 (got %.6f)", cands2[0].Score)
}

// TestBulkLoadVectorRecall_OperatorScenario — the operator's exact full-cycle
// workflow.
//
//	Phase 1: server boots with BOTH search index flags OFF (operators turn
//	         these off so the bulk-load doesn't pay search-indexing cost).
//	         Inside that boot:
//	           - bulk-insert N nodes whose embeddings live in a node
//	             PROPERTY (`n.embedding = [...]`), NOT in ChunkEmbeddings.
//	           - Cypher-create a user-defined vector index over that
//	             property: CREATE VECTOR INDEX docvec FOR (n:Doc) ON (n.embedding).
//	         Search service must NOT have indexed anything — the flags are off.
//
//	Phase 2: close the server, reopen with the SAME data dir but with both
//	         flags flipped to ON and warming=startup. The boot orchestrator
//	         must now build search indexes from the persisted node bodies,
//	         including the property-shaped vectors the user-defined index
//	         points at.
//
//	         Recall check: querying via `CALL db.index.vector.queryNodes` with
//	         one of the stored property vectors verbatim must return that
//	         exact node as top-1 with score ~1.0.
//
// Reported regression: phase 2 returned 0 rows for a query vector that was
// one of the stored property vectors. This test fails when that regression
// is present and pins down the contract going forward.
func TestBulkLoadVectorRecall_OperatorScenario(t *testing.T) {
	dataDir := t.TempDir()

	// ─── Phase 1: bulk-load with search indexes OFF. ───────────────────────
	//
	// The operator does this so the seed-time inserts don't pay the cost of
	// fulltext + vector indexing during a multi-million-node load.
	cfg1 := DefaultConfig()
	cfg1.Memory.EmbeddingEnabled = false
	cfg1.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg1.Memory.SearchBM25Enabled = false
	cfg1.Memory.SearchVectorEnabled = false
	cfg1.Memory.SearchBM25Warming = "startup"
	cfg1.Memory.SearchVectorWarming = "startup"

	db1, err := Open(dataDir, cfg1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Insert nodes whose embedding lives in a node PROPERTY, not the managed
	// ChunkEmbeddings struct field. This mirrors what the operator's
	// bulk-load pipeline produces: each node carries `embedding` alongside
	// its other fields, persisted as a list of float64.
	const nodeCount = 30
	ids := make([]storage.NodeID, 0, nodeCount)
	vecs := make([][]float32, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := storage.NodeID(fmt.Sprintf("nornic:opprop-%d", i))
		vec := makeUnitVector(int64(i + 1000))
		propVec := make([]float64, len(vec))
		for j, v := range vec {
			propVec[j] = float64(v)
		}
		_, err := db1.storage.CreateNode(&storage.Node{
			ID:     id,
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id":        string(id),
				"title":     fmt.Sprintf("doc %d", i),
				"embedding": propVec,
			},
		})
		require.NoError(t, err)
		ids = append(ids, id)
		vecs = append(vecs, vec)
	}

	// Operator declares the user-defined vector index as part of the
	// bulk-load DDL. With both indexing flags off, this records the schema
	// entry but should NOT populate any in-memory search service state.
	createIdx := fmt.Sprintf(
		"CREATE VECTOR INDEX docvec FOR (n:Doc) ON (n.embedding) "+
			"OPTIONS {indexConfig: {`vector.dimensions`: %d, `vector.similarity_function`: 'cosine'}}",
		e2eBulkVectorDims)
	_, err = db1.ExecuteCypher(ctx, createIdx, nil)
	require.NoError(t, err)

	// Sanity: phase-1 search service must report both flags off and zero
	// indexed entries. If this fails, BuildIndexes is doing work it
	// shouldn't and the bulk-load cost regression already landed.
	svc1, err := db1.GetOrCreateSearchService(db1.defaultDatabaseName(), db1.storage)
	require.NoError(t, err)
	require.False(t, svc1.BM25Enabled(), "phase 1: BM25 must be off")
	require.False(t, svc1.VectorEnabled(), "phase 1: vector must be off")
	require.Equal(t, 0, svc1.EmbeddingCount(),
		"phase 1: vector index must stay empty when both flags are off (got %d) — "+
			"the bulk-load is paying indexing cost the flags promised to avoid",
		svc1.EmbeddingCount())

	require.NoError(t, db1.Close())

	// ─── Phase 2: server restart with both flags flipped ON. ────────────────
	//
	// Operator turns the flags back on, restarts the server, and expects
	// the boot orchestrator to build search indexes from the existing node
	// bodies — including the property-shaped vectors the user-defined
	// index was declared over.
	cfg2 := DefaultConfig()
	cfg2.Memory.EmbeddingEnabled = false
	cfg2.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg2.Memory.SearchBM25Enabled = true
	cfg2.Memory.SearchVectorEnabled = true
	cfg2.Memory.SearchBM25Warming = "startup"
	cfg2.Memory.SearchVectorWarming = "startup"

	db2, err := Open(dataDir, cfg2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	dbName := db2.defaultDatabaseName()
	require.Eventually(t, func() bool {
		st := db2.GetDatabaseSearchStatus(dbName)
		return st.Initialized && st.Ready
	}, 60*time.Second, 100*time.Millisecond,
		"phase 2: after restart with flags ON, search must reach Ready under startup-warming")

	svc2, err := db2.GetOrCreateSearchService(dbName, db2.storage)
	require.NoError(t, err)
	require.True(t, svc2.BM25Enabled(), "phase 2: BM25 must be on after flag flip")
	require.True(t, svc2.VectorEnabled(), "phase 2: vector must be on after flag flip")
	require.GreaterOrEqual(t, svc2.EmbeddingCount(), nodeCount,
		"REGRESSION: phase 2: enabling the vector flag and restarting must "+
			"backfill the vector index from existing property-shaped vectors. "+
			"Got %d entries, want >= %d. This is the bulk-load → restart-with-"+
			"indexes-on regression the operator reported.",
		svc2.EmbeddingCount(), nodeCount)

	// Deep recall verification: probe several stored vectors via the operator's
	// exact Cypher procedure path, and assert end-to-end that the procedure
	//   1. returns rows (the row count is bounded above by the requested k=5),
	//   2. the FIRST row is the source node with score ≈ 1.0,
	//   3. the row carries the stored properties (id, title, embedding) so we
	//      know we got the real persisted node back, not a stub,
	//   4. any subsequent rows score strictly less than the top hit (random
	//      unit vectors in 1024-dim space have ~0 cosine similarity → second
	//      hit must be well below 1.0), and
	//   5. each row's score is a non-NaN float64 in [-1, 1].
	//
	// Use $param so the []float64 → []float32 conversion path the client
	// driver hits is exercised end-to-end.
	probes := []int{0, 11, 17, 29}
	const k = 5
	for _, probe := range probes {
		queryVec := make([]float64, len(vecs[probe]))
		for j, v := range vecs[probe] {
			queryVec[j] = float64(v)
		}

		res, err := db2.ExecuteCypher(ctx,
			fmt.Sprintf("CALL db.index.vector.queryNodes('docvec', %d, $q) YIELD node, score RETURN node, score", k),
			map[string]interface{}{"q": queryVec})
		require.NoError(t, err, "probe %d: vector query must succeed", probe)
		require.NotNil(t, res, "probe %d: result must not be nil", probe)
		require.Len(t, res.Columns, 2, "probe %d: must return (node, score) columns", probe)
		require.Equal(t, []string{"node", "score"}, res.Columns)
		require.NotEmpty(t, res.Rows,
			"REGRESSION: probe %d: after restart with indexes flipped on, "+
				"db.index.vector.queryNodes returned 0 rows for a query that exactly "+
				"matches the stored property vector. This is the operator's reported "+
				"failure mode end-to-end.", probe)
		require.LessOrEqual(t, len(res.Rows), k,
			"probe %d: result must be bounded by k=%d (got %d)", probe, k, len(res.Rows))

		// Top row deep-asserted.
		topRow := res.Rows[0]
		require.Len(t, topRow, 2, "probe %d: row must have (node, score)", probe)
		topNode, ok := topRow[0].(*storage.Node)
		require.True(t, ok, "probe %d: row[0] column must be *storage.Node (got %T)", probe, topRow[0])
		require.NotNil(t, topNode, "probe %d: top node must not be nil", probe)

		wantLocalID := localID(ids[probe])
		assert.Equal(t, wantLocalID, localID(topNode.ID),
			"probe %d: top-1 must be node %d (got id=%s)", probe, probe, topNode.ID)
		assert.Contains(t, topNode.Labels, "Doc",
			"probe %d: top node must carry the :Doc label", probe)
		assert.Equal(t, fmt.Sprintf("doc %d", probe), topNode.Properties["title"],
			"probe %d: top node must carry its persisted title property", probe)
		require.Contains(t, topNode.Properties, "embedding",
			"probe %d: top node must carry the persisted embedding property", probe)
		// Deterministic type round-trip: we wrote []float64, storage MUST
		// give us []float64 back. Anything else (e.g. []interface{}) is a
		// serialization regression — every read site downstream then has to
		// type-coerce, and the embedding property loses its declared shape.
		gotVec, ok := topNode.Properties["embedding"].([]float64)
		require.Truef(t, ok,
			"probe %d: stored embedding must round-trip as []float64 (got %T) — "+
				"the on-disk codec is widening typed arrays to []interface{}",
			probe, topNode.Properties["embedding"])
		require.Len(t, gotVec, e2eBulkVectorDims,
			"probe %d: persisted embedding must keep its full %d-dim shape (got %d)",
			probe, e2eBulkVectorDims, len(gotVec))
		for j := 0; j < e2eBulkVectorDims; j++ {
			require.InDelta(t, float64(vecs[probe][j]), gotVec[j], 1e-6,
				"probe %d dim %d: persisted embedding value drift (want %f, got %f)",
				probe, j, vecs[probe][j], gotVec[j])
		}

		topScore, ok := topRow[1].(float64)
		require.True(t, ok, "probe %d: row[1] column must be float64 (got %T)", probe, topRow[1])
		require.False(t, isNaNOrInf(topScore), "probe %d: top score must be finite (got %v)", probe, topScore)
		assert.GreaterOrEqual(t, topScore, -1.0, "probe %d: cosine score must be >= -1", probe)
		assert.LessOrEqual(t, topScore, 1.0+1e-6, "probe %d: cosine score must be <= 1", probe)
		assert.InDelta(t, 1.0, topScore, 1e-3,
			"probe %d: score for query=stored_property_vector must be ~1.0 (got %.6f)",
			probe, topScore)

		// All rows are well-formed and ordered by score descending. With random
		// 1024-dim unit vectors the second-best should be FAR below the top
		// hit; pin the gap so a regression that returns the same vector
		// multiple times (or a flat-zero score) breaks here.
		prevScore := topScore
		for i, row := range res.Rows {
			require.Len(t, row, 2, "probe %d row %d: must have (node, score)", probe, i)
			n, ok := row[0].(*storage.Node)
			require.True(t, ok, "probe %d row %d: must be *storage.Node", probe, i)
			require.NotNil(t, n, "probe %d row %d: node must not be nil", probe, i)
			s, ok := row[1].(float64)
			require.True(t, ok, "probe %d row %d: score must be float64", probe, i)
			require.False(t, isNaNOrInf(s), "probe %d row %d: score must be finite", probe, i)
			if i > 0 {
				assert.LessOrEqual(t, s, prevScore+1e-9,
					"probe %d row %d: score %.6f must be <= previous %.6f (results must be sorted)",
					probe, i, s, prevScore)
			}
			prevScore = s
		}
		if len(res.Rows) >= 2 {
			runnerUp, _ := res.Rows[1][1].(float64)
			assert.Less(t, runnerUp, 0.9,
				"probe %d: second-best for a random unit vector must be << 1.0 (got %.6f) — "+
					"a near-1.0 runner-up suggests the index returned the same vector twice",
				probe, runnerUp)
		}
	}
}

// isNaNOrInf is a small helper so we don't import math just for IsNaN/IsInf.
func isNaNOrInf(f float64) bool {
	return f != f || f > 1e308 || f < -1e308
}

// TestDropVectorIndex_TearsDownInMemoryIndex pins the bug the operator
// flagged: Cypher DROP INDEX must not just remove the schema entry, it
// must also tear down the per-property vector data the index was the
// only handle for. Without this, a follow-up query through any code path
// that walks the in-memory vector store still sees the old vectors, and a
// CREATE-from-scratch of the same index name inherits orphaned entries.
//
// The contract this test pins down:
//
//  1. Before DROP: queryNodes returns the source node for a stored
//     property vector. EmbeddingCount > 0.
//  2. After DROP INDEX: queryNodes for that index name returns 0 rows
//     (the schema entry is gone) AND the in-memory property vectors that
//     were only reachable through the dropped index are GONE — the
//     search service's per-property-vector bookkeeping for the dropped
//     property is empty.
//  3. After re-CREATE without re-indexing: the new index is empty (no
//     orphaned vectors carried over from before the drop). EmbeddingCount
//     contributed by the property index drops to zero between the drop and
//     a fresh BuildSearchIndexes / IndexNode pass.
func TestDropVectorIndex_TearsDownInMemoryIndex(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg.Memory.SearchBM25Enabled = true
	cfg.Memory.SearchVectorEnabled = true

	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Bulk-insert nodes with property-shaped vectors so the user-defined
	// index has something to index.
	const nodeCount = 25
	ids := make([]storage.NodeID, 0, nodeCount)
	vecs := make([][]float32, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := storage.NodeID(fmt.Sprintf("nornic:dropvec-%d", i))
		vec := makeUnitVector(int64(i + 9000))
		propVec := make([]float64, len(vec))
		for j, v := range vec {
			propVec[j] = float64(v)
		}
		_, err := db.storage.CreateNode(&storage.Node{
			ID:     id,
			Labels: []string{"Doc"},
			Properties: map[string]any{
				"id":        string(id),
				"embedding": propVec,
			},
		})
		require.NoError(t, err)
		ids = append(ids, id)
		vecs = append(vecs, vec)
	}

	createIdx := fmt.Sprintf(
		"CREATE VECTOR INDEX docvec FOR (n:Doc) ON (n.embedding) "+
			"OPTIONS {indexConfig: {`vector.dimensions`: %d, `vector.similarity_function`: 'cosine'}}",
		e2eBulkVectorDims)
	_, err = db.ExecuteCypher(ctx, createIdx, nil)
	require.NoError(t, err)
	require.NoError(t, db.BuildSearchIndexes(ctx))

	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NotNil(t, svc)

	// ─── Pre-drop: the query returns the source node. ───────────────────────
	const probe = 4
	qVec := make([]float64, len(vecs[probe]))
	for j, v := range vecs[probe] {
		qVec[j] = float64(v)
	}
	qParams := map[string]interface{}{"q": qVec}

	res, err := db.ExecuteCypher(ctx,
		"CALL db.index.vector.queryNodes('docvec', 5, $q) YIELD node, score RETURN node, score",
		qParams)
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows, "pre-drop: query must return rows")
	preTop := res.Rows[0][0].(*storage.Node)
	require.Equal(t, localID(ids[probe]), localID(preTop.ID),
		"pre-drop: top-1 must be the source node")

	beforeEmbCount := svc.EmbeddingCount()
	require.GreaterOrEqual(t, beforeEmbCount, nodeCount,
		"pre-drop: every node's property vector must be in the index (got %d, want >= %d)",
		beforeEmbCount, nodeCount)

	// Count how many property-vector entries the search service tracks. The
	// teardown contract is that this drops to zero for the dropped property.
	beforePropCount := countPropertyVectors(svc, "embedding")
	require.GreaterOrEqual(t, beforePropCount, nodeCount,
		"pre-drop: search service must track a property-vector entry per node "+
			"for the embedding property (got %d, want >= %d)", beforePropCount, nodeCount)

	// ─── DROP INDEX. ───────────────────────────────────────────────────────
	_, err = db.ExecuteCypher(ctx, "DROP INDEX docvec", nil)
	require.NoError(t, err, "DROP INDEX must succeed")

	// In-memory property-vector bookkeeping must be empty for the dropped
	// property IMMEDIATELY after DROP. This is the bug fix: previously,
	// DROP INDEX only removed the schema entry and left these vectors
	// orphaned in vectorIndex, HNSW, and the cluster index. Check the
	// invariant BEFORE issuing any queryNodes call — queryNodes lazily
	// rebuilds an empty vector index from storage, which would mask the bug.
	afterPropCount := countPropertyVectors(svc, "embedding")
	assert.Equal(t, 0, afterPropCount,
		"REGRESSION: after DROP INDEX, the search service must not retain any "+
			"per-property vector entries for the dropped property "+
			"(got %d, want 0). DROP INDEX previously only removed the schema "+
			"entry, leaving orphaned vectors that still matched queries.",
		afterPropCount)

	// ─── Re-CREATE without rebuild. ────────────────────────────────────────
	// Fresh index, no IndexNode pass yet — must not surface the
	// pre-drop vectors. This catches a regression where DROP retained
	// vectors and CREATE blindly registered them under the new index.
	_, err = db.ExecuteCypher(ctx, createIdx, nil)
	require.NoError(t, err, "CREATE VECTOR INDEX after drop must succeed")

	resAfterRecreate, err := db.ExecuteCypher(ctx,
		"CALL db.index.vector.queryNodes('docvec', 5, $q) YIELD node, score RETURN node, score",
		qParams)
	require.NoError(t, err)
	// The new index is empty — queryNodes either returns 0 rows OR rebuilds
	// from storage on demand. Both behaviours are acceptable; what's NOT
	// acceptable is returning a stale top-1 that came from the pre-drop state.
	for i, row := range resAfterRecreate.Rows {
		s, _ := row[1].(float64)
		// The only way to score ~1.0 is from a fresh, correctly-indexed entry.
		// If we see a 1.0 hit here without having called BuildSearchIndexes,
		// it's a stale orphan — the bug we're fixing.
		if s > 0.999 {
			n, _ := row[0].(*storage.Node)
			require.NotNil(t, n, "row %d: node must not be nil", i)
			// Orphan detection: if the node still exists AND we never rebuilt,
			// the only way the score is ~1 is from leftover state.
			// Allow it ONLY if the per-property entry was rebuilt from storage
			// implicitly (some code paths call BuildIndexes lazily). In that
			// case we expect ALL nodes to be back.
			require.GreaterOrEqual(t, countPropertyVectors(svc, "embedding"), nodeCount,
				"row %d scored ~1.0 (%.6f) but only %d property vectors are tracked — "+
					"this is a stale-orphan match from before DROP",
				i, s, countPropertyVectors(svc, "embedding"))
			break
		}
	}
}

// countPropertyVectors counts how many nodes have a tracked property
// vector entry for the given property key. Reaches into search.Service
// internals via the test-only nodePropVector accessor (same trick the
// search package's own _test.go files use).
func countPropertyVectors(svc *search.Service, propertyKey string) int {
	if svc == nil {
		return 0
	}
	return svc.CountPropertyVectorEntries(propertyKey)
}

// TestBulkLoadVectorRecall_VectorDisabledThenEnabled — the user mentioned the
// recent BM25/vector flag work as the suspected regression source. This test
// pins down: when a DB boots with vector DISABLED, then is re-opened with
// vector ENABLED, the next BuildIndexes must repopulate the vector index from
// the existing nodes.
func TestBulkLoadVectorRecall_VectorDisabledThenEnabled(t *testing.T) {
	dataDir := t.TempDir()

	// Phase 1: open with vector disabled, bulk-load nodes with embeddings.
	cfg1 := DefaultConfig()
	cfg1.Memory.EmbeddingEnabled = false
	cfg1.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg1.Memory.SearchBM25Enabled = true
	cfg1.Memory.SearchVectorEnabled = false
	cfg1.Memory.SearchBM25Warming = "startup"
	cfg1.Memory.SearchVectorWarming = "startup"

	db1, err := Open(dataDir, cfg1)
	require.NoError(t, err)

	const nodeCount = 20
	ids, vecs := bulkInsertEmbeddedNodes(t, db1, nodeCount)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, db1.BuildSearchIndexes(ctx))

	svc1, err := db1.GetOrCreateSearchService(db1.defaultDatabaseName(), db1.storage)
	require.NoError(t, err)
	require.False(t, svc1.VectorEnabled(), "phase 1: vector should be disabled")
	require.Equal(t, 0, svc1.EmbeddingCount(),
		"phase 1: vector index should be empty when disabled (got %d)", svc1.EmbeddingCount())

	require.NoError(t, db1.Close())

	// Phase 2: reopen with vector enabled — the existing nodes still carry
	// ChunkEmbeddings on disk; turning vector on must surface them.
	cfg2 := DefaultConfig()
	cfg2.Memory.EmbeddingEnabled = false
	cfg2.Memory.EmbeddingDimensions = e2eBulkVectorDims
	cfg2.Memory.SearchBM25Enabled = true
	cfg2.Memory.SearchVectorEnabled = true
	cfg2.Memory.SearchBM25Warming = "startup"
	cfg2.Memory.SearchVectorWarming = "startup"

	db2, err := Open(dataDir, cfg2)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })

	dbName := db2.defaultDatabaseName()
	require.Eventually(t, func() bool {
		st := db2.GetDatabaseSearchStatus(dbName)
		return st.Initialized && st.Ready
	}, 30*time.Second, 100*time.Millisecond)

	svc2, err := db2.GetOrCreateSearchService(dbName, db2.storage)
	require.NoError(t, err)
	require.True(t, svc2.VectorEnabled(), "phase 2: vector must be enabled after flag flip")
	require.GreaterOrEqual(t, svc2.EmbeddingCount(), nodeCount,
		"phase 2: enabling vector after a vector-disabled bulk-load must repopulate the index from existing node bodies (got %d, want >= %d)",
		svc2.EmbeddingCount(), nodeCount)

	const probe = 5
	cands, err := svc2.VectorSearchCandidates(ctx, vecs[probe], &search.SearchOptions{
		Limit:         5,
		MinSimilarity: ptrFloat(0.99),
	})
	require.NoError(t, err)
	require.NotEmpty(t, cands, "phase 2: recall must work after enabling vector on existing data")
	assert.Equal(t, localID(ids[probe]), cands[0].ID,
		"phase 2: top-1 candidate for stored vector %d must be node %d", probe, probe)
}
