package cypher

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Cached node checkpoints contain no relationship vectors. A pre-build live
// refresh must not make a partial relationship population authoritative when
// the node checkpoint is reused. These restart tests exercise the procedure
// and both scalar similarity routes against an independent storage oracle.

// cachedStartupRelationshipQueries are the public relationship vector reads
// bound to the relationship vector index rels.
func cachedStartupRelationshipQueries() []string {
	return []string{
		"MATCH ()-[r:R]->() RETURN r.id,vector.similarity.cosine(r.emb,[1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 10",
		"MATCH ()-[r:R]->() WITH r,vector.similarity.cosine(r.emb,[1.0,0.0,0.0]) AS score RETURN r.id,score ORDER BY score DESC LIMIT 10",
		"CALL db.index.vector.queryRelationships('rels',10,[1.0,0.0,0.0]) YIELD relationship,score RETURN relationship.id,score",
	}
}

// storedRelationshipID resolves the fixture's public property to its stored
// identity, so the pre-build refresh targets an actual relationship.
func storedRelationshipID(t *testing.T, exec *StorageExecutor, id string) storage.EdgeID {
	t.Helper()
	result, err := exec.Execute(t.Context(), "MATCH ()-[r:R {id:$id}]->() RETURN id(r)", map[string]interface{}{"id": id})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return storage.EdgeID(fmt.Sprint(result.Rows[0][0]))
}

// seedRelationshipRestartPopulation stores node vectors (the artifacts a
// cached startup restores them from) beside two relationships whose vectors
// score 1 and 0.5 against the query [1,0,0], publishes their relationship
// vector index, and builds the search population the restart restores from
// indexDir. It closes the database and its artifacts so the caller reopens
// both, as a restart does.
func seedRelationshipRestartPopulation(t *testing.T, dataDir, indexDir string) {
	t.Helper()
	base, closeStore := nor283OpenStore(t, dataDir)
	store := storage.NewNamespacedEngine(base, "test")
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(t.Context(), "CREATE (:Doc {id:'a',emb:[1.0,0.0,0.0]})-[:R {id:'r',emb:[1.0,0.0,0.0]}]->(:Doc {id:'b',emb:[0.0,1.0,0.0]})-[:R {id:'s',emb:[0.0,1.0,0.0]}]->(:Doc {id:'c',emb:[0.0,0.0,1.0]})", nil)
	require.NoError(t, err)
	_, err = writer.Execute(t.Context(), "CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb)"+incarnationVectorOptions, nil)
	require.NoError(t, err)
	seed, closeSeed := nor283Service(t, store, indexDir, nil)
	require.NoError(t, seed.BuildIndexes(t.Context()))
	require.True(t, seed.IsReady())
	hits, err := seed.VectorQueryRelationships(t.Context(), []float32{1, 0, 0}, search.RelationshipVectorQuerySpec{
		Type: "R", Property: "emb", Similarity: "cosine", Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, hits, 2, "the saved population's relationship metadata is the full stored population")
	closeSeed()
	closeStore()
}

func TestCachedStartupServesEveryStoredRelationshipVector(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dataDir, indexDir := filepath.Join(dir, "data"), filepath.Join(dir, "indexes")
	seedRelationshipRestartPopulation(t, dataDir, indexDir)

	base, _ := nor283OpenStore(t, dataDir)
	store := storage.NewNamespacedEngine(base, "test")
	// The restarted service reads its baseline from storage; the node iterator
	// counts what the cached startup must not walk.
	source, nodeReads := nor283Source(t, store, "iterator")
	service, _ := nor283Service(t, source, indexDir, nil)
	require.False(t, service.IsReady())
	// A live write before the restarted service's build: one of the two stored
	// relationships reaches the relationship metadata ahead of the baseline.
	require.NoError(t, service.RefreshEdge(ctx, storedRelationshipID(t, NewStorageExecutor(source), "r")))

	require.NoError(t, service.BuildIndexes(ctx))
	require.True(t, service.IsReady())
	require.Zero(t, nodeReads.Load(), "the cached startup restores the node population from its saved artifacts")

	reader := NewStorageExecutor(source)
	reader.SetSearchService(service)
	fallback := NewStorageExecutor(source)
	for _, query := range cachedStartupRelationshipQueries() {
		t.Run(query, func(t *testing.T) {
			want, err := fallback.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Len(t, want.Rows, 2, "both stored relationships are readable without a search service")
			got, err := reader.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, incarnationScores(t, want.Rows), incarnationScores(t, got.Rows),
				"a ready schema-backed read must serve both stored relationships")
			require.Equal(t, map[interface{}]float64{"r": 1, "s": 0.5}, incarnationScores(t, got.Rows),
				"the cached startup serves the stored vectors, not only the refreshed one")
		})
	}
}

// The cached startup's relationship stream is the same owner the other
// baselines use, so a stored relationship its index memory budget refuses is
// still the build's failure: readiness is not published over the relationships
// the stream admitted, and every schema-bound read falls back to storage.
func TestCachedStartupRelationshipVectorBudgetRefusalFailsBuild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dataDir, indexDir := filepath.Join(dir, "data"), filepath.Join(dir, "indexes")
	seedRelationshipRestartPopulation(t, dataDir, indexDir)

	base, _ := nor283OpenStore(t, dataDir)
	store := storage.NewNamespacedEngine(base, "test")
	source, nodeReads := nor283Source(t, store, "iterator")
	options := search.DefaultServiceOptions("")
	options.BM25MemoryMaxBytes = 1 << 20
	options.VectorMemoryMaxBytes = 20
	service, _ := nor283Service(t, source, indexDir, &options)
	require.NoError(t, service.RefreshEdge(ctx, storedRelationshipID(t, NewStorageExecutor(source), "r")))

	buildErr := service.BuildIndexes(ctx)
	require.ErrorIs(t, buildErr, search.ErrIndexMemoryBudgetExceeded)
	require.False(t, service.IsReady(), "a refused stored relationship leaves the cached population incomplete")
	require.Zero(t, nodeReads.Load(), "budget refusal must exercise cached relationship population, not a node rebuild")

	reader := NewStorageExecutor(source)
	reader.SetSearchService(service)
	fallback := NewStorageExecutor(source)
	for _, query := range cachedStartupRelationshipQueries() {
		t.Run(query, func(t *testing.T) {
			want, err := fallback.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Len(t, want.Rows, 2)
			got, err := reader.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, incarnationScores(t, want.Rows), incarnationScores(t, got.Rows),
				"a refused population is not served: no stored relationship is silently omitted")
		})
	}
}
