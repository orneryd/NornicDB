package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// A relationship schema vector index that exists before the search service
// starts is answered from storage until the service's build is ready. Two
// stored relationships with three-dimensional vectors hold 12 bytes of vector
// each; a 20-byte relationship vector budget admits the first and refuses the
// second. The startup build's relationship stream reports that refusal, so the
// build must fail with it and must not publish readiness over the one
// relationship it installed. The procedure and the scalar MATCH then read
// storage, so neither silently omits a stored relationship.
func TestRelationshipVectorBudgetRefusalFailsStartupBuild(t *testing.T) {
	ctx := t.Context()
	store := newTestBadgerNamespacedStore(t)
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(ctx, "CREATE (:Doc {id:'a'})-[:R {id:'r',emb:[1.0,0.0,0.0]}]->(:Doc {id:'b'})-[:R {id:'s',emb:[0.0,1.0,0.0]}]->(:Doc {id:'c'})", nil)
	require.NoError(t, err)
	_, err = writer.Execute(ctx, "CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb)"+incarnationVectorOptions, nil)
	require.NoError(t, err)
	edges, err := store.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 2)
	svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 20})
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	reader := NewStorageExecutor(store)
	reader.SetSearchService(svc)

	t.Run("startup build fails with the vector budget refusal", func(t *testing.T) {
		require.ErrorIs(t, svc.BuildIndexes(ctx), search.ErrIndexMemoryBudgetExceeded)
		require.False(t, svc.IsReady(), "a refused stored relationship leaves the population incomplete, so startup must not publish readiness")
		require.True(t, svc.HasRelationshipVectorEntries("R", "emb"), "the refused stream stops after installing the relationship it admitted")
	})

	queries := []string{
		"MATCH ()-[r:R]->() RETURN r.id,vector.similarity.cosine(r.emb,[1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 10",
		"MATCH ()-[r:R]->() WITH r,vector.similarity.cosine(r.emb,[1.0,0.0,0.0]) AS score RETURN r.id,score ORDER BY score DESC LIMIT 10",
		"CALL db.index.vector.queryRelationships('rels',10,[1.0,0.0,0.0]) YIELD relationship,score RETURN relationship.id,score",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			want, err := writer.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Len(t, want.Rows, 2, "both stored relationships are readable without a search service")
			got, err := reader.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, incarnationScores(t, want.Rows), incarnationScores(t, got.Rows), "schema-backed reads must not omit a stored relationship")
		})
	}
}
