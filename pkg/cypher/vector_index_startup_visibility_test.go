package cypher

import (
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// The same failed startup must not publish a new index ONLINE, must not report
// an already committed index ONLINE for a service that cannot serve it, and
// must not make that index read the failed build's partial serving population.
func TestVectorIndexPopulationAfterFailedStartup(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "created_after_failure"
		if existing {
			name = "committed_before_startup"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store := newTestBadgerNamespacedStore(t)
			writer := NewStorageExecutor(store)
			_, err := writer.Execute(ctx, "CREATE (:Doc {id:'a',emb:[1.0,0.0,0.0]}),(:Doc {id:'b',emb:[0.0,1.0,0.0]})", nil)
			require.NoError(t, err)
			const create = "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb)" + incarnationVectorOptions
			if existing {
				_, err = writer.Execute(ctx, create, nil)
				require.NoError(t, err)
			}
			svc := search.NewService(store, 3, &search.ServiceOptions{VectorMemoryMaxBytes: 30})
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			require.ErrorIs(t, svc.BuildIndexes(ctx), search.ErrIndexMemoryBudgetExceeded)
			require.False(t, svc.IsReady())
			require.Equal(t, 1, svc.CountPropertyVectorEntries("emb"))
			reader := NewStorageExecutor(store)
			reader.SetSearchService(svc)
			// The committed index is served by this service's own build, which
			// failed, so the index is FAILED for this service too; creating it
			// after the failure reports the same state.
			if !existing {
				_, err = reader.Execute(ctx, create, nil)
				require.ErrorIs(t, err, search.ErrIndexMemoryBudgetExceeded)
			}
			shown, err := reader.Execute(ctx, "SHOW VECTOR INDEXES YIELD name,state RETURN name,state", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"docs", "FAILED"}}, shown.Rows)
			const query = "MATCH (n:Doc) RETURN n.id,vector.similarity.cosine(n.emb,[1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 10"
			want, err := writer.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Len(t, want.Rows, 2)
			got, err := reader.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, want.Rows, got.Rows, "schema-backed MATCH must not expose a partial failed startup population")
		})
	}
}

// Accepted live refreshes may arrive before initial warmup. They do not prove
// that an existing relationship index holds its complete stored population.
func TestExistingRelationshipVectorIndexReadsStorageUntilStartupCompletes(t *testing.T) {
	ctx := t.Context()
	store := newTestBadgerNamespacedStore(t)
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(ctx, "CREATE (:Doc {id:'a'})-[:R {id:'r',emb:[1.0,0.0,0.0]}]->(:Doc {id:'b'})-[:R {id:'s',emb:[0.0,1.0,0.0]}]->(:Doc {id:'c'})", nil)
	require.NoError(t, err)
	_, err = writer.Execute(ctx, "CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb)"+incarnationVectorOptions, nil)
	require.NoError(t, err)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	edges, err := store.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 2)
	require.NoError(t, svc.RefreshEdge(ctx, edges[0].ID))
	require.False(t, svc.IsReady())
	require.True(t, svc.HasRelationshipVectorEntries("R", "emb"))
	reader := NewStorageExecutor(store)
	reader.SetSearchService(svc)
	queries := []string{
		"MATCH ()-[r:R]->() RETURN r.id,vector.similarity.cosine(r.emb,[1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 10",
		"CALL db.index.vector.queryRelationships('rels',10,[1.0,0.0,0.0]) YIELD relationship,score RETURN relationship.id,score",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			want, err := writer.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Len(t, want.Rows, 2)
			got, err := reader.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, incarnationScores(t, want.Rows), incarnationScores(t, got.Rows), query)
		})
	}
	require.NoError(t, svc.BuildIndexes(ctx))
	require.True(t, svc.IsReady())
	for _, read := range vectorQueryReads {
		served, err := reader.vectorIndexServiceAnswers(ctx, svc, "rels", search.VectorIndexTarget{Label: "R", Property: "emb", Relationship: true}, 3, read)
		require.NoError(t, err)
		require.True(t, served, "a complete ready population remains eligible for service reads")
	}
}

func TestSchemaVectorIndexWarmReadFinishesLazyStartup(t *testing.T) {
	for _, relationships := range []bool{false, true} {
		name := "nodes"
		if relationships {
			name = "relationships"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store := newTestBadgerNamespacedStore(t)
			writer := NewStorageExecutor(store)
			_, err := writer.Execute(ctx, "CREATE (:Doc {id:'a',emb:[1.0,0.0,0.0]})-[:R {id:'r',emb:[1.0,0.0,0.0]}]->(:Doc {id:'b',emb:[0.0,1.0,0.0]})", nil)
			require.NoError(t, err)
			create := "CREATE VECTOR INDEX chosen FOR (n:Doc) ON (n.emb)"
			target := search.VectorIndexTarget{Label: "Doc", Property: "emb"}
			if relationships {
				create = "CREATE VECTOR INDEX chosen FOR ()-[r:R]-() ON (r.emb)"
				target = search.VectorIndexTarget{Label: "R", Property: "emb", Relationship: true}
			}
			_, err = writer.Execute(ctx, create+incarnationVectorOptions, nil)
			require.NoError(t, err)
			svc := search.NewService(store, 3, nil)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			var warmed atomic.Int32
			buildErr := make(chan error, 1)
			svc.SetLazyWarming(true, search.WarmFunc(func() { warmed.Add(1); buildErr <- svc.BuildIndexes(ctx) }))
			reader := NewStorageExecutor(store)
			reader.SetSearchService(svc)
			served, err := reader.vectorIndexServiceAnswers(ctx, svc, "chosen", target, 3, vectorReadPassive)
			require.NoError(t, err)
			require.False(t, served)
			require.Zero(t, warmed.Load())
			served, err = reader.vectorIndexServiceAnswers(ctx, svc, "chosen", target, 3, vectorReadWarm)
			require.NoError(t, err)
			require.True(t, served)
			require.True(t, svc.IsReady())
			require.Equal(t, int32(1), warmed.Load())
			require.NoError(t, <-buildErr)
			for _, read := range vectorQueryReads {
				allocs := testing.AllocsPerRun(100, func() {
					served, err := reader.vectorIndexServiceAnswers(ctx, svc, "chosen", target, 3, read)
					if err != nil || !served {
						t.Fatalf("ready source changed during allocation check: %v", err)
					}
				})
				require.Zero(t, allocs, "ready schema source selection must not add allocations")
			}
		})
	}
}

func TestRelationshipVectorIndexWithOtherDimensionsUsesStoredPopulation(t *testing.T) {
	ctx := t.Context()
	store := newTestBadgerNamespacedStore(t)
	writer := NewStorageExecutor(store)
	_, err := writer.Execute(ctx, "CREATE (:Doc)-[:R {id:'two',emb:[1.0,0.0]}]->(:Doc)-[:R {id:'three',emb:[0.0,1.0,0.0]}]->(:Doc)", nil)
	require.NoError(t, err)
	_, err = writer.Execute(ctx, "CREATE VECTOR INDEX rels FOR ()-[r:R]-() ON (r.emb) OPTIONS {indexConfig: {`vector.dimensions`:2,`vector.similarity_function`:'cosine'}}", nil)
	require.NoError(t, err)
	svc := search.NewService(store, 3, nil)
	svc.FixVectorDimensions()
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(ctx))
	require.True(t, svc.IsReady())
	require.True(t, svc.HasRelationshipVectorEntries("R", "emb"), "the service holds the other, three-dimensional representation")
	reader := NewStorageExecutor(store)
	reader.SetSearchService(svc)
	const query = "CALL db.index.vector.queryRelationships('rels',10,[1.0,0.0]) YIELD relationship,score RETURN relationship.id,score"
	want, err := writer.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, map[interface{}]float64{"two": 1}, incarnationScores(t, want.Rows))
	got, err := reader.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, incarnationScores(t, want.Rows), incarnationScores(t, got.Rows))
}

func TestVectorIndexQueryRejectsInvalidShapeBeforeSourceSelection(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		t.Run(map[bool]string{false: "nodes", true: "relationships"}[relationship], func(t *testing.T) {
			exec := NewStorageExecutor(newTestBadgerNamespacedStore(t))
			pattern, procedure := "(n:Doc) ON (n.emb)", "queryNodes"
			if relationship {
				pattern, procedure = "()-[r:R]-() ON (r.emb)", "queryRelationships"
			}
			_, err := exec.Execute(t.Context(), "CREATE VECTOR INDEX vectors FOR "+pattern+incarnationVectorOptions, nil)
			require.NoError(t, err)
			for _, coordinates := range []string{"[]", "[1.0,0.0]"} {
				_, err = exec.Execute(t.Context(), "CALL db.index.vector."+procedure+"('vectors',10,"+coordinates+") YIELD score RETURN score", nil)
				require.Error(t, err, "invalid dimensions must not become an empty successful result")
			}
		})
	}
}

func TestScalarVectorSimilarityIgnoresIncompatibleIndexDimensions(t *testing.T) {
	exec := NewStorageExecutor(newTestBadgerNamespacedStore(t))
	_, err := exec.Execute(t.Context(), "CREATE (:Doc {id:'a',emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	const query = "MATCH (n:Doc) RETURN n.id,vector.similarity.cosine(n.emb,[1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 10"
	want, err := exec.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.Len(t, want.Rows, 1)
	_, err = exec.Execute(t.Context(), "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`:2,`vector.similarity_function`:'cosine'}}", nil)
	require.NoError(t, err)
	got, err := exec.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.Equal(t, want.Rows, got.Rows)
}
