package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func retrievalIndexProfileFixture(t testing.TB) *StorageExecutor {
	t.Helper()
	t.Setenv(search.EnvSearchResultCacheEnabled, "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "retrieval_profile")
	for _, node := range []*storage.Node{
		{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"uuid": "a", "content": "needle alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}},
		{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"uuid": "b", "content": "needle beta"}, ChunkEmbeddings: [][]float32{{0.9, 0.1}}},
		{ID: "c", Labels: []string{"Doc"}, Properties: map[string]any{"uuid": "c", "content": "needle gamma"}, ChunkEmbeddings: [][]float32{{0, 1}}},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	service := search.NewService(engine, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	require.NoError(t, service.BuildIndexes(t.Context()))
	exec := NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0})
	exec.SetSearchService(service)
	return exec
}

func TestProfileRetrievalReportsIndexPrefixesBeforeFinalResultLimit(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := retrievalIndexProfileFixture(t)
		for _, name := range []string{"db.retrieve", "db.rretrieve"} {
			query := "CALL " + name + "({query:'needle', embedding:[1.0,0.0], limit:1, candidateTarget:3, maxCandidateLimit:3, minSimilarity:-1.0}) YIELD node, score RETURN node.uuid AS id, score"
			rows, plan := vectorProfileParity(t, exec, query, nil, nil)
			require.Len(t, rows, 1)
			for _, kind := range []string{"NodeFulltextCandidates", "NodeVectorCandidates"} {
				reads := profilePlanOperators(plan.Root, kind)
				require.Len(t, reads, 1, kind)
				require.EqualValues(t, 3, reads[0].ActualRows, kind)
				require.EqualValues(t, 1, reads[0].DBHits, kind)
			}
			require.Empty(t, plan.Unobserved)
			require.True(t, plan.TotalDBHitsObserved)
			require.Nil(t, planOperatorOfType(plan.Root, "NodeVectorIndexQuery"), "this is a generic retrieval, not a named index query")
		}
	})
}

// The graph procedure composes independent node and relationship retrievals.
// Neither its emitted row count nor its outer registration substitutes for the
// actual index prefix on either side.
func TestProfileGraphRetrievalReportsBothCandidateSources(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Setenv(search.EnvSearchResultCacheEnabled, "false")
		_, exec, service := newRetrievalEndpointFixture(t)
		require.NoError(t, service.BuildIndexes(t.Context()))
		for _, name := range []string{"db.retrieveRelationships", "db.retrieveGraph", "db.rretrieveGraph"} {
			query := "CALL " + name + "({query:'zephyrine', limit:1}) YIELD relationship RETURN relationship.reason AS reason"
			rows, plan := vectorProfileParity(t, exec, query, nil, nil)
			require.Equal(t, [][]interface{}{{"zephyrine bridge"}}, rows)
			reads := profilePlanOperators(plan.Root, "RelationshipFulltextCandidates")
			require.Len(t, reads, 1)
			require.EqualValues(t, 1, reads[0].ActualRows)
			require.EqualValues(t, 1, reads[0].DBHits)
			nodeReads := profilePlanOperators(plan.Root, "NodeFulltextCandidates")
			if name == "db.retrieveRelationships" {
				require.Empty(t, nodeReads)
			} else {
				require.Len(t, nodeReads, 1)
				require.Zero(t, nodeReads[0].ActualRows, "the node index was queried but no node matches")
				require.EqualValues(t, 1, nodeReads[0].DBHits)
			}
			require.Empty(t, plan.Unobserved)
			require.True(t, plan.TotalDBHitsObserved)
		}
	})
}

// Reuse the public fixture to isolate ordinary retrieval overhead at the
// changed candidate-query boundary; query and result caches are disabled.
func BenchmarkRetrievalIndexObservation(b *testing.B) {
	for _, variant := range []struct{ name, request string }{
		{"bm25", "query:'needle', limit:1"},
		{"hybrid", "query:'needle', embedding:[1.0,0.0], limit:1, candidateTarget:3, maxCandidateLimit:3, minSimilarity:-1.0"},
	} {
		b.Run(variant.name, func(b *testing.B) {
			exec := retrievalIndexProfileFixture(b)
			query := "CALL db.retrieve({" + variant.request + "}) YIELD node, score RETURN node.uuid AS id, score"
			result, err := exec.Execute(b.Context(), query, nil)
			require.NoError(b, err)
			require.Len(b, result.Rows, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err = exec.Execute(b.Context(), query, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			require.Len(b, result.Rows, 1)
		})
	}
}
