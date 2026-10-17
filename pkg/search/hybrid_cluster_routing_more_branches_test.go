package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func buildClusteredServiceForRouting(t *testing.T) *Service {
	t.Helper()
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "a1", Properties: map[string]interface{}{"text": "alpha one"}, ChunkEmbeddings: [][]float32{{1, 0}}}))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "a2", Properties: map[string]interface{}{"text": "alpha two"}, ChunkEmbeddings: [][]float32{{0.9, 0.1}}}))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "b1", Properties: map[string]interface{}{"text": "beta one"}, ChunkEmbeddings: [][]float32{{0, 1}}}))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "b2", Properties: map[string]interface{}{"text": "beta two"}, ChunkEmbeddings: [][]float32{{0.1, 0.9}}}))
	require.NoError(t, svc.TriggerClustering(context.Background()))
	require.True(t, svc.clusterIndex.IsClustered())
	return svc
}

func TestHybridRouting_RebuildProfilesGuardBranches(t *testing.T) {
	svc := buildClusteredServiceForRouting(t)
	clustering, err := svc.clusterIndex.ComputeClustering(context.Background())
	require.NoError(t, err)
	svc.setFulltext(nil)
	require.Empty(t, svc.clusterLexicalProfilesFor(clustering))
}

func TestHybridRouting_SelectHybridClustersMoreBranches(t *testing.T) {
	svc := buildClusteredServiceForRouting(t)
	semantic := svc.clusterIndex.FindNearestClusters([]float32{1, 0}, 2)
	require.NotEmpty(t, semantic)

	// queryText empty branch.
	out := svc.selectHybridClusters(context.Background(), []float32{1, 0}, 1)
	require.Len(t, out, 1)

	// defaultN <= 0 branch + queryTokens empty branch.
	out = svc.selectHybridClusters(withQueryText(context.Background(), "   \t"), []float32{1, 0}, 0)
	require.NotEmpty(t, out)

	// profiles empty branch.
	svc.clusterLexicalMu.Lock()
	svc.clusterLexicalProfiles = map[int]map[string]float64{}
	svc.clusterLexicalMu.Unlock()
	out = svc.selectHybridClusters(withQueryText(context.Background(), "alpha"), []float32{1, 0}, 2)
	require.NotEmpty(t, out)

	// Invalid query dims should still return a deterministic selection without panic.
	out = svc.selectHybridClusters(withQueryText(context.Background(), "alpha"), []float32{}, 2)
	require.NotNil(t, out)

	// negative lexical/semantic weights branch and sum==0 normalization branch.
	t.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM", "-1")
	t.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX", "-1")
	svc.clusterLexicalMu.Lock()
	svc.clusterLexicalProfiles = map[int]map[string]float64{
		semantic[0]: {"alpha": 1.0},
		semantic[1]: {"beta": 1.0},
	}
	svc.clusterLexicalMu.Unlock()
	out = svc.selectHybridClusters(withQueryText(context.Background(), "beta"), []float32{1, 0}, 1)
	require.Len(t, out, 1)
}

func TestHybridRouting_ApplyBM25SeedHintsMoreBranches(t *testing.T) {
	t.Run("nil_fields_and_nonclustered_empty_index", func(t *testing.T) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.applyBM25SeedHints() // nil cluster/fulltext

		svc.EnableClustering(nil, 2)
		svc.setFulltext(NewFulltextIndex())
		svc.applyBM25SeedHints() // !IsClustered && Count==0
	})

	t.Run("no_indices_for_seed_ids", func(t *testing.T) {
		svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
		svc.EnableClustering(nil, 2)
		require.NoError(t, svc.clusterIndex.Add("existing", []float32{1, 0}))
		svc.setFulltext(&seedOverrideFulltext{FulltextIndex: NewFulltextIndex(), seedIDs: []string{"missing"}})
		svc.applyBM25SeedHints()
		require.Empty(t, preferredSeedsForTest(t, svc.clusterIndex))
	})
}
