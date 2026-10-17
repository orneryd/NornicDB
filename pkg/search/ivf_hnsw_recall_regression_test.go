package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A query's nearest neighbours commonly belong to one topic/cluster. The
// candidate budget is global top-K, not an equal quota for each probed list.
func TestIVFHNSWPublicQueryPreservesConcentratedNeighbours(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "1")
	t.Setenv("NORNICDB_KMEANS_NPROBE", "32")
	const dimensions, clusters, perCluster = 32, 32, 40
	engine := newNamespacedEngine(t)
	want := make([]string, 0, perCluster)
	for cluster := 0; cluster < clusters; cluster++ {
		for point := 0; point < perCluster; point++ {
			vec := make([]float32, dimensions)
			vec[cluster] = 1
			vec[(cluster+1)%dimensions] = float32(point) / 1000
			id := fmt.Sprintf("topic-%02d-point-%02d", cluster, point)
			_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, NamedEmbeddings: map[string][]float32{"default": vec}})
			require.NoError(t, err)
			if cluster == 0 {
				want = append(want, id)
			}
		}
	}
	svc := NewServiceWithDimensions(engine, dimensions)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.EnableClustering(nil, clusters)
	svc.SetMinEmbeddingsForClustering(1)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen, "exercise the routed ANN consumer, not a brute-force fallback")

	query := make([]float32, dimensions)
	query[0] = 1
	for _, exact := range []bool{true, false} {
		hits, err := svc.VectorQueryNodesWithOptions(context.Background(), query, VectorQuerySpec{Limit: perCluster, Exact: exact}, nil)
		require.NoError(t, err)
		ids := make([]string, len(hits))
		for i, hit := range hits {
			ids[i] = hit.ID
		}
		require.ElementsMatch(t, want, ids, "exact=%t: all nearest points are in one probed cluster", exact)
	}
}

// Every nonempty cluster, including small lexical-selected clusters, has a graph.
func TestIVFHNSWPublicHybridSearchPreservesLexicalGraphRoute(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_ENABLED", "true")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "3")
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_MAX_CLUSTERS", "1")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "scalar")
	t.Setenv("NORNICDB_KMEANS_NPROBE", "1")
	t.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM", "0.1")
	t.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX", "0.9")
	engine := newNamespacedEngine(t)
	for i := 0; i < 6; i++ {
		text, vec := "alpha graph", []float32{1, 0}
		if i >= 4 {
			text, vec = "beta auth", []float32{0.8, 0.6}
		}
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("point-%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": text}, NamedEmbeddings: map[string][]float32{"default": vec}})
		require.NoError(t, err)
	}
	svc := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen)
	query := []float32{1, 0}
	semantic := svc.clusterIndex.FindNearestClusters(query, 1)
	selected := svc.selectHybridClusters(withQueryText(context.Background(), "beta auth"), query, 1)
	require.Len(t, selected, 1)
	require.NotEqual(t, semantic, selected, "the real BM25-derived profile must select the lexical topic")
	require.NotNil(t, svc.clusterHNSW[selected[0]], "small lexical-selected cluster must have a graph")
	require.Len(t, svc.clusterHNSW, svc.clusterIndex.NumClusters())
	require.Positive(t, svc.clusterHNSW[selected[0]].QuantizationBytes())
	options := DefaultSearchOptions()
	options.Limit = 6
	options.MinRRFScore = 0.001
	options.SkipMatches = true
	response, err := svc.Search(context.Background(), "beta auth", query, options)
	require.NoError(t, err)
	found := 0
	for _, hit := range response.Results {
		if hit.ID == "point-4" || hit.ID == "point-5" {
			found++
			require.Positive(t, hit.BM25Rank)
			require.Positive(t, hit.VectorRank, "lexical routing must reach the cluster graph")
		}
	}
	require.Equal(t, 2, found)
}
