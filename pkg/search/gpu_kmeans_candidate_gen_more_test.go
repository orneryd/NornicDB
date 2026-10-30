package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

func TestGPUKMeansCandidateGen_SearchCandidatesMoreBranches(t *testing.T) {
	embCfg := gpu.DefaultEmbeddingIndexConfig(2)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false

	clusterIndex := gpu.NewClusterIndex(nil, embCfg, &gpu.KMeansConfig{
		NumClusters:   1,
		MaxIterations: 5,
		Tolerance:     0.001,
		InitMethod:    "kmeans++",
	})
	require.NoError(t, clusterIndex.Add("a", []float32{1, 0}))
	require.NoError(t, clusterIndex.Add("b", []float32{0, 1}))
	require.NoError(t, clusterIndex.Cluster())

	// Selector returns invalid cluster id => no member IDs.
	gen := NewGPUKMeansCandidateGen(clusterIndex, 1)
	gen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{99} })
	// k below the population: a request covering it would route every cluster.
	out, err := gen.SearchCandidates(context.Background(), []float32{1, 0}, 1, -1)
	require.NoError(t, err)
	require.Empty(t, out)

	// Valid cluster with mismatched query dims triggers ScoreSubset error.
	gen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{0} })
	_, err = gen.SearchCandidates(context.Background(), []float32{1, 0, 0}, 5, -1)
	require.Error(t, err)

	// High minSimilarity filters all candidates.
	out, err = gen.SearchCandidates(context.Background(), []float32{1, 0}, 5, 2.0)
	require.NoError(t, err)
	require.Empty(t, out)
}
