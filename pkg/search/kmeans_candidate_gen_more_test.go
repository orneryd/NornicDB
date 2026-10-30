package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

func TestKMeansCandidateGen_SearchCandidatesMoreBranches(t *testing.T) {
	vectorIndex := NewVectorIndex(2)
	require.NoError(t, vectorIndex.Add("a", []float32{1, 0}))
	require.NoError(t, vectorIndex.Add("b", []float32{0, 1}))

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

	t.Run("invalid selected cluster returns empty results", func(t *testing.T) {
		gen := NewKMeansCandidateGen(clusterIndex, vectorIndex, 1)
		gen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{99} })
		// k below the population: a request covering it would route every cluster.
		out, err := gen.SearchCandidates(context.Background(), []float32{1, 0}, 1, -1)
		require.NoError(t, err)
		require.Empty(t, out)
	})

	t.Run("cluster search error is wrapped", func(t *testing.T) {
		gen := NewKMeansCandidateGen(clusterIndex, vectorIndex, 1)
		_, err := gen.SearchCandidates(context.Background(), []float32{1, 0, 0}, 5, -1)
		require.Error(t, err)
		require.ErrorContains(t, err, "cluster search failed")
	})

	t.Run("min similarity and context cancellation branches", func(t *testing.T) {
		gen := NewKMeansCandidateGen(clusterIndex, vectorIndex, 1)
		gen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{0} })

		// High threshold filters out all results.
		out, err := gen.SearchCandidates(context.Background(), []float32{1, 0}, 5, 2.0)
		require.NoError(t, err)
		require.Empty(t, out)

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = gen.SearchCandidates(cancelled, []float32{1, 0}, 5, -1)
		require.ErrorIs(t, err, context.Canceled)
	})
}
