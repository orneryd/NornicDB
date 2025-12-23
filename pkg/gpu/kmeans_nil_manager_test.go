package gpu

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterIndex_SearchWithClusters_NilManagerDoesNotPanic(t *testing.T) {
	embConfig := DefaultEmbeddingIndexConfig(4)
	embConfig.GPUEnabled = true
	embConfig.AutoSync = true

	index := NewClusterIndex(nil, embConfig, &KMeansConfig{
		NumClusters:   2,
		MaxIterations: 5,
		Tolerance:     0.001,
		InitMethod:    "kmeans++",
	})

	require.NoError(t, index.Add("a", []float32{1, 0, 0, 0}))
	require.NoError(t, index.Add("b", []float32{0, 1, 0, 0}))
	require.NoError(t, index.Add("c", []float32{0, 0, 1, 0}))

	// Not clustered yet; SearchWithClusters should fall back to brute-force search
	// without panicking even though GPU manager is nil.
	results, err := index.SearchWithClusters([]float32{1, 0, 0, 0}, 2, 2)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "a", results[0].ID)
}

