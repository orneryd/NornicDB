package gpu

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmptyWithDimensionsPreservesClusterPolicy(t *testing.T) {
	manager, err := NewManager(&Config{Enabled: false})
	require.NoError(t, err)
	embeddings := DefaultEmbeddingIndexConfig(4)
	embeddings.AutoSync = false
	embeddings.BatchThreshold = 17
	embeddings.InitialCap = 32
	config := DefaultKMeansConfig()
	config.NumClusters = 7
	config.MaxIterations = 13
	original := NewClusterIndex(manager, embeddings, config)
	require.NoError(t, original.Add("point", []float32{1, 0, 0, 0}))
	replacement := original.EmptyWithDimensions(8)
	require.Same(t, manager, replacement.manager)
	require.Equal(t, 8, replacement.Dimensions())
	require.Zero(t, replacement.Count())
	require.Equal(t, original.Config(), replacement.Config())
	require.Equal(t, original.autoSync, replacement.autoSync)
	require.Equal(t, 17, replacement.batchThreshold)
	require.Equal(t, 32, cap(replacement.nodeIDs))
	require.Equal(t, 4, original.Dimensions())
	require.Equal(t, 1, original.Count())
	require.NoError(t, replacement.Add("new", []float32{1, 0, 0, 0, 0, 0, 0, 0}))
}
