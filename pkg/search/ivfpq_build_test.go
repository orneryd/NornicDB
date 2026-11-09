package search

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildIVFPQFromVectorStore(t *testing.T) {
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 16)
	require.NoError(t, err)
	defer vfs.Close()

	for i := 0; i < 1200; i++ {
		vec := make([]float32, 16)
		vec[i%16] = 1
		require.NoError(t, vfs.Add(fmt.Sprintf("doc-%d", i), vec))
	}

	profile := IVFPQProfile{
		Dimensions:          16,
		IVFLists:            32,
		PQSegments:          4,
		PQBits:              4,
		NProbe:              4,
		RerankTopK:          50,
		TrainingSampleMax:   1000,
		KMeansMaxIterations: 8,
	}
	idx, stats, err := BuildIVFPQFromVectorStore(context.Background(), vfs, profile, []string{"doc-1", "doc-2"})
	require.NoError(t, err)
	require.NotNil(t, idx)
	require.NotNil(t, stats)
	require.Equal(t, 1200, idx.Count())
	require.Equal(t, 32, stats.ListCount)
	require.Greater(t, stats.AvgListSize, 0.0)
}

func TestNearestCentroidIndexCosine(t *testing.T) {
	centroids := [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	}
	idx := nearestCentroidIndexCosine([]float32{0.9, 0.1, 0}, centroids)
	require.Equal(t, 0, idx)
}
