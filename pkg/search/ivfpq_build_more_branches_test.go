package search

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildIVFPQFromVectorStore_InputValidationBranches(t *testing.T) {
	profile := IVFPQProfile{Dimensions: 4, IVFLists: 2, PQSegments: 2, PQBits: 2, TrainingSampleMax: 4, KMeansMaxIterations: 2}

	_, _, err := BuildIVFPQFromVectorStore(context.Background(), nil, profile, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "vector file store")

	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()

	_, _, err = BuildIVFPQFromVectorStore(context.Background(), vfs, IVFPQProfile{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid dimensions")

	badSeg := profile
	badSeg.PQSegments = 3
	_, _, err = BuildIVFPQFromVectorStore(context.Background(), vfs, badSeg, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid pq segments")

	require.NoError(t, vfs.Add("only", []float32{1, 0, 0, 0}))
	_, _, err = BuildIVFPQFromVectorStore(context.Background(), vfs, profile, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "insufficient training vectors")
}

func TestIVFPQCollectAndTrainHelperBranches(t *testing.T) {
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(filepath.Join(dir, "vectors"), 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()
	for i := 0; i < 6; i++ {
		vec := []float32{0, 0, 0, 0}
		vec[i%4] = 1
		require.NoError(t, vfs.Add(fmt.Sprintf("doc-%d", i), vec))
	}

	ctxCancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ivfpqCollectTrainingSample(ctxCancelled, vfs, 4, []string{"doc-1"}, rand.New(rand.NewSource(7)))
	require.ErrorIs(t, err, context.Canceled)

	ctxCancelled2, cancel2 := context.WithCancel(context.Background())
	sample, err := ivfpqCollectTrainingSample(context.Background(), vfs, 4, []string{"doc-1", "", "missing"}, rand.New(rand.NewSource(9)))
	require.NoError(t, err)
	require.Len(t, sample, 4)
	cancel2()
	_, err = ivfpqCollectTrainingSample(ctxCancelled2, vfs, 4, nil, rand.New(rand.NewSource(11)))
	require.ErrorIs(t, err, context.Canceled)

	profile := IVFPQProfile{Dimensions: 4, PQSegments: 2, PQBits: 2, KMeansMaxIterations: 2}
	_, err = ivfpqTrainPQCodebooks(ctxCancelled, sample, [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}, profile, rand.New(rand.NewSource(1)))
	require.ErrorIs(t, err, context.Canceled)

	_, err = ivfpqTrainPQCodebooks(context.Background(), [][]float32{}, [][]float32{{1, 0, 0, 0}}, profile, rand.New(rand.NewSource(1)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty segment training set")

	_, err = ivfpqTrainKMeans(context.Background(), nil, 2, 2, rand.New(rand.NewSource(3)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no vectors")

	_, err = ivfpqTrainKMeans(context.Background(), [][]float32{{1, 0}}, 0, 2, rand.New(rand.NewSource(3)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid k")

	centers, err := ivfpqTrainKMeans(context.Background(), [][]float32{{1, 0}, {0, 1}}, 10, 0, rand.New(rand.NewSource(5)))
	require.NoError(t, err)
	require.Len(t, centers, 2)
}

func TestIVFPQBuildMathHelperBranches(t *testing.T) {
	require.Nil(t, flatToVectors([]float32{1, 2, 3}, 0))
	require.Nil(t, flatToVectors([]float32{1, 2, 3}, 4))
	require.Equal(t, 0.0, ivfpqAvgListSize(nil))
	require.Equal(t, 0.0, ivfpqBytesPerVector(IVFPQProfile{PQSegments: 0}))

	centroids := [][]float32{{1, 0}, {0, 1}}
	require.Nil(t, ivfpqTopCentroidsByQuery([]float32{1, 0}, nil, 1))
	require.Equal(t, []int{0}, ivfpqTopCentroidsByQuery([]float32{1, 0}, centroids, 0))
	require.Len(t, ivfpqTopCentroidsByQuery([]float32{1, 0}, centroids, 10), 2)
}
