package search

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type testResidentBuffer struct {
	values   []float32
	released int
}

func (b *testResidentBuffer) Release() { b.released++ }

func TestResidentBuildVectorsUploadOnceAndMergeGlobalIDs(t *testing.T) {
	var buffers []*testResidentBuffer
	r := residentBuildVectors[*testResidentBuffer]{dimensions: 2,
		upload: func(flat []float32) (*testResidentBuffer, error) {
			b := &testResidentBuffer{values: flat}
			buffers = append(buffers, b)
			return b, nil
		},
		score: func(ctx context.Context, b *testResidentBuffer, n int, qs [][]float32, k int) ([][]uint32, [][]float32, error) {
			vectors := make([][]float32, n)
			for i := range vectors {
				vectors[i] = b.values[i*2 : (i+1)*2]
			}
			ids, dists, err := exactGraphBuildAccelerator{}.candidateSearch(ctx, qs, vectors, k, true)
			out := make([][]uint32, len(ids))
			for qi, row := range ids {
				for _, id := range row {
					out[qi] = append(out[qi], uint32(id))
				}
			}
			return out, dists, err
		},
	}
	require.NoError(t, r.append(nil))
	ids, _, err := r.search(context.Background(), [][]float32{{1, 0}}, 0)
	require.NoError(t, err)
	require.Empty(t, ids[0])
	ids, _, err = r.search(context.Background(), [][]float32{{1, 0}}, 1)
	require.NoError(t, err)
	require.Empty(t, ids[0])
	require.NoError(t, r.append([][]float32{{1, 0}, {0, 1}}))
	require.NoError(t, r.append([][]float32{{-1, 0}, {1, 0}}))
	for i := 0; i < 2; i++ {
		ids, dists, err := r.search(context.Background(), [][]float32{{1, 0}, {0, 1}}, 3)
		require.NoError(t, err)
		require.Equal(t, [][]uint32{{0, 3, 1}, {1, 0, 2}}, ids)
		require.InDeltaSlice(t, []float32{0, 0, 1}, dists[0], 1e-6)
	}
	require.Len(t, buffers, 2, "searches reuse the two uploads")
	require.ErrorIs(t, r.append([][]float32{{1}}), ErrDimensionMismatch)
	require.Equal(t, 4, r.count)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = r.search(cancelled, [][]float32{{1, 0}}, 1)
	require.ErrorIs(t, err, context.Canceled)
	r.close()
	r.close()
	for _, b := range buffers {
		require.Equal(t, 1, b.released)
	}
	require.Zero(t, r.count)
}

func TestResidentBuildVectorsPropagatesDeviceErrors(t *testing.T) {
	failure := errors.New("device failure")
	r := residentBuildVectors[*testResidentBuffer]{dimensions: 2, upload: func([]float32) (*testResidentBuffer, error) { return nil, failure }}
	require.ErrorIs(t, r.append([][]float32{{1, 0}}), failure)
	require.Zero(t, r.count)
	require.Empty(t, r.shards)
	r.upload = func(flat []float32) (*testResidentBuffer, error) { return &testResidentBuffer{values: flat}, nil }
	require.NoError(t, r.append([][]float32{{1, 0}}))
	defer r.close()
	r.score = func(context.Context, *testResidentBuffer, int, [][]float32, int) ([][]uint32, [][]float32, error) {
		return nil, nil, failure
	}
	_, _, err := r.search(context.Background(), [][]float32{{1, 0}}, 1)
	require.ErrorIs(t, err, failure)
}
