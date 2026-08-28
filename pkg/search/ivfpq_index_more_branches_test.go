package search

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIVFPQIndex_SearchApproxMoreGuardBranches(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 2, NProbe: 0, RerankTopK: 0},
		centroids:    [][]float32{{1, 0}},
		centroidNorm: nil, // mismatch triggers normalize branch
		codebooks: []ivfpqCodebook{
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"a", "b"}, CodeSize: 0, Codes: []byte{1, 1}}, // codeSize<=0 branch skip
		},
	}

	out, err := idx.SearchApprox(nil, []float32{1, 0}, -5, -1, -1)
	require.NoError(t, err)
	require.Empty(t, out)

	idx.lists[0] = ivfpqList{IDs: []string{"a"}, CodeSize: 2, Codes: []byte{1, 1, 0, 0}} // IDs shorter than maxCodes
	out, err = idx.SearchApprox(context.Background(), []float32{1, 0}, 1, -1, -1)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "a", out[0].ID)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = idx.SearchApprox(cancelled, []float32{1, 0}, 1, -1, 1)
	require.ErrorIs(t, err, context.Canceled)
}

func TestIVFPQIndex_ScratchAndLimitMoreBranches(t *testing.T) {
	idx := &IVFPQIndex{}
	s := idx.getScratch(0)
	require.NotNil(t, s)
	require.Empty(t, s.lut)

	idx.codebooks = []ivfpqCodebook{{SubDim: 1, Codeword: [][]float32{{0}, {1}}}}
	idx.scratchPool = syncPoolZero()
	s = idx.getScratch(3)
	require.NotNil(t, s)
	require.Len(t, s.lut, 1)
	require.GreaterOrEqual(t, cap(s.heapData), 3)
	idx.putScratch(s)

	// putScratch guard when pool has nil New and when scratch is nil.
	idx.scratchPool = syncPoolZero()
	idx.putScratch(nil)
	idx.putScratch(&ivfpqScratch{})

}

func syncPoolZero() sync.Pool {
	return sync.Pool{}
}

func TestIVFPQIndex_SearchApprox_ListBoundsAndSimilarityFilter(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 1, NProbe: 2, RerankTopK: 1},
		centroids:    [][]float32{{1}, {0}},
		centroidNorm: [][]float32{{1}, {0}},
		codebooks: []ivfpqCodebook{
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
		},
		// Only one list for two centroids: second centroid id is out-of-range and skipped.
		lists: []ivfpqList{
			{IDs: []string{"doc-1"}, CodeSize: 1, Codes: []byte{1}},
		},
	}

	// High threshold filters the only candidate.
	out, err := idx.SearchApprox(context.Background(), []float32{1}, 3, 100, 2)
	require.NoError(t, err)
	require.Empty(t, out)

	// Lower threshold returns the candidate and keeps deterministic id.
	out, err = idx.SearchApprox(context.Background(), []float32{1}, 3, -1, 2)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, "doc-1", out[0].ID)
}

func TestIVFPQResidualScore_TruncatedBranches(t *testing.T) {
	lut := [][]float32{
		{0.5, 1.0},
		{0.25, 0.75},
	}

	// code has fewer segments than LUT.
	score := ivfpqResidualScore(lut, []byte{1})
	require.InDelta(t, 1.0, score, 1e-6)

	// codeSize smaller than LUT segment count.
	score = ivfpqResidualScoreAt(lut, []byte{1, 0, 0, 1}, 2, 1)
	require.InDelta(t, 0.5, score, 1e-6)
}
