package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIVFPQIndex_SearchApprox(t *testing.T) {
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), 8)
	require.NoError(t, err)
	defer vfs.Close()

	for i := 0; i < 800; i++ {
		vec := []float32{1, 0, 0, 0, 0, 0, 0, 0}
		if i%2 == 1 {
			vec = []float32{0, 1, 0, 0, 0, 0, 0, 0}
		}
		require.NoError(t, vfs.Add(fmt.Sprintf("doc-%d", i), vec))
	}

	profile := IVFPQProfile{
		Dimensions:          8,
		IVFLists:            16,
		PQSegments:          4,
		PQBits:              4,
		NProbe:              4,
		RerankTopK:          50,
		TrainingSampleMax:   700,
		KMeansMaxIterations: 6,
	}
	idx, _, err := BuildIVFPQFromVectorStore(context.Background(), vfs, profile, nil)
	require.NoError(t, err)

	out, err := idx.SearchApprox(context.Background(), []float32{1, 0, 0, 0, 0, 0, 0, 0}, 10, -1, 4)
	require.NoError(t, err)
	require.NotEmpty(t, out)
	require.GreaterOrEqual(t, out[0].Score, out[len(out)-1].Score)
}

func TestIVFPQIndexSearchApproxHonorsRequestedDepthOverLegacyRerankCap(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 1, NProbe: 2, RerankTopK: 1},
		centroids:    [][]float32{{1}, {1}},
		centroidNorm: [][]float32{{1}, {1}},
		codebooks: []ivfpqCodebook{
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"doc-1"}, CodeSize: 1, Codes: []byte{1}},
			{IDs: []string{"doc-2"}, CodeSize: 1, Codes: []byte{1}},
		},
	}

	out, err := idx.SearchApprox(context.Background(), []float32{1}, 2, -1, 2)

	require.NoError(t, err)
	require.Len(t, out, 2)
}

func TestIVFPQ_InternalHelpersAndScratch(t *testing.T) {
	codebooks := []ivfpqCodebook{
		{SubDim: 1, Codeword: [][]float32{{0.1}, {0.9}}},
		{SubDim: 1, Codeword: [][]float32{{0.2}, {0.8}}},
	}
	query := []float32{1, 1}

	lut := ivfpqQueryLUT(query, codebooks)
	require.Len(t, lut, 2)
	require.InDelta(t, 0.9, lut[0][1], 1e-6)
	require.InDelta(t, 0.8, lut[1][1], 1e-6)

	prealloc := [][]float32{make([]float32, 2), make([]float32, 2)}
	ivfpqQueryLUTInto(prealloc, query, codebooks)
	require.InDelta(t, lut[0][1], prealloc[0][1], 1e-6)

	code := []byte{1, 1}
	require.InDelta(t, 1.7, ivfpqResidualScore(lut, code), 1e-6)
	flatCodes := []byte{0, 0, 1, 1}
	require.InDelta(t, 1.7, ivfpqResidualScoreAt(lut, flatCodes, 2, 2), 1e-6)

	h := newCandidateMinHeap(2)
	h.push(Candidate{ID: "a", Score: 0.1})
	h.push(Candidate{ID: "b", Score: 0.9})
	h.push(Candidate{ID: "c", Score: 0.5}) // should evict a
	out := h.toSortedDescending()
	require.Equal(t, []string{"b", "c"}, []string{out[0].ID, out[1].ID})

	buf := make([]Candidate, 0, 1)
	h2 := newCandidateMinHeapWithBuffer(buf, 2)
	h2.push(Candidate{ID: "x", Score: 0.4})
	h2.push(Candidate{ID: "y", Score: 0.3})
	h2.push(Candidate{ID: "z", Score: 0.8})
	out = h2.toSortedDescending()
	require.Equal(t, []string{"z", "x"}, []string{out[0].ID, out[1].ID})

	h3 := newCandidateMinHeap(0)
	require.Equal(t, 1, h3.cap)
	h4 := newCandidateMinHeapWithBuffer(make([]Candidate, 0, 4), 0)
	require.Equal(t, 1, h4.cap)

	idx := &IVFPQIndex{
		profile:   IVFPQProfile{Dimensions: 2, NProbe: 1, RerankTopK: 10},
		codebooks: codebooks,
	}
	s := idx.getScratch(4)
	require.NotNil(t, s)
	require.Len(t, s.lut, 2)
	idx.putScratch(s)
	idx.putScratch(nil)
}

func TestIVFPQ_SearchApproxBranchesAndProfileHelpers(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 2, NProbe: 1, RerankTopK: 10, IVFLists: 2, PQSegments: 2, PQBits: 4},
		centroids:    [][]float32{{1, 0}, {0, 1}},
		centroidNorm: [][]float32{{1, 0}, {0, 1}},
		codebooks: []ivfpqCodebook{
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"a"}, CodeSize: 2, Codes: []byte{1, 0}},
			{IDs: []string{"b"}, CodeSize: 2, Codes: []byte{0, 1}},
		},
	}

	out, err := idx.SearchApprox(context.Background(), []float32{1, 0}, 2, -1.0, 1)
	require.NoError(t, err)
	require.NotEmpty(t, out)
	require.Equal(t, "a", out[0].ID)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = idx.SearchApprox(cancelled, []float32{1, 0}, 2, -1.0, 1)
	require.ErrorIs(t, err, context.Canceled)

	var nilIdx *IVFPQIndex
	out, err = nilIdx.SearchApprox(context.Background(), []float32{1, 0}, 2, 0, 1)
	require.NoError(t, err)
	require.Nil(t, out)

	require.Equal(t, 2, idx.Count())
	require.Equal(t, idx.profile, idx.Profile())
	require.True(t, idx.compatibleProfile(IVFPQProfile{Dimensions: 2, IVFLists: 2, PQSegments: 2, PQBits: 4}))
	require.False(t, idx.compatibleProfile(IVFPQProfile{Dimensions: 3, IVFLists: 2, PQSegments: 2, PQBits: 4}))
	require.Equal(t, IVFPQProfile{}, nilIdx.Profile())
	require.Equal(t, 0, nilIdx.Count())
	require.False(t, nilIdx.compatibleProfile(idx.profile))
	require.Equal(t, 0.0, ivfpqBytesPerVector(IVFPQProfile{PQSegments: 0}))
	require.Equal(t, 10.0, ivfpqBytesPerVector(IVFPQProfile{PQSegments: 8}))
}

func TestIVFPQList_AppendCodeAndCodeAt(t *testing.T) {
	var l ivfpqList
	l.appendCode(nil)
	require.Equal(t, 0, l.CodeSize)

	l.appendCode([]byte{1, 2})
	require.Equal(t, 2, l.CodeSize)
	l.appendCode([]byte{3, 4})
	require.Equal(t, []byte{1, 2, 3, 4}, l.Codes)

	code, ok := l.codeAt(1)
	require.True(t, ok)
	require.Equal(t, []byte{3, 4}, code)

	_, ok = l.codeAt(-1)
	require.False(t, ok)
	_, ok = l.codeAt(5)
	require.False(t, ok)

	var nilList *ivfpqList
	_, ok = nilList.codeAt(0)
	require.False(t, ok)
}
