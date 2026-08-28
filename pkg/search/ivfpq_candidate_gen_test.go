package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIVFPQCandidateGen_SearchCandidates(t *testing.T) {
	dir := t.TempDir()
	vfs, err := NewVectorFileStore(fmt.Sprintf("%s/vectors", dir), 8)
	require.NoError(t, err)
	defer vfs.Close()

	for i := 0; i < 700; i++ {
		vec := []float32{0, 0, 1, 0, 0, 0, 0, 0}
		if i%3 == 0 {
			vec = []float32{0, 0, 0, 1, 0, 0, 0, 0}
		}
		require.NoError(t, vfs.Add(fmt.Sprintf("d-%d", i), vec))
	}
	idx, _, err := BuildIVFPQFromVectorStore(context.Background(), vfs, IVFPQProfile{
		Dimensions:          8,
		IVFLists:            12,
		PQSegments:          4,
		PQBits:              4,
		NProbe:              3,
		RerankTopK:          50,
		TrainingSampleMax:   600,
		KMeansMaxIterations: 6,
	}, nil)
	require.NoError(t, err)

	gen := NewIVFPQCandidateGen(idx, 3)
	cands, err := gen.SearchCandidates(context.Background(), []float32{0, 0, 1, 0, 0, 0, 0, 0}, 20, -1)
	require.NoError(t, err)
	require.NotEmpty(t, cands)
	require.LessOrEqual(t, len(cands), 20, "candidate generators must honor the adaptive caller's requested depth")
}

func TestIVFPQCandidateGen_DefaultNProbeAndNilIndex(t *testing.T) {
	gen := NewIVFPQCandidateGen(nil, 0)
	require.Equal(t, 1, gen.nprobe)

	_, err := gen.SearchCandidates(nil, []float32{1, 0, 0}, 5, 0.0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not configured")
}

func TestIVFPQCandidateGen_DefaultNProbeFromIndexProfile(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 1, NProbe: 4},
		centroids:    [][]float32{{1}},
		centroidNorm: [][]float32{{1}},
		codebooks: []ivfpqCodebook{
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"doc-1"}, CodeSize: 1, Codes: []byte{1}},
		},
	}

	gen := NewIVFPQCandidateGen(idx, 0)
	require.Equal(t, 4, gen.nprobe)

	cands, err := gen.SearchCandidates(nil, []float32{1}, 1, -1)
	require.NoError(t, err)
	require.Len(t, cands, 1)
	require.Equal(t, "doc-1", cands[0].ID)
}
