package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancelOnDoneContext struct {
	context.Context
	cancel context.CancelFunc
}

func (ctx *cancelOnDoneContext) Done() <-chan struct{} {
	ctx.cancel()
	return ctx.Context.Done()
}

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
		OverflowMax:         10,
	}, nil)
	require.NoError(t, err)
	require.Len(t, idx.overflow, 10)

	gen := NewIVFPQCandidateGen(idx, 3)
	cands, err := gen.SearchCandidates(context.Background(), []float32{0, 0, 1, 0, 0, 0, 0, 0}, 20, -1)
	require.NoError(t, err)
	require.NotEmpty(t, cands)
	require.LessOrEqual(t, len(cands), 20, "candidate generators must honor the adaptive caller's requested depth")
}

func TestIVFPQCandidateGen_DefaultNProbeAndNilIndex(t *testing.T) {
	gen := NewIVFPQCandidateGen(nil, 0)
	require.Equal(t, 1, gen.nprobe)

	_, err := gen.SearchCandidates(context.Background(), []float32{1, 0, 0}, 5, 0.0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not configured")
}

func TestIVFPQCandidateGen_EmptyIndexPropagatesCancellation(t *testing.T) {
	overlay := newANNMutationOverlay()
	overlay.Add("fresh", []float32{1, 0})
	gen := NewIVFPQCandidateGenWithOverlay(&IVFPQIndex{}, 1, overlay)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	candidates, err := gen.SearchCandidates(ctx, []float32{1, 0}, 5, -1)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, candidates)

	duringOverlay, cancelOverlay := context.WithCancel(context.Background())
	defer cancelOverlay()
	candidates, err = gen.SearchCandidates(&cancelOnDoneContext{Context: duringOverlay, cancel: cancelOverlay}, []float32{1, 0}, 5, -1)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, candidates)
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

	cands, err := gen.SearchCandidates(context.Background(), []float32{1}, 1, -1)
	require.NoError(t, err)
	require.Len(t, cands, 1)
	require.Equal(t, "doc-1", cands[0].ID)
}

func TestIVFPQCandidateGenMergesFreshVectorsAndSuppressesRemovedBaseVectors(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 2, NProbe: 1, RerankTopK: 10},
		centroids:    [][]float32{{1, 0}},
		centroidNorm: [][]float32{{1, 0}},
		codebooks: []ivfpqCodebook{
			{SubDim: 2, Codeword: [][]float32{{0, 0}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"removed", "base"}, CodeSize: 1, Codes: []byte{0, 0}},
		},
	}
	overlay := newANNMutationOverlay()
	overlay.Add("fresh", []float32{1, 0})
	overlay.Remove("removed")
	gen := NewIVFPQCandidateGenWithOverlay(idx, 1, overlay)

	candidates, err := gen.SearchCandidates(context.Background(), []float32{1, 0}, 10, -1)

	require.NoError(t, err)
	require.ElementsMatch(t, []string{"fresh", "base"}, candidateIDs(candidates))
}

func TestIVFPQCandidateGenSearchesExactOutliersOutsideProbedLists(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 2, NProbe: 1, RerankTopK: 10},
		centroids:    [][]float32{{0.8, 0.6}, {0, 1}},
		centroidNorm: [][]float32{{0.8, 0.6}, {0, 1}},
		codebooks: []ivfpqCodebook{
			{SubDim: 2, Codeword: [][]float32{{0, 0}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"decoy"}, CodeSize: 1, Codes: []byte{0}},
			{IDs: []string{"true-nearest"}, CodeSize: 1, Codes: []byte{0}},
		},
		overflow: []ivfpqOverflowVector{
			{ID: "true-nearest", Vector: []float32{1, 0}},
		},
	}
	gen := NewIVFPQCandidateGen(idx, 1)

	candidates, err := gen.SearchCandidates(context.Background(), []float32{1, 0}, 1, -1)

	require.NoError(t, err)
	require.Equal(t, []string{"true-nearest"}, candidateIDs(candidates))
}

func TestIVFPQCandidateGenDefersSimilarityThresholdUntilExactScoring(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 2, NProbe: 1, RerankTopK: 10},
		centroids:    [][]float32{{0.6, 0.8}},
		centroidNorm: [][]float32{{0.6, 0.8}},
		codebooks: []ivfpqCodebook{
			{SubDim: 2, Codeword: [][]float32{{0, 0}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"approx-underestimate"}, CodeSize: 1, Codes: []byte{0}},
		},
	}

	candidates, err := NewIVFPQCandidateGen(idx, 1).SearchCandidates(context.Background(), []float32{1, 0}, 1, 0.9)

	require.NoError(t, err)
	require.Equal(t, []string{"approx-underestimate"}, candidateIDs(candidates))
}

func TestServiceTracksCompressedIndexMutationsWithoutRebuild(t *testing.T) {
	service := NewServiceWithDimensions(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2)
	service.ivfpqIndex = &IVFPQIndex{profile: IVFPQProfile{Dimensions: 2}}

	service.indexMu.Lock()
	require.NoError(t, service.addVectorLocked("fresh", []float32{1, 0}))
	service.indexMu.Unlock()
	require.Equal(t, []string{"fresh"}, candidateIDs(service.ivfpqOverlay.Search(context.Background(), []float32{1, 0}, 10, -1)))

	service.indexMu.Lock()
	service.removeVectorLocked("fresh")
	service.indexMu.Unlock()
	require.Empty(t, service.ivfpqOverlay.Search(context.Background(), []float32{1, 0}, 10, -1))
	require.True(t, service.ivfpqOverlay.IsRemoved("fresh"))
}

func candidateIDs(candidates []Candidate) []string {
	ids := make([]string, len(candidates))
	for i := range candidates {
		ids[i] = candidates[i].ID
	}
	return ids
}
