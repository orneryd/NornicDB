package search

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The loose L2 bound includes error perpendicular to a selective query. Its
// coordinate projection must stay useful after persistence, without changing
// the complete Service result or relying on new fields in older bundles.
func TestIVFPQQueryBoundAndReload(t *testing.T) {
	svc, query := selectiveClusteredService(t, "ivfpq")
	pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	index := pipeline.candidateGen.(*IVFPQCandidateGen).index
	bound := index.queryScoreBound(query)
	// An exact codebook already has the optimal zero bound.
	require.LessOrEqual(t, bound, index.scoreBound())
	scaled := append([]float32(nil), query...)
	for d := range scaled {
		scaled[d] *= 7
	}
	require.InDelta(t, bound, index.queryScoreBound(scaled), 1e-7)

	path := filepath.Join(t.TempDir(), "index")
	require.NoError(t, SaveIVFPQBundle(path, index))
	loaded, err := LoadIVFPQBundle(path)
	require.NoError(t, err)
	require.Equal(t, index.maxCoordinateError, loaded.maxCoordinateError)
	require.Equal(t, bound, loaded.queryScoreBound(query))

	legacy := loaded
	legacy.maxCoordinateError = nil
	require.NoError(t, SaveIVFPQBundle(path, legacy))
	loaded, err = LoadIVFPQBundle(path)
	require.NoError(t, err)
	require.Equal(t, legacy.scoreBound(), loaded.queryScoreBound(query))
	legacy.residualKnown = false
	require.NoError(t, SaveIVFPQBundle(path, legacy))
	loaded, err = LoadIVFPQBundle(path)
	require.NoError(t, err)
	require.True(t, math.IsInf(loaded.queryScoreBound(query), 1))

	opts := DefaultSearchOptions()
	opts.Limit = 20
	opts.MaxOverfetchRatio = math.MaxFloat64
	threshold := 0.95
	opts.MinSimilarity = &threshold
	fallback := false
	opts.FallbackEnabled = &fallback
	response, err := svc.Search(context.Background(), "", query, opts)
	require.NoError(t, err)
	require.Len(t, response.Results, 11)
	require.True(t, response.RetrievalExhausted)
	for _, result := range response.Results {
		require.InDelta(t, 1, result.Similarity, 1e-6)
	}
	t.Logf("global bound=%g query bound=%g retries=%d", index.scoreBound(), bound, response.Metrics.VectorOverfetchRetries)
}
