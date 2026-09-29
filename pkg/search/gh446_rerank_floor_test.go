package search

// gh446_rerank_floor_test.go — regression tests for #446: the compressed
// (IVF/PQ) rescoring floor from profile.RerankTopK must not be clamped by
// the request-derived candidate limits. A request for k=10 still scores
// RerankTopK candidates, then cuts back to k after rescoring.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func gh446CompressedPipeline(floor int) *VectorSearchPipeline {
	gen := &IVFPQCandidateGen{index: &IVFPQIndex{profile: IVFPQProfile{RerankTopK: floor}}}
	return &VectorSearchPipeline{candidateGen: gen}
}

func TestGh446_CompressedRerankFloorNotClampedByRequestLimit(t *testing.T) {
	const floor = 2000
	pipeline := gh446CompressedPipeline(floor)

	t.Run("default_max_candidate_limit", func(t *testing.T) {
		opts := DefaultSearchOptions()
		opts.Limit = 10
		opts.CandidateTarget = 10
		opts.InitialOverfetchRatio = 2
		opts.MaxOverfetchRatio = 10
		opts.MaxCandidateLimit = 0 // the default: unlimited

		config := resolveVectorAdaptiveOverfetch(opts, pipeline)
		require.GreaterOrEqual(t, config.initialLimit, floor, "initial candidate depth must meet the rescore floor")
		require.GreaterOrEqual(t, config.maxLimit, floor, "maximum candidate depth must meet the rescore floor")

		// The generator's depth planning must not re-clamp the floor.
		gen := pipeline.candidateGen.(*IVFPQCandidateGen)
		require.Equal(t, floor, gen.preferredCandidateDepth(config.target, config.maxLimit))
	})

	t.Run("explicit_low_max_candidate_limit", func(t *testing.T) {
		opts := DefaultSearchOptions()
		opts.Limit = 10
		opts.CandidateTarget = 10
		opts.InitialOverfetchRatio = 2
		opts.MaxOverfetchRatio = 10
		opts.MaxCandidateLimit = 500

		config := resolveVectorAdaptiveOverfetch(opts, pipeline)
		require.GreaterOrEqual(t, config.initialLimit, floor)
		require.GreaterOrEqual(t, config.maxLimit, floor)
	})

	t.Run("uncompressed_pipeline_unaffected", func(t *testing.T) {
		opts := DefaultSearchOptions()
		opts.Limit = 10
		opts.CandidateTarget = 10
		opts.InitialOverfetchRatio = 2
		opts.MaxOverfetchRatio = 10
		opts.MaxCandidateLimit = 100

		pipeline := &VectorSearchPipeline{candidateGen: &recordingCandidateGenerator{}}
		config := resolveVectorAdaptiveOverfetch(opts, pipeline)
		require.Less(t, config.initialLimit, 100, "non-compressed pipelines stay request-bound")
	})
}
