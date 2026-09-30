package search

// gh446_rerank_floor_test.go — regression tests for #446: the compressed
// (IVF/PQ) rescoring floor from profile.RerankTopK must not be clamped by
// the request-derived candidate limits. A request for k=10 still scores
// RerankTopK candidates, then cuts back to k after rescoring.

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func gh446ServiceRecallFixture(t testing.TB) (*VectorSearchPipeline, *SearchOptions) {
	t.Helper()
	const floor = 32
	vectors, err := NewVectorFileStore(t.TempDir()+"/vectors", 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, vectors.Close()) })
	index := &IVFPQIndex{
		profile:   IVFPQProfile{Dimensions: 2, NProbe: 2, RerankTopK: floor},
		centroids: [][]float32{{0.1, 0}, {0.9, 0}},
		codebooks: []ivfpqCodebook{{SubDim: 2, Codeword: [][]float32{
			{0.6, float32(math.Sqrt(1 - 0.7*0.7))},
			{0.05, float32(math.Sqrt(1 - 0.95*0.95))},
		}}},
		lists: []ivfpqList{{CodeSize: 1}, {CodeSize: 1}},
	}
	for position := 0; position < 50; position++ {
		listID, code, cosine := 0, byte(0), float32(0.7)
		if position < 10 {
			listID, code, cosine = 1, 1, 0.95
		}
		id := fmt.Sprintf("doc-%02d", position)
		require.NoError(t, vectors.Add(id, []float32{cosine, float32(math.Sqrt(1 - float64(cosine*cosine)))}))
		index.lists[listID].IDs = append(index.lists[listID].IDs, id)
		index.lists[listID].Codes = append(index.lists[listID].Codes, code)
	}
	pipeline := NewVectorSearchPipeline(NewIVFPQCandidateGen(index, 2), NewCPUExactScorer(vectors))
	options := DefaultSearchOptions()
	options.Limit, options.CandidateTarget = 10, 10
	return pipeline, options
}

func TestGh446_ServiceRecallWithUnequalCentroidNorms(t *testing.T) {
	pipeline, options := gh446ServiceRecallFixture(t)
	service := &Service{}
	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, options, nil, nil)
	require.NoError(t, err)
	require.Len(t, results, 10)
	require.Equal(t, 32, stats.rawCandidates)
	for _, result := range results {
		require.InDelta(t, 0.95, result.Score, 1e-6, "top ten exact neighbors must survive compressed candidate selection")
	}
}

func BenchmarkGh446_ServiceRecall(b *testing.B) {
	for _, baseline := range []bool{true, false} {
		name := "raw_centroid_reconstruction"
		if baseline {
			name = "normalized_centroid_baseline"
		}
		b.Run(name, func(b *testing.B) {
			pipeline, options := gh446ServiceRecallFixture(b)
			if baseline {
				index := pipeline.candidateGen.(*IVFPQCandidateGen).index
				index.centroids = normalizeCentroids(index.centroids)
			}
			service := &Service{}
			query := []float32{1, 0}
			var results []indexResult
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				var err error
				results, _, err = service.adaptiveVectorSearch(context.Background(), pipeline, query, options, nil, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			hits := 0
			for _, result := range results {
				if result.Score > 0.9 {
					hits++
				}
			}
			b.ReportMetric(float64(hits)/10, "recall@10")
		})
	}
}

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
