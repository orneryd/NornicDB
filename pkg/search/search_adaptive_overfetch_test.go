package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type recordingCandidateGenerator struct {
	candidates []Candidate
	limits     []int
}

type recordingApproximateCandidateGenerator struct {
	recordingCandidateGenerator
}

type shortNonExhaustiveCandidateGenerator struct {
	limits []int
}

func (g *shortNonExhaustiveCandidateGenerator) SearchCandidates(_ context.Context, _ []float32, limit int, _ float64) ([]Candidate, error) {
	results, _, err := g.searchCandidatesWithExhaustion(context.Background(), nil, limit, 0)
	return results, err
}

func (g *shortNonExhaustiveCandidateGenerator) searchCandidatesWithExhaustion(_ context.Context, _ []float32, limit int, _ float64) ([]Candidate, bool, error) {
	g.limits = append(g.limits, limit)
	if limit <= 3 {
		return []Candidate{
			{ID: "doc-1-chunk-0", Score: 1},
			{ID: "doc-1-chunk-1", Score: 0.9},
		}, false, nil
	}
	return []Candidate{
		{ID: "doc-1-chunk-0", Score: 1},
		{ID: "doc-1-chunk-1", Score: 0.9},
		{ID: "doc-2-chunk-0", Score: 0.8},
		{ID: "doc-3-chunk-0", Score: 0.7},
	}, false, nil
}

func (g *recordingApproximateCandidateGenerator) preferredCandidateDepth(_, maximum int) int {
	return maximum
}

type recordingBM25Index struct {
	bm25Index
	results []indexResult
	limits  []int
}

func (i *recordingBM25Index) Search(query string, limit int) []indexResult {
	return i.SearchAllowed(query, limit, nil)
}

func (i *recordingBM25Index) SearchAllowed(_ string, limit int, allowed func(string) bool) []indexResult {
	i.limits = append(i.limits, limit)
	var ranked []indexResult
	for _, result := range i.results {
		if allowed == nil || allowed(result.ID) {
			ranked = append(ranked, result)
		}
	}
	if limit > len(ranked) {
		limit = len(ranked)
	}
	return ranked[:limit]
}

func (g *recordingCandidateGenerator) SearchCandidates(_ context.Context, _ []float32, limit int, _ float64) ([]Candidate, error) {
	g.limits = append(g.limits, limit)
	if limit > len(g.candidates) {
		limit = len(g.candidates)
	}
	return g.candidates[:limit], nil
}

func TestAdaptiveVectorSearchWidensOnlyWhenUniqueNodesUnderfill(t *testing.T) {
	generator := &recordingCandidateGenerator{candidates: []Candidate{
		{ID: "doc-1-chunk-0", Score: 1.0},
		{ID: "doc-1-chunk-1", Score: 0.9},
		{ID: "doc-2-chunk-0", Score: 0.8},
		{ID: "doc-3-chunk-0", Score: 0.7},
	}}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	pipeline := NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(2)

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil, nil)

	require.NoError(t, err)
	require.Equal(t, []int{2, 4}, generator.limits)
	require.Equal(t, []string{"doc-1", "doc-2"}, indexResultIDs(results))
	require.Equal(t, 1, stats.retries)
	require.Equal(t, 4, stats.rawCandidates)
}

func TestAdaptiveVectorSearchDoesNotRetryWhenInitialResultsFillTarget(t *testing.T) {
	generator := &recordingCandidateGenerator{candidates: []Candidate{
		{ID: "doc-1", Score: 1.0},
		{ID: "doc-2", Score: 0.9},
	}}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	pipeline := NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(2)

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil, nil)

	require.NoError(t, err)
	require.Equal(t, []int{2}, generator.limits)
	require.Len(t, results, 2)
	require.Zero(t, stats.retries)
}

func TestAdaptiveVectorSearchWidensShortNonExhaustiveApproximateResults(t *testing.T) {
	generator := &shortNonExhaustiveCandidateGenerator{}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	pipeline := NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(3)

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil, nil)

	require.NoError(t, err)
	require.Equal(t, []int{3, 6}, generator.limits)
	require.Equal(t, []string{"doc-1", "doc-2", "doc-3"}, indexResultIDs(results))
	require.Equal(t, 1, stats.retries)
}

func TestAdaptiveVectorSearchScoresFullApproximateBudgetBeforeNodeCollapse(t *testing.T) {
	generator := &recordingApproximateCandidateGenerator{recordingCandidateGenerator: recordingCandidateGenerator{candidates: []Candidate{
		{ID: "doc-1-chunk-0", Score: 1.0},
		{ID: "doc-2-chunk-0", Score: 0.9},
		{ID: "doc-3-chunk-0", Score: 0.8},
		{ID: "best-node-chunk-7", Score: 0.99},
	}}}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	pipeline := NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(2)
	opts.MaxOverfetchRatio = 2

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil, nil)

	require.NoError(t, err)
	require.Equal(t, []int{4}, generator.limits)
	require.Equal(t, []string{"doc-1", "best-node"}, indexResultIDs(results))
	require.Zero(t, stats.retries)
}

func TestHNSWRecallBudgetIsWideButBoundedForLargeResultSets(t *testing.T) {
	generator := &HNSWCandidateGen{}

	require.Equal(t, 200, generator.preferredCandidateDepth(20, 200))
	require.Equal(t, 400, generator.preferredCandidateDepth(100, 1_000))
	require.Equal(t, 75, generator.preferredCandidateDepth(20, 75))
}

func TestAdaptiveVectorSearchStopsAtConfiguredCap(t *testing.T) {
	generator := &recordingCandidateGenerator{candidates: []Candidate{
		{ID: "doc-1-chunk-0", Score: 1.0},
		{ID: "doc-1-chunk-1", Score: 0.9},
		{ID: "doc-1-chunk-2", Score: 0.8},
		{ID: "doc-2", Score: 0.7},
	}}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	pipeline := NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(3)
	opts.MaxCandidateLimit = 4

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil, nil)

	require.NoError(t, err)
	require.Equal(t, []int{3, 4}, generator.limits)
	require.Len(t, results, 2)
	require.Equal(t, 1, stats.retries)
}

func TestResolveAdaptiveOverfetchAllowsConfiguredBudgetAboveDefault(t *testing.T) {
	opts := DefaultSearchOptions()
	opts.Limit = 6_000
	opts.CandidateTarget = 6_000
	opts.InitialOverfetchRatio = 1
	opts.MaxOverfetchRatio = 2
	opts.MaxCandidateLimit = 10_000

	config := resolveAdaptiveOverfetch(opts)

	require.Equal(t, 6_000, config.target)
	require.Equal(t, 6_000, config.initialLimit)
	require.Equal(t, 10_000, config.maxLimit)
}

func TestAdaptiveVectorSearchUsesIVFPQRerankDepthIndependentOfResultLimit(t *testing.T) {
	index := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 1, NProbe: 1, RerankTopK: 3},
		centroids:    [][]float32{{1}},
		centroidNorm: [][]float32{{1}},
		codebooks: []ivfpqCodebook{
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"doc-1", "doc-2", "doc-3"}, CodeSize: 1, Codes: []byte{1, 1, 1}},
		},
	}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 1)
	pipeline := NewVectorSearchPipeline(NewIVFPQCandidateGen(index, 1), &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(1)
	opts.MaxCandidateLimit = 1

	results, _, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1}, opts, nil, nil)

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 3, resolveVectorAdaptiveOverfetch(opts, pipeline).initialLimit)
	require.Equal(t, 3, resolveVectorAdaptiveOverfetch(opts, pipeline).maxLimit)

	deeper := adaptiveOverfetchTestOptions(4)
	deeper.MaxCandidateLimit = 0
	require.GreaterOrEqual(t, resolveVectorAdaptiveOverfetch(deeper, pipeline).initialLimit, 4)
}

func TestCompressedVectorSearchPlansRescoreFloorForSmallRequest(t *testing.T) {
	index := &IVFPQIndex{profile: IVFPQProfile{RerankTopK: 2_000}}
	pipeline := NewVectorSearchPipeline(NewIVFPQCandidateGen(index, 1), &IdentityExactScorer{})
	opts := DefaultSearchOptions()
	opts.Limit = 10
	opts.CandidateTarget = 10
	opts.MaxCandidateLimit = 0

	config := resolveVectorAdaptiveOverfetch(opts, pipeline)

	require.Equal(t, 10, config.target)
	require.Equal(t, 2_000, config.initialLimit)
	require.Equal(t, 2_000, config.maxLimit)
}

func TestCompressedVectorSearchRescoresFloorBeforeTrimmingResults(t *testing.T) {
	index := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 1, NProbe: 1, RerankTopK: 5},
		centroids:    [][]float32{{1}},
		centroidNorm: [][]float32{{1}},
		codebooks: []ivfpqCodebook{
			{SubDim: 1, Codeword: [][]float32{{0}, {1}}},
		},
		lists: []ivfpqList{
			{IDs: []string{"doc-1", "doc-2", "doc-3", "doc-4", "doc-5"}, CodeSize: 1, Codes: []byte{1, 1, 1, 1, 1}},
		},
	}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 1)
	pipeline := NewVectorSearchPipeline(NewIVFPQCandidateGen(index, 1), &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(1)
	opts.MaxCandidateLimit = 0

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1}, opts, nil, nil)

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 5, stats.rawCandidates)
	require.Zero(t, stats.retries)
}

func TestAdaptiveBM25SearchWidensAfterFiltering(t *testing.T) {
	index := &recordingBM25Index{results: []indexResult{
		{ID: "skip", Score: 1.0},
		{ID: "doc-1", Score: 0.9},
		{ID: "doc-2", Score: 0.8},
		{ID: "doc-3", Score: 0.7},
	}}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	opts := adaptiveOverfetchTestOptions(2)

	results, stats, err := service.adaptiveBM25Search(context.Background(), index, "query", opts, func(results []indexResult) []indexResult {
		return results[1:]
	})

	require.NoError(t, err)
	require.Equal(t, []int{2, 4}, index.limits)
	require.Equal(t, []string{"doc-1", "doc-2"}, indexResultIDs(results))
	require.Equal(t, 1, stats.retries)
}

func TestAdaptiveBM25SearchStopsWhenSourceIsExhausted(t *testing.T) {
	index := &recordingBM25Index{results: []indexResult{{ID: "doc-1", Score: 1.0}}}
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	opts := adaptiveOverfetchTestOptions(2)

	results, stats, err := service.adaptiveBM25Search(context.Background(), index, "query", opts, nil)

	require.NoError(t, err)
	require.Equal(t, []int{2}, index.limits)
	require.Len(t, results, 1)
	require.Zero(t, stats.retries)
}

func TestFullTextSearchOnlyUsesAdaptiveWidening(t *testing.T) {
	engine := storage.NewMemoryEngine()
	service := NewServiceWithDimensions(engine, 2)
	for _, node := range []*storage.Node{
		{ID: "nornic:skip", Labels: []string{"Other"}},
		{ID: "nornic:doc-1", Labels: []string{"Doc"}},
		{ID: "nornic:doc-2", Labels: []string{"Doc"}},
		{ID: "nornic:doc-3", Labels: []string{"Doc"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	index := &recordingBM25Index{results: []indexResult{
		{ID: "nornic:skip", Score: 1.0},
		{ID: "nornic:doc-1", Score: 0.9},
		{ID: "nornic:doc-2", Score: 0.8},
		{ID: "nornic:doc-3", Score: 0.7},
	}}
	service.setFulltext(index)
	opts := adaptiveOverfetchTestOptions(2)
	opts.Types = []string{"Doc"}

	response, err := service.fullTextSearchOnly(context.Background(), "query", opts)

	require.NoError(t, err)
	require.Equal(t, []int{2, 4}, index.limits)
	require.Equal(t, 1, response.Metrics.BM25OverfetchRetries)
	require.Equal(t, 4, response.Metrics.BM25RawCandidates)
}

func TestVectorQueryNodesIndexedUsesAdaptiveWidening(t *testing.T) {
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	for _, node := range []*storage.Node{
		{ID: "skip", Labels: []string{"Other"}, ChunkEmbeddings: [][]float32{{1, 0}}},
		{ID: "doc-1", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.9, 0.1}}},
		{ID: "doc-2", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.8, 0.2}}},
		{ID: "doc-3", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.7, 0.3}}},
	} {
		require.NoError(t, service.IndexNode(node))
	}
	generator := &recordingCandidateGenerator{candidates: []Candidate{
		{ID: "skip-chunk-0", Score: 1.0},
		{ID: "doc-1-chunk-0", Score: 0.9},
		{ID: "doc-2-chunk-0", Score: 0.8},
		{ID: "doc-3-chunk-0", Score: 0.7},
	}}
	service.pipelineMu.Lock()
	service.vectorPipeline = NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	service.pipelineMu.Unlock()

	hits, err := service.vectorQueryNodesIndexedWithOptions(
		context.Background(),
		[]float32{1, 0},
		VectorQuerySpec{Label: "Doc", Similarity: "cosine", Limit: 2},
		"default",
		adaptiveOverfetchTestOptions(2),
	)

	require.NoError(t, err)
	require.Equal(t, []int{2, 4}, generator.limits)
	require.Equal(t, []string{"doc-1", "doc-2"}, vectorQueryHitIDs(hits))
}

func TestVectorQueryNodesIndexedWidensBeyondChunkRatioUntilDistinctNodesFill(t *testing.T) {
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	for _, node := range []*storage.Node{
		{ID: "doc-1", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}},
		{ID: "doc-2", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.9, 0.1}}},
	} {
		require.NoError(t, service.IndexNode(node))
	}
	candidates := make([]Candidate, 0, 13)
	for chunk := 0; chunk < 12; chunk++ {
		candidates = append(candidates, Candidate{ID: fmt.Sprintf("doc-1-chunk-%d", chunk), Score: 1})
	}
	candidates = append(candidates, Candidate{ID: "doc-2-chunk-0", Score: 0.9})
	generator := &recordingCandidateGenerator{candidates: candidates}
	service.pipelineMu.Lock()
	service.vectorPipeline = NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	service.pipelineMu.Unlock()

	opts := adaptiveOverfetchTestOptions(2)
	opts.MaxCandidateLimit = 0
	hits, err := service.vectorQueryNodesIndexedWithOptions(context.Background(), []float32{1, 0}, VectorQuerySpec{Label: "Doc", Similarity: "cosine", Limit: 2}, "default", opts)

	require.NoError(t, err)
	require.Equal(t, []string{"doc-1", "doc-2"}, vectorQueryHitIDs(hits))
	require.Greater(t, generator.limits[len(generator.limits)-1], 8)

	generator.limits = nil
	opts.MaxCandidateLimit = 8
	hits, err = service.vectorQueryNodesIndexedWithOptions(context.Background(), []float32{1, 0}, VectorQuerySpec{Label: "Doc", Similarity: "cosine", Limit: 2}, "default", opts)
	require.NoError(t, err)
	require.Equal(t, []string{"doc-1"}, vectorQueryHitIDs(hits))
	require.Equal(t, 8, generator.limits[len(generator.limits)-1])
}

func adaptiveOverfetchTestOptions(target int) *SearchOptions {
	opts := DefaultSearchOptions()
	opts.Limit = target
	opts.CandidateTarget = target
	opts.InitialOverfetchRatio = 1
	opts.MaxOverfetchRatio = 4
	opts.OverfetchGrowthFactor = 2
	opts.MaxCandidateLimit = 100
	return opts
}

func indexResultIDs(results []indexResult) []string {
	ids := make([]string, len(results))
	for index := range results {
		ids[index] = results[index].ID
	}
	return ids
}

func vectorQueryHitIDs(results []VectorQueryHit) []string {
	ids := make([]string, len(results))
	for index := range results {
		ids[index] = results[index].ID
	}
	return ids
}
