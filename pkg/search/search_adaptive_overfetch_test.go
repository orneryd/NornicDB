package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type recordingCandidateGenerator struct {
	candidates []Candidate
	limits     []int
}

type recordingBM25Index struct {
	bm25Index
	results []indexResult
	limits  []int
}

func (i *recordingBM25Index) Search(_ string, limit int) []indexResult {
	i.limits = append(i.limits, limit)
	if limit > len(i.results) {
		limit = len(i.results)
	}
	return i.results[:limit]
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

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil)

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

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil)

	require.NoError(t, err)
	require.Equal(t, []int{2}, generator.limits)
	require.Len(t, results, 2)
	require.Zero(t, stats.retries)
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

	results, stats, err := service.adaptiveVectorSearch(context.Background(), pipeline, []float32{1, 0}, opts, nil)

	require.NoError(t, err)
	require.Equal(t, []int{3, 4}, generator.limits)
	require.Len(t, results, 2)
	require.Equal(t, 1, stats.retries)
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
