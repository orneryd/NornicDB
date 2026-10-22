package search

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NORNICDB_SEARCH_QUERY_MAX_CHUNKS bounds the chunks searched (default 32);
// NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW=error fails a query over it instead
// (#907).
func TestLimitQueryChunks(t *testing.T) {
	chunks := []string{"a", "b", "c", "d", "e"}
	t.Setenv(EnvSearchQueryMaxChunks, "")
	t.Setenv(EnvSearchQueryChunkOverflow, "")
	require.Equal(t, 32, QueryMaxChunks())
	kept, err := LimitQueryChunks(chunks)
	require.NoError(t, err)
	require.Equal(t, chunks, kept)

	t.Setenv(EnvSearchQueryMaxChunks, "3")
	kept, err = LimitQueryChunks(chunks)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, kept)

	t.Setenv(EnvSearchQueryChunkOverflow, " Error ")
	_, err = LimitQueryChunks(chunks)
	require.ErrorContains(t, err, "search query has 5 chunks; at most 3 are searched")
	kept, err = LimitQueryChunks(chunks[:3])
	require.NoError(t, err)
	require.Len(t, kept, 3)

	t.Setenv(EnvSearchQueryMaxChunks, "0")
	require.Equal(t, 32, QueryMaxChunks())
}

// A long query's search reports how many chunks it searched of how many, and
// marks dropped chunks as a fallback unless an earlier fallback is reported.
func TestSearchTextChunksReportsQueryChunks(t *testing.T) {
	ctx := context.Background()
	five := func(context.Context, string) ([]string, error) { return []string{"c1", "c2", "c3", "c4", "c5"}, nil }
	embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
	var searched []string
	reason := SearchFallbackNone
	searchQuery := func(_ context.Context, text string, _ []float32, _ *SearchOptions) (*SearchResponse, error) {
		searched = append(searched, text)
		return &SearchResponse{Status: "success", Results: []SearchResult{{ID: "n-" + text, Score: 1}}, FallbackReason: reason}, nil
	}
	t.Setenv(EnvSearchQueryChunkOverflow, "")

	t.Setenv(EnvSearchQueryMaxChunks, "3")
	response, err := SearchTextChunks(ctx, "long query", nil, five, embed, searchQuery)
	require.NoError(t, err)
	require.Equal(t, []string{"c1", "c2", "c3"}, searched)
	require.Equal(t, 3, response.QueryChunksSearched)
	require.Equal(t, 5, response.QueryChunksTotal)
	require.True(t, response.FallbackTriggered)
	require.Equal(t, SearchFallbackQueryChunksDropped, response.FallbackReason)
	metadata := response.ResultMetadata()
	require.Equal(t, int64(3), metadata["query_chunks_searched"])
	require.Equal(t, int64(5), metadata["query_chunks_total"])

	// Every chunk searched: the counts, and no fallback.
	searched = nil
	t.Setenv(EnvSearchQueryMaxChunks, "10")
	response, err = SearchTextChunks(ctx, "long query", nil, five, embed, searchQuery)
	require.NoError(t, err)
	require.Len(t, searched, 5)
	require.Equal(t, 5, response.QueryChunksSearched)
	require.Equal(t, 5, response.QueryChunksTotal)
	require.False(t, response.FallbackTriggered)

	// An earlier fallback (a failed rerank) keeps its reason.
	t.Setenv(EnvSearchQueryMaxChunks, "3")
	reason = SearchFallbackRerankFailed
	response, err = SearchTextChunks(ctx, "long query", nil, five, embed, searchQuery)
	require.NoError(t, err)
	require.Equal(t, SearchFallbackRerankFailed, response.FallbackReason)
	require.Equal(t, 3, response.QueryChunksSearched)
	reason = SearchFallbackNone

	// A one-chunk query reports no counts.
	one := func(context.Context, string) ([]string, error) { return []string{"only"}, nil }
	response, err = SearchTextChunks(ctx, "only", nil, one, embed, searchQuery)
	require.NoError(t, err)
	require.Zero(t, response.QueryChunksTotal)
	require.NotContains(t, response.ResultMetadata(), "query_chunks_total")

	// Error mode: the search fails before any chunk is searched.
	searched = nil
	t.Setenv(EnvSearchQueryChunkOverflow, "error")
	_, err = SearchTextChunks(ctx, "long query", nil, five, embed, searchQuery)
	require.ErrorContains(t, err, "at most 3 are searched")
	require.Empty(t, searched)
}

// RequireAllQueryChunks fails over the cap whatever the overflow setting
// says, for callers that can't report dropped chunks.
func TestRequireAllQueryChunks(t *testing.T) {
	t.Setenv(EnvSearchQueryMaxChunks, "2")
	t.Setenv(EnvSearchQueryChunkOverflow, "truncate")
	require.NoError(t, RequireAllQueryChunks([]string{"a", "b"}))
	require.ErrorContains(t, RequireAllQueryChunks([]string{"a", "b", "c"}), "search query has 3 chunks; at most 2 are searched")
}

// A chunk whose embedding failed or was unusable isn't counted as searched,
// and its reason is reported over a benign one another chunk's search gave
// (rerank_scores_flat), so a caller can tell the query wasn't wholly searched
// by meaning.
func TestChunkedSearchReportsUnusableChunkEmbeddings(t *testing.T) {
	ctx := context.Background()
	chunkQuery := func(context.Context, string) ([]string, error) { return []string{"a", "b", "c"}, nil }
	searchQuery := func(_ context.Context, query string, embedding []float32, _ *SearchOptions) (*SearchResponse, error) {
		return &SearchResponse{Results: []SearchResult{{ID: query, Score: 1}}, FallbackReason: SearchFallbackRerankScoresFlat, FallbackTriggered: true}, nil
	}
	for name, chunkB := range map[string]struct {
		vector []float32
		err    error
		reason SearchFallbackReason
	}{
		"failed":   {err: errors.New("provider down"), reason: SearchFallbackQueryEmbeddingFailed},
		"unusable": {reason: SearchFallbackQueryEmbeddingUnavailable},
	} {
		embedQuery := func(_ context.Context, chunk string) ([]float32, error) {
			if chunk == "b" {
				return chunkB.vector, chunkB.err
			}
			return []float32{1, 0}, nil
		}
		response, err := SearchTextChunks(ctx, "a b c", &SearchOptions{Limit: 5}, chunkQuery, embedQuery, searchQuery)
		require.NoError(t, err, name)
		require.Equal(t, 2, response.QueryChunksSearched, name)
		require.Equal(t, 3, response.QueryChunksTotal, name)
		require.Equal(t, chunkB.reason, response.FallbackReason, name)
		require.True(t, response.FallbackTriggered, name)
	}
	require.True(t, isQueryEmbeddingFallback(SearchFallbackNoEmbedder))
	require.False(t, isQueryEmbeddingFallback(SearchFallbackRerankScoresFlat))
}

type selectiveChunkReranker struct{}

func (selectiveChunkReranker) Name() string                     { return "selective" }
func (selectiveChunkReranker) Enabled() bool                    { return true }
func (selectiveChunkReranker) IsAvailable(context.Context) bool { return true }
func (selectiveChunkReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	results := make([]RerankResult, 0, len(candidates))
	for _, id := range []string{"a", "c", "b"} {
		for index, candidate := range candidates {
			if candidate.ID == id {
				score := map[string]float64{"a": 0.9, "b": 0.5, "c": 0.1}[id]
				results = append(results, RerankResult{ID: id, OriginalRank: index + 1, CrossScore: score, FinalScore: score, Filtered: id == "b"})
			}
		}
	}
	return results, nil
}

type emptySelectionReranker struct{ providerFiltered bool }

func (emptySelectionReranker) Name() string                     { return "empty-selection" }
func (emptySelectionReranker) Enabled() bool                    { return true }
func (emptySelectionReranker) IsAvailable(context.Context) bool { return true }
func (r emptySelectionReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	results := make([]RerankResult, len(candidates))
	for index, candidate := range candidates {
		score := 0.3 - 0.1*float64(index)
		results[index] = RerankResult{
			ID: candidate.ID, OriginalRank: index + 1,
			CrossScore: score, FinalScore: score, Filtered: r.providerFiltered,
		}
	}
	return results, nil
}

func TestSearchTextChunksKeepsAppliedEmptyRerankSelection(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		providerFiltered bool
		minScore         float64
	}{
		{name: "provider filtered", providerFiltered: true},
		{name: "request minimum", minScore: 0.5},
	} {
		for _, chunks := range [][]string{{"alpha"}, {"chunk one", "chunk two"}} {
			name := fmt.Sprintf("%s/%d_chunks", testCase.name, len(chunks))
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				engine := newNamespacedEngine(t)
				service := NewServiceWithDimensions(engine, 2)
				t.Cleanup(func() { require.NoError(t, service.Close()) })
				service.SetSearchResultCachePolicy(0, 0)
				for _, id := range []string{"a", "b", "c"} {
					node := &storage.Node{
						ID: storage.NodeID(id), Labels: []string{"Doc"},
						Properties:      map[string]any{"content": "alpha complete query chunk one chunk two " + id},
						ChunkEmbeddings: [][]float32{{1, 0}, {0, 1}},
					}
					_, err := engine.CreateNode(node)
					require.NoError(t, err)
					require.NoError(t, service.IndexNode(node))
				}
				service.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(service.vectorIndex), NewCPUExactScorer(service.vectorIndex))
				service.SetReranker(emptySelectionReranker{providerFiltered: testCase.providerFiltered})
				options := DefaultSearchOptions()
				options.Limit = 3
				options.RerankEnabled = true
				options.RerankTopK = 3
				options.RerankMinScore = testCase.minScore
				options.MinRRFScore = 0

				response, err := SearchTextChunks(ctx, "complete query", options,
					func(context.Context, string) ([]string, error) { return chunks, nil },
					func(_ context.Context, text string) ([]float32, error) {
						if text == "chunk two" {
							return []float32{0, 1}, nil
						}
						return []float32{1, 0}, nil
					}, service.Search)
				require.NoError(t, err)
				require.Empty(t, response.Results)
				require.False(t, response.FallbackTriggered)
				require.Equal(t, SearchFallbackNone, response.FallbackReason)
				require.True(t, response.rerankApplied)
				require.Len(t, response.rerankExcluded, 3)
				if len(chunks) == 1 {
					require.Equal(t, "rrf_hybrid+rerank", response.SearchMethod)
				} else {
					require.Equal(t, "chunked_rrf_hybrid+rerank", response.SearchMethod)
				}
			})
		}
	}
}

func TestMultiChunkCataloguePreservesRerankEligibility(t *testing.T) {
	ctx := context.Background()
	engine := newNamespacedEngine(t)
	service := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetSearchResultCachePolicy(0, 0)
	for _, id := range []string{"a", "b", "c"} {
		node := &storage.Node{
			ID: storage.NodeID(id), Labels: []string{"Doc"},
			Properties:      map[string]any{"content": "alpha beta document " + id},
			ChunkEmbeddings: [][]float32{{1, 0}},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	service.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(service.vectorIndex), NewCPUExactScorer(service.vectorIndex))
	service.SetReranker(selectiveChunkReranker{})
	for _, chunks := range [][]string{{"alpha"}, {"alpha", "beta"}} {
		options := DefaultSearchOptions()
		options.Limit = 3
		options.RerankEnabled = true
		options.RerankTopK = 3
		options.MinRRFScore = 0
		page, err := service.SearchTextContinuation(ctx, "alpha beta", options, SearchContinuationRequest{
			Owner: fmt.Sprintf("chunks-%d", len(chunks)), Database: "nornic",
			Mode: SearchContinuationRankedThenID, N: 1, RankedLimit: 3,
		}, func(context.Context, string) ([]string, error) { return chunks, nil }, func(context.Context, string) ([]float32, error) {
			return []float32{1, 0}, nil
		}, service.Search, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		var ids []string
		for page != nil {
			for _, result := range page.Results {
				ids = append(ids, result.ID)
			}
			if !page.HasMore {
				break
			}
			page, err = service.SearchTextContinuation(ctx, "", nil, SearchContinuationRequest{
				Owner: fmt.Sprintf("chunks-%d", len(chunks)), Database: "nornic", QID: page.QID, N: 1,
			}, nil, nil, nil, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
		}
		require.Equal(t, []string{"a", "c"}, ids)
	}
}

// A query of one chunk is searched as itself: a failed, empty or failing
// search falls back with its reason; an adapter's fatal error is returned
// on both paths.
func TestChunkedSearchSingleChunkAndFatalErrors(t *testing.T) {
	ctx := context.Background()
	one := func(context.Context, string) ([]string, error) { return []string{"q"}, nil }
	ok := func(_ context.Context, query string, embedding []float32, _ *SearchOptions) (*SearchResponse, error) {
		if len(embedding) == 0 {
			return &SearchResponse{Results: []SearchResult{{ID: "bm25"}}}, nil
		}
		return &SearchResponse{Results: []SearchResult{{ID: "vector"}}}, nil
	}
	failing := func(_ context.Context, _ string, embedding []float32, _ *SearchOptions) (*SearchResponse, error) {
		if len(embedding) > 0 {
			return nil, errors.New("hybrid down")
		}
		return &SearchResponse{Results: []SearchResult{{ID: "bm25"}}}, nil
	}
	vector := func(context.Context, string) ([]float32, error) { return []float32{1}, nil }
	for name, check := range map[string]struct {
		embed  EmbedQueryFunc
		search SearchQueryFunc
		reason SearchFallbackReason
	}{
		"searched":         {vector, ok, SearchFallbackNone},
		"embedding failed": {func(context.Context, string) ([]float32, error) { return nil, errors.New("down") }, ok, SearchFallbackQueryEmbeddingFailed},
		"no vector":        {func(context.Context, string) ([]float32, error) { return nil, nil }, ok, SearchFallbackQueryEmbeddingUnavailable},
		"search failed":    {vector, failing, SearchFallbackHybridSearchFailed},
	} {
		response, err := SearchTextChunks(ctx, "q", &SearchOptions{Limit: 5}, one, check.embed, check.search)
		require.NoError(t, err, name)
		require.Equal(t, check.reason, response.FallbackReason, name)
		require.Zero(t, response.QueryChunksTotal, name)
	}
	fatal := errors.New("fatal")
	policy := ChunkedSearchErrorPolicy{
		FatalEmbeddingError: func(err error) bool { return errors.Is(err, fatal) },
		FatalSearchError:    func(err error) bool { return errors.Is(err, fatal) },
	}
	fatalEmbed := func(context.Context, string) ([]float32, error) { return nil, fatal }
	fatalSearch := func(context.Context, string, []float32, *SearchOptions) (*SearchResponse, error) { return nil, fatal }
	many := func(context.Context, string) ([]string, error) { return []string{"a", "b"}, nil }
	for _, chunks := range []ChunkQueryFunc{one, many} {
		_, err := SearchTextChunksWithErrorPolicy(ctx, "q", &SearchOptions{Limit: 5}, chunks, fatalEmbed, ok, policy)
		require.ErrorIs(t, err, fatal)
		_, err = SearchTextChunksWithErrorPolicy(ctx, "q", &SearchOptions{Limit: 5}, chunks, vector, fatalSearch, policy)
		require.ErrorIs(t, err, fatal)
	}
}
