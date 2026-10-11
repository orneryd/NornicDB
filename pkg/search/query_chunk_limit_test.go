package search

import (
	"context"
	"errors"
	"testing"

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
