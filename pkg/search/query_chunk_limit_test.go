package search

import (
	"context"
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
