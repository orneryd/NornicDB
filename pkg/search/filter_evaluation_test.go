package search

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A Where that fails ends the search with its own error, marked
// ErrFilterEvaluation. No consumer replaces the failed search with another
// one that leaves out the candidate the filter could not decide: neither
// Service.Search's vector-only and full-text fallbacks, nor a one-chunk
// query's full-text fallback, nor a several-chunk query's other chunks.
func TestFilterEvaluationErrorEndsSearch(t *testing.T) {
	eng := newFilterTestEngine(t)
	ctx := context.Background()
	for _, node := range []*storage.Node{
		{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "walrus notes"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
		{ID: "nornic:b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "walrus notes"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
		// Only the vector search reaches it: it lacks the query's term.
		{ID: "nornic:undecided", Labels: []string{"Doc"}, Properties: map[string]any{"content": "unrelated topic"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
	} {
		_, err := eng.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	svc := NewService(eng, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(ctx))

	cause := errors.New("the filter could not decide")
	options := func() *SearchOptions {
		opts := DefaultSearchOptions()
		opts.Limit = 10
		opts.Where = func(_ context.Context, node *storage.Node) (bool, error) {
			if node.ID == "nornic:undecided" {
				return false, cause
			}
			return true, nil
		}
		return opts
	}
	requireFilterError := func(response *SearchResponse, err error) {
		t.Helper()
		require.ErrorIs(t, err, ErrFilterEvaluation)
		require.ErrorIs(t, err, cause)
		require.EqualError(t, err, cause.Error(), "the filter's own message")
		require.Nil(t, response)
	}
	embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0, 0}, nil }

	requireFilterError(svc.Search(ctx, "walrus", []float32{1, 0, 0}, options()))
	requireFilterError(SearchTextChunksWithErrorPolicy(ctx, "walrus", options(), nil, embed, svc.Search, ChunkedSearchErrorPolicy{}))
	twoChunks := func(context.Context, string) ([]string, error) { return []string{"walrus", "walrus notes"}, nil }
	requireFilterError(SearchTextChunksWithErrorPolicy(ctx, "walrus notes", options(), twoChunks, embed, svc.Search, ChunkedSearchErrorPolicy{}))

	// Without a failing filter the same searches succeed.
	opts := DefaultSearchOptions()
	opts.Limit = 10
	response, err := svc.Search(ctx, "walrus", []float32{1, 0, 0}, opts)
	require.NoError(t, err)
	require.NotEmpty(t, response.Results)
}
