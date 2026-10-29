package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// SearchOptions.Where admits candidates with Types and Filters before the
// top-K cut, on the batched and per-node paths, and a search with a Where
// isn't cached: two predicates over the same options give their own results.
func TestSearchWhereAdmitsCandidates(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng, DefaultVectorDimensions, nil)
	ctx := context.Background()
	makeNode(t, eng, "nornic:a", []string{"Doc"}, map[string]any{"content": "late fee clause", "state": "current"})
	makeNode(t, eng, "nornic:b", []string{"Doc"}, map[string]any{"content": "late fee clause", "state": "retired"})
	makeNode(t, eng, "nornic:c", []string{"Note"}, map[string]any{"content": "late fee clause"})
	current := func(node *storage.Node) bool { return node.Properties["state"] != "retired" }

	results := buildFilterResults("nornic:a", "nornic:b", "nornic:c")
	ids := func(results []indexResult) []string {
		out := []string{}
		for _, result := range results {
			out = append(out, result.ID)
		}
		return out
	}
	require.Equal(t, []string{"nornic:a", "nornic:c"}, ids(svc.filterByTypeAndProperties(ctx, results, &SearchOptions{Where: current}, map[string]bool{})))
	require.Equal(t, []string{"nornic:a"}, ids(svc.filterByTypeAndProperties(ctx, results, &SearchOptions{Types: []string{"Doc"}, Where: current}, map[string]bool{})))
	require.Equal(t, []string{"nornic:a", "nornic:c"}, ids(svc.filterByProperties(ctx, results, &SearchOptions{Where: current}, map[string]bool{})))

	require.NoError(t, svc.BuildIndexes(ctx))
	search := func(where func(*storage.Node) bool) []string {
		opts := DefaultSearchOptions()
		opts.Limit = 10
		opts.Where = where
		response, err := svc.Search(ctx, "late fee clause", nil, opts)
		require.NoError(t, err)
		out := []string{}
		for _, result := range response.Results {
			out = append(out, string(result.ID))
		}
		return out
	}
	require.ElementsMatch(t, []string{"nornic:a", "nornic:c"}, search(current))
	require.ElementsMatch(t, []string{"nornic:b"}, search(func(node *storage.Node) bool { return node.Properties["state"] == "retired" }))
	require.ElementsMatch(t, []string{"nornic:a", "nornic:b", "nornic:c"}, search(nil))
}
