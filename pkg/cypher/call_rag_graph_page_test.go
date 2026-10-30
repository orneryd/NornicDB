package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.retrieveGraph's page can be yielded after other clauses and inside a
// UNION subquery, and its rows and page report how many chunks of a query
// split into chunks were searched, as db.rretrieve's do.
func TestRetrieveGraphPageYieldsAndReportsQueryChunks(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&chunkCountingEmbedder{chunks: []string{"alpha", "beta", "gamma", "delta"}})
	_, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	service := search.NewService(store, 2, nil)
	t.Cleanup(func() { _ = service.Close() })
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)
	t.Setenv(search.EnvSearchQueryMaxChunks, "2")
	t.Setenv(search.EnvSearchQueryChunkOverflow, "")

	column := func(result *ExecuteResult, name string) interface{} {
		t.Helper()
		for index, candidate := range result.Columns {
			if candidate == name {
				return result.Rows[0][index]
			}
		}
		t.Fatalf("no column %s in %v", name, result.Columns)
		return nil
	}

	// Rows.
	result, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'alpha beta gamma delta', limit: 5})", nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.Rows)
	require.Equal(t, int64(2), column(result, "query_chunks_searched"))
	require.Equal(t, int64(4), column(result, "query_chunks_total"))
	yielded, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'alpha beta gamma delta', limit: 5}) YIELD kind, query_chunks_searched, query_chunks_total RETURN kind, query_chunks_searched, query_chunks_total", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"node", int64(2), int64(4)}, yielded.Rows[0])

	// The page, yielded after another clause and inside a UNION subquery.
	for _, statement := range []string{
		"WITH 'alpha beta gamma delta' AS q CALL db.retrieveGraph({query: q, limit: 5, n: 5}) YIELD page RETURN size(page.results) AS hits, page.query_chunks_searched AS searched, page.query_chunks_total AS total",
		`CALL () {
			CALL db.retrieveGraph({query: 'alpha beta gamma delta', limit: 5, n: 5, max_results: 5}) YIELD page RETURN page
			UNION ALL
			RETURN null AS page
		}
		WITH page WHERE page IS NOT NULL
		RETURN size(page.results) AS hits, page.query_chunks_searched AS searched, page.query_chunks_total AS total`,
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, [][]interface{}{{int64(1), int64(2), int64(4)}}, result.Rows, statement)
	}

	// A query searched whole: 0 and 0 on rows, no counts on the page.
	exec.SetEmbedder(&chunkCountingEmbedder{chunks: []string{"alpha"}})
	result, err = exec.Execute(ctx, "CALL db.retrieveGraph({query: 'alpha', limit: 5})", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), column(result, "query_chunks_searched"))
	require.Equal(t, int64(0), column(result, "query_chunks_total"))
	page, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'alpha', n: 5}) YIELD page RETURN page", nil)
	require.NoError(t, err)
	require.NotContains(t, page.Rows[0][0].(map[string]interface{}), "query_chunks_total")
}

// db.retrieveGraph's request errors, and a relationship hit whose endpoint is
// no longer visible.
func TestRetrieveGraphRequestErrorsAndMissingEndpoint(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	// Each fails at its own check: a null request before the query check, a
	// fractional limit at the fail-closed policy check, not at the embedder.
	_, err := exec.Execute(ctx, "CALL db.retrieveGraph(null)", nil)
	require.ErrorContains(t, err, "argument request is null")
	_, err = exec.Execute(ctx, "CALL db.retrieveGraph({query: 'alpha', failClosed: true, limit: 1.5})", nil)
	require.ErrorContains(t, err, "invalid limit value")

	kind, node, relationship, start, end := exec.graphSearchEntity(&search.SearchResult{Relationship: &storage.Edge{ID: "r", Type: "R", StartNode: "gone", EndNode: "gone too"}})
	require.Equal(t, "relationship", kind)
	require.Nil(t, node)
	require.NotNil(t, relationship)
	require.Nil(t, start)
	require.Nil(t, end)
}
