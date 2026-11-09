package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.retrieveGraph ranks matching nodes and relationships in one pool: each
// row's kind says which it is; types filters node labels and relationshipTypes
// relationship types; n gives one terminal page; a stateful continuation is
// refused; without relationship search it ranks nodes alone.
func TestCallDbRetrieveGraph(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	svc := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	exec.SetSearchService(svc)

	_, err := exec.Execute(ctx, `CREATE (a:Doc {name: 'a', content: 'zephyrine harbour notes'})-[:CITES {reason: 'zephyrine bridge'}]->(b:Doc {name: 'b', content: 'unrelated'}),
		(:Note {name: 'n', content: 'zephyrine'})`, nil)
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(ctx))

	kinds := func(query string) map[string]int {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		counts := map[string]int{}
		for _, row := range result.Rows {
			counts[row[0].(string)]++
		}
		return counts
	}
	var result *ExecuteResult
	require.Eventually(t, func() bool {
		result, err = exec.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine'})", nil)
		return err == nil && len(result.Rows) == 3
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, []string{"kind", "node", "relationship", "start", "end", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered", "fallback_reason", "query_chunks_searched", "query_chunks_total", "matches"}, result.Columns)
	for _, row := range result.Rows {
		switch row[0] {
		case "node":
			require.NotNil(t, row[1])
			require.Nil(t, row[2])
		case "relationship":
			require.Nil(t, row[1])
			relationship := row[2].(*storage.Edge)
			require.Equal(t, "CITES", relationship.Type)
			require.Equal(t, "a", row[3].(*storage.Node).Properties["name"])
			require.Equal(t, "b", row[4].(*storage.Node).Properties["name"])
		default:
			t.Fatalf("kind %v", row[0])
		}
		require.Equal(t, "graph_bm25", row[9])
		require.Greater(t, row[8].(int), 0)
		// Each hit's matches: the query term where it occurs.
		matches := row[14].([]any)
		require.NotEmpty(t, matches, row[0])
		match := matches[0].(map[string]any)
		require.Equal(t, "term", match["kind"])
		require.Equal(t, "zephyrine", match["term"])
		require.Equal(t, int64(0), match["start"])
		require.Equal(t, int64(9), match["end"])
		if row[0] == "relationship" {
			require.Equal(t, "reason", match["property"])
		}
	}
	require.Equal(t, map[string]int{"node": 1, "relationship": 1}, kinds("CALL db.retrieveGraph({query: 'zephyrine', types: ['Note']})"))
	require.Equal(t, map[string]int{"node": 2}, kinds("CALL db.retrieveGraph({query: 'zephyrine', relationshipTypes: ['OTHER']})"))
	require.Len(t, kinds("CALL db.retrieveGraph({query: 'zephyrine', limit: 1})"), 1)
	yielded, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine'}) YIELD kind, relationship WHERE kind = 'relationship' RETURN type(relationship) AS type", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"CITES"}}, yielded.Rows)

	page, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine', n: 2, maxResults: 10})", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"page"}, page.Columns)
	pageMap := page.Rows[0][0].(map[string]interface{})
	require.Len(t, pageMap["results"], 2)
	require.Equal(t, false, pageMap["has_more"])
	require.Equal(t, "graph_bm25", pageMap["search_method"])
	first := pageMap["results"].([]interface{})[0].(map[string]interface{})
	require.Contains(t, []interface{}{"node", "relationship"}, first["kind"])
	for _, hit := range pageMap["results"].([]interface{}) {
		require.NotEmpty(t, hit.(map[string]interface{})["matches"])
	}
	unmatched, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine', matches: false})", nil)
	require.NoError(t, err)
	for _, row := range unmatched.Rows {
		require.Empty(t, row[14])
	}
	capped, err := exec.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine', maxResults: 1})", nil)
	require.NoError(t, err)
	require.Len(t, capped.Rows[0][0].(map[string]interface{})["results"], 1)

	for _, request := range []string{"{query: 'zephyrine', qid: 'x'}", "{query: 'zephyrine', mode: 'ranked', n: 1}", "{query: 'zephyrine', groupBy: 'name', n: 1}"} {
		_, err := exec.Execute(ctx, "CALL db.retrieveGraph("+request+")", nil)
		require.ErrorContains(t, err, "db.retrieveGraph returns one page", request)
	}

	plain := NewStorageExecutor(store)
	plainService := search.NewService(store, 3, nil)
	t.Cleanup(func() { _ = plainService.Close() })
	require.NoError(t, plainService.BuildIndexes(ctx))
	plain.SetSearchService(plainService)
	nodesOnly, err := plain.Execute(ctx, "CALL db.retrieveGraph({query: 'zephyrine'}) YIELD kind RETURN kind", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"node"}, {"node"}}, nodesOnly.Rows)
}
