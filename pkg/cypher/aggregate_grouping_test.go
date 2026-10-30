package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestAggregateGroupingAndOrderedCollections pins §6.1's aggregate discovery,
// grouping keys, projection aliases and ordered collections: aggregate ORDER
// BY keys, WITH-aggregation plus WHERE over the aggregate (HAVING-style),
// DISTINCT counts, nested aggregate arithmetic and size-of-collect ordering.
// Expected rows follow Neo4j 5.26 semantics on the same deterministic data.
func TestAggregateGroupingAndOrderedCollections(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "agg_grouping"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:G {k: 'a', v: 1}), (:G {k: 'a', v: 2}), (:G {k: 'b', v: 3})", nil)
	require.NoError(t, err)

	for query, want := range map[string][][]interface{}{
		"MATCH (n:G) RETURN n.k AS g, count(*) AS c ORDER BY count(*) DESC, g":    {{"a", int64(2)}, {"b", int64(1)}},
		"MATCH (n:G) RETURN n.k AS g, collect(n.v) AS vs ORDER BY g":              {{"a", []interface{}{int64(1), int64(2)}}, {"b", []interface{}{int64(3)}}},
		"MATCH (n:G) RETURN round(sum(n.v) * 100) / 100 AS r":                     {{float64(6)}},
		"MATCH (n:G) WITH n.k AS g, count(*) AS c ORDER BY c DESC RETURN g, c":    {{"a", int64(2)}, {"b", int64(1)}},
		"MATCH (n:G) RETURN n.k AS g, size(collect(n.v)) AS s ORDER BY s DESC, g": {{"a", int64(2)}, {"b", int64(1)}},
		"MATCH (n:G) RETURN count(DISTINCT n.k) AS d":                             {{int64(2)}},
		"MATCH (n:G) RETURN n.k AS g, avg(n.v) AS a ORDER BY a DESC, g":           {{"b", 3.0}, {"a", 1.5}},
		"MATCH (n:G) WITH n.k AS g, count(*) AS c WHERE c > 1 RETURN g, c":        {{"a", int64(2)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}
