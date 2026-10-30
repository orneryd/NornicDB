package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An aggregation inside an EXISTS / COUNT / COLLECT { … } body is the body's
// own, and over no rows it has one row, as in Neo4j 5.26 and 2026.09. In
// Cypher 5 the body's imported variables stay in scope, so an aggregating
// WITH groups by the ones read after it and a RETURN that aggregates and
// reads one outside its aggregates is an implicit-grouping error (#907).
func TestSubqueryBodyAggregationMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subquery_agg"))
	ctx := context.Background()
	l := func(values ...interface{}) []interface{} { return values }
	both := map[string][][]interface{}{
		"RETURN COLLECT { UNWIND [] AS x RETURN count(*) AS c } AS v":                                         {l(l(int64(0)))},
		"RETURN COLLECT { UNWIND [1] AS x WITH x WHERE x < 0 RETURN count(*) AS c } AS v":                     {l(l(int64(0)))},
		"RETURN COLLECT { WITH 1 AS y WHERE y < 0 RETURN count(*) AS c } AS v":                                {l(l(int64(0)))},
		"RETURN COLLECT { MATCH (n:Nope) RETURN count(n) AS c } AS v":                                         {l(l(int64(0)))},
		"RETURN EXISTS { UNWIND [] AS x RETURN count(*) AS c } AS v":                                          {l(true)},
		"RETURN COUNT { UNWIND [] AS x RETURN count(*) AS c } AS v":                                           {l(int64(1))},
		"RETURN COLLECT { UNWIND [] AS x RETURN collect(x) AS c } AS v":                                       {l(l([]interface{}{}))},
		"RETURN COLLECT { UNWIND [] AS x WITH count(*) AS c RETURN c } AS v":                                  {l(l(int64(0)))},
		"WITH 1 AS g RETURN COLLECT { UNWIND [] AS x WITH count(*) AS c RETURN c } AS v":                      {l(l(int64(0)))},
		"WITH 1 AS g RETURN COLLECT { UNWIND [1] AS x WITH count(*) AS c RETURN c + g } AS v":                 {l(l(int64(2)))},
		"WITH 1 AS g RETURN COLLECT { UNWIND [1, 2] AS x WITH * WITH count(*) AS c RETURN c + g } AS v":       {l(l(int64(3)))},
		"WITH 1 AS g RETURN COLLECT { UNWIND [] AS x WITH * RETURN count(*) AS c } AS v":                      {l(l(int64(0)))},
		"MATCH (n:Nope) RETURN COLLECT { MATCH (m:Nope) RETURN count(m) AS c } AS v":                          {},
		"UNWIND [1, 2] AS x RETURN x, COLLECT { UNWIND [1, 2, 3] AS y RETURN count(y) AS c } AS v ORDER BY x": {l(int64(1), l(int64(3))), l(int64(2), l(int64(3)))},
	}
	byVersion := map[string]map[string][][]interface{}{
		"": {
			"WITH 1 AS g RETURN COLLECT { UNWIND [] AS x WITH count(*) AS c RETURN c + g } AS v":                               {l([]interface{}{})},
			"WITH 1 AS g RETURN COLLECT { UNWIND [1, 2, 3] AS x WITH * WHERE x < 0 WITH count(*) AS agg RETURN agg + g } AS x": {l([]interface{}{})},
		},
		"CYPHER 25 ": {
			"WITH 1 AS g RETURN COLLECT { UNWIND [] AS x WITH count(*) AS c RETURN c + g } AS v":                               {l(l(int64(1)))},
			"WITH 1 AS g RETURN COLLECT { UNWIND [1, 2, 3] AS x WITH * WHERE x < 0 WITH count(*) AS agg RETURN agg + g } AS x": {l(l(int64(1)))},
			"UNWIND [1, 2] AS g RETURN g, COLLECT { UNWIND [] AS x RETURN count(*) + g AS c } AS v ORDER BY g":                 {l(int64(1), l(int64(1))), l(int64(2), l(int64(2)))},
		},
	}
	// Cypher 5 first: a cached Cypher 5 result must not answer Cypher 25.
	for _, prefix := range []string{"", "CYPHER 25 "} {
		for query, want := range both {
			result, err := exec.Execute(ctx, prefix+query, nil)
			require.NoError(t, err, prefix+query)
			require.Equal(t, want, result.Rows, prefix+query)
		}
		for query, want := range byVersion[prefix] {
			result, err := exec.Execute(ctx, prefix+query, nil)
			require.NoError(t, err, prefix+query)
			require.Equal(t, want, result.Rows, prefix+query)
		}
	}
	_, err := exec.Execute(ctx, "UNWIND [1, 2] AS g RETURN g, COLLECT { UNWIND [] AS x RETURN count(*) + g AS c } AS v ORDER BY g", nil)
	require.ErrorContains(t, err, "Aggregation column contains implicit grouping expressions.")
	require.ErrorContains(t, err, "Illegal expression(s): g")
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}
