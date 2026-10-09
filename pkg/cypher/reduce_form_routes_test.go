package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// reduce and allReduce on the routes the other tests don't take, and the
// statement rewrites and zones around the Cypher 25 functions, with Neo4j
// 2026.09's answers.
func TestReduceFormRoutes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "reduce_routes"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:RR {id: 1, s: 'a'})-[:R {w: 2}]->(:RR {id: 2})", nil)
	require.NoError(t, err)
	value := func(query string) interface{} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		return result.Rows[0][0]
	}
	for query, want := range map[string]interface{}{
		"MATCH (a:RR)-[r:R]->(b) RETURN reduce(s = 0, x IN [1, 2] | s + x * r.w) AS v":           int64(6),
		"MATCH (a:RR)-[r:R]->(b) RETURN allReduce(s = 0, x IN [1, 2] | s + r.w, s < 5) AS v":     true,
		"MATCH (n:RR {id: 1}) RETURN allReduce(a = 0, x IN n.s | a, true) AS v":                  true,
		"MATCH (n:RR {id: 1}) RETURN reduce(a = '', x IN n.s | a + x) AS v":                      "a",
		"RETURN /* ln(1) */ ceiling(1.2) AS v":                                                   2.0,
		"RETURN string.regexReplace('abcdefghij', '(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)', '$10') AS v": "j",
		"RETURN string.regexReplace('a1', '(\\\\d)', '\\\\x') AS v":                              "ax",
		"RETURN datetime({year: 2020, timezone: 'GMT-05:30'}).timezone AS v":                     "GMT-05:30",
		"RETURN datetime({year: 2020, timezone: 'GMT-05:30'}).offset AS v":                       "-05:30",
		"RETURN datetime({year: 2020, timezone: 'UTC+00:00'}).timezone AS v":                     "UTC",
		"RETURN toString(datetime('2020-01-01 10:00 UT+01:00', 'yyyy-MM-dd HH:mm VV')) AS v":     "2020-01-01T10:00:00+01:00[UT+01:00]",
	} {
		require.Equal(t, want, value(query), query)
	}
	for query, code := range map[string]string{
		"MATCH (n:RR {id: 1}) RETURN allReduce(a = 0, x IN [1] | a, n.s) AS v": "Neo.ClientError.Statement.TypeError",
		"RETURN reduce(a, x IN [1] | x) AS v":                                  "Neo.ClientError.Statement.SyntaxError",
		"RETURN reduce(a = 0, x IN 5 | a) AS v":                                "Neo.ClientError.Statement.SyntaxError",
		"WITH 1 AS i RETURN reduce(s = 0, x IN [1] | s + i.p) AS v":            "Neo.ClientError.Statement.SyntaxError",
		"WITH 1 AS i RETURN allReduce(s = 0, x IN [1] | s, i.p) AS v":          "Neo.ClientError.Statement.SyntaxError",
	} {
		_, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, code)
	}
	_, err = exec.Execute(ctx, "CYPHER 25 MATCH (n:RR {id: 1}) RETURN allReduce(a = 0, x IN [1] | a, n.s) AS v", nil)
	require.ErrorContains(t, err, `Don't know how to treat that as a predicate: String("a")`)
	if !config.IsANTLRParser() {
		// The ANTLR parser rejects the call first, as an expression it can't read.
		_, err = exec.Execute(ctx, "CYPHER 25 RETURN reduce(a, x IN [1] | x) AS v", nil)
		require.ErrorContains(t, err, "Invalid syntax for the `reduce` function.")
	}

	// Guards for forms the statement checks reject first.
	require.Nil(t, exec.evaluateExpressionWithContextFull(ctx, "reduce(a, x IN [1] | x)", nil, nil, nil, nil, nil, 0))
	_, resolved, err := exec.evaluateRowReduce("reduce", "a, x IN [1] | x", map[string]interface{}{})
	require.NoError(t, err)
	require.False(t, resolved)
	_, resolved, err = exec.evaluateRowReduce("reduce", "a = 0, x IN [1] | a + $missing.p", map[string]interface{}{})
	require.False(t, resolved, "a step the row evaluator can't resolve")
	_ = err
}
