package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPaginationParameters: SKIP and LIMIT read the statement's parameters
// on every projection route (standalone RETURN, after UNWIND, after a
// procedure's YIELD), as Neo4j does.
func TestPaginationParameters(t *testing.T) {
	exec, _ := newTestExecutor(t)
	params := map[string]interface{}{"p": int64(5), "zero": int64(0), "one": int64(1), "two": int64(2)}
	for query, want := range map[string][][]interface{}{
		"RETURN $p AS x LIMIT $zero":                            {},
		"RETURN $p AS x SKIP $one":                              {},
		"RETURN $p AS x LIMIT $one":                             {{int64(5)}},
		"UNWIND [1, 2, 3] AS x RETURN x LIMIT $two":             {{int64(1)}, {int64(2)}},
		"UNWIND [1, 2, 3] AS x RETURN x SKIP $one LIMIT $one":   {{int64(2)}},
		"CALL db.labels() YIELD label RETURN label LIMIT $zero": {},
	} {
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err, query)
		require.Equal(t, len(want), len(result.Rows), query)
		for i := range want {
			require.Equal(t, want[i], result.Rows[i], query)
		}
	}
}

// TestPaginationExpressionsWithoutRowVariables: SKIP and LIMIT accept any
// expression that reads no row variable, including properties of a map
// parameter, map literals and variables the expression binds itself, and
// reject one that reads a row variable (#829; results from Neo4j 5.26.30).
func TestPaginationExpressionsWithoutRowVariables(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Item {v: 'a'}), (:Item {v: 'b'}), (:Item {v: 'c'})", nil)
	require.NoError(t, err)
	params := map[string]interface{}{"p": map[string]interface{}{"vals": []interface{}{"a", "b"}, "n": int64(1)}, "l": []interface{}{"x", "y"}}
	for query, want := range map[string][]interface{}{
		"MATCH (n:Item) RETURN n.v ORDER BY n.v SKIP $p.n":                                   {"b", "c"},
		"MATCH (n:Item) RETURN n.v ORDER BY n.v SKIP size($p.vals) - 1":                      {"b", "c"},
		"MATCH (n:Item) WHERE n.v IN $p.vals RETURN n.v ORDER BY n.v SKIP size($p.vals) - 1": {"b"},
		"MATCH (n:Item) RETURN n.v ORDER BY n.v LIMIT size($p.vals)":                         {"a", "b"},
		"MATCH (n:Item) RETURN n.v ORDER BY n.v LIMIT size([x IN $l | x])":                   {"a", "b"},
		"MATCH (n:Item) RETURN n.v ORDER BY n.v LIMIT size(keys({a: 1, b: 2}))":              {"a", "b"},
		"MATCH (n:Item) RETURN n.v ORDER BY n.v LIMIT reduce(s = 0, x IN $l | s + 1)":        {"a", "b"},
		"MATCH (n:Item) WITH n.v AS v ORDER BY v SKIP $p.n LIMIT $p.n RETURN v":              {"b"},
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		got := make([]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			got = append(got, row[0])
		}
		require.Equal(t, want, got, query)
	}
	_, err = exec.Execute(ctx, "MATCH (n:Item) RETURN n.v ORDER BY n.v SKIP n.v", params)
	require.ErrorContains(t, err, "SKIP requires an expression independent of row variables")
}
