package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestExpressionFamiliesAcrossPositions pins the §5.2 families — list
// comprehensions, numeric operators, reduce, postfix access, maps and
// temporal arithmetic — in positions beyond the plain RETURN projection:
// ORDER BY keys, WITH projections, WHERE predicates, SET values and nested
// comprehension bodies. Expected values follow the pinned TCK and Neo4j
// 5.26 semantics (^ is left-associative per Precedence2).
func TestExpressionFamiliesAcrossPositions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "expr_families"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:P {x: 3, y: 1}), (:P {x: 1, y: 5}), (:P {x: 2, y: 2})", nil)
	require.NoError(t, err)

	for query, want := range map[string][][]interface{}{
		"RETURN reduce(a = 0, x IN [1, 2] | a + x) AS r ORDER BY reduce(a = 0, x IN [1, 2] | a + x)": {{int64(3)}},
		"WITH [x IN [1, 2, 3] WHERE x > 1 | x] AS l RETURN l, size(l) AS s":                          {{[]interface{}{int64(2), int64(3)}, int64(2)}},
		"RETURN {a: {b: 1}}.a.b AS x":                                      {{int64(1)}},
		"RETURN 2^3^2 AS x":                                                {{float64(64)}},
		"RETURN (2^3)^2 AS x":                                              {{float64(64)}},
		"RETURN date('2024-01-01') + duration('P1D') AS d":                 {{CypherDate{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)}}},
		"RETURN [x IN range(1, 3) | [y IN range(1, 2) | x * y]] AS nested": {{[]interface{}{[]interface{}{int64(1), int64(2)}, []interface{}{int64(2), int64(4)}, []interface{}{int64(3), int64(6)}}}},
		"MATCH (n:P) RETURN n.x AS x, CASE WHEN n.x > 1 THEN 0 ELSE 1 END AS k ORDER BY CASE WHEN n.x > 1 THEN 0 ELSE 1 END, n.x": {{int64(2), int64(0)}, {int64(3), int64(0)}, {int64(1), int64(1)}},
		"MATCH (n:P) WITH collect(n.x) AS xs RETURN reduce(s = 0, x IN xs | s + x) AS s":                                          {{int64(6)}},
		"UNWIND [1, 2] AS a WITH a UNWIND [1, 2] AS b RETURN collect(a * b) AS products":                                          {{[]interface{}{int64(1), int64(2), int64(2), int64(4)}}},
		"MATCH (n:P) SET n.computed = [x IN range(1, 3) WHERE x <> n.x | x * 10] RETURN n.computed AS c ORDER BY n.x":             {{[]interface{}{int64(20), int64(30)}}, {[]interface{}{int64(10), int64(30)}}, {[]interface{}{int64(10), int64(20)}}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}
