package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestAggregatingProjectionScope: after a WITH or RETURN with DISTINCT or
// an aggregation, WHERE and ORDER BY see only the projected names and the
// expressions the projection projects, as in Neo4j 5.26.30 (#907). Expected
// results are Neo4j's.
func TestAggregatingProjectionScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for _, query := range []string{
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH count(*) AS c ORDER BY x RETURN *",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y RETURN count(*) AS c ORDER BY x",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH count(*) AS c ORDER BY x DESC SKIP 1 RETURN *",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y RETURN count(*) AS c ORDER BY x DESC SKIP 1",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH count(*) AS c WHERE x > 1 RETURN *",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH collect(x) AS c ORDER BY x RETURN *",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y RETURN collect(x) AS c ORDER BY x",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH collect(x) AS c WHERE x > 1 RETURN *",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH DISTINCT count(*) AS c WHERE x > 1 RETURN *",
		"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH DISTINCT collect(x) AS c WHERE x > 1 RETURN *",
		"UNWIND [1, 2, 3] AS x RETURN sum(x) AS s ORDER BY x",
		"UNWIND [1, 2, 2] AS x RETURN x + 1 AS y, count(*) AS c ORDER BY x",
		"UNWIND [1, 2, 2] AS x RETURN count(*) AS c ORDER BY sum(x)",
		"UNWIND [1, 2, 2] AS x RETURN DISTINCT x + 1 AS y ORDER BY x",
		"UNWIND [{a: 1}, {a: 2}] AS m RETURN m.a AS a, count(*) AS c ORDER BY m",
		"UNWIND [1, 2, 2] AS x WITH count(*) AS c ORDER BY x LIMIT 1 WHERE c > 0 RETURN c",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
		require.Contains(t, err.Error(), "it is not possible to access variables declared before the WITH/RETURN: ", query)
	}
	for query, rows := range map[string][][]interface{}{
		"UNWIND [1, 2, 2] AS x RETURN x AS y, count(*) AS c ORDER BY x":                   {{int64(1), int64(1)}, {int64(2), int64(2)}},
		"UNWIND [1, 2, 2] AS x RETURN x, count(*) AS c ORDER BY x":                        {{int64(1), int64(1)}, {int64(2), int64(2)}},
		"UNWIND [1, 2, 2] AS x RETURN x + 1 AS y, count(*) AS c ORDER BY x + 1":           {{int64(2), int64(1)}, {int64(3), int64(2)}},
		"UNWIND [1, 2, 2] AS x RETURN count(*) AS c ORDER BY count(*)":                    {{int64(3)}},
		"UNWIND [1, 2, 2] AS x RETURN DISTINCT x AS y ORDER BY x":                          {{int64(1)}, {int64(2)}},
		"UNWIND [1, 2, 2] AS x WITH DISTINCT x AS y WHERE x > 1 RETURN y":                  {{int64(2)}},
		"UNWIND [1, 2, 2] AS x WITH x AS y, count(*) AS c WHERE x > 1 RETURN y, c":         {{int64(2), int64(2)}},
		"UNWIND [1, 2, 2] AS x WITH x, count(*) AS c WHERE c > 1 RETURN x, c":              {{int64(2), int64(2)}},
		"UNWIND [{a: 1}, {a: 2}] AS m RETURN m.a AS a, count(*) AS c ORDER BY m.a":         {{int64(1), int64(1)}, {int64(2), int64(1)}},
		"UNWIND [1, 2, 2] AS x RETURN DISTINCT x ORDER BY x":                               {{int64(1)}, {int64(2)}},
		"UNWIND [1, 2, 2] AS x RETURN collect(x) AS l ORDER BY size(l)":                    {{[]interface{}{int64(1), int64(2), int64(2)}}},
		"UNWIND [1, 2, 2] AS x RETURN DISTINCT x AS y ORDER BY y":                          {{int64(1)}, {int64(2)}},
		"UNWIND [1, 2, 2] AS x WITH *, count(*) AS c ORDER BY x RETURN c":                  {{int64(1)}, {int64(2)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, rows, result.Rows, query)
	}
}

func TestMaskProjectedExpression(t *testing.T) {
	require.Equal(t, "__nornic_projected + 1", maskProjectedExpression("x + 1", "x"))
	require.Equal(t, "n.x + __nornic_projected", maskProjectedExpression("n.x + x", "x"))
	require.Equal(t, "__nornic_projected.a", maskProjectedExpression("m.a", "m"))
	require.Equal(t, "xy + $x + 'x'", maskProjectedExpression("xy + $x + 'x'", "x"))
	require.Equal(t, "\"a\\\"x\" + __nornic_projected", maskProjectedExpression("\"a\\\"x\" + x", "x"))
	require.Equal(t, "y", maskProjectedExpression("y", "x"))
}
