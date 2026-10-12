package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFunctionArities(t *testing.T) {
	for name, want := range map[string]functionArity{
		"abs":                {1, 1},
		"round":              {1, 3},
		"substring":          {2, 3},
		"normalize":          {1, 2},
		"date":               {0, 2}, // date(input, pattern) since Cypher 25
		"date.truncate":      {1, 3},
		"duration":           {1, 1},
		"point.distance":     {2, 2},
		"rand":               {0, 0},
		"count":              {1, 1},
		"percentileCont":     {2, 2},
		"coalesce":           {1, -1},
		"trim":               {1, 3},
		"timestamp":          {0, 1},
		"DATETIME.FROMEPOCH": {2, 2},
	} {
		arity, ok := lookupFunctionArity(name)
		require.True(t, ok, name)
		require.Equal(t, want, arity, name)
	}
	for _, name := range []string{"reduce", "allReduce", "any", "all", "none", "single", "exists", "decay", "cosh", "kalman.init", "shortestPath", "nosuch", "apoc.coll.sum", strings.Repeat("x", 80)} {
		_, ok := lookupFunctionArity(name)
		require.False(t, ok, name)
	}

	require.NoError(t, checkFunctionArity("coalesce", functionArities["coalesce"], 7))
	require.ErrorContains(t, checkFunctionArity("coalesce", functionArities["coalesce"], 0), "Insufficient parameters for function 'coalesce'")
	require.ErrorContains(t, checkFunctionArity("abs", functionArities["abs"], 2), "Too many parameters for function 'abs'")
}

// TestFunctionArityThroughExecute: a call with too few or too many arguments
// is Neo4j's compile-time SyntaxError, whatever the data and the evaluator,
// for plain, namespaced and aggregating functions; valid counts still run.
func TestFunctionArityThroughExecute(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for _, query := range []string{
		"RETURN coalesce() AS v",
		"RETURN isEmpty('a', 1) AS v",
		"RETURN point() AS v",
		"RETURN point.distance(point({x: 1, y: 2})) AS v",
		"RETURN date('2020-01-01', 'yyyy', 1) AS v",
		"RETURN date.truncate('day', date('2020-01-02'), {}, 1) AS v",
		"RETURN datetime.fromepoch(1) AS v",
		"RETURN duration() AS v",
		"UNWIND [1, 2] AS x RETURN collect() AS v",
		"UNWIND [1, 2] AS x RETURN min(x, 1) AS v",
		"MATCH (n) WHERE size() > 0 RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "parameters for function", query)
	}
	for _, query := range []string{
		"RETURN coalesce(null, null, 3) AS v",
		"RETURN date() IS NOT NULL AS v",
		"RETURN date.truncate('month', date('2020-01-02')) AS v",
		"RETURN trim('  a ') AS v",
		"RETURN trim(BOTH 'x' FROM 'xax') AS v",
		"RETURN round(1.25, 1) AS v",
		"RETURN reduce(s = 0, x IN [1, 2] | s + x) AS v",
		"RETURN any(x IN [1] WHERE x = 1) AS v",
		"RETURN 'date.truncate()' AS v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}
