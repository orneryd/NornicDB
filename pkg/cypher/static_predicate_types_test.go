package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A predicate whose type is known before the statement runs and isn't
// Boolean is a SyntaxError (Neo4j 5.26.30, #907): the temporal namespaces'
// functions and reduce with a literal accumulator have known types.
func TestStaticPredicateTypesMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "static_predicates"))
	ctx := context.Background()
	for _, query := range []string{
		"WITH [1, 2] AS l WHERE reduce(a = 0, x IN l | a + x) RETURN 1 AS v",
		"WITH 1 AS i WHERE reduce(a = '', x IN ['a', 'b'] | a + x) RETURN 1 AS v",
		"WITH 1 AS i WHERE reduce(a = 0, x IN [] | a + x) RETURN 1 AS v",
		"WITH date('2020-01-01') AS d WHERE duration.between(d, date('2021-01-01')) RETURN 1 AS v",
		"WITH date('2020-01-01') AS d WHERE duration.inDays(d, date('2021-01-01')) RETURN 1 AS v",
		"WITH date('2020-01-01') AS d WHERE date.truncate('month', d) RETURN 1 AS v",
		"WITH date('2020-01-01') AS d WHERE datetime.truncate('day', d) RETURN 1 AS v",
		"WITH 1 AS i WHERE localtime.truncate('hour', localtime()) RETURN 1 AS v",
		"WITH 1 AS i RETURN reduce(a = 0, x IN [1] | a + x) AND true AS v",
		"WITH {a: 1} AS m WITH m WHERE m.a ^ 2 RETURN 1 AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		})
	}
	for _, query := range []string{
		"WITH [true] AS l WHERE reduce(a = false, x IN l | a OR x) RETURN 1 AS v",
		"WITH [1] AS l WHERE reduce(a = null, x IN l | x > 0) RETURN 1 AS v",
		"WITH 1 AS i RETURN reduce(a = 0, x IN [1, 2] | a + x) AS v",
		"WITH date('2020-01-01') AS d RETURN duration.between(d, date('2021-01-01')) AS v",
		"WITH {a: 2} AS m RETURN m.a ^ 2 AS v",
	} {
		t.Run(query, func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
		})
	}
}

// The function table types only results that don't depend on the
// arguments; reduce is typed by the operator check from its initial value and
// its step, wherever it is read.
func TestStaticFunctionResultType(t *testing.T) {
	require.Equal(t, "", staticFunctionResultType("REDUCE", "a = 0, x IN l | a + x"))
	require.Equal(t, "Integer", staticTextType("reduce(a = 0, x IN [1] | a + x)"))
	require.Equal(t, "String", staticTextType("reduce(a = '', x IN [1] | a + x)"))
	// An initial value of no known type leaves the result unknown.
	require.Equal(t, "", staticTextType("reduce(a = $p, x IN [1] | a + x)"))
	// Neo4j types reduce as its accumulator, whatever its step.
	require.Equal(t, "Integer", staticTextType("reduce(a = 1, x IN l | x)"))
	require.Equal(t, "Date", staticFunctionResultType("date.truncate", "'day', d"))
}
