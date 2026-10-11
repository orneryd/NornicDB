package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// apoc.cypher.run / runMany and apoc.periodic.* read the call's evaluated
// arguments: a statement or parameter map bound earlier in the statement is
// the same argument as a literal (#907).
func TestApocDynamicProceduresReadEvaluatedArguments(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	result, err := exec.Execute(ctx, "WITH 'RETURN $n + 1 AS x' AS statement, {n: 41} AS params CALL apoc.cypher.run(statement, params) YIELD value RETURN value.x AS x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(42)}}, result.Rows)

	// The statement text holds its own quotes, which the text reader cut short.
	result, err = exec.Execute(ctx, `WITH "RETURN 'it\\'s' AS s" AS statement CALL apoc.cypher.doitall(statement, {}) YIELD value RETURN value.s AS s`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"it's"}}, result.Rows)

	result, err = exec.Execute(ctx, "UNWIND ['CREATE (:Gen {i: 1})', 'CREATE (:Gen {i: 2})'] AS s WITH collect(s) AS parts CALL apoc.cypher.runMany(parts[0] + '; ' + parts[1], {}) YIELD row RETURN count(row) AS c", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "MATCH (g:Gen) RETURN count(g) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

	result, err = exec.Execute(ctx, "WITH 'UNWIND range(1, 5) AS i RETURN i' AS iterate, {batchSize: 2} AS config CALL apoc.periodic.iterate(iterate, 'CREATE (:Batch {v: i})', config) YIELD batches, total RETURN batches, total", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3), int64(5)}}, result.Rows)

	_, err = exec.Execute(ctx, "CALL apoc.cypher.run(null, {})", nil)
	require.ErrorContains(t, err, "argument statement is null")
}
