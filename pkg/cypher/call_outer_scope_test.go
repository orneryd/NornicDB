package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A CALL subquery's returned columns are new variables of the enclosing
// query: a name it already binds is declared twice, in every form of CALL
// (Neo4j 5.26.30, VariableAlreadyBound, #907). The one exception, a NornicDB
// extension, is the outer variable itself returned unchanged.
func TestCallSubqueryColumnsCannotRedeclareOuterVariables(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_outer_scope"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:CO {id: 1})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"WITH 1 AS a CALL (a) { RETURN 2 AS a } RETURN a",
		"WITH 1 AS a CALL { RETURN 2 AS a } RETURN a",
		"WITH 1 AS a CALL (a) { WITH a AS b RETURN b AS a } RETURN a",
		"WITH 1 AS a CALL () { RETURN 2 AS a } RETURN a",
		"WITH 1 AS a, 5 AS c CALL (a) { RETURN 2 AS c } RETURN c",
		"WITH 1 AS a CALL (*) { RETURN 2 AS a } RETURN a",
		"WITH 1 AS a CALL { RETURN 2 AS a UNION RETURN 3 AS a } RETURN a",
		"UNWIND [1] AS a CALL { RETURN 2 AS a } RETURN a",
		"WITH 1 AS a CALL (a) { WITH 2 AS a RETURN a } RETURN a",
		"WITH 1 AS a CALL (a) { UNWIND [2] AS a RETURN a } RETURN a",
		"WITH 1 AS a CALL (a) { CALL { RETURN 2 AS a } RETURN a } RETURN a",
		"WITH 1 AS a CALL (a) { RETURN a UNION RETURN 2 AS a } RETURN a",
		"WITH 1 AS a CALL (a) { CALL db.labels() YIELD label AS a RETURN a } RETURN a",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
	for query, rows := range map[string][][]interface{}{
		"WITH 1 AS a CALL (a) { RETURN 2 AS b } RETURN a, b":         {{int64(1), int64(2)}},
		"WITH 1 AS a WITH 2 AS b CALL { RETURN 3 AS a } RETURN a, b": {{int64(3), int64(2)}},
		"CALL { RETURN 1 AS a } CALL { RETURN 2 AS b } RETURN a, b":  {{int64(1), int64(2)}},
		"WITH 1 AS a CALL { RETURN a AS b } RETURN a, b":             {{int64(1), int64(1)}},
		// NornicDB extension: the outer variable returned unchanged (Neo4j
		// rejects it as VariableAlreadyBound).
		"WITH 1 AS a CALL { WITH a RETURN a } RETURN a":                                {{int64(1)}},
		"WITH 1 AS a CALL (a) { RETURN a } RETURN a":                                   {{int64(1)}},
		"WITH 1 AS a CALL (a) { RETURN a AS a } RETURN a":                              {{int64(1)}},
		"MATCH (n:CO) CALL (n) { RETURN n } RETURN count(n) AS c":                      {{int64(1)}},
		"MATCH (n:CO) CALL (n) { MATCH (n) RETURN n, n.id AS i } RETURN n.id AS id, i": {{int64(1), int64(1)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, rows, result.Rows, query)
	}
}

func TestCallSubqueryReturnsOuterUnchangedBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_outer_branches"))
	// RETURN * returns the body's variables (Neo4j 5.26.30: 1, 2).
	for _, query := range []string{
		"WITH 1 AS a CALL { UNWIND [2] AS b RETURN * } RETURN a, b",
		"WITH 1 AS a CALL () { UNWIND [2] AS b RETURN *, 3 AS c } RETURN a, b",
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{int64(1), int64(2)}}, result.Rows, query)
	}
	require.True(t, callSubqueryReturnsOuterUnchanged([]string{"WITH a RETURN a"}, "a"))
	for _, branch := range []string{
		"MATCH (n)", // no RETURN: the branch returns nothing unchanged
		"CALL db.labels() YIELD * RETURN a",
		"CALL db.labels() YIELD label AS a RETURN a",
	} {
		require.False(t, callSubqueryReturnsOuterUnchanged([]string{branch}, "a"), branch)
	}
}
