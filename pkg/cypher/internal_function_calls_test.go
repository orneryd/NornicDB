package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A user's statement that calls one of NornicDB's internal __nornic_*
// functions fails with Unknown function, in any clause and letter case,
// over Execute and in the statement text a procedure runs; the standard
// syntax NornicDB rewrites into those calls keeps working (#1105).
func TestUserStatementsCannotCallInternalFunctions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "internal_functions"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:IF {v: 1})-[:R]->(:IF {v: 2})", nil)
	require.NoError(t, err)

	for _, query := range []string{
		"RETURN __nornic_interpolate(1) AS v",
		"RETURN __NORNIC_INTERPOLATE(1) AS v",
		"RETURN `__nornic_interpolate`(1) AS v",
		"RETURN __nornic_map_from_pairs([['a', 1]]) AS v",
		"MATCH (n:IF) WHERE __nornic_haslabels(n, 'IF', false) RETURN n.v AS v",
		"WITH __nornic_vector('[1, 2]', 2, 'INTEGER') AS v RETURN v",
		"UNWIND [1] AS x RETURN __nornic_vector_norm(x, 'EUCLIDEAN') AS v",
		"RETURN __nornic_vector_distance(1, 2, 'EUCLIDEAN') AS v",
		"MATCH p = (:IF)-->(:IF) RETURN __nornic_acyclic(p) AS v",
		"RETURN __nornic_path_selector('SHORTEST', 1) AS v",
		"RETURN __nornic_repeatable_elements() AS v",
		"CALL apoc.cypher.run('RETURN __nornic_interpolate(1) AS v', {}) YIELD value RETURN value",
		"CALL apoc.cypher.doitall('RETURN __nornic_interpolate(1) AS v', {}) YIELD value RETURN value",
		"CALL apoc.periodic.iterate('RETURN __nornic_interpolate(1) AS v', 'RETURN 1', {}) YIELD batches RETURN batches",
		"CALL apoc.periodic.commit('RETURN __nornic_interpolate(1) AS v', {}) YIELD updates RETURN updates",
		"CALL apoc.export.json.query('RETURN __nornic_interpolate(1) AS v', null, {}) YIELD data RETURN data",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "unknown function: __", query)
	}
	// A runMany statement that calls one is the call's error, as for
	// apoc.cypher.run.
	_, err = exec.Execute(ctx, "CALL apoc.cypher.runMany('RETURN __nornic_interpolate(1) AS v;', {}) YIELD result RETURN result", nil)
	require.ErrorContains(t, err, "unknown function: __nornic_interpolate")
	// A periodic.iterate action that calls one is the call's error, and runs
	// for no row.
	_, err = exec.Execute(ctx, "CALL apoc.periodic.iterate('UNWIND [1, 2] AS x RETURN x', 'CREATE (:IFX {v: __nornic_interpolate(x)})', {}) YIELD batches RETURN batches", nil)
	require.ErrorContains(t, err, "unknown function: __nornic_interpolate")
	count, err := exec.Execute(ctx, "MATCH (n:IFX) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, count.Rows)

	for query, want := range map[string]interface{}{
		// The names in strings and comments, and as property keys, are no calls.
		"RETURN '__nornic_interpolate(1)' AS v":            "__nornic_interpolate(1)",
		"RETURN 1 AS v // __nornic_interpolate(1)":         int64(1),
		"RETURN {__nornic_vector: 1}.__nornic_vector AS v": int64(1),
		// The syntax NornicDB rewrites into internal calls.
		`CYPHER 25 WITH 2 AS n RETURN s"n={n}" AS v`:                                "n=2",
		"CYPHER 25 WITH {a: 1} AS m RETURN {k: v IN m | k: v + 1} AS v":             map[string]interface{}{"a": int64(2)},
		"MATCH (n:$('IF')) RETURN count(n) AS v":                                    int64(2),
		"MATCH p = SHORTEST 1 (a:IF {v: 1})-->+(b:IF {v: 2}) RETURN length(p) AS v": int64(1),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	require.Nil(t, internalFunctionCallError("RETURN 1"))
	// The text holds the prefix, but the call is an ordinary function.
	require.Nil(t, internalFunctionCallError("RETURN toUpper('__nornic_x') AS v"))
}

// executeAllowingInternalFunctions runs a test statement through the
// internal path when it calls an internal __nornic_* function, the path the
// rewrites that produce those calls take; users can't call them (#1105).
func executeAllowingInternalFunctions(ctx context.Context, exec *StorageExecutor, query string, params map[string]interface{}) (*ExecuteResult, error) {
	if strings.Contains(query, "__nornic_") {
		return exec.executeInternal(ctx, query, params)
	}
	return exec.Execute(ctx, query, params)
}

// evaluateInternalFunction evaluates an internal function call as the main
// evaluator does when a rewritten statement reaches it, returning its value
// and the statement error it records.
func evaluateInternalFunction(exec *StorageExecutor, call string) (interface{}, error) {
	ctx := withExpressionFailureSlot(context.Background())
	fixtureAdmission684Context, fixtureAdmission684 := expressionTestContext(ctx, call)
	_ = fixtureAdmission684
	value := exec.evaluateExpressionWithContext(fixtureAdmission684, fixtureAdmission684Context, call, nil, nil)
	return value, getExpressionFailure(ctx)
}
