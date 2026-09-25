package cypher

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestValidateStatementParametersSkipsComments verifies the ParameterMissing
// check ignores $names in line and block comments, terminated or not (#657).
func TestValidateStatementParametersSkipsComments(t *testing.T) {
	for _, query := range []string{
		"RETURN 1 AS x // uses $a\n",
		"RETURN 1 AS x // uses $a",
		"RETURN /* $a */ 1 AS x",
		"RETURN 1 AS x /* $a",
	} {
		require.NoError(t, validateStatementParameters(query, nil), query)
	}
	err := validateStatementParameters("RETURN /* $a */ $b AS x", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Expected parameter(s): b")
}

// TestIsCreateProcedureCommandBranches covers CREATE OR without REPLACE.
func TestIsCreateProcedureCommandBranches(t *testing.T) {
	require.True(t, isCreateProcedureCommand("CREATE OR REPLACE PROCEDURE p() AS RETURN 1"))
	require.False(t, isCreateProcedureCommand("CREATE OR FOO PROCEDURE p()"))
	require.False(t, isCreateProcedureCommand("MATCH (n) RETURN n"))
}

// TestMergeUniqueConflictRetryBranches covers the MERGE retry decision for
// inputs the statement tests don't reach: an error that isn't a constraint
// violation, a statement that can't be split, quoted text and an unclosed map
// in the MERGE pattern, label items, map literals without the key, and
// float / boolean key values (#657).
func TestMergeUniqueConflictRetryBranches(t *testing.T) {
	violation := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "U", Properties: []string{"k"}}
	statement := func(query string, params map[string]interface{}) []CommitStatement {
		return []CommitStatement{{Query: query, Params: params}}
	}
	require.False(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: 1})", nil), stderrors.New("other")))
	require.False(t, MergeUniqueConflictIsRetrySafe(statement("CALL { MERGE (u:U {k: 1}) } IN TRANSACTIONS", nil), violation))
	require.True(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: 1, s: 'a{'}) SET u:Seen", nil), violation))
	require.True(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: 1}) SET u += {x: 1}", nil), violation))
	require.True(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: 1}) SET u = {x: 1, k: 1}", nil), violation))
	require.True(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: 1.5}) SET u.k = 1.5", nil), violation))
	require.True(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: true}) SET u.k = true", nil), violation))
	require.True(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: false}) SET u.k = false", nil), violation))
	require.False(t, MergeUniqueConflictIsRetrySafe(statement("MERGE (u:U {k: 1}) SET u.k = 2", nil), violation))
	require.Equal(t, map[string][]string(nil), appendPatternPropertyExpressions(nil, "(u:U {k: 1"))
}

// TestStaticTypeValidationClauseBranches runs statements whose static type
// errors sit in each clause the MATCH validation visits: a WITH projection, a
// WITH operator, UNWIND, CREATE, an ORDER BY over a projection alias, and a
// WITH * that carries a variable's type (#657).
func TestStaticTypeValidationClauseBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "statictypes"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:ST {v: 1})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n:ST) WITH n, toUpper(n) AS u RETURN u",
		"MATCH (n:ST) WITH n, true * 2 AS x RETURN x",
		"MATCH (n:ST) UNWIND toUpper(n) AS x RETURN x",
		"MATCH (n:ST) CREATE (:ST2 {v: toUpper(n)})",
		"MATCH (n:ST) RETURN n AS m ORDER BY toUpper(m)",
		"WITH 1 AS a WITH * RETURN toUpper(a) AS x",
		"WITH 1 AS a, 2 AS b WITH a, b RETURN toUpper(a) AS x",
		"RETURN toUpper(2 ^ 2) AS x",
		"RETURN toUpper([1, 'a']) AS x",
		"RETURN toUpper([1, 1.5]) AS x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Type mismatch", query)
	}
	for _, query := range []string{
		"WITH [1, 2] AS l RETURN head(l) AS h, size(l) AS s, reverse(l) AS r",
		"UNWIND ['a'] AS x RETURN collect(DISTINCT toUpper(x)) AS c",
		"RETURN toUpper([1] + 1) AS x",
		"WITH 1 AS a WITH a, a + 1 RETURN toUpper(a) AS x",
		"RETURN toUpper(`x",
		"RETURN toUpper('a'",
		"RETURN toUpper('a', 'b') AS x",
	} {
		_, _ = exec.Execute(ctx, query, nil)
	}
}

// TestRowPropertyAccessOnNullEntity covers property access on a null node or
// relationship binding in the row evaluator: the result is null.
func TestRowPropertyAccessOnNullEntity(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rownull"))
	for _, row := range []pipelineRow{
		{"n": (*storage.Node)(nil)},
		{"n": (*storage.Edge)(nil)},
		{"n": nil},
	} {
		value, ok := exec.evaluateRowExpression("n.x", row)
		require.True(t, ok)
		require.Nil(t, value)
	}
}

// TestCommitFailureWroteNothing pins which failed COMMITs have a known
// outcome: a local transaction's constraint violation or size-limit failure
// (#703) wrote nothing; any other failure, and any fabric failure, is unknown.
func TestCommitFailureWroteNothing(t *testing.T) {
	violation := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "U", Properties: []string{"k"}}
	tooBig := stderrors.New("materializing mvcc commit state: Txn is too big to fit into one request")
	require.True(t, commitFailureWroteNothing(true, violation))
	require.True(t, commitFailureWroteNothing(true, tooBig))
	require.False(t, commitFailureWroteNothing(true, stderrors.New("disk full")))
	require.False(t, commitFailureWroteNothing(false, violation))
	require.False(t, commitFailureWroteNothing(false, tooBig))
}
