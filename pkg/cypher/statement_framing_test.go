package cypher

// Neo4j 5 statement framing (#743, #744): the CYPHER … query-option preamble
// runs the statement it precedes; a trailing FINISH (on every UNION branch)
// runs the statement and returns no rows; EXPLAIN and PROFILE are mutually
// exclusive.

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func framingExec(t *testing.T, name string) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "framing_"+name))
}

func TestCypherPreambleRunsTheStatement(t *testing.T) {
	exec := framingExec(t, "preamble")
	ctx := context.Background()
	cases := []struct {
		query string
		value interface{}
	}{
		{"CYPHER 5 RETURN 1 AS x", int64(1)},
		{"CYPHER RETURN 1 AS x", int64(1)},
		{"CYPHER 25 RETURN 1 AS x", int64(1)},
		{"cypher RETURN 1 AS x", int64(1)},
		{"CYPHER runtime=slotted RETURN 1 AS x", int64(1)},
		{"CYPHER runtime = slotted RETURN 1 AS x", int64(1)},
		{"CYPHER planner=cost runtime=slotted RETURN 1 AS x", int64(1)},
		{"CYPHER 5 planner=dp RETURN 2 AS x", int64(2)},
		{"CYPHER expressionEngine=interpreted RETURN 3 AS x", int64(3)},
		{"CYPHER CYPHER 5 RETURN 4 AS x", int64(4)},
		{"  CYPHER 5 RETURN 5 AS x", int64(5)},
	}
	for _, tc := range cases {
		res, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, [][]interface{}{{tc.value}}, res.Rows, tc.query)
	}

	// A preamble before a write runs the write.
	_, err := exec.Execute(ctx, "CYPHER 5 CREATE (n:Framing {v: 1})", nil)
	require.NoError(t, err)
	rows, err := exec.Execute(ctx, "MATCH (n:Framing) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), rows.Rows[0][0])

	// A preamble without a statement is still a syntax error.
	_, err = exec.Execute(ctx, "CYPHER 5", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
}

func TestExplainProfileConflictRejected(t *testing.T) {
	exec := framingExec(t, "explainprofile")
	ctx := context.Background()
	for _, query := range []string{
		"EXPLAIN PROFILE MATCH (n) RETURN n",
		"PROFILE EXPLAIN MATCH (n) RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
	// Each alone still runs.
	for _, query := range []string{"EXPLAIN MATCH (n) RETURN n", "PROFILE MATCH (n) RETURN n"} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

func TestFinishTerminatorReturnsNoRows(t *testing.T) {
	exec := framingExec(t, "finish")
	ctx := context.Background()

	assertNoRows := func(t *testing.T, query string) *ExecuteResult {
		t.Helper()
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.NotNil(t, res, query)
		require.Empty(t, res.Rows, query)
		require.Empty(t, res.Columns, query)
		return res
	}
	count := func(t *testing.T, query string) int64 {
		t.Helper()
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		return res.Rows[0][0].(int64)
	}

	assertNoRows(t, "FINISH")
	assertNoRows(t, "CYPHER 5 FINISH")
	_, err := exec.Execute(ctx, "CREATE (:Fin {v: 1})", nil)
	require.NoError(t, err)
	assertNoRows(t, "MATCH (n:Fin) FINISH")
	assertNoRows(t, "CREATE (:FinA) FINISH")
	require.Equal(t, int64(1), count(t, "MATCH (n:FinA) RETURN count(n)"))
	assertNoRows(t, "UNWIND [1, 2] AS x CREATE (:Fin2 {x: x}) FINISH")
	require.Equal(t, int64(2), count(t, "MATCH (n:Fin2) RETURN count(n)"))
	assertNoRows(t, "MATCH (n:Fin2) DETACH DELETE n FINISH")
	require.Equal(t, int64(0), count(t, "MATCH (n:Fin2) RETURN count(n)"))
	assertNoRows(t, "MATCH (n) FINISH UNION MATCH (m) FINISH")
	assertNoRows(t, "CALL { CREATE (:Fin3) FINISH }")
	require.Equal(t, int64(1), count(t, "MATCH (n:Fin3) RETURN count(n)"))

	// A quoted 'FINISH' is a value, not a terminator.
	res, err := exec.Execute(ctx, "RETURN 'FINISH' AS s", nil)
	require.NoError(t, err)
	require.Equal(t, "FINISH", res.Rows[0][0])

	// FINISH must be last, and a bare FINISH identifier is the reserved
	// keyword: these are syntax errors, as in Neo4j.
	for _, query := range []string{
		"FINISH RETURN 1",
		"WITH 1 AS finish RETURN finish",
		"MATCH (n:Fin) RETURN n FINISH",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
}
