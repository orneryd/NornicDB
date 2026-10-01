package cypher

// Neo4j 5 statement framing (#743, #744): the CYPHER … query-option preamble
// runs the statement it precedes; a trailing FINISH (on every UNION branch)
// runs the statement and returns no rows; EXPLAIN and PROFILE are mutually
// exclusive.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
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
		"EXPLAIN PROFILE RETURN 1",
		"EXPLAIN PROFILE MATCH (n) RETURN n",
		"PROFILE EXPLAIN MATCH (n) RETURN n",
		"EXPLAIN CYPHER 5 PROFILE RETURN 1",
		"PROFILE CYPHER 5 EXPLAIN RETURN 1",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.ArgumentError", code, query)
	}
	// Each alone still runs.
	for _, query := range []string{"EXPLAIN MATCH (n) RETURN n", "PROFILE MATCH (n) RETURN n"} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}

func TestMonsterStatementBoundaries(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, query := range []string{
				"CREATE (:Semi); MATCH (n:Semi) RETURN count(n) AS c",
				"RETURN 1 AS x UNION FINISH",
				"CREATE (:Semi) RETURN 1 AS x UNION FINISH",
				"FINISH UNION RETURN 1 AS x",
			} {
				t.Run(query, func(t *testing.T) {
					exec := framingExec(t, "boundaries")
					_, err := exec.Execute(context.Background(), query, nil)
					require.Error(t, err)
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
					result, err := exec.Execute(context.Background(), "MATCH (n:Semi) RETURN count(n)", nil)
					require.NoError(t, err)
					require.Equal(t, int64(0), result.Rows[0][0])
				})
			}
		})
	}
}

func TestMonsterScopedCallParserModes(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := framingExec(t, "scopedcall")
			for _, query := range []string{
				"UNWIND [1, 2] AS x CALL (x) { RETURN x * 2 AS y } RETURN y ORDER BY y",
				"UNWIND [1, 2] AS x CALL (*) { RETURN x * 2 AS y } RETURN y ORDER BY y",
				"UNWIND [1, 2] AS x WITH x, 2 AS factor CALL (x, factor) { RETURN x * factor AS y } RETURN y ORDER BY y",
			} {
				t.Run(query, func(t *testing.T) {
					result, err := exec.Execute(context.Background(), query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, result.Rows)
				})
			}
			result, err := exec.Execute(context.Background(), "UNWIND [1, 2] AS x CALL () { RETURN 3 AS y } RETURN y", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(3)}, {int64(3)}}, result.Rows)
		})
	}
}

func TestMonsterStatementFramingLexicalBoundaries(t *testing.T) {
	exec := framingExec(t, "lexical")
	require.Equal(t, []string{"RETURN 1 /* ; */", " RETURN 2 / 1 // ;\n"},
		exec.splitBySemicolon("RETURN 1 /* ; */; RETURN 2 / 1 // ;\n"))
	require.Empty(t, exec.splitBySemicolon(""))
	require.Equal(t, []string{"RETURN 1"}, exec.splitBySemicolon("RETURN 1;"))
	require.Equal(t, []string{"RETURN `semi;colon`", "RETURN ';'"},
		exec.splitBySemicolon("RETURN `semi;colon`;RETURN ';'"))
	for _, query := range []string{
		"RETURN ';' AS x;",
		"RETURN 1 AS `semi;colon`;",
		"RETURN 1 AS x; /* ; ignored */",
		"RETURN 1 AS x // ; ignored\n",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, query)
	}
	for query, count := range map[string]int{
		"RETURN 1 AS `semi;colon`; RETURN 2 AS x":       2,
		"RETURN ';' AS x; RETURN 2 AS y; RETURN 3 AS z": 3,
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.ErrorContains(t, err, fmt.Sprintf("Expected exactly one statement per query but got: %d", count), query)
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
	assertNoRows(t, "CREATE (:FinUnionA) UNION CREATE (:FinUnionB) FINISH")
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionA) RETURN count(n)"))
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionB) RETURN count(n)"))
	assertNoRows(t, "CREATE (:FinUnionC) FINISH UNION CREATE (:FinUnionD)")
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionC) RETURN count(n)"))
	require.Equal(t, int64(1), count(t, "MATCH (n:FinUnionD) RETURN count(n)"))
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
