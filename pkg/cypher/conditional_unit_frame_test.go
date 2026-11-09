package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A conditional that writes lowers to the caller's value in a branch variable
// and one guarded arm per branch:
//
//	UNWIND [CASE WHEN c THEN 1 … END] AS b
//	CALL (*) { FILTER b = 1 <arm1> UNION ALL FILTER b = 2 <arm2> }
//
// Each arm's guard reads a variable the arm does not declare (the branch
// variable, or a variable of the part or CALL the conditional stands in), and
// an arm that only writes declares no columns. These tests are the frame
// contract of that lowering, run through the public Execute on both parsers:
// the result carries the arms' columns (none for writing arms), the rows of
// the chosen arm only, and exactly the mutations the chosen arm performs.
func TestConditionalUnitCallFrameCreatesChosenArm(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		// No arm declares a column, so the conditional adds none: the same
		// header the single-arm form has.
		single, err := exec.Execute(ctx, "CYPHER 25 CALL () { WHEN true THEN CREATE (:Single) }", nil)
		require.NoError(t, err)
		require.Empty(t, single.Columns)

		result, err := exec.Execute(ctx, "CYPHER 25 CALL () { WHEN true THEN CREATE (:A) ELSE CREATE (:B) }", nil)
		require.NoError(t, err)
		require.Empty(t, result.Columns, "a writing conditional declares no columns")
		require.Empty(t, result.Rows)
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "A"))
		require.Equal(t, int64(0), countLabelNodes(t, ctx, exec, "B"))

		// The condition is false here: the other arm is the one that runs, and
		// the first arm's mutation must not happen.
		result, err = exec.Execute(ctx, "CYPHER 25 CALL () { WHEN false THEN CREATE (:A) ELSE CREATE (:B) }", nil)
		require.NoError(t, err)
		require.Empty(t, result.Columns)
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "A"), "the untaken arm did not run")
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "B"))
	})
}

// A conditional in a later part of the statement reads that part's rows: the
// branch variable and the guard stand on the part the NEXT composition
// produced, so the arms must see it.
func TestConditionalUnitNextPartCreatesChosenArm(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		query := "CYPHER 25 UNWIND [1, 2] AS i RETURN i NEXT WHEN i = 1 THEN CREATE (:N1) ELSE CREATE (:N2)"
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Empty(t, result.Columns, "a writing conditional declares no columns")
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "N1"))
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "N2"))
	})
}

// A conditional inside a scoped CALL reads the variables the CALL imports:
// the arms' guards and their clauses see the imported row.
func TestConditionalUnitCallImportFrameCreatesChosenArm(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		_, err := exec.Execute(ctx, "CREATE (:S25 {name: 'old', age: 40}), (:S25 {name: 'young', age: 20})", nil)
		require.NoError(t, err)
		query := "CYPHER 25 MATCH (p:S25) CALL (p) { WHEN p.age > 35 THEN CREATE (:Old {name: p.name}) ELSE CREATE (:Young {name: p.name}) }"
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Empty(t, result.Columns, "a writing conditional declares no columns")
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "Old"))
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "Young"))
		written, err := exec.Execute(ctx, "MATCH (n:Old) RETURN n.name AS name", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"old"}}, written.Rows, "the chosen arm read the imported row")
	})
}

// The behaviors around a conditional frame that already hold: a single-arm
// writing conditional, a returning conditional, an aggregate in a returning
// arm, an undefined import (reported before anything runs) and the boundary
// the rewriter owns — a conditional starts a query part, so a WHEN after MATCH
// in the same part is not that form and stays a syntax error.
func TestConditionalUnitFrameControls(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)

		result, err := exec.Execute(ctx, "CYPHER 25 CALL () { WHEN true THEN CREATE (:One) }", nil)
		require.NoError(t, err)
		require.Empty(t, result.Columns)
		require.Equal(t, int64(1), countLabelNodes(t, ctx, exec, "One"))
		result, err = exec.Execute(ctx, "CYPHER 25 CALL () { WHEN false THEN CREATE (:Never) }", nil)
		require.NoError(t, err)
		require.Empty(t, result.Columns)
		require.Equal(t, int64(0), countLabelNodes(t, ctx, exec, "Never"), "a false condition writes nothing")

		// A returning conditional keeps its columns and chosen rows, and the
		// arm's own clauses keep running in the imported frame.
		result, err = exec.Execute(ctx, "CYPHER 25 UNWIND [1, 2] AS i CALL (i) { WHEN i = 1 THEN RETURN 'a' AS r } RETURN i, r", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"i", "r"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(1), "a"}}, result.Rows)

		// An aggregate in a chosen arm keeps aggregating the arm's rows.
		_, err = exec.Execute(ctx, "CREATE (:Agg), (:Agg)", nil)
		require.NoError(t, err)
		result, err = exec.Execute(ctx, "CYPHER 25 CALL () { WHEN true THEN MATCH (n:Agg) RETURN count(n) AS c } RETURN c", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"c"}, result.Columns)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

		// An import the scope clause names but the query doesn't bind is
		// rejected before the conditional runs or writes.
		_, err = exec.Execute(ctx, "CYPHER 25 CALL (nope) { WHEN true THEN CREATE (:Bad) }", nil)
		require.Error(t, err)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		require.Contains(t, err.Error(), "Variable `nope` not defined")
		require.Equal(t, int64(0), countLabelNodes(t, ctx, exec, "Bad"))

		// The conditional form starts a query part: the grammar has no WHEN
		// query after a MATCH in the same part, on either parser.
		for _, query := range []string{
			"CYPHER 25 MATCH (p:Agg) WHEN p.age > 1 THEN RETURN 1 AS x ELSE RETURN 2 AS x",
			"CYPHER 25 MATCH (p:Agg) WHEN p.age > 1 THEN CREATE (:Bad)",
		} {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err, query)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		}
		require.Equal(t, int64(0), countLabelNodes(t, ctx, exec, "Bad"))
	})
}

// The lowering this file's owner produces for the reported query: one chosen
// branch behind a CASE, one guarded arm per branch, no RETURN because no arm
// declares a column.
func TestConditionalUnitLoweringText(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	rewritten, rewrite, err := desugarQueryStructure(ctx, "CALL () { WHEN true THEN CREATE (:A) ELSE CREATE (:B) }", exec)
	require.NoError(t, err)
	require.NotNil(t, rewrite)
	require.Equal(t,
		"CALL () { UNWIND [CASE WHEN true THEN 1 ELSE 2 END] AS "+whenBranchVariable+
			" CALL (*) { FILTER "+whenBranchVariable+" = 1 CALL (*) { CREATE (:A) } UNION ALL FILTER "+whenBranchVariable+" = 2 CALL (*) { CREATE (:B) } } }",
		rewritten)
}

// countLabelNodes is the number of nodes with label in the store.
func countLabelNodes(t *testing.T, ctx context.Context, exec *StorageExecutor, label string) int64 {
	t.Helper()
	result, err := exec.Execute(ctx, "MATCH (n:"+label+") RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	count, ok := result.Rows[0][0].(int64)
	require.True(t, ok, "count is an integer")
	return count
}

func TestConditionalUntakenAggregateHasNoRow(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newUnitExecutor(t)
		for _, query := range []string{
			"CYPHER 25 CALL () { WHEN false THEN RETURN count(*) AS c } RETURN c",
			"CYPHER 25 WHEN false THEN RETURN count(*) AS c",
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, []string{"c"}, result.Columns)
			require.Empty(t, result.Rows)
		}
		result, err := exec.Execute(ctx, "CYPHER 25 UNWIND [1,2] AS i CALL (i) { WHEN i=1 THEN RETURN count(*) AS c ELSE RETURN 0 AS c } RETURN i,c ORDER BY i", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(0)}}, result.Rows)
	})
}
