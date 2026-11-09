package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// callScopeTestClause is the single CALL clause of a CALL-only statement, for
// the pipeline CALL step's checks.
func callScopeTestClause(t *testing.T, query string) pipelineClause {
	t.Helper()
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(t.Context(), query)
	if !ok || len(clauses) != 1 || !clauses[0].syntax.Valid() {
		t.Fatalf("could not split CALL: %q", query)
	}
	return clauses[0]
}

func newCallScopeExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	return NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope"))
}

// TestCallScopeAdmissionBoundaries checks the admission and public execution
// boundaries of CALL subquery scope: a statement of either import form binds
// its whole header, and a scope clause that is not a list of variable names is
// the syntax error its diagnostic already reports, before any body runs.
func TestCallScopeAdmissionBoundaries(t *testing.T) {
	const (
		multipleSubqueries = "WITH 1 AS x CALL (x) { RETURN x AS y } CALL (y) { RETURN y AS z } RETURN z"
		nestedSubquery     = "WITH 1 AS x CALL (x) { CALL (x) { RETURN x AS y } RETURN y } RETURN y"
		optionalSubquery   = "WITH 1 AS x OPTIONAL CALL (x) { RETURN x AS y } RETURN y"
		procedureCall      = "CALL db.labels() YIELD label RETURN label"
		quotedKeyword      = "RETURN 'CALL' AS text"
		emptyImportList    = "CALL () { RETURN 1 AS x } RETURN x"
		starImport         = "WITH 1 AS x CALL (*) { RETURN x AS y } RETURN y"
		literalImport      = "WITH 1 AS x CALL (x, 1) { RETURN x AS y } RETURN y"
		nestedLiteralList  = "WITH 1 AS x CALL (x) { CALL (x, 1) { RETURN x AS y } RETURN y } RETURN y"
	)
	for _, test := range []struct {
		name    string
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{name: "multiple subqueries", query: multipleSubqueries, columns: []string{"z"}, rows: [][]interface{}{{int64(1)}}},
		{name: "nested subquery", query: nestedSubquery, columns: []string{"y"}, rows: [][]interface{}{{int64(1)}}},
		{name: "optional subquery", query: optionalSubquery, columns: []string{"y"}, rows: [][]interface{}{{int64(1)}}},
		{name: "procedure call", query: procedureCall, columns: []string{"label"}},
		{name: "quoted keyword", query: quotedKeyword, columns: []string{"text"}, rows: [][]interface{}{{"CALL"}}},
		{name: "empty import list", query: emptyImportList, columns: []string{"x"}, rows: [][]interface{}{{int64(1)}}},
		{name: "star import", query: starImport, columns: []string{"y"}, rows: [][]interface{}{{int64(1)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := newCallScopeExecutor(t)
			admission, err := exec.admitStatement(t.Context(), test.query, fabricAdmission{})
			require.NoError(t, err)
			require.True(t, admission.graph, "a CALL subquery statement is a graph statement")
			require.NoError(t, admission.verifyColumns(test.columns))
			result, err := exec.Execute(t.Context(), test.query, nil)
			require.NoError(t, err)
			require.Equal(t, test.columns, result.Columns)
			if test.rows != nil {
				require.Equal(t, test.rows, result.Rows)
			}
		})
	}
	for _, test := range []struct {
		name, query string
	}{
		{name: "literal import", query: literalImport},
		{name: "nested literal import", query: nestedLiteralList},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := newCallScopeExecutor(t)
			admission, err := exec.admitStatement(t.Context(), test.query, fabricAdmission{})
			require.ErrorContains(t, err, "CALL imports must contain only variable names")
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
			require.False(t, admission.graph, "a rejected statement admits no graph statement")
			require.False(t, admission.command)
			_, err = exec.Execute(t.Context(), test.query, nil)
			require.ErrorContains(t, err, "CALL imports must contain only variable names")
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		})
	}
}

// TestCallScopeUpdatingSubqueryRunsBody checks that an importing body that
// updates runs once through the public path: its writes are the statement's
// side effects, and the enclosing query still projects its own column.
func TestCallScopeUpdatingSubqueryRunsBody(t *testing.T) {
	exec := newCallScopeExecutor(t)
	query := "WITH 1 AS x CALL (x) { CREATE (n:CallScopeWrite {v: x}) } RETURN x"
	admission, err := exec.admitStatement(t.Context(), query, fabricAdmission{})
	require.NoError(t, err)
	require.True(t, admission.graph)
	require.NoError(t, admission.verifyColumns([]string{"x"}))
	result, err := exec.Execute(t.Context(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	counted, err := exec.Execute(t.Context(), "MATCH (n:CallScopeWrite) RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, counted.Rows)
}

// TestCallScopeAdmissionPreservesCancellation checks that a canceled context is
// reported as the cancellation at both boundaries, never as a semantic query
// error and never as an admitted statement.
func TestCallScopeAdmissionPreservesCancellation(t *testing.T) {
	exec := newCallScopeExecutor(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	admission, err := exec.admitStatement(ctx, "WITH 1 AS x CALL (x) { RETURN x AS y } RETURN y", fabricAdmission{})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, admission.graph)
	require.False(t, admission.command)
	_, err = exec.Execute(ctx, "WITH 1 AS x CALL (x) { RETURN x AS y } RETURN y", nil)
	require.ErrorIs(t, err, context.Canceled)
}

// TestCallSubqueryOperatorCancellation checks the pipeline CALL step's own
// cancellation gate: the clause is handled by the operator, which reports the
// caller's cancellation.
func TestCallSubqueryOperatorCancellation(t *testing.T) {
	clause := callScopeTestClause(t, "CALL () { RETURN 1 AS value }")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	exec := newCallScopeExecutor(t)
	_, _, handled, err := exec.pipelineApplyCallSubqueryWithMetadata(ctx, []pipelineRow{{}}, clause, nil)
	if !handled || err != context.Canceled {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}
