package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// iterationClauseFor reads an UNWIND / FOR clause's list expression and bound
// variable from the grammar node the clause was split from. The native scanner
// (splitUnwindBody) stays the reader of raw statement text, so both must agree
// on every UNWIND clause the scanner can admit.
func TestIterationClauseMatchesNativeScanner(t *testing.T) {
	for _, statement := range []string{
		"UNWIND [1, 2] AS x RETURN x",
		"UNWIND range(1, 3) AS x RETURN x",
		"UNWIND /* lead */ [1, 2] /* tail */ AS x RETURN x",
		"UNWIND [CASE WHEN true THEN 1 ELSE 2 END] AS x RETURN x",
		"UNWIND ['a AS b', 'AS'] AS x RETURN x",
		"UNWIND [[1], [2]] AS x RETURN x[0] AS v",
		"UNWIND $list AS x RETURN x",
		"UNWIND { k: [1, 2] }.k AS x RETURN x",
		"UNWIND [1, 2] AS `odd name` RETURN `odd name`",
		"MATCH (n) UNWIND labels(n) AS label RETURN label",
	} {
		t.Run(statement, func(t *testing.T) {
			clauses, ok := canExecuteAsPipeline(t.Context(), statement)
			require.True(t, ok, statement)
			unwinds := 0
			for _, clause := range clauses {
				if clause.kind != pipelineClauseUnwind {
					continue
				}
				unwinds++
				native, found := splitUnwindBody(t.Context(), clause.text)
				require.True(t, found, clause.text)
				iteration, ok := iterationClauseFor(clause)
				require.True(t, ok, clause.text)
				require.Equal(t, native.expression, iteration.expression, clause.text)
				// The scanner reports the written alias tail; the node reader
				// reports the decoded symbol. Both name the same variable.
				require.Equal(t, cyphertextSymbolicName(native.aliasTail), iteration.variable, clause.text)
				require.Empty(t, iteration.trailing, clause.text)
				require.False(t, forIterationClause(clause), clause.text)
			}
			require.Equal(t, 1, unwinds, statement)
		})
	}
}

// cyphertextSymbolicName is the alias tail as the node reader names it: the
// first symbolic name, decoded.
func cyphertextSymbolicName(aliasTail string) string {
	name, _, ok := scanSymbolicName(aliasTail, 0)
	if !ok {
		return ""
	}
	return symbolicNameValue(name)
}

// The FOR form is a shared-grammar iteration clause; its binding comes from
// the same retained node reader.
func TestIterationClauseReadsForClauses(t *testing.T) {
	for statement, want := range map[string]iterationClause{
		"FOR x IN [1, 2] RETURN x":                   {expression: "[1, 2]", variable: "x"},
		"FOR /* gap */ x IN [1, 2] RETURN x":         {expression: "[1, 2]", variable: "x"},
		"FOR x IN/* gap */[1, 2] RETURN x":           {expression: "/* gap */[1, 2]", variable: "x"},
		"FOR `odd name` IN [1, 2] RETURN `odd name`": {expression: "[1, 2]", variable: "odd name"},
	} {
		t.Run(statement, func(t *testing.T) {
			clauses, ok := canExecuteAsPipeline(t.Context(), statement)
			require.True(t, ok, statement)
			require.NotEmpty(t, clauses)
			require.True(t, forIterationClause(clauses[0]), statement)
			iteration, ok := iterationClauseFor(clauses[0])
			require.True(t, ok, statement)
			require.Equal(t, want.expression, iteration.expression, statement)
			require.Equal(t, want.variable, iteration.variable, statement)
			require.Empty(t, iteration.trailing, statement)
		})
	}
}

// The execution path binds and iterates the clause's retained expression and
// variable: the public statements return the same rows as the clause text.
func TestIterationClauseExecution(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "iteration_syntax"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:IterationRow {name: 'a'})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		query     string
		params    map[string]interface{}
		wantRows  [][]interface{}
		wantError string
	}{
		{name: "unwind list", query: "UNWIND [1, 2] AS x RETURN x ORDER BY x", wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "unwind range", query: "UNWIND range(1, 3) AS x RETURN x ORDER BY x", wantRows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
		{name: "unwind comments", query: "UNWIND /* lead */ [1, 2] /* tail */ AS x RETURN x ORDER BY x", wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "unwind parameter", query: "UNWIND $list AS x RETURN x ORDER BY x", params: map[string]interface{}{"list": []interface{}{int64(1), int64(2)}}, wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "unwind quoted variable", query: "UNWIND [1, 2] AS `odd name` RETURN `odd name` AS v ORDER BY v", wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "unwind node labels", query: "MATCH (n:IterationRow) UNWIND labels(n) AS label RETURN label", wantRows: [][]interface{}{{"IterationRow"}}},
		{name: "for list", query: "FOR x IN [1, 2] RETURN x ORDER BY x", wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "for comment after keyword", query: "FOR /* gap */ x IN [1, 2] RETURN x ORDER BY x", wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "unwind trailing where", query: "UNWIND [1, 2] AS x WHERE x > 1 RETURN x", wantError: "Invalid input 'WHERE'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, tc.params)
			if tc.wantError != "" {
				require.Error(t, err, tc.query)
				require.Contains(t, err.Error(), tc.wantError)
				return
			}
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.wantRows, result.Rows, tc.query)
		})
	}
}

// Malformed iteration clauses keep their statement-level diagnostics: no
// clause node exists for them, so the grammar's own error reports them.
func TestIterationClauseMalformedDiagnostics(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "iteration_malformed"))
	ctx := context.Background()
	for query, message := range map[string]string{
		"UNWIND [1, 2] RETURN 1": "unexpected input",
		"UNWIND [1, 2] AS":       "UNWIND must be followed by a clause",
		"UNWIND AS x RETURN x":   "unexpected input",
		"FOR x [1, 2] RETURN x":  "unexpected input",
		"FOR x IN RETURN x":      "FOR must be followed by a clause",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}
}

// A cancelled statement stays a cancelled statement: the node reader adds no
// work and the pipeline still observes the context.
func TestIterationClauseCancellation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "iteration_cancel"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, query := range []string{
		"UNWIND [1, 2] AS x RETURN x",
		"FOR x IN [1, 2] RETURN x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorIs(t, err, context.Canceled, query)
	}
}
