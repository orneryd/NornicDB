package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
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

// iterationStatementForParser writes a shared-grammar FOR statement in the
// form the active front end admits. The ANTLR parser keeps the strict Cypher
// 5.26 admission for the additive clauses, so it reads FOR only behind the
// CYPHER 25 header the shared grammar makes optional; the SRD parser also
// reads the headerless form. Both are the same statement to the executor.
func iterationStatementForParser(statement string) string {
	if config.IsANTLRParser() && startsWithKeywordFold(statement, "FOR") {
		return "CYPHER 25 " + statement
	}
	return statement
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
		{name: "for list", query: iterationStatementForParser("FOR x IN [1, 2] RETURN x ORDER BY x"), wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "for comment after keyword", query: iterationStatementForParser("FOR /* gap */ x IN [1, 2] RETURN x ORDER BY x"), wantRows: [][]interface{}{{int64(1)}, {int64(2)}}},
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

// The additive iteration clause's admission is the one documented difference
// between the front ends, so each form is pinned where it is admitted: the
// ANTLR parser requires the CYPHER 25 header (strict Cypher 5.26) and the SRD
// parser reads both forms, and every admitted form returns the same rows.
func TestIterationClauseForAdmissionAcrossParsers(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "iteration_admission"))
	ctx := context.Background()
	const headerless = "FOR x IN [1, 2] RETURN x ORDER BY x"
	headered := "CYPHER 25 " + headerless
	want := [][]interface{}{{int64(1)}, {int64(2)}}

	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			// The optional header is accepted by both parsers and discards
			// nothing of the statement it precedes.
			result, err := exec.Execute(ctx, headered, nil)
			require.NoError(t, err, headered)
			require.Equal(t, want, result.Rows, headered)
			if parser == config.ParserTypeANTLR {
				_, err = exec.Execute(ctx, headerless, nil)
				require.ErrorContains(t, err, "LET, FILTER and FOR require CYPHER 25 with the ANTLR parser", headerless)
				return
			}
			headerlessResult, err := exec.Execute(ctx, headerless, nil)
			require.NoError(t, err, headerless)
			require.Equal(t, want, headerlessResult.Rows, headerless)
		})
	}
}

// Malformed iteration clauses keep their statement-level diagnostics: no
// clause node exists for them, so the grammar's own error reports them. The
// whole-statement "must be followed by a clause" rule speaks after syntax
// validation, so it names the error for the text the SRD parser admits, while
// the ANTLR grammar reports its own input error for the same text.
func TestIterationClauseMalformedDiagnostics(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "iteration_malformed"))
	ctx := context.Background()
	clauseFollows := "must be followed by a clause"
	for query, message := range map[string]string{
		"UNWIND [1, 2] RETURN 1": "unexpected input",
		"UNWIND AS x RETURN x":   "unexpected input",
		"FOR x [1, 2] RETURN x":  "unexpected input",
		"UNWIND [1, 2] AS":       iterationClauseDiagnostic(config.IsANTLRParser(), "UNWIND", clauseFollows),
		"FOR x IN RETURN x":      iterationClauseDiagnostic(config.IsANTLRParser(), "FOR", clauseFollows),
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), message, query)
	}
}

// iterationClauseDiagnostic names a malformed iteration clause the way the
// active front end reports it: the ANTLR grammar's own input error, or the
// statement-level rule the SRD parser's admission lets speak.
func iterationClauseDiagnostic(antlr bool, keyword, followedBy string) string {
	if antlr {
		return "unexpected input"
	}
	return keyword + " " + followedBy
}

// A cancelled statement stays a cancelled statement: the node reader adds no
// work and the pipeline still observes the context.
func TestIterationClauseCancellation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "iteration_cancel"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, query := range []string{
		"UNWIND [1, 2] AS x RETURN x",
		iterationStatementForParser("FOR x IN [1, 2] RETURN x"),
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorIs(t, err, context.Canceled, query)
	}
}
