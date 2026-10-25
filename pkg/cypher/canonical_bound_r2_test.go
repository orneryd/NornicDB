package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCanonicalBoundDeclarationAdmission(t *testing.T) {
	for _, sample := range []struct {
		query    string
		cypher25 bool
	}{
		{"WITH 'Hi' AS label CALL db.labels() YIELD label RETURN label", false},
		{"CYPHER 25 LET a = 1 LET a = 2 RETURN a", true},
		{"CYPHER 25 WITH 1 AS x FOR x IN [2] RETURN x", true},
	} {
		t.Run(sample.query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "declaration-owner"))
			var failure error
			exec.StatementFacts(sample.query, resultmetadata.KnownScope(nil), sample.cypher25, RawSource).Visit(func(resultmetadata.Statement) {}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { failure = reason.Err })
			require.Error(t, failure, "facts must reject the same declaration as Execute")
			_, err := exec.StatementResultColumnsContext(context.Background(), sample.query, RawSource)
			require.Error(t, err, "headers must not advertise an unadmitted declaration")
			_, err = exec.Execute(context.Background(), sample.query, nil)
			require.Error(t, err)
			requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		})
	}
}

func TestCanonicalBoundFinishHorizons(t *testing.T) {
	for _, query := range []string{
		"WITH 1 AS x FINISH",
		"CALL () { WITH 1 AS x FINISH } RETURN 2 AS kept",
		"CALL () { WITH 1 AS x FINISH UNION ALL WITH 2 AS x FINISH } RETURN 2 AS kept",
	} {
		t.Run(query, func(t *testing.T) {
			(&StorageExecutor{}).StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
				var visit func(resultmetadata.Statement)
				visit = func(statement resultmetadata.Statement) {
					require.NotNil(t, statement.Entry)
					require.NotNil(t, statement.Exit)
					for _, clause := range statement.Clauses {
						require.NotNil(t, clause.Entry)
						require.NotNil(t, clause.Exit)
						if clause.Kind == resultmetadata.ClauseFinish {
							require.Equal(t, []string{"x"}, scopeValues(clause.Entry.Current))
							require.Equal(t, []string{"x"}, scopeValues(clause.Exit.Current))
							before, _ := clause.Entry.Current.Binding("x")
							after, _ := clause.Exit.Current.Binding("x")
							require.NotZero(t, before)
							require.Equal(t, before, after)
							require.Empty(t, canonicalDeclaredColumns(statement.Columns))
						}
						if clause.Call != nil {
							visit(*clause.Call.Statement)
						}
					}
					for _, branch := range statement.Branches {
						visit(branch)
					}
					for _, fragment := range statement.Fragments {
						require.NotNil(t, fragment.Entry)
						require.NotNil(t, fragment.Exit)
						if statement.Clauses[fragment.End-1].Kind == resultmetadata.ClauseFinish {
							require.Empty(t, canonicalDeclaredColumns(fragment.Columns))
						}
					}
				}
				visit(statement)
			}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
		})
	}
}

func TestCanonicalBoundWeakShadowDropDecision(t *testing.T) {
	// Cypher WITH scoping: the unscoped CALL declares a new local value;
	// WITH scratch drops it, so RETURN value is undefined. TCK
	// WithOrderBy1.feature [46] covers dropping; it has no CALL-body counterpart.
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "weak-shadow-drop"))
	for _, sample := range statementfactstest.CallRoleCases() {
		if sample.Code == "" {
			continue
		}
		for _, query := range []string{sample.Query, sample.Local} {
			var failure error
			exec.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) { t.Fatal("dropped weak shadow admitted") }, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { failure = reason.Err })
			require.Error(t, failure)
			requireStatusCode(t, failure, sample.Code)
			require.Contains(t, failure.Error(), "value")
		}
		_, err := exec.StatementResultColumnsContext(t.Context(), sample.Local, RawSource)
		require.Error(t, err)
		requireStatusCode(t, err, sample.Code)
		_, err = exec.Execute(t.Context(), sample.Local, nil)
		require.Error(t, err)
		requireStatusCode(t, err, sample.Code)
	}
	// NornicDB extension, diverging from Neo4j: a bare CALL can read outer
	// variables implicitly. Keep the initial UNWIND [value] read accepted.
	result, err := exec.Execute(t.Context(), "WITH 7 AS value CALL { UNWIND [value] AS first RETURN first AS x } RETURN x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
}

func TestCanonicalBoundKeywordOperandView(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "keyword-types"))
	for _, input := range []string{"WITH 5 AS distinct", "WITH 'x' AS distinct", "UNWIND [1] AS distinct"} {
		query := "UNWIND [] AS r " + input + " WITH distinct WHERE (distinct).x IS NULL RETURN 1 AS v"
		_, err := exec.Execute(t.Context(), query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}

func TestCanonicalBoundTypedScalarAtom(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "typed-scalar-atom"))
	query := "WITH $flag AS flag UNWIND [] AS r RETURN toUpper(COUNT { RETURN flag AS v }) AS x"
	ctx := withParams(t.Context(), map[string]interface{}{"flag": true})
	_, _, err := exec.enrichBoundTypes(ctx, query, typeScope{}, nil, "", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Type mismatch")
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	_, err = exec.Execute(t.Context(), query, map[string]interface{}{"flag": true})
	require.Error(t, err)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}

func TestCanonicalBoundTypesUseAdmittedProduct(t *testing.T) {
	exec := &StorageExecutor{}
	query := "WITH 7 AS a CALL (a) { UNWIND [a] AS x RETURN x } RETURN x"
	program := exec.statementProjectionProgram(t.Context(), query, resultmetadata.KnownScope(nil), false, nil)
	require.True(t, program.complete)
	values, _, err := exec.enrichBoundTypes(withBoundStatementProgram(t.Context(), program.bound), query, typeScope{}, nil, "", false)
	require.NoError(t, err)
	require.Same(t, program.bound, values.program)
}

func TestCanonicalBoundDeleteDiagnostic(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete-diagnostic"))
	for _, sample := range []struct{ query, message string }{
		{"DELETE n", `Neo.ClientError.Statement.SyntaxError: DELETE expression "n" refers to an undefined variable`},
		{"DETACH DELETE n", `Neo.ClientError.Statement.SyntaxError: DELETE expression "n" refers to an undefined variable`},
		{"DELETE n.id", `Neo.ClientError.Statement.SyntaxError: DELETE expression "n.id" refers to an undefined variable`},
		{"DELETE ns[0]", `Neo.ClientError.Statement.SyntaxError: DELETE expression "ns[0]" refers to an undefined variable`},
	} {
		t.Run(sample.query, func(t *testing.T) {
			var failure error
			exec.StatementFacts(sample.query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) { t.Fatal("unbound DELETE admitted") }, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { failure = reason.Err })
			require.EqualError(t, failure, sample.message)
			requireStatusCode(t, failure, "Neo.ClientError.Statement.SyntaxError")
			_, err := exec.StatementResultColumnsContext(t.Context(), sample.query, RawSource)
			require.EqualError(t, err, sample.message)
			_, err = exec.Execute(t.Context(), sample.query, nil)
			require.EqualError(t, err, sample.message)
		})
	}
}
