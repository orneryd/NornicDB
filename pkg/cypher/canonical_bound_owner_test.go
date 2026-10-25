package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCanonicalBoundExportAdmission(t *testing.T) {
	for _, sample := range []struct {
		query string
		valid bool
	}{
		{"WITH 1 AS a CALL () { RETURN 2 AS a } RETURN a", false},
		{"WITH 1 AS a CALL (a) { WITH 2 AS a RETURN a } RETURN a", false},
		{"WITH 1 AS a CALL (a) { RETURN a } RETURN a", true},
		{"WITH 1 AS a CALL { WITH 2 AS a RETURN a AS x } RETURN x", true},
		{"WITH 1 AS a CALL { CALL () { RETURN 2 AS a } RETURN a AS x } RETURN x", true},
	} {
		t.Run(sample.query, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "owner-export"))
			var factsErr error
			exec.StatementFacts(sample.query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) {}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { factsErr = reason.Err })
			_, admissionErr := exec.admitBoundStatement(context.Background(), sample.query)
			_, typeErr := exec.callQueryScope(context.Background(), sample.query[len("WITH 1 AS a "):], typeScope{"a": {}}, subqueryScope{}, false)
			for _, err := range []error{factsErr, admissionErr, typeErr} {
				if sample.valid {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				}
			}
		})
	}
}

// Independent event/set expectations from #1583 exercise the admitted compiler
// product before any row exists. Execution/transport consumers live above it.
func TestCanonicalBoundTransitionProperty(t *testing.T) {
	for _, sample := range callscopetest.TransitionCases() {
		t.Run(sample.Name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "owner-transition"))
			var columns []string
			var failure error
			exec.StatementFacts(sample.Query, resultmetadata.KnownScope(nil), sample.Cypher25, RawSource).Visit(func(statement resultmetadata.Statement) {
				statement.Columns.Visit(func(names []string) { columns = names }, func() { t.Fatal("unknown header") })
				if sample.Code == "" && len(sample.Steps) != 0 {
					var body *resultmetadata.Statement
					var parentInputs resultmetadata.Scope
					for _, clause := range statement.Clauses {
						if clause.Call != nil {
							body = clause.Call.Statement
							parentInputs = clause.Before
							break
						}
					}
					require.NotNil(t, body)
					branches := body.Branches
					if len(branches) == 0 {
						branches = []resultmetadata.Statement{*body}
					}
					for branchIndex, branch := range branches {
						assertDeclaredGraph(t, branch)
						if branchIndex != 0 {
							require.Equal(t, []string{"x"}, canonicalDeclaredColumns(branch.Columns))
							require.Len(t, branch.Clauses, 1)
							require.Empty(t, scopeValues(branch.Clauses[0].Exit.Dependencies))
							require.Equal(t, []string{"x"}, scopeValues(branch.Clauses[0].Exit.Current))
							continue
						}
						require.Len(t, branch.Clauses, len(sample.Steps))
						for index, step := range sample.Steps {
							clause := branch.Clauses[index]
							entry, exit := clause.Entry, clause.Exit
							require.ElementsMatch(t, step.Current, scopeValues(entry.Current), step.Text)
							require.ElementsMatch(t, step.Readable, scopeValues(entry.Readable), step.Text)
							require.ElementsMatch(t, step.ExitCurrent, scopeValues(exit.Current), step.Text)
							require.ElementsMatch(t, step.ExitReadable, scopeValues(exit.Readable), step.Text)
							for _, horizon := range []*resultmetadata.BindingHorizon{entry, exit} {
								require.ElementsMatch(t, step.Retained, scopeValues(horizon.Retained), step.Text)
								require.ElementsMatch(t, step.Weak, scopeValues(horizon.Weak), step.Text)
								require.ElementsMatch(t, step.Dependencies, scopeValues(horizon.Dependencies), step.Text)
								for _, name := range sample.Incoming {
									expected, _ := parentInputs.Binding(name)
									for _, scope := range []resultmetadata.Scope{horizon.Readable, horizon.Current, horizon.Retained, horizon.Weak, horizon.Dependencies} {
										if identity, ok := scope.Binding(name); ok {
											require.Equal(t, expected, identity, step.Text+" input identity "+name)
										}
									}
								}
							}
							for _, name := range scopeValues(exit.Current) {
								if identity, known := entry.Current.Binding(name); known {
									after, _ := exit.Current.Binding(name)
									require.Equal(t, identity, after, step.Text+" passthrough identity "+name)
								}
							}
						}
					}
				}
			}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { failure = reason.Err })
			if sample.Code != "" {
				require.Error(t, failure)
				requireStatusCode(t, failure, sample.Code)
				return
			}
			require.NoError(t, failure, sample.Query)
			require.Equal(t, sample.Columns, columns)
			model, err := exec.walkQueryScope(context.Background(), sample.Query, typeScope{}, subqueryScope{}, sample.Cypher25)
			require.NoError(t, err)
			require.Equal(t, sample.Columns, model.columns)
		})
	}
}

func TestCanonicalBoundEmptyUnionContract(t *testing.T) {
	query := "USE comp UNWIND [] AS a CALL () { USE comp.one RETURN 2 AS x UNION USE comp.two RETURN 3 AS x } RETURN count(*) AS n"
	exec := &StorageExecutor{}
	facts := exec.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource)
	facts.Visit(func(statement resultmetadata.Statement) {
		require.Equal(t, []string{"n"}, canonicalDeclaredColumns(statement.Columns))
		call := statement.Clauses[2].Call
		require.Equal(t, resultmetadata.CallReturning, call.Kind)
		require.Equal(t, []string{"x"}, canonicalDeclaredColumns(call.Body))
		require.Equal(t, []string{"x"}, canonicalDeclaredColumns(call.Statement.Columns))
		require.Len(t, call.Statement.Branches, 2)
		for _, branch := range call.Statement.Branches {
			require.Equal(t, []string{"x"}, canonicalDeclaredColumns(branch.Columns))
		}
		continuation := statement.Fragments[len(statement.Fragments)-1]
		require.True(t, continuation.WholeStream)
		require.Equal(t, resultmetadata.CompleteStream, continuation.Multiplicity)
		require.Equal(t, []string{"a", "x"}, scopeValues(continuation.Incoming))
		require.Equal(t, []string{"n"}, canonicalDeclaredColumns(continuation.Columns))
		require.NotNil(t, continuation.Entry)
		require.Equal(t, []string{"a", "x"}, scopeValues(continuation.Entry.Current))
		continuation.Entry.Current = resultmetadata.UnknownScope()
	}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
	// A consumer cannot mutate a later reader's immutable transition container.
	facts.Visit(func(statement resultmetadata.Statement) {
		require.Equal(t, []string{"a", "x"}, scopeValues(statement.Fragments[len(statement.Fragments)-1].Entry.Current))
	}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
}

func canonicalDeclaredColumns(columns resultmetadata.Columns) (names []string) {
	columns.Visit(func(values []string) { names = values }, func() {})
	return names
}

func TestCanonicalBoundNestedIdentity(t *testing.T) {
	query := "WITH 1 AS value CALL { RETURN EXISTS { CALL () { MATCH (value) RETURN * } RETURN value } AS x } RETURN x"
	exec := &StorageExecutor{}
	program := exec.compileStatementProgram(context.Background(), query, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	require.NotNil(t, program.statement)
	require.Empty(t, program.statement.Clauses[1].Call.Imports, "the private MATCH declaration cannot consume the ancestor's identity")
}

func TestCanonicalBoundFragmentInputs(t *testing.T) {
	query := "WITH 1 AS textKey, 2 AS textKey128 CALL { WITH textKey128 USE comp.one RETURN textKey128 AS x } RETURN x"
	(&StorageExecutor{}).StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
		call := statement.Clauses[1].Call
		require.Equal(t, []string{"textKey128"}, call.Imports)
		for _, fragment := range call.Statement.Fragments {
			if fragment.Kind == resultmetadata.ClauseOther {
				require.Equal(t, []string{"textKey128"}, scopeValues(fragment.Incoming), "unused weak readability is not a transported value")
			}
		}
	}, func() { t.Fatal("declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
}

// Every nested body/UNION sibling is observed. Stream and header assertions
// are declarations made before execution, including authoritative empty input.
func assertDeclaredGraph(t *testing.T, statement resultmetadata.Statement) {
	t.Helper()
	require.NotNil(t, statement.Entry)
	require.NotNil(t, statement.Exit)
	for _, clause := range statement.Clauses {
		require.NotNil(t, clause.Entry)
		require.NotNil(t, clause.Exit)
		for _, horizon := range []*resultmetadata.BindingHorizon{clause.Entry, clause.Exit} {
			for _, scope := range []resultmetadata.Scope{horizon.Readable, horizon.Current, horizon.Retained, horizon.Weak, horizon.Dependencies} {
				scope.Visit(func(names []string) {
					for _, name := range names {
						id, ok := scope.Binding(name)
						require.True(t, ok, name)
						require.NotZero(t, id, name)
					}
				}, func() { t.Fatal("unknown admitted horizon") })
			}
		}
		if call := clause.Call; call != nil {
			assertDeclaredGraph(t, *call.Statement)
			require.Equal(t, canonicalDeclaredColumns(call.Body), canonicalDeclaredColumns(call.Statement.Columns))
			if call.Kind == resultmetadata.CallUnit {
				require.Empty(t, canonicalDeclaredColumns(call.Body))
			}
		}
	}
	for _, branch := range statement.Branches {
		assertDeclaredGraph(t, branch)
	}
	for _, fragment := range statement.Fragments {
		require.NotNil(t, fragment.Entry)
		require.NotNil(t, fragment.Exit)
		if fragment.Kind == resultmetadata.ClauseOther {
			require.True(t, fragment.WholeStream)
			require.Equal(t, resultmetadata.CompleteStream, fragment.Multiplicity)
		}
		if fragment.Kind == resultmetadata.ClauseCallSubquery {
			require.Equal(t, resultmetadata.PerInputRow, fragment.Multiplicity)
		}
	}
}
