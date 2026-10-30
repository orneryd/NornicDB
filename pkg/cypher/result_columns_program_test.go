package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStatementFactsProgram(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "statement-facts"))
	for _, tc := range []struct {
		query                   string
		columns, body, retained []string
		mode                    resultmetadata.ImportMode
		kind                    resultmetadata.CallKind
	}{
		{"CYPHER 25 USE comp CALL () { USE comp.two WITH 1 AS innerValue RETURN * } RETURN *", []string{"innerValue"}, []string{"innerValue"}, []string{}, resultmetadata.ImportListed, resultmetadata.CallReturning},
		{"WITH 1 AS outerValue CALL (outerValue) { USE comp.two WITH 2 AS innerValue RETURN * } RETURN *", []string{"innerValue", "outerValue"}, []string{"innerValue", "outerValue"}, []string{"outerValue"}, resultmetadata.ImportListed, resultmetadata.CallReturning},
		{"WITH 1 AS outerValue CALL { WITH outerValue USE comp.two WITH 2 AS innerValue RETURN * } RETURN *", []string{"innerValue", "outerValue"}, []string{"innerValue"}, []string{}, resultmetadata.ImportNone, resultmetadata.CallReturning},
		{"CALL () { CREATE (:FactsUnit) } RETURN 1 AS value", []string{"value"}, []string{}, []string{}, resultmetadata.ImportListed, resultmetadata.CallUnit},
	} {
		t.Run(tc.query, func(t *testing.T) {
			exec.StatementFacts(tc.query, resultmetadata.KnownScope(nil), true, RawSource).Visit(func(statement resultmetadata.Statement) {
				statement.Columns.Visit(func(names []string) { require.Equal(t, tc.columns, names) }, func() { t.Fatal("known program has unknown columns") })
				for _, clause := range statement.Clauses {
					require.GreaterOrEqual(t, clause.Start, 0)
					require.LessOrEqual(t, clause.End, len(tc.query))
					require.Less(t, clause.Start, clause.End)
					if call := clause.Call; call != nil {
						require.Equal(t, tc.mode, call.Mode)
						require.Equal(t, tc.kind, call.Kind)
						call.Body.Visit(func(names []string) { require.Equal(t, tc.body, names) }, func() { t.Fatal("known CALL has unknown columns") })
						call.Retained.Visit(func(names []string) { require.Equal(t, tc.retained, names) }, func() { t.Fatal("known CALL has unknown retained scope") })
						require.NotNil(t, call.Statement)
						for _, inner := range call.Statement.Clauses {
							require.Greater(t, inner.Start, clause.Start)
							require.Less(t, inner.End, clause.End)
							require.True(t, strings.HasPrefix(tc.query[inner.Start:inner.End], "WITH") || strings.HasPrefix(tc.query[inner.Start:inner.End], "RETURN") || strings.HasPrefix(tc.query[inner.Start:inner.End], "USE") || strings.HasPrefix(tc.query[inner.Start:inner.End], "CREATE"))
						}
					}
				}
			}, func() { t.Fatal("owner declined to a nonexistent continuation") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Reason, reason.Position, reason.Err) })
		})
	}
	for _, query := range []string{"CALL (missing) { RETURN 1 AS value } RETURN value", "CALL (x,) { RETURN 1 AS value } RETURN value", "LOAD CSV FROM 'unused' AS row RETURN *", "CALL missing.procedure()"} {
		exec.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) { t.Fatal("incomplete statement became known", query) }, func() { t.Fatal("owner declined", query) }, func(reason resultmetadata.Unresolved) { require.Error(t, reason.Err) })
	}
}

func TestStatementFactsParticipantPreflight(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "participant-facts"))
	query := "USE comp.remote CALL custom.participantOnly(outer)"
	for _, output := range []string{"first", "second"} {
		calls := 0
		facts := exec.StatementFactsWith(query, resultmetadata.KnownScope([]string{"outer"}), false, func(obligation resultmetadata.SchemaObligation) (resultmetadata.Columns, error) {
			calls++
			require.Equal(t, "custom.participantOnly", obligation.Procedure)
			require.Equal(t, "CALL custom.participantOnly(outer)", query[obligation.Start:obligation.End])
			require.Equal(t, "USE comp.remote", strings.TrimSpace(query[obligation.UseStart:obligation.UseEnd]))
			obligation.Incoming.Visit(func(names []string) { require.Equal(t, []string{"outer"}, names) }, func() { t.Fatal("unknown argument scope reached participant") })
			return resultmetadata.KnownColumns([]string{output}), nil
		}, RawSource)
		facts.Visit(func(statement resultmetadata.Statement) {
			statement.Columns.Visit(func(names []string) { require.Equal(t, []string{output}, names) }, func() { t.Fatal("participant schema was not completed") })
			require.True(t, statement.Clauses[len(statement.Clauses)-1].ParticipantResolved)
		}, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
		require.Equal(t, 1, calls, "participant answers must not be cached by text")
	}
	exec.StatementFacts(query, resultmetadata.KnownScope([]string{"outer"}), false, RawSource).Visit(func(resultmetadata.Statement) { t.Fatal("participant answer leaked into nil-schema cache") }, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) {
		require.Equal(t, resultmetadata.ReasonDynamicSchema, reason.Reason)
		require.Error(t, reason.Err)
	})
	for _, answer := range []func(resultmetadata.SchemaObligation) (resultmetadata.Columns, error){
		func(resultmetadata.SchemaObligation) (resultmetadata.Columns, error) {
			return resultmetadata.UnknownColumns(), nil
		},
		func(resultmetadata.SchemaObligation) (resultmetadata.Columns, error) {
			return resultmetadata.UnknownColumns(), errors.New("participant failure")
		},
	} {
		exec.StatementFactsWith(query, resultmetadata.KnownScope([]string{"outer"}), false, answer, RawSource).Visit(func(resultmetadata.Statement) { t.Fatal("unknown participant schema became complete") }, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) { require.Error(t, reason.Err) })
	}
	nested := "USE comp.remote CALL (outer) { CALL custom.participantOnly() YIELD value AS kept RETURN * } RETURN *"
	exec.StatementFactsWith(nested, resultmetadata.KnownScope([]string{"outer"}), false, func(resultmetadata.SchemaObligation) (resultmetadata.Columns, error) {
		t.Fatal("explicit YIELD must not require signature preflight")
		return resultmetadata.UnknownColumns(), nil
	}, RawSource).Visit(func(statement resultmetadata.Statement) {
		statement.Columns.Visit(func(names []string) { require.Equal(t, []string{"kept", "outer"}, names) }, func() { t.Fatal("explicit yields unresolved") })
		for _, clause := range statement.Clauses {
			if clause.Call != nil {
				require.Equal(t, resultmetadata.ResolvedByParticipant, clause.Call.Resolution)
			}
		}
	}, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
	withoutUse := "CALL custom.participantOnly()"
	exec.StatementFactsWith(withoutUse, resultmetadata.KnownScope(nil), false, func(resultmetadata.SchemaObligation) (resultmetadata.Columns, error) {
		t.Fatal("a procedure without USE is owned by the local registry")
		return resultmetadata.UnknownColumns(), nil
	}, RawSource).Visit(func(resultmetadata.Statement) { t.Fatal("unknown local procedure accepted") }, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) {
		require.Equal(t, resultmetadata.ReasonUnknownProcedure, reason.Reason)
	})
}

func TestStatementFactsOriginalSourceWindows(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "source-windows"))
	for _, query := range []string{
		" /* decoy CALL { RETURN 0 AS decoy } */ CYPHER 25 EXPLAIN USE comp CALL (outer) { WITH outer USE comp.two OPTIONAL CALL () { WITH 'USE FINISH' AS `quoted name` RETURN * } RETURN * } RETURN *;",
		"CALL () { USE comp.a UNWIND [] AS before RETURN * UNION ALL USE comp.b UNWIND [1] AS before RETURN * } RETURN *",
		"CALL () { CREATE (:FactsUnit) FINISH /* trailing comment */ } RETURN 1 AS value",
	} {
		t.Run(query, func(t *testing.T) {
			var check func(resultmetadata.Statement)
			check = func(statement resultmetadata.Statement) {
				require.GreaterOrEqual(t, statement.Start, 0)
				require.LessOrEqual(t, statement.End, len(query))
				for _, clause := range statement.Clauses {
					require.GreaterOrEqual(t, clause.Start, statement.Start)
					require.LessOrEqual(t, clause.End, statement.End)
					require.Less(t, clause.Start, clause.End)
					text := query[clause.Start:clause.End]
					if clause.Kind == resultmetadata.ClauseUse {
						require.True(t, strings.HasPrefix(text, "USE "), text)
					}
					if clause.Kind == resultmetadata.ClauseFinish {
						require.Equal(t, "FINISH", text)
					}
					if clause.Call != nil {
						require.True(t, strings.HasPrefix(text, "CALL "), text)
						check(*clause.Call.Statement)
					}
				}
				for _, branch := range statement.Branches {
					check(branch)
				}
			}
			exec.StatementFacts(query, resultmetadata.KnownScope([]string{"outer"}), true, RawSource).Visit(check, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err, reason.Position) })
		})
	}
}

func TestStatementFactsPublicHeaderUnknownIsTerminal(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-unknown"))
	query := "USE comp.remote CREATE (:MustNotExist) CALL custom.participantOnly()"
	_, err := exec.StatementResultColumnsContext(context.Background(), query, RawSource)
	require.Error(t, err)
	_, err = exec.executeExplain(context.Background(), originalBraceView(query))
	require.Error(t, err)
	_, err = exec.executeProfile(context.Background(), originalBraceView(query))
	require.Error(t, err)
	result, err := exec.Execute(context.Background(), "MATCH (n:MustNotExist) RETURN count(n) AS count", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestStatementFactsSeparatesScopeFromVisibleColumns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-identities"))
	// WHEN lowering passes this control variable into CALL (*). It is an
	// execution identity even though RETURN * must not publish it as a column.
	query := "CALL (*) { FILTER " + whenBranchVariable + " = 1 RETURN 1 AS value } RETURN *"
	exec.StatementFacts(query, resultmetadata.KnownScope([]string{whenBranchVariable, "outer"}), true, RawSource).Visit(func(statement resultmetadata.Statement) {
		call := statement.Clauses[0].Call
		require.NotNil(t, call)
		require.Equal(t, []string{whenBranchVariable, "outer"}, call.Imports)
		call.Retained.Visit(func(names []string) { require.Equal(t, []string{whenBranchVariable, "outer"}, names) }, func() { t.Fatal("retained execution scope is unknown") })
		statement.Columns.Visit(func(names []string) { require.Equal(t, []string{"outer", "value"}, names) }, func() { t.Fatal("visible columns are unknown") })
	}, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
}

func TestStatementFactsUnitAdministration(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unit-command-facts"))
	for _, query := range []string{
		"GRANT EXECUTE PROCEDURE db.labels ON DBMS TO reader",
		"GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer",
		"REVOKE EXECUTE PROCEDURE db.labels ON DBMS FROM reader",
		"REVOKE GRANT EXECUTE BOOSTED PROCEDURES db.* ON DBMS FROM reader",
	} {
		exec.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
			statement.Columns.Visit(func(names []string) { require.Empty(t, names) }, func() { t.Fatal("a parsed unit command has unknown columns") })
			require.Len(t, statement.Clauses, 1)
			require.Equal(t, resultmetadata.ClauseWrite, statement.Clauses[0].Kind)
		}, func() { t.Fatal("owner declined a supported command") }, func(reason resultmetadata.Unresolved) { t.Fatal(query, reason.Err) })
	}
	exec.StatementFacts("GRANT EXECUTE PROCEDURE db.labels", resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) {
		t.Fatal("malformed command became known")
	}, func() { t.Fatal("malformed command declined") }, func(reason resultmetadata.Unresolved) {
		require.Contains(t, statusText(reason.Err), "Neo.ClientError.Statement.SyntaxError")
	})
}

func TestResultHeaderOrdinaryCallUsesExecutionColumns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lazy-call-metadata"))
	query := "WITH 1 AS headerOwnerLazyValue CALL { WITH headerOwnerLazyValue RETURN headerOwnerLazyValue AS headerOwnerLazyResult } RETURN headerOwnerLazyResult"
	call := "CALL { WITH headerOwnerLazyValue RETURN headerOwnerLazyValue AS headerOwnerLazyResult }"
	keys := []projectionFactsKey{
		projectionKey(query, resultmetadata.Known(nil), resultmetadata.Known(nil), false),
		projectionKey(call, resultmetadata.Known([]string{"headerOwnerLazyValue"}), resultmetadata.Known(nil), false),
	}
	for _, key := range keys {
		_, cached := projectionFactsCache.get(key)
		require.False(t, cached, "test requires a cold program")
	}
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"headerOwnerLazyResult"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	_, cached := projectionFactsCache.get(keys[0])
	require.True(t, cached, "execution consumes the one bound statement program")
	_, cached = projectionFactsCache.get(keys[1])
	require.False(t, cached, "CALL tails reuse nested facts without compiling an independent fragment program")
	result, err = exec.Execute(context.Background(), "EXPLAIN "+query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"headerOwnerLazyResult"}, result.Columns)
	_, cached = projectionFactsCache.get(keys[0])
	require.True(t, cached, "EXPLAIN consumes the one cached owner program")
}
