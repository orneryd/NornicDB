package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallImportCanonicalAdmission(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical-import-admission"))
	for _, sample := range statementfactstest.CallImportAdmissionCases() {
		t.Run(sample.Name, func(t *testing.T) {
			executor.StatementFacts(sample.Query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
				var check func(resultmetadata.Statement)
				check = func(body resultmetadata.Statement) {
					for _, branch := range body.Branches {
						check(branch)
					}
					for _, clause := range body.Clauses {
						if clause.Call != nil {
							clause.Call.Statement.Incoming.Visit(func(names []string) { require.Contains(t, names, "value") }, func() { t.Fatal("unknown incoming") })
							check(*clause.Call.Statement)
						}
					}
				}
				check(statement)
			}, func() { t.Fatal("declined facts") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
			require.NoError(t, executor.validateCallSubqueryScopes(context.Background(), sample.Query, false))
		})
	}
}

func TestCallImportCanonicalErrorsAndUpdates(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical-import-rules"))
	for _, sample := range []struct{ name, query, detail string }{
		{"missing", "CALL { CALL { WITH missing RETURN missing AS kept } RETURN kept AS x } RETURN x", "UndefinedVariable"},
		{"empty_scope", "WITH 7 AS value CALL () { CALL { WITH value RETURN value AS kept } RETURN kept AS x } RETURN x", "UndefinedVariable"},
		{"legacy_expires", "WITH 7 AS value CALL { WITH value WITH 1 AS keep CALL { WITH value RETURN value AS kept } RETURN kept AS x } RETURN x", "UndefinedVariable"},
		{"implicit_retained", "WITH 7 AS value CALL { WITH 1 AS keep CALL { WITH value RETURN value AS kept } RETURN kept AS x } RETURN x", ""},
		{"scoped_retained", "WITH 7 AS value CALL (value) { WITH 1 AS keep CALL { WITH value RETURN value AS kept } RETURN kept AS x } RETURN x", ""},
		{"invalid_import_alias", "WITH 7 AS value CALL { WITH value AS renamed RETURN renamed AS x } RETURN x", "InvalidVariableImport"},
		{"invalid_import_where", "WITH 7 AS value CALL { WITH value WHERE value > 0 RETURN value AS x } RETURN x", "InvalidVariableImport"},
		{"duplicate_columns", "CALL { RETURN 1 AS x, 2 AS x } RETURN x", "ColumnNameConflict"},
		{"empty_update", "MATCH (value:Absent) CALL { CALL { SET value.n = 7 } } RETURN value", ""},
		{"empty_star_update", "MATCH (value:Absent) CALL (*) { CALL (*) { SET value.n = 7 } } RETURN value", ""},
		{"undefined_update", "UNWIND [] AS emptyInput CALL { CALL { SET missing.n = 7 } } RETURN emptyInput", "UndefinedVariable"},
		{"invalid_update_type", "WITH 7 AS value CALL { CALL { SET value.n = 7 } } RETURN value", "InvalidArgumentType"},
	} {
		t.Run(sample.name, func(t *testing.T) {
			_, err := executor.Execute(context.Background(), sample.query, nil)
			if sample.detail == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			detail, ok := err.(interface{ BoltErrorDetail() string })
			require.True(t, ok, "%T: %v", err, err)
			require.Equal(t, sample.detail, detail.BoltErrorDetail())
		})
	}
}

// Independent validator entry points must use the same import verdict, while
// schema commands containing CALL in an identifier keep their command owner.
func TestCallImportCanonicalConsumerErrors(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "canonical-import-consumer-errors"))
	require.NoError(t, executor.validateCallSubqueryScopes(context.Background(), "CREATE INDEX call_index FOR (n:ImportProbe) ON (n.id)", false))
	err := executor.validateMatchSemanticScopes(context.Background(), "WITH 7 AS value CALL { WITH missing RETURN missing AS kept } RETURN kept", false)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing")
	detail, ok := err.(interface{ BoltErrorDetail() string })
	require.True(t, ok)
	require.Equal(t, "UndefinedVariable", detail.BoltErrorDetail())
	// A malformed map update must be rejected even when no row reaches either CALL.
	_, err = executor.Execute(context.Background(), "MATCH (value:Absent) CALL { CALL { SET value += 7 } } RETURN value", nil)
	require.Error(t, err)
	detail, ok = err.(interface{ BoltErrorDetail() string })
	require.True(t, ok)
	require.Equal(t, "InvalidArgumentType", detail.BoltErrorDetail())
}
