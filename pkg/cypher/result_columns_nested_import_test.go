package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Nested consumers read the same declared input identities as ordinary CALL.
// A procedure's source-column spelling is not an outer-variable dependency.
func TestResultHeaderNestedImportConsumers(t *testing.T) {
	for _, sample := range []struct {
		name, query      string
		imports, columns []string
		rows             [][]interface{}
	}{
		{"scope_clause", "WITH 1 AS value CALL { CALL (value) { RETURN value AS x } RETURN x } RETURN x", []string{"value"}, []string{"x"}, [][]interface{}{{int64(1)}}},
		{"expression_pattern_strong", "MATCH (value:NestedImport) CALL (value) { RETURN EXISTS { MATCH (value) } AS x } RETURN x", []string{"value"}, []string{"x"}, [][]interface{}{{true}}},
		{"expression_pattern_weak", "MATCH (value:NestedImport) CALL { RETURN EXISTS { MATCH (value) } AS x } RETURN x", nil, []string{"x"}, [][]interface{}{{true}}},
		{"procedure_source_alias", "WITH 1 AS label CALL { CALL db.labels() YIELD label AS kept RETURN kept } RETURN label, kept", nil, []string{"label", "kept"}, [][]interface{}{{int64(1), "NestedImport"}}},
	} {
		t.Run(sample.name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nested-import"))
			_, err := executor.Execute(context.Background(), "CREATE (:NestedImport)", nil)
			require.NoError(t, err)
			executor.StatementFacts(sample.query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
				found := false
				for _, clause := range statement.Clauses {
					if clause.Call != nil {
						found = true
						require.ElementsMatch(t, sample.imports, clause.Call.Imports)
					}
				}
				require.True(t, found)
			}, func() { t.Fatal("nested input facts declined") }, func(reason resultmetadata.Unresolved) { t.Fatalf("nested input facts unresolved: %v", reason.Err) })
			result, err := executor.Execute(context.Background(), sample.query, nil)
			require.NoError(t, err)
			require.Equal(t, sample.columns, result.Columns)
			require.Equal(t, sample.rows, result.Rows)
		})
	}
}
