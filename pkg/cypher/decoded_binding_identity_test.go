package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedBindingIdentity(t *testing.T) {
	for _, name := range []string{" ", "  ", "\t", "`literal`", "`a`", "a`b", "`", "a`", "`a", "ordinary"} {
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "binding_identity"))
			written := cyphertext.QuoteName(name)
			t.Run("primitive_and_projection", func(t *testing.T) {
				token, ok := cyphertext.ScanIdentifier(written, 0)
				decoded, end := token.Value, token.End
				require.True(t, ok)
				require.Equal(t, len(written), end)
				require.Equal(t, name, decoded)
				require.Equal(t, name, cyphertext.SymbolicNameValue(written))
				item := readProjectionItemSource(context.Background(), "7 AS "+written, projectionAdmitted)
				alias := item.alias
				require.Equal(t, name, alias)
				require.Equal(t, name, unwindBindingName("UNWIND [7] AS "+written))
			})
			t.Run("decoded_metadata_consumers", func(t *testing.T) {
				ctx := context.Background()
				require.Equal(t, "Integer", propertyAccessExpressionType(written, map[string]string{name: "Integer"}, nil))
				ctx25 := withCypherVersion(ctx, "CYPHER 25")
				projected := projectTypeScope(ctx25, make(typeScope), true, "WITH {value:7} AS "+written)
				require.Contains(t, projected, name)
				require.NotEmpty(t, projected[name].members)
				if name != "literal" {
					require.NoError(t, validateProjectionSemantics(ctx, "WITH 7 AS "+written+", 99 AS literal RETURN "+written))
					require.NoError(t, validateProcedureYieldBindings(&yieldClause{items: []yieldItem{{name: "first", alias: name, aliased: true}, {name: "second", alias: "literal", aliased: true}}}, true))
				}
			})
			for _, tc := range []struct {
				kind, query string
				admit       func(context.Context, string) error
			}{
				{"create", "WITH 7 AS " + written + " WITH * CREATE (n {value: " + written + "}) RETURN n.value AS value", func(ctx context.Context, query string) error {
					return exec.validateCreateSemanticScopes(ctx, query, false)
				}},
				{"merge", "WITH 7 AS " + written + " WITH * MERGE (n:R4Binding {value: " + written + "}) RETURN n.value AS value", func(ctx context.Context, query string) error {
					return exec.validateMergeSemanticScopes(ctx, query, false)
				}},
				{"set", "CREATE (n) WITH n, 7 AS " + written + " WITH * SET n.value = " + written + " RETURN n.value AS value", func(ctx context.Context, query string) error { return exec.validateSetSemanticScopes(ctx, query) }},
				{"foreach", "WITH 7 AS " + written + " CREATE (n) FOREACH (i IN [1] | SET n.value = " + written + ") RETURN n.value AS value", func(ctx context.Context, query string) error { return exec.validateSetSemanticScopes(ctx, query) }},
				{"unwind", "UNWIND [7] AS " + written + " CREATE (n {value: " + written + "}) RETURN n.value AS value", func(ctx context.Context, query string) error {
					return exec.validateCreateSemanticScopes(ctx, query, false)
				}},
				{"node_binding", "CREATE (" + written + " {value:7}) WITH * SET " + written + ".value=7 RETURN " + written + ".value AS value", func(ctx context.Context, query string) error { return exec.validateSetSemanticScopes(ctx, query) }},
				{"relationship_binding", "CREATE ()-[" + written + ":R4Scope {value:7}]->() WITH * SET " + written + ".value=7 RETURN " + written + ".value AS value", func(ctx context.Context, query string) error { return exec.validateSetSemanticScopes(ctx, query) }},
				{"foreach_binding", "CREATE (n) FOREACH (" + written + " IN [7] | SET n.value=" + written + ") RETURN n.value AS value", func(ctx context.Context, query string) error { return exec.validateSetSemanticScopes(ctx, query) }},
				{"call_import", "WITH 7 AS " + written + " CALL (" + written + ") { RETURN " + written + " AS v } RETURN v AS value", func(ctx context.Context, query string) error {
					return exec.validateCallSubqueryScopes(ctx, query, false)
				}},
				{"let", "CYPHER 25 LET " + written + "=7 CREATE (n {value:" + written + "}) RETURN n.value AS value", func(ctx context.Context, query string) error {
					return exec.validateCreateSemanticScopes(withCypherVersion(ctx, query), query, true)
				}},
				{"distinct", "WITH 7 AS " + written + " RETURN DISTINCT " + written + " AS value ORDER BY value", func(ctx context.Context, query string) error { return validateProjectionSemantics(ctx, query) }},
				{"aggregation", "UNWIND [7] AS x WITH sum(x) AS " + written + " RETURN " + written + " AS value ORDER BY value", func(ctx context.Context, query string) error { return validateProjectionSemantics(ctx, query) }},
				{"map_binding", "WITH {value:7} AS " + written + " WITH * RETURN " + written + ".value AS value", func(ctx context.Context, query string) error { return validatePropertyAccessTypes(ctx, query, nil) }},
			} {
				t.Run(tc.kind, func(t *testing.T) {
					t.Run("scope_consumer", func(t *testing.T) {
						ctx := withCypherVersion(context.Background(), tc.query)
						canonical, names := canonicalizeQuotedVariables(ctx, tc.query)
						if names != nil {
							ctx = withQuotedVariableNames(ctx, names)
						}
						require.NoError(t, tc.admit(ctx, canonical))
					})
					result, err := exec.Execute(context.Background(), tc.query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
				})
			}
			require.Error(t, exec.validateCreateSemanticScopes(context.Background(), "WITH 7 AS "+written+" CREATE (n {value: missing})", false))
		})
	}
}

func TestDecodedProcedureIdentity(t *testing.T) {
	for _, name := range []string{" ", "  ", "\t", "`literal`", "a`b", "`", "r4ordinary"} {
		t.Run(name, func(t *testing.T) {
			cleanupAdmissionProcedure(t, name)
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{
				Name: name, Mode: ProcedureModeRead, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}, {Name: "other", Type: "INTEGER"}},
			}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
				return &ExecuteResult{Columns: []string{"value", "other"}, Rows: [][]interface{}{{int64(19), int64(29)}}}, nil
			}))
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "procedure_identity"))
			result, err := exec.Execute(context.Background(), "CALL "+cyphertext.QuoteName(name)+"() YIELD value RETURN value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(19)}}, result.Rows)
			// The YIELD identities stay distinct from a plain literal-named binding.
			alias := cyphertext.QuoteName(name)
			result, err = exec.Execute(context.Background(), "CALL "+cyphertext.QuoteName(name)+"() YIELD value AS "+alias+", other AS literal RETURN "+alias+" AS value, literal AS other", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"value", "other"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(19), int64(29)}}, result.Rows)
		})
	}
	require.Error(t, validateProcedureSpec(ProcedureSpec{Name: ""}))
}
