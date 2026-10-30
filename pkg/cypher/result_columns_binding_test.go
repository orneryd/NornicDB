package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type legacyImportBindingCase struct {
	name  string
	query string
	call  string
	valid bool
}

func generatedLegacyImportBindingCases() []legacyImportBindingCase {
	var cases []legacyImportBindingCase
	add := func(name, seed, call string, valid bool) {
		cases = append(cases, legacyImportBindingCase{name, seed + " " + call + " RETURN v, r", call, valid})
	}
	seeds := []string{"WITH 1 AS v", "UNWIND [] AS v", "UNWIND range(1, 0) AS i WITH i AS v"}
	imports := []string{"WITH v", "WITH *"}
	reads := []string{"v + y", "coalesce(v, y)", "CASE WHEN false THEN v ELSE y END", "[v, y]", "{value: v}", "count(v)"}
	for seed, prefix := range seeds {
		for imported, importing := range imports {
			for read, expression := range reads {
				for depth := 0; depth < 2; depth++ {
					call := "CALL { " + importing + " WITH 10 AS y RETURN " + expression + " AS r }"
					if depth > 0 {
						call = "CALL { WITH v " + call + " RETURN r }"
					}
					add(fmt.Sprintf("dropped_%d_%d_%d_%d", seed, imported, read, depth), prefix, call, false)
				}
			}
		}
	}
	for seed, prefix := range seeds[:2] {
		for imported, importing := range imports {
			add(fmt.Sprintf("use_%d_%d", seed, imported), "USE comp "+prefix, "CALL { "+importing+" USE comp.one WITH 10 AS y RETURN v + y AS r }", false)
			add(fmt.Sprintf("mixed_star_%d_%d", seed, imported), prefix, "CALL { "+importing+" WITH 10 AS y RETURN *, v + y AS r }", false)
			add(fmt.Sprintf("with_read_%d_%d", seed, imported), prefix, "CALL { "+importing+" WITH 10 AS y WITH v + y AS r RETURN r }", false)
		}
	}
	for seed, prefix := range seeds {
		for _, importing := range []string{"v", "*"} {
			add(fmt.Sprintf("retained_%d_%s", seed, importing), prefix, "CALL ("+importing+") { WITH 10 AS y RETURN v + y AS r }", true)
		}
	}
	for imported, importing := range imports {
		add(fmt.Sprintf("projected_%d", imported), seeds[0], "CALL { "+importing+" WITH v, 10 AS y RETURN v + y AS r }", true)
	}
	add("reintroduced", seeds[0], "CALL { WITH v WITH 10 AS y WITH y AS v, y RETURN v + y AS r }", true)
	add("expression_local", seeds[0], "CALL { WITH * WITH 10 AS y RETURN [v IN [y] | v] AS r }", true)
	return cases
}

// Names and projection expressions belong to the same compiled program. A
// legacy import can leave scope; a scope-clause import remains visible.
func TestStatementFactsLegacyImportBinding(t *testing.T) {
	ctx := context.Background()
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	cases := generatedLegacyImportBindingCases()
	require.Len(t, cases, 94)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
			query := strings.ReplaceAll(strings.ReplaceAll(tc.query, "USE comp.one", "USE neo4j"), "USE comp ", "USE neo4j ")
			checkError := func(t *testing.T, err error) {
				if tc.valid {
					require.NoError(t, err)
					return
				}
				require.Error(t, err)
				requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				require.ErrorContains(t, err, "Variable `v` not defined")
			}
			if reference != nil {
				t.Run("neo4j", func(t *testing.T) {
					for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
						result, err := neo4j.ExecuteQuery(ctx, reference, prefix+query, nil, neo4j.EagerResultTransformer)
						if !tc.valid {
							require.Error(t, err)
							failure, ok := err.(*neo4j.Neo4jError)
							require.True(t, ok, "%T: %v", err, err)
							require.Equal(t, "Neo.ClientError.Statement.SyntaxError", failure.Code)
							require.Contains(t, failure.Msg, "Variable `v` not defined")
						} else {
							require.NoError(t, err)
							require.Equal(t, []string{"v", "r"}, result.Keys)
						}
					}
				})
			}
			for _, cypher25 := range []bool{false, true} {
				for _, preflight := range []bool{false, true} {
					t.Run(fmt.Sprintf("facts/dialect25=%v/preflight=%v", cypher25, preflight), func(t *testing.T) {
						var schema resultmetadata.ParticipantSchema
						if preflight {
							schema = func(resultmetadata.SchemaObligation) (resultmetadata.Columns, error) {
								t.Fatal("no procedure schema is needed")
								return resultmetadata.UnknownColumns(), nil
							}
						}
						executor.StatementFactsWith(tc.query, resultmetadata.KnownScope(nil), cypher25, schema, RawSource).Visit(func(statement resultmetadata.Statement) {
							require.True(t, tc.valid, "a projection read a dropped import but its facts became Known")
							statement.Columns.Visit(func(names []string) { require.Equal(t, []string{"v", "r"}, names) }, func() { t.Fatal("Known facts contain unknown columns") })
						}, func() { t.Fatal("the owner has no decline continuation") }, func(reason resultmetadata.Unresolved) {
							require.False(t, tc.valid, "%v", reason.Err)
							require.Equal(t, resultmetadata.ReasonUndefinedVariable, reason.Reason)
							require.GreaterOrEqual(t, reason.Position, 0)
							require.Less(t, reason.Position, len(tc.query))
							checkError(t, reason.Err)
						})
					})
				}
			}
			t.Run("strict_columns", func(t *testing.T) {
				columns, err := executor.StatementResultColumns(tc.query, RawSource)
				checkError(t, err)
				if tc.valid {
					require.Equal(t, []string{"v", "r"}, columns)
				}
			})
			t.Run("projected_scope", func(t *testing.T) {
				executor.ProjectedScopeAfterClause(tc.call, []string{"v"}, false).Visit(func(names []string) {
					require.True(t, tc.valid, "invalid projection produced authoritative names")
					require.ElementsMatch(t, []string{"v", "r"}, names)
				}, func() { require.False(t, tc.valid, "valid CALL scope became unknown") })
			})
			if strings.Contains(tc.query, "USE comp ") {
				// Composite public admission is exercised by the Fabric layer.
				// These calls pin the graph-bearing header owner's own error.
				t.Run("selected_graph_explain_owner", func(t *testing.T) {
					_, err := executor.executeExplain(ctx, originalBraceView(query))
					checkError(t, err)
				})
				t.Run("selected_graph_profile_owner", func(t *testing.T) {
					_, err := executor.executeProfile(ctx, originalBraceView(query))
					checkError(t, err)
				})
			}
			for _, explicit := range []bool{false, true} {
				for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
					t.Run(fmt.Sprintf("execute/explicit=%v/prefix=%s", explicit, prefix), func(t *testing.T) {
						if explicit {
							_, err := executor.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							defer func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) }()
						}
						// Native public modes use this fixture's session graph. Exact
						// selectors stay in facts and direct header-owner checks;
						// composite public admission belongs to the Fabric layer.
						executionQuery := strings.ReplaceAll(query, "USE neo4j ", "")
						result, err := executor.Execute(ctx, prefix+executionQuery, nil)
						checkError(t, err)
						if tc.valid {
							require.Equal(t, []string{"v", "r"}, result.Columns)
						}
					})
				}
			}
		})
	}
}
