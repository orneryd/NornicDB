package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Neo4j5.26's SCHEMA runtime executes PROFILE DDL but exposes no DB-hit
// statistics. An unknown metric must stay unknown, rather than a claimed zero.
func TestProfileSchemaRuntimeDoesNotClaimDatabaseHitMetrics(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct{ name, setup, command string }{
			{"index", "", "CREATE INDEX schema_profile_index FOR (n:SchemaProfile) ON (n.v)"},
			{"constraint", "", "CREATE CONSTRAINT schema_profile_unique FOR (n:SchemaProfile) REQUIRE n.v IS UNIQUE"},
			{"constraint-noop", "CREATE CONSTRAINT schema_profile_unique FOR (n:SchemaProfile) REQUIRE n.v IS UNIQUE", "CREATE CONSTRAINT schema_profile_unique IF NOT EXISTS FOR (n:SchemaProfile) REQUIRE n.v IS UNIQUE"},
			{"drop-index", "CREATE INDEX schema_profile_index FOR (n:SchemaProfile) ON (n.v)", "DROP INDEX schema_profile_index"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var plain *ExecuteResult
				for _, profiled := range []bool{false, true} {
					exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:SchemaProfile {v:1}),(:SchemaProfile {v:2}),(:SchemaProfile {v:3})")
					if tc.setup != "" {
						_, err := exec.Execute(t.Context(), tc.setup, nil)
						require.NoError(t, err)
					}
					query := tc.command
					if profiled {
						query = "PROFILE " + query
					}
					result, err := exec.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					require.Empty(t, result.Columns)
					require.Empty(t, result.Rows)
					if !profiled {
						plain = result
					} else {
						require.Equal(t, plain.Columns, result.Columns)
						require.Equal(t, plain.Rows, result.Rows)
						require.Equal(t, plain.Stats, result.Stats)
						plan := executionPlanFromResult(t, result)
						require.False(t, plan.TotalDBHitsObserved)
						require.NotEmpty(t, plan.Unobserved)
					}
					schema := exec.storage.GetSchema()
					if tc.name == "index" || tc.name == "drop-index" {
						found := false
						for _, item := range schema.GetIndexes() {
							if item.(map[string]interface{})["name"] == "schema_profile_index" {
								found = true
							}
						}
						require.Equal(t, tc.name == "index", found)
					}
					if tc.name == "constraint" || tc.name == "constraint-noop" {
						require.True(t, schema.HasConstraintNamed("schema_profile_unique"))
					}
				}
			})
		}
	})
}

func TestStatementProcedureBodiesRejectSchemaCommands(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{
			"CALL apoc.cypher.run($ddl,{})",
			"CALL apoc.cypher.doIt($ddl,{})",
			"CALL apoc.cypher.doitall($ddl,{})",
			"CALL apoc.cypher.runMany($ddl,{})",
			"CALL apoc.export.json.query($ddl,'')",
			"CALL apoc.export.csv.query($ddl,'')",
		} {
			t.Run(query, func(t *testing.T) {
				exec, _ := newProfileRuntimeTestExecutor(t)
				before := exec.storage.GetSchema().GetIndexes()
				_, err := exec.Execute(t.Context(), query, map[string]interface{}{"ddl": "CREATE INDEX nested_schema_probe FOR (n:NestedSchema) ON (n.v)"})
				require.Error(t, err, "a data-statement procedure does not admit schema operations")
				require.ElementsMatch(t, before, exec.storage.GetSchema().GetIndexes(), "rejected nested DDL must not publish schema")
			})
		}
	})
}

// A schema procedure keeps its declared capability, while a data wrapper cannot
// gain it by invoking that same procedure through a nested CALL.
func TestStatementProcedureSchemaModeIsInvocationScoped(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	const name = "schemaowner.createIndex"
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeSchema}, func(ctx context.Context, e *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		return e.executeUserStatement(ctx, "CREATE INDEX scoped_schema_owner FOR (n:ScopedSchemaOwner) ON (n.v)", nil)
	}))
	forEachParser(t, func(t *testing.T) {
		for _, wrapped := range []bool{false, true} {
			exec, _ := newProfileRuntimeTestExecutor(t)
			before := exec.storage.GetSchema().GetIndexes()
			query := "CALL " + name + "()"
			if wrapped {
				query = "CALL apoc.cypher.doIt($query,{})"
			}
			_, err := exec.Execute(t.Context(), query, map[string]interface{}{"query": "CALL " + name + "()"})
			if wrapped {
				require.Error(t, err)
				require.ElementsMatch(t, before, exec.storage.GetSchema().GetIndexes())
			} else {
				require.NoError(t, err)
				require.Len(t, exec.storage.GetSchema().GetIndexes(), len(before)+1)
			}
		}
	})
}
