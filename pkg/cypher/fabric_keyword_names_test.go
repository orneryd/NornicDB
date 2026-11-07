package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/fabricnametest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCompositeKeywordNamesNeo4j(t *testing.T) {
	driver := fabricCallRowsReference(t)
	if driver == nil {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI for live Neo4j 5.26.30")
	}
	_, err := neo4j.ExecuteQuery(context.Background(), driver, "CREATE (:KeywordNameSeed {v:7})", nil, neo4j.EagerResultTransformer)
	require.NoError(t, err)
	for _, tc := range fabricnametest.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			result, err := neo4j.ExecuteQuery(context.Background(), driver, tc.Reference, tc.Params, neo4j.EagerResultTransformer)
			require.NoError(t, err, "%s", tc.Reference)
			require.Equal(t, []string{"k"}, result.Keys)
			require.Len(t, result.Records, 1)
			require.Equal(t, []interface{}{int64(7)}, result.Records[0].Values)
		})
	}
}

// HANDOFF (NOR-72, reproduced on release/v1.4.4 @ 29ecd7909): this test still
// fails there. Failing families: parameter_identity and selection_parameter
// (a caller parameter $name resolves to the same-named imported variable, e.g.
// 9 instead of 7, or the wrong graph) plus a few "lookup" cases. Plain
// keyword-named variables mostly pass. The same queries on a non-composite
// executor return the parameter correctly, so the suspect is the Fabric/composite
// path that merges record imports with caller params
// (pkg/fabric/executor.go importStatement/remoteImportStatement and
// pkg/cypher/executor_fabric.go stripLeadingWithImportsForFabricRecord).
func TestCompositeKeywordNamesExecute(t *testing.T) {
	ctx := context.Background()
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("raw=%v", raw), func(t *testing.T) {
			if raw {
				restore := config.WithCypherQueryNormalizationDisabled()
				defer restore()
			}
			exec := newFabricCallRowsExecutor(t)
			_, err := exec.Execute(ctx, "USE comp.one CREATE (:KeywordNameSeed {v:7})", nil)
			require.NoError(t, err)
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
					for _, tc := range fabricnametest.Cases() {
						t.Run(tc.Name, func(t *testing.T) {
							queryExec := exec
							if explicit {
								queryExec = fabricCallRowsCompositeExecutor(t, exec)
								_, err := queryExec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer queryExec.Execute(ctx, "ROLLBACK", nil)
							}
							result, err := queryExec.Execute(ctx, tc.Query, tc.Params)
							require.NoError(t, err, "%s", tc.Query)
							require.Equal(t, []string{"k"}, result.Columns)
							require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, "%s", tc.Query)
						})
					}
				})
			}
		})
	}
}

// A caller parameter remains distinct from an imported variable of the same name.
func TestCompositeKeywordParameterIdentity(t *testing.T) {
	executor := newFabricCallRowsExecutor(t)
	result, err := executor.Execute(t.Context(),
		"USE comp WITH 9 AS call CALL (call) { USE comp.one RETURN $call AS k } RETURN k",
		map[string]interface{}{"call": int64(7)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
}
