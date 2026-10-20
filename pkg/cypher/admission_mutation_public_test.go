package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestAdmissionWithMutationPublic(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, route := range []string{"native", "call", "use", "composite"} {
			for _, explicit := range []bool{false, true} {
				for _, empty := range []bool{false, true} {
					for _, projection := range []string{"name", "alias", "star"} {
						for _, operation := range []string{"CREATE", "MERGE", "SET", "REMOVE", "DELETE", "FOREACH"} {
							t.Run(fmt.Sprintf("%s/%s/%s/empty_%v/norm_%v/tx_%v", operation, projection, route, empty, normalize, explicit), func(t *testing.T) {
								exec := newFabricCallRowsExecutor(t)
								seed := "CREATE (:AdmissionMutation {id:1,flag:1})"
								if route == "use" || route == "composite" {
									seed = "USE comp.one " + seed
								}
								_, err := exec.Execute(ctx, seed, nil)
								require.NoError(t, err)
								if route == "use" || route == "composite" {
									exec = fabricCallRowsCompositeExecutor(t, exec)
								}
								if explicit {
									_, err = exec.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
									defer exec.Execute(ctx, "ROLLBACK", nil)
								}
								label := "AdmissionMutation"
								if empty {
									label += "Missing"
								}
								prefix := "MATCH (source:" + label + ") "
								if operation == "CREATE" || operation == "MERGE" {
									prefix = "WITH 7 AS source "
									if empty {
										prefix = "MATCH (none:AdmissionMutationMissing) " + prefix
									}
								}
								name, with := "source", "WITH source "
								if projection == "alias" {
									name, with = "value", "WITH source AS value "
								}
								if projection == "star" {
									with = "WITH * "
								}
								var tail string
								var value interface{} = int64(7)
								switch operation {
								case "CREATE":
									tail = "CREATE (made:AdmissionMade {value:" + name + "}) RETURN made.value AS value"
								case "MERGE":
									tail = "MERGE (made:AdmissionMade {value:" + name + "}) RETURN made.value AS value"
								case "SET":
									tail = "SET " + name + ".flag=2 RETURN " + name + ".flag AS value"
									value = int64(2)
								case "REMOVE":
									tail = "REMOVE " + name + ".flag RETURN " + name + ".flag AS value"
									value = nil
								case "DELETE":
									tail = "DELETE " + name + " RETURN count(*) AS value"
									value = int64(1)
								case "FOREACH":
									tail = "FOREACH (item IN [1] | SET " + name + ".flag=3) RETURN " + name + ".flag AS value"
									value = int64(3)
								}
								query := prefix + with + tail
								if route == "call" {
									query = "CALL { " + query + " } RETURN value"
								}
								if route == "use" {
									query = "USE comp.one " + query
								}
								if route == "composite" {
									query = "USE comp CALL { USE comp.one " + query + " } RETURN value"
								}
								result, err := exec.Execute(ctx, query, nil)
								require.NoError(t, err, query)
								want := [][]interface{}{{value}}
								if empty {
									want = [][]interface{}{}
									if operation == "DELETE" {
										want = [][]interface{}{{int64(0)}}
									}
								}
								require.Equal(t, []string{"value"}, result.Columns, query)
								require.Equal(t, want, result.Rows, query)
							})
						}
					}
				}
			}
		}
	}
	for _, query := range []string{
		"MATCH (n:AdmissionMissing) WITH n SET missing.flag=1",
		"MATCH (n:AdmissionMissing) WITH n CREATE (:AdmissionMade {v:missing})",
		"MATCH (n:AdmissionMissing) WITH n MERGE (:AdmissionMade {v:missing})",
		"WITH [] AS values FOREACH (item IN values | SET missing.flag=1)",
	} {
		exec := newFabricCallRowsExecutor(t)
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
}

func TestAdmissionProjectionNoDemandContract(t *testing.T) {
	input := newSemanticStatementInput("CREATE (t:TestNode {name:'temp'}) WITH t DELETE t", nil)
	require.Nil(t, input.syntax())
	clauses, ok := input.pipeline(true)
	require.True(t, ok)
	for _, clause := range clauses {
		require.True(t, clause.noLocalScopes)
		require.Nil(t, clause.syntax)
		if clause.kind == pipelineClauseWith {
			require.NotNil(t, clause.projection)
			require.Len(t, clause.projection.Items, 1)
			require.Equal(t, "t", clause.projection.Items[0].Column)
		}
	}
	missing := pipelineClause{kind: pipelineClauseWith, text: "WITH kept"}
	require.Equal(t, "kept", admittedProjection(missing).Items[0].Column)
	empty := &fabric.QueryProjection{}
	missing.projection = empty
	require.Same(t, empty, admittedProjection(missing), "an admitted empty projection must not be replaced by a text reconstruction")
}
