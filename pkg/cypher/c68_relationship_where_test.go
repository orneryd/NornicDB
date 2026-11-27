package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestC68RelationshipWhereIndexSeedBoundaries(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for _, settings := range []struct {
				name    string
				options relationshipEqualityFixtureOptions
			}{
				{"unique", relationshipEqualityFixtureOptions{unrelatedRelationships: 128}},
				{"ordinary", relationshipEqualityFixtureOptions{unrelatedRelationships: 128, ordinaryIndex: true}},
				{"wal", relationshipEqualityFixtureOptions{unrelatedRelationships: 128, durabilityStack: true}},
			} {
				t.Run(settings.name, func(t *testing.T) {
					fixture := newRelationshipEqualityAccessFixture(t, settings.options)
					exec := fixture.exec
					ctx := context.Background()
					run := func(query string, params map[string]interface{}) *ExecuteResult {
						t.Helper()
						result, err := exec.Execute(ctx, query, params)
						require.NoError(t, err, query)
						return result
					}
					for _, tc := range []struct {
						name, query string
						params      map[string]interface{}
						want        [][]interface{}
						bounded     bool
					}{
						{"parameter", "PROFILE MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id RETURN r.id,a.id,b.id", map[string]interface{}{"id": "r289999"}, [][]interface{}{{"r289999", "n73", "n128561"}}, true},
						{"reversed", "PROFILE MATCH (a)-[r:PD_RELATION]->(b) WHERE $id=r.id RETURN r.id,a.id,b.id", map[string]interface{}{"id": "r289999"}, [][]interface{}{{"r289999", "n73", "n128561"}}, true},
						{"conjunction", "PROFILE MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id AND a.id='n73' RETURN r.id,a.id,b.id", map[string]interface{}{"id": "r289999"}, [][]interface{}{{"r289999", "n73", "n128561"}}, true},
						{"false residual", "PROFILE MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id AND a.id='other' RETURN r.id,a.id,b.id", map[string]interface{}{"id": "r289999"}, [][]interface{}{}, true},
						{"missing", "PROFILE MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id RETURN r.id,a.id,b.id", map[string]interface{}{"id": "missing"}, [][]interface{}{}, true},
						{"null", "PROFILE MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id RETURN r.id,a.id,b.id", map[string]interface{}{"id": nil}, [][]interface{}{}, false},
						{"or fallback", "PROFILE MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id OR r.id='x0' RETURN r.id,a.id,b.id ORDER BY r.id", map[string]interface{}{"id": "r289999"}, [][]interface{}{{"r289999", "n73", "n128561"}, {"x0", "n73", "n128561"}}, false},
					} {
						t.Run(tc.name, func(t *testing.T) {
							result := run(tc.query, tc.params)
							require.Equal(t, tc.want, result.Rows)
							plan, ok := result.Metadata["plan"].(*ExecutionPlan)
							require.True(t, ok)
							if tc.bounded {
								require.Less(t, plan.TotalDBHits, int64(50), tc.query)
							} else if tc.name == "or fallback" {
								require.Greater(t, plan.TotalDBHits, int64(100), tc.query)
							}
						})
					}
					t.Run("subscript is not an indexed property", func(t *testing.T) {
						_, err := exec.Execute(ctx, "MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id[0] = 'r' RETURN count(r)", nil)
						require.Error(t, err)
						code, _ := nornicerrors.Neo4jStatus(err)
						require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
					})
				})
			}
		})
	}
}

func TestC68RelationshipWherePendingWritesAndOrdinaryDuplicates(t *testing.T) {
	for _, parser := range relationshipEqualityParsers {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			fixture := newRelationshipEqualityAccessFixture(t, relationshipEqualityFixtureOptions{unrelatedRelationships: 128, ordinaryIndex: true})
			exec := fixture.exec
			ctx := beginRelationshipEqualityTransaction(t, exec)
			query := "MATCH (a)-[r:PD_RELATION]->(b) WHERE r.id=$id RETURN r.id,a.id,b.id"
			run := func(id string) [][]interface{} {
				t.Helper()
				result, err := exec.Execute(ctx, query, map[string]interface{}{"id": id})
				require.NoError(t, err)
				return result.Rows
			}
			_, err := exec.Execute(ctx, "CREATE (:PDGraphNode {id: 'txa'})-[:PD_RELATION {id: 'tx-only'}]->(:PDGraphNode {id: 'txb'})", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"tx-only", "txa", "txb"}}, run("tx-only"))
			_, err = exec.Execute(ctx, "MATCH ()-[r:PD_RELATION {id: 'r289999'}]->() SET r.id = 'tx-moved'", nil)
			require.NoError(t, err)
			require.Empty(t, run("r289999"))
			require.Equal(t, [][]interface{}{{"tx-moved", "n73", "n128561"}}, run("tx-moved"))
			_, err = exec.Execute(ctx, "CREATE (:PDGraphNode {id: 'dupa'})-[:PD_RELATION {id: 'tx-moved'}]->(:PDGraphNode {id: 'dupb'})", nil)
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{"tx-moved", "n73", "n128561"}, {"tx-moved", "dupa", "dupb"}}, run("tx-moved"))
		})
	}
}
