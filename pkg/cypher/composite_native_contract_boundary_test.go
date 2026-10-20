package cypher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeNativeProcedureSourceBoundary(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, test := range []struct {
			name, query string
			edge        bool
		}{
			{"node vector", "CALL db.create.setNodeVectorProperty($entity,'sourceVector',[1.0,0.0])", false},
			{"relationship vector", "CALL db.create.setRelationshipVectorProperty($entity,'sourceVector',[1.0,0.0])", true},
			{"node suppression", "CALL kp.suppress($entity)", false},
			{"relationship suppression", "CALL kp.suppress($entity)", true},
		} {
			for _, foreign := range []bool{false, true} {
				t.Run(test.name+"/"+map[bool]string{false: "auto", true: "explicit"}[explicit]+"/"+map[bool]string{false: "own", true: "foreign"}[foreign], func(t *testing.T) {
					_, manager := newCompositeIdentityExecutor(t, false)
					own, err := manager.GetStorage("one")
					require.NoError(t, err)
					source := own
					if foreign {
						source, err = manager.GetStorage("two")
						require.NoError(t, err)
					}
					var entity interface{}
					if test.edge {
						entity, err = source.GetEdge("same-rel")
					} else {
						entity, err = source.GetNode("same")
					}
					require.NoError(t, err)
					executor := NewStorageExecutor(own)
					executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
					ctx := context.Background()
					if explicit {
						_, err = executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
					}
					_, err = executor.Execute(ctx, test.query, map[string]interface{}{"entity": entity})
					if foreign {
						require.Error(t, err, "a foreign native entity must not select a same-local-ID mutation target")
					} else {
						require.NoError(t, err)
					}
					if foreign && !explicit {
						if test.edge {
							edge, err := own.GetEdge("same-rel")
							require.NoError(t, err)
							require.NotContains(t, edge.Properties, "sourceVector")
						} else {
							node, err := own.GetNode("same")
							require.NoError(t, err)
							require.NotContains(t, node.Properties, "sourceVector")
						}
					}
				})
			}
		}
	}
}

func TestNativePropertyProjectionSourceBoundary(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			_, manager := newCompositeIdentityExecutor(t, false)
			own, err := manager.GetStorage("one")
			require.NoError(t, err)
			foreign, err := manager.GetStorage("two")
			require.NoError(t, err)
			profile := knowledgepolicy.DecayProfileBundle{Name: "sourceProfile", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}
			require.NoError(t, own.GetSchema().CreateDecayProfileBundle(profile))
			require.NoError(t, own.GetSchema().CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "sourceBinding", ProfileRef: profile.Name, TargetLabels: []string{"P"}, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}}}))
			backend, ok := storage.UnwrapEngine(own).(*storage.BadgerEngine)
			require.True(t, ok)
			backend.SetDecayEnabled(true)
			for _, store := range []storage.Engine{own, foreign} {
				node, err := store.GetNode("same")
				require.NoError(t, err)
				node.CreatedAt = time.Now().Add(-time.Hour)
				node.UpdatedAt = node.CreatedAt
				node.Properties["secret"] = "visible at source"
				require.NoError(t, store.UpdateNode(node))
			}
			executor := NewStorageExecutor(own)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			if explicit {
				_, err = executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, test := range []struct {
				store    storage.Engine
				secret   interface{}
				database string
			}{{own, nil, "one"}, {foreign, "visible at source", "two"}} {
				node, err := test.store.GetNode("same")
				require.NoError(t, err)
				result, err := executor.Execute(ctx, "WITH $entity AS n RETURN n.secret,n,id(n),elementId(n),apoc.convert.toJson(n)", map[string]interface{}{"entity": node})
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				row := result.Rows[0]
				require.Equal(t, test.secret, row[0])
				output, ok := row[1].(*storage.Node)
				require.True(t, ok)
				require.Equal(t, node.Identity(), output.Identity())
				require.Equal(t, "same", row[2])
				require.Equal(t, "4:"+test.database+":same", row[3])
				var serialized map[string]interface{}
				require.NoError(t, json.Unmarshal([]byte(row[4].(string)), &serialized))
				require.Equal(t, "same", serialized["id"])
				require.Equal(t, "visible at source", node.Properties["secret"], "query projection cannot mutate the source snapshot")
			}
		})
	}
}

func TestCompositeNativeCorrelatedDegreeSourceBoundary(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			_, manager := newCompositeIdentityExecutor(t, false)
			own, err := manager.GetStorage("one")
			require.NoError(t, err)
			foreign, err := manager.GetStorage("two")
			require.NoError(t, err)
			executor := NewStorageExecutor(own)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			if explicit {
				_, err = executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, source := range []struct {
				store storage.Engine
				count int64
			}{{own, 1}, {foreign, 0}} {
				node, err := source.store.GetNode("same")
				require.NoError(t, err)
				for _, expression := range []string{"COUNT { MATCH (n)-[:R]->() }"} {
					result, err := executor.Execute(ctx, "WITH $entity AS n RETURN "+expression, map[string]interface{}{"entity": node})
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{source.count}}, result.Rows, expression)
				}
			}
		})
	}
}
