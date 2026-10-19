package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQueryMemoryBoltPropertyVisibility(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()
	schema := be.GetSchemaForNamespace("review")
	for _, edge := range []bool{false, true} {
		name, scope := "doc", knowledgepolicy.ScopeNode
		if edge {
			name, scope = "link", knowledgepolicy.ScopeEdge
		}
		require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
		binding := knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "secret", HalfLifeSeconds: 1}}}
		if edge {
			binding.IsEdge = true
			binding.TargetEdgeType = "LINK"
		} else {
			binding.TargetLabels = []string{"Doc"}
		}
		require.NoError(t, schema.CreateDecayProfileBinding(binding))
	}
	be.SetDecayEnabled(true)
	old := time.Now().Add(-time.Hour)
	for _, id := range []storage.NodeID{"review:a", "review:b"} {
		_, err = be.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"secret": "node-hidden", "title": string(id)}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	require.NoError(t, be.CreateEdge(&storage.Edge{ID: "review:r", StartNode: "review:a", EndNode: "review:b", Type: "LINK", Properties: map[string]interface{}{"secret": "edge-hidden", "weight": int64(7)}, CreatedAt: old, UpdatedAt: old}))
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(be, "review"))
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	controls := map[string][]string{}
	for _, explicit := range []bool{true, false} {
		for _, projection := range []string{"a", "r", "p", "{n:a,r:r,p:p}", "[a,r,p]"} {
			t.Run(fmt.Sprintf("tx=%v/%s", explicit, projection), func(t *testing.T) {
				session := driver.NewSession(ctx, neo4j.SessionConfig{FetchSize: 1})
				defer session.Close(ctx)
				query := "UNWIND range(1,1200) AS i MATCH p=(a:Doc)-[r:LINK]->(b) RETURN " + projection
				var result neo4j.ResultWithContext
				if explicit {
					tx, err := session.BeginTransaction(ctx)
					require.NoError(t, err)
					defer tx.Rollback(ctx)
					result, err = tx.Run(ctx, query, nil)
					require.NoError(t, err)
				} else {
					result, err = session.Run(ctx, query, nil)
					require.NoError(t, err)
				}
				var walk func(interface{})
				walk = func(value interface{}) {
					switch v := value.(type) {
					case neo4j.Node:
						require.NotContains(t, v.Props, "secret")
					case neo4j.Relationship:
						require.NotContains(t, v.Props, "secret")
					case neo4j.Path:
						for _, n := range v.Nodes {
							walk(n)
						}
						for _, r := range v.Relationships {
							walk(r)
						}
					case map[string]interface{}:
						for _, x := range v {
							walk(x)
						}
					case []interface{}:
						for _, x := range v {
							walk(x)
						}
					}
				}
				var rows []string
				for result.Next(ctx) {
					walk(result.Record().Values)
					encoded, err := json.Marshal(result.Record().Values)
					require.NoError(t, err)
					rows = append(rows, string(encoded))
				}
				require.NoError(t, result.Err())
				require.Len(t, rows, 1200, "consume beyond Bolt's actual 1000-row streaming threshold")
				if explicit {
					controls[projection] = rows
				} else {
					require.Equal(t, controls[projection], rows, "wire-streamed and explicit materialized outputs")
				}
			})
		}
	}
	raw, err := be.GetQueryNode("review:a")
	require.NoError(t, err)
	require.Contains(t, raw.Properties, "secret")
	edge, err := be.GetEdge("review:r")
	require.NoError(t, err)
	require.Contains(t, edge.Properties, "secret")
}
