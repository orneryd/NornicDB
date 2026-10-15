package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestKnowledgePolicyUsesScopedAccessMetadata(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, database := range []string{"nornic", "tenant"} {
				for _, id := range []string{"plain", "a:b"} {
					t.Run(database+"/"+id, func(t *testing.T) {
						base := newTestMemoryEngine(t).BadgerEngine
						base.SetDecayEnabled(true)
						for _, ns := range []string{"nornic", "tenant"} {
							nodeProfile, edgeProfile := ns+"_doc", ns+"_links"
							bundles := map[string]*knowledgepolicy.DecayProfileBundle{}
							for name, scope := range map[string]knowledgepolicy.ScopeType{nodeProfile: knowledgepolicy.ScopeNode, edgeProfile: knowledgepolicy.ScopeEdge} {
								bundles[name] = &knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromLastAccessed, Enabled: true, DecayEnabled: true}
							}
							bindings := map[string]*knowledgepolicy.DecayProfileBinding{
								"doc":   {Name: "doc", TargetLabels: []string{"Doc"}, ProfileRef: nodeProfile},
								"links": {Name: "links", IsEdge: true, TargetEdgeType: "LINKS", ProfileRef: edgeProfile},
							}
							table, err := knowledgepolicy.BuildBindingTable(bundles, bindings, nil, nil)
							require.NoError(t, err)
							base.GetSchemaForNamespace(ns).SetBindingTable(table)
						}
						store := storage.NewNamespacedEngine(base, database)
						now := time.Now()
						old := now.Add(-72 * time.Hour)
						for _, node := range []*storage.Node{
							{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]interface{}{"body": "visible"}, CreatedAt: old, UpdatedAt: old},
							{ID: "end", Labels: []string{"Other"}, Properties: map[string]interface{}{}, CreatedAt: old, UpdatedAt: old},
						} {
							_, err := store.CreateNode(node)
							require.NoError(t, err)
						}
						require.NoError(t, store.CreateEdge(&storage.Edge{ID: "edge:1", StartNode: storage.NodeID(id), EndNode: "end", Type: "LINKS", Properties: map[string]interface{}{"summary": "visible"}, CreatedAt: old, UpdatedAt: old}))
						for entity, scope := range map[string]knowledgepolicy.ScopeType{id: knowledgepolicy.ScopeNode, "edge:1": knowledgepolicy.ScopeEdge} {
							storedID := database + ":" + entity
							require.NoError(t, base.PutAccessMeta(storedID, &knowledgepolicy.AccessMetaEntry{TargetID: storedID, TargetScope: scope, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 3, LastAccessedAt: now.UnixNano()}}))
						}
						exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
						ctx := context.Background()
						for _, explicit := range []bool{false, true} {
							if explicit {
								_, err := exec.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
							}
							result, err := exec.Execute(ctx, "MATCH (n:Doc)-[r:LINKS]->() RETURN decay(n).policy AS nodePolicy, decayScore(n) AS nodeScore, policy(n).accessCount AS nodeAccess, decay(r).policy AS edgePolicy, decayScore(r) AS edgeScore, policy(r).accessCount AS edgeAccess", nil)
							require.NoError(t, err)
							require.Len(t, result.Rows, 1)
							row := result.Rows[0]
							require.Equal(t, database+"_doc", row[0])
							require.InDelta(t, 1.0, row[1], 0.01)
							require.EqualValues(t, 3, row[2])
							require.Equal(t, database+"_links", row[3])
							require.InDelta(t, 1.0, row[4], 0.01)
							require.EqualValues(t, 3, row[5])
							if explicit {
								_, err = exec.Execute(ctx, "ROLLBACK", nil)
								require.NoError(t, err)
							}
						}
						for _, entity := range []string{id, "edge:1"} {
							result, err := exec.callNornicDbKnowledgePolicyResolve([]interface{}{entity})
							require.NoError(t, err)
							require.InDelta(t, 1.0, result.Rows[0][12], 0.01)
							require.Equal(t, false, result.Rows[0][14])
						}
						plain, err := store.GetNode(storage.NodeID(id))
						require.NoError(t, err)
						require.Equal(t, "visible", exec.nodeToMap(plain)["body"], "property filter must use the same access record")
						edge, err := store.GetEdge("edge:1")
						require.NoError(t, err)
						require.Equal(t, "visible", exec.decayVisibleEdgeProperties(edge)["summary"])
					})
				}
			}
		})
	}
}
