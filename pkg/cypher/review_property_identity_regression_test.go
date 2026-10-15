package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func reviewPropertyExecutor(t *testing.T) (*StorageExecutor, *storage.BadgerEngine) {
	t.Helper()
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { be.Close() })
	schema := be.GetSchemaForNamespace("review")
	for _, edge := range []bool{false, true} {
		name, scope, prop := "doc", knowledgepolicy.ScopeNode, "body"
		if edge {
			name, scope, prop = "link", knowledgepolicy.ScopeEdge, "summary"
		}
		require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
		binding := knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: prop, HalfLifeSeconds: 1}}}
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
	for _, id := range []string{"a", "b", "c"} {
		_, err = be.CreateNode(&storage.Node{ID: storage.NodeID("review:" + id), Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": id, "body": "hidden"}, CreatedAt: old, UpdatedAt: old})
		require.NoError(t, err)
	}
	for _, ids := range [][3]string{{"ab", "a", "b"}, {"bc", "b", "c"}} {
		require.NoError(t, be.CreateEdge(&storage.Edge{ID: storage.EdgeID("review:" + ids[0]), StartNode: storage.NodeID("review:" + ids[1]), EndNode: storage.NodeID("review:" + ids[2]), Type: "LINK", Properties: map[string]interface{}{"summary": "hidden"}, CreatedAt: old, UpdatedAt: old}))
	}
	return NewStorageExecutor(storage.NewNamespacedEngine(be, "review")), be
}
func TestReviewWholePathMergeUsesStoredIdentity(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "auto", true: "explicit"}[explicit], func(t *testing.T) {
			e, be := reviewPropertyExecutor(t)
			ctx := context.Background()
			if explicit {
				_, err := e.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer e.Execute(ctx, "ROLLBACK", nil)
			}
			result, err := e.Execute(ctx, "MERGE (a:Doc {title:'a',body:'hidden'})-[r:LINK {summary:'hidden'}]->(b:Doc {title:'b',body:'hidden'})-[s:LINK {summary:'hidden'}]->(c:Doc {title:'c',body:'hidden'}) RETURN a.body,r.summary,c.body", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{nil, nil, nil}}, result.Rows)
			if result.Stats != nil {
				require.Zero(t, result.Stats.NodesCreated)
				require.Zero(t, result.Stats.RelationshipsCreated)
			}
			if explicit {
				_, err = e.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			nodes, err := be.AllNodes()
			require.NoError(t, err)
			require.Len(t, nodes, 3)
			edges, err := be.AllEdges()
			require.NoError(t, err)
			require.Len(t, edges, 2)
			for _, n := range nodes {
				require.Equal(t, "hidden", n.Properties["body"])
			}
			for _, r := range edges {
				require.Equal(t, "hidden", r.Properties["summary"])
			}
			result, err = e.Execute(ctx, "MATCH (a:Doc {body:'hidden'}) RETURN count(a)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
		})
	}
}
func TestReviewSortNodesUsesVisibleProperties(t *testing.T) {
	e, be := reviewPropertyExecutor(t)
	for id, value := range map[storage.NodeID]int64{"review:a": 2, "review:b": 1} {
		n, err := be.GetNode(id)
		require.NoError(t, err)
		n.Properties["body"] = value
		require.NoError(t, be.UpdateNode(n))
	}
	result, err := e.Execute(context.Background(), "MATCH (a:Doc {title:'a'}),(b:Doc {title:'b'}) RETURN [a.body,b.body] AS hidden,[n IN apoc.coll.sortNodes([a,b], 'body') | n.title] AS titles", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{nil, nil}, []interface{}{"a", "b"}}}, result.Rows)
	_, err = e.Execute(context.Background(), "MATCH (a:Doc {title:'a'}),(b:Doc {title:'b'}) WITH apoc.coll.sortNodes([a,b], 'body') AS xs UNWIND xs AS n SET n.extra=1 RETURN n.body", nil)
	require.NoError(t, err)
	raw, err := be.GetNode("review:a")
	require.NoError(t, err)
	require.EqualValues(t, 2, raw.Properties["body"])

}
