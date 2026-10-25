package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFailedCypherMutationPreservesVersionTime(t *testing.T) {
	old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	want := errors.New("storage rejected mutation")
	store := &updateErrorEngine{nodeErr: want, edgeErr: want}
	node := &storage.Node{CreatedAt: old, UpdatedAt: old}
	edge := &storage.Edge{CreatedAt: old, UpdatedAt: old}
	require.ErrorIs(t, updateMutatedNode(context.Background(), store, node), want)
	require.Equal(t, old, node.UpdatedAt)
	require.Equal(t, old, node.CreatedAt)
	require.ErrorIs(t, updateMutatedEdge(store, edge), want)
	require.Equal(t, old, edge.UpdatedAt)
	require.Equal(t, old, edge.CreatedAt)
}

func TestCypherMutationUpdatesVersionTime(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, explicit := range []bool{false, true} {
				for _, tc := range []struct {
					query           string
					edge, unchanged bool
				}{
					{"MATCH (n:Clock) SET n.title = 'v2'", false, false},
					{"MATCH (n:Clock) CALL db.create.setNodeVectorProperty(n, 'emb', [2.0,3.0])", false, false},
					{"MATCH ()-[r:VERSION]->() CALL db.create.setRelationshipVectorProperty(r, 'emb', [2.0,3.0])", true, false},
					{"MATCH (n:Clock) CALL db.create.setNodeVectorProperty(n, 'emb', [1.0,2.0])", false, true},
					{"MATCH ()-[r:VERSION]->() CALL db.create.setRelationshipVectorProperty(r, 'emb', [1.0,2.0])", true, true},
					{"MATCH (n:Clock) SET n += {title: 'v2'}", false, false},
					{"MATCH (n:Clock) SET n = {title: 'v2'}", false, false},
					{"MATCH (n:Clock) SET n:Changed", false, false},
					{"MATCH (n:Clock) REMOVE n.title", false, false},
					{"MATCH (n:Clock) REMOVE n:Old", false, false},
					{"MERGE (n:Clock) ON MATCH SET n.title = 'v2'", false, false},
					{"MATCH ()-[r:VERSION]->() SET r.v = 2", true, false},
					{"MATCH ()-[r:VERSION]->() SET r = {v: 2}", true, false},
					{"MATCH ()-[r:VERSION]->() SET r += {v: 2}", true, false},
					{"MATCH ()-[r:VERSION]->() REMOVE r.v", true, false},
					{"MATCH (n:Clock) SET n.title = 'v1'", false, true},
					{"MATCH (n:Clock) REMOVE n.missing", false, true},
					{"MATCH (n:Clock) REMOVE n:Missing", false, true},
					{"MATCH ()-[r:VERSION]->() REMOVE r.missing", true, true},
				} {
					t.Run(tc.query+map[bool]string{true: "/explicit", false: "/auto"}[explicit], func(t *testing.T) {
						ctx := context.Background()
						store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "clock")
						old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
						for _, node := range []*storage.Node{
							{ID: "a", Labels: []string{"Clock", "Old"}, Properties: map[string]interface{}{"title": "v1", "emb": []float64{1, 2}}, CreatedAt: old, UpdatedAt: old},
							{ID: "b", Labels: []string{"Other"}, Properties: map[string]interface{}{}, CreatedAt: old, UpdatedAt: old},
						} {
							_, err := store.CreateNode(node)
							require.NoError(t, err)
						}
						require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "VERSION", Properties: map[string]interface{}{"v": int64(1), "emb": []float64{1, 2}}, CreatedAt: old, UpdatedAt: old}))
						exec := NewStorageExecutor(store)
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
						}
						before := time.Now()
						_, err := exec.Execute(ctx, tc.query, nil)
						require.NoError(t, err)
						if explicit {
							_, err = exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						var created, updated time.Time
						if tc.edge {
							edge, err := store.GetEdge("e")
							require.NoError(t, err)
							created, updated = edge.CreatedAt, edge.UpdatedAt
						} else {
							node, err := store.GetNode("a")
							require.NoError(t, err)
							created, updated = node.CreatedAt, node.UpdatedAt
						}
						require.True(t, created.Equal(old), "mutation must preserve creation time")
						if tc.unchanged {
							require.True(t, updated.Equal(old), "a no-op must preserve version time")
						} else {
							require.False(t, updated.Before(before), "successful mutation must advance version time")
							require.False(t, updated.After(time.Now()), "version time must describe this mutation")
						}
					})
				}
			}
		})
	}
}
