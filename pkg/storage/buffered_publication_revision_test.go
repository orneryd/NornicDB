package storage

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestBufferedPublicationCallbacksKeepAcknowledgedRevision(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	var mu sync.Mutex
	calls := map[string]int{}
	record := func(name string) { mu.Lock(); calls[name]++; mu.Unlock() }
	engine.OnNodeCreated(func(*Node) { record("node create") })
	engine.OnNodeUpdated(func(*Node) { record("node update") })
	engine.OnNodeDeleted(func(NodeID) { record("node delete") })
	engine.OnEdgeCreated(func(*Edge) { record("edge create") })
	engine.OnEdgeUpdated(func(*Edge) { record("edge update") })
	engine.OnEdgeDeleted(func(EdgeID) { record("edge delete") })
	write := func(change func(*Transaction)) {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		change(tx)
		require.NoError(t, tx.Commit())
		acknowledged, _ := engine.GraphMutationVersionInNamespace("test")
		require.NoError(t, engine.FlushWriteBehind())
		persisted, _ := engine.GraphMutationVersionInNamespace("test")
		require.Equal(t, acknowledged, persisted, "durability does not change the logical graph")
	}
	write(func(tx *Transaction) {
		_, err := tx.CreateNode(&Node{ID: "test:a", Properties: map[string]interface{}{"v": int64(1)}})
		require.NoError(t, err)
		_, err = tx.CreateNode(&Node{ID: "test:b"})
		require.NoError(t, err)
		require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "T", Properties: map[string]interface{}{"v": int64(1)}}))
	})
	write(func(tx *Transaction) {
		node, err := tx.GetNode("test:a")
		require.NoError(t, err)
		node.Properties["v"] = int64(2)
		require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
		edge, err := tx.GetEdge("test:e")
		require.NoError(t, err)
		edge.Properties["v"] = int64(2)
		require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: edge}))
	})
	write(func(tx *Transaction) {
		require.NoError(t, tx.DeleteEdge("test:e"))
		require.NoError(t, tx.DeleteGraphNode("test:a", CascadeEdges))
	})
	mu.Lock()
	require.Equal(t, map[string]int{"node create": 2, "node update": 1, "node delete": 1, "edge create": 1, "edge update": 1, "edge delete": 1}, calls)
	mu.Unlock()
	before, _ := engine.GraphMutationVersionInNamespace("test")
	_, err := engine.CreateNode(&Node{ID: "test:direct"})
	require.NoError(t, err)
	after, _ := engine.GraphMutationVersionInNamespace("test")
	require.Greater(t, after, before, "direct writes still publish a new graph revision")
}
