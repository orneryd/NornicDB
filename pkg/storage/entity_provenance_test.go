package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespacedEntityProvenance(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	for _, database := range []string{"one", "two"} {
		store := NewNamespacedEngine(base, database)
		for _, id := range []NodeID{"same", "target"} {
			_, err := store.CreateNode(&Node{ID: id, Labels: []string{"P"}})
			require.NoError(t, err)
		}
		require.NoError(t, store.CreateEdge(&Edge{ID: "rel", StartNode: "same", EndNode: "target", Type: "R"}))
		node, err := store.GetNode("same")
		require.NoError(t, err)
		edge, err := store.GetEdge("rel")
		require.NoError(t, err)
		require.Equal(t, database, node.Database)
		require.Equal(t, database, edge.Database)
		require.Equal(t, "4:"+database+":same", CopyNode(node).ElementID("comp"))
		require.Equal(t, "5:"+database+":rel", CopyEdge(edge).ElementID("comp"))
		require.Equal(t, "4:"+database+":same", edge.StartElementID("comp"))
		require.Equal(t, "4:"+database+":target", edge.EndElementID("comp"))
		batch, err := store.BatchGetNodes([]NodeID{"same"})
		require.NoError(t, err)
		require.Equal(t, database, batch["same"].Database)
		physical, err := base.GetNode(NodeID(database+":same"))
		require.NoError(t, err)
		require.Empty(t, physical.Database, "query provenance must not mutate the underlying snapshot")
	}
}
