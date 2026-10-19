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
		physical, err := base.GetNode(NodeID(database + ":same"))
		require.NoError(t, err)
		require.Empty(t, physical.Database, "query provenance must not mutate the underlying snapshot")
	}
}

func TestCompositeScansKeepConstituentIdentities(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	one, two := NewNamespacedEngine(base, "one"), NewNamespacedEngine(base, "two")
	for _, store := range []*NamespacedEngine{one, two} {
		for _, id := range []NodeID{"same", "target"} {
			_, err := store.CreateNode(&Node{ID: id, Labels: []string{"P"}})
			require.NoError(t, err)
		}
		require.NoError(t, store.CreateEdge(&Edge{ID: "rel", StartNode: "same", EndNode: "target", Type: "R"}))
	}
	composite := NewCompositeEngine(
		map[string]Engine{"one": one, "alias": one, "two": two},
		map[string]string{"one": "one", "alias": "one", "two": "two"},
		map[string]string{"one": "read", "alias": "read", "two": "read"},
	)
	nodes, err := composite.AllNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 4, "aliases deduplicate, distinct databases do not")
	nodes, err = composite.GetNodesByLabel("P")
	require.NoError(t, err)
	require.Len(t, nodes, 4)
	edges, err := composite.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 2)
	var streamed []*Node
	require.NoError(t, composite.StreamNodesByLabelProjected("P", nil, func(node *Node) error {
		streamed = append(streamed, node)
		return nil
	}))
	require.Len(t, streamed, 4)
}

func TestEntityProvenanceDoesNotChangeStoredFormat(t *testing.T) {
	for _, value := range []interface{}{
		&Node{ID: "test:n", Database: "source"},
		&Edge{ID: "test:r", Database: "source", StartNode: "test:n", EndNode: "test:m"},
	} {
		encoded, err := encodeValue(value)
		require.NoError(t, err)
		switch value.(type) {
		case *Node:
			var decoded Node
			require.NoError(t, decodeValue(encoded, &decoded))
			require.Empty(t, decoded.Database)
		case *Edge:
			var decoded Edge
			require.NoError(t, decodeValue(encoded, &decoded))
			require.Empty(t, decoded.Database)
		}
	}
}
