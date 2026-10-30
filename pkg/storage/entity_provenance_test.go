package storage

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespacedEntityProvenance(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, NewMemoryEngine(), ErrStorageClosed)
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
		require.Equal(t, database, node.RuntimeMetadata().Database)
		require.Equal(t, database, edge.Database)
		require.Equal(t, "4:"+database+":same", CopyNode(node).ElementID("comp"))
		require.Equal(t, "5:"+database+":rel", CopyEdge(edge).ElementID("comp"))
		require.Equal(t, "4:"+database+":same", edge.StartElementID("comp"))
		require.Equal(t, "4:"+database+":target", edge.EndElementID("comp"))
		batch, err := store.BatchGetNodes([]NodeID{"same"})
		require.NoError(t, err)
		require.Equal(t, database, batch["same"].RuntimeMetadata().Database)
		physical, err := base.GetNode(NodeID(database + ":same"))
		require.NoError(t, err)
		require.Empty(t, physical.RuntimeMetadata().Database, "query provenance must not mutate the underlying snapshot")
	}
}

func TestCompositeScansKeepConstituentIdentities(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, NewMemoryEngine(), ErrStorageClosed)
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
	streamedEdges := 0
	require.NoError(t, composite.StreamEdgesByType(context.Background(), "R", func(edge *Edge) error {
		streamedEdges++
		require.NotEmpty(t, edge.Database)
		return nil
	}))
	require.Equal(t, 2, streamedEdges)
	var ids []NodeID
	require.NoError(t, composite.ForEachNodeIDByLabel("P", func(id NodeID) bool {
		ids = append(ids, id)
		node, err := composite.GetNode(id)
		require.NoError(t, err)
		require.NotEmpty(t, node.RuntimeMetadata().Database)
		return true
	}))
	require.ElementsMatch(t, []NodeID{"4:one:same", "4:one:target", "4:two:same", "4:two:target"}, ids)
	for _, database := range []string{"one", "two"} {
		node, err := composite.GetNode(NodeID("4:" + database + ":same"))
		require.NoError(t, err)
		require.Equal(t, database, node.RuntimeMetadata().Database)
		node, err = composite.GetNodeProjected(NodeID("4:"+database+":same"), nil)
		require.NoError(t, err)
		require.Equal(t, database, node.RuntimeMetadata().Database)
		edge, err := composite.GetEdge(EdgeID("5:" + database + ":rel"))
		require.NoError(t, err)
		require.Equal(t, database, edge.Database)
	}
	_, err = composite.GetNode("4:missing:same")
	require.ErrorIs(t, err, ErrNotFound)
}

type provenanceStreamEngine struct {
	Engine
	count    int
	database string
	ids      []EdgeID
}

func (s *provenanceStreamEngine) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	edge := &Edge{Database: s.database, Type: edgeType}
	for index := 0; index < s.count; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		edge.ID = s.ids[index]
		if err := visit(edge); err != nil {
			return err
		}
	}
	return nil
}

func TestCompositeIdentityStreamKeepsBoundedMemory(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, NewMemoryEngine(), ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	one := &provenanceStreamEngine{Engine: base, database: "one"}
	two := &provenanceStreamEngine{Engine: base, database: "two"}
	ids := make([]EdgeID, 10000)
	for index := range ids {
		ids[index] = EdgeID(strconv.Itoa(index))
	}
	one.ids, two.ids = ids, ids
	composite := NewCompositeEngine(
		map[string]Engine{"one": one, "alias": one, "two": two}, nil,
		map[string]string{"one": "read", "alias": "read", "two": "read"},
	)
	run := func(count int) float64 {
		one.count, two.count = count, count
		visits := 0
		allocations := testing.AllocsPerRun(2, func() {
			visits = 0
			require.NoError(t, composite.StreamEdgesByType(context.Background(), "R", func(*Edge) error {
				visits++
				return nil
			}))
		})
		require.Equal(t, 2*count, visits)
		return allocations
	}
	small := run(50)
	large := run(10000)
	require.LessOrEqual(t, large, small+16, "stream bookkeeping must scale with constituents, not entities")
}

func TestEntityProvenanceDoesNotChangeStoredFormat(t *testing.T) {
	for _, value := range []interface{}{
		&Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{Database: "source"}}, ID: "test:n"},
		&Edge{ID: "test:r", Database: "source", StartNode: "test:n", EndNode: "test:m"},
	} {
		encoded, err := encodeValue(value)
		require.NoError(t, err)
		switch value.(type) {
		case *Node:
			var decoded Node
			require.NoError(t, decodeValue(encoded, &decoded))
			require.Empty(t, decoded.RuntimeMetadata().Database)
		case *Edge:
			var decoded Edge
			require.NoError(t, decodeValue(encoded, &decoded))
			require.Empty(t, decoded.Database)
		}
	}
}
