package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeEngine_Deduplication_GetNodesByLabel(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create different nodes with the same local ID in both constituents.
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node1) // Same ID

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get nodes by label - preserve both databases.
	nodes, err := composite.GetNodesByLabel("Person")
	require.NoError(t, err)
	// Equal stored IDs in distinct databases identify two nodes.
	assert.Equal(t, 2, len(nodes))
	assert.Equal(t, node1.ID, nodes[0].ID)
}

func TestCompositeEngine_GetNodesByLabelPropagatesConstituentError(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	readErr := errors.New("constituent label scan failed")
	composite := NewCompositeEngine(
		map[string]Engine{"failed": failingLabelScanEngine{Engine: backing, err: readErr}},
		nil,
		map[string]string{"failed": "read"},
	)
	nodes, err := composite.GetNodesByLabel("Evidence")
	require.ErrorIs(t, err, readErr)
	require.Nil(t, nodes)
}

func TestCompositeEngine_StreamNodesByLabelProjected(t *testing.T) {
	first := newTestMemoryEngine(t)
	second := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	t.Cleanup(func() { require.NoError(t, second.Close()) })
	shared := NodeID(prefixTestID("shared-projected"))
	unique := NodeID(prefixTestID("unique-projected"))
	for _, engine := range []*MemoryEngine{first, second} {
		_, err := engine.CreateNode(&Node{ID: shared, Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": "shared", "unused": "hidden"}})
		require.NoError(t, err)
	}
	_, err := second.CreateNode(&Node{ID: unique, Labels: []string{"Evidence"}, Properties: map[string]any{"asset_id": "unique", "unused": "hidden"}})
	require.NoError(t, err)
	composite := NewCompositeEngine(
		map[string]Engine{"first": first, "second": second}, nil,
		map[string]string{"first": "read", "second": "read"},
	)
	seen := make(map[NodeID]map[string]any)
	err = composite.StreamNodesByLabelProjected("Evidence", []string{"asset_id"}, func(node *Node) error {
		seen[node.ID] = node.Properties
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[NodeID]map[string]any{
		shared: {"asset_id": "shared"},
		unique: {"asset_id": "unique"},
	}, seen)

	visited := 0
	err = composite.StreamNodesByLabelProjected("Evidence", nil, func(*Node) error {
		visited++
		return ErrIterationStopped
	})
	require.ErrorIs(t, err, ErrIterationStopped)
	require.Equal(t, 1, visited)
}

func TestCompositeEngine_StreamNodesByLabelProjectedUnsupportedConstituent(t *testing.T) {
	backing := newTestMemoryEngine(t)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	unsupported := struct{ Engine }{backing}
	composite := NewCompositeEngine(
		map[string]Engine{"supported": backing, "unsupported": unsupported}, nil,
		map[string]string{"supported": "read", "unsupported": "read"},
	)
	visited := false
	err := composite.StreamNodesByLabelProjected("Evidence", nil, func(*Node) error {
		visited = true
		return nil
	})
	require.ErrorIs(t, err, ErrNotImplemented)
	require.False(t, visited)
}

func TestCompositeEngine_Deduplication_GetEdgesByType(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node1) // Same nodes
	engine2.CreateNode(node2)
	engine2.CreateEdge(edge1) // Same edge ID

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get edges by type - preserve both databases.
	edges, err := composite.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	// Equal stored IDs in distinct databases identify two relationships.
	assert.Equal(t, 2, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_AllNodes(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create same node in both constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node1) // Same ID

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get all nodes - preserve both databases.
	nodes, err := composite.AllNodes()
	require.NoError(t, err)
	// Equal stored IDs in distinct databases identify two nodes.
	assert.Equal(t, 2, len(nodes))
	assert.Equal(t, node1.ID, nodes[0].ID)
}

func TestCompositeEngine_Deduplication_AllEdges(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node1)
	engine2.CreateNode(node2)
	engine2.CreateEdge(edge1) // Same edge ID

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get all edges - preserve both databases.
	edges, err := composite.AllEdges()
	require.NoError(t, err)
	// Equal stored IDs in distinct databases identify two relationships.
	assert.Equal(t, 2, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_GetOutgoingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node1)
	engine2.CreateNode(node2)
	engine2.CreateEdge(edge1) // Same edge ID

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get outgoing edges - preserve both databases.
	edges, err := composite.GetOutgoingEdges(node1.ID)
	require.NoError(t, err)
	// Equal stored IDs in distinct databases identify two relationships.
	assert.Equal(t, 2, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_GetIncomingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node1)
	engine2.CreateNode(node2)
	engine2.CreateEdge(edge1) // Same edge ID

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get incoming edges - preserve both databases.
	edges, err := composite.GetIncomingEdges(node2.ID)
	require.NoError(t, err)
	// Equal stored IDs in distinct databases identify two relationships.
	assert.Equal(t, 2, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_GetEdgesBetween(t *testing.T) {
	// Create constituent engines
	engine1 := newTestMemoryEngine(t)
	engine2 := newTestMemoryEngine(t)

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node1)
	engine2.CreateNode(node2)
	engine2.CreateEdge(edge1) // Same edge ID

	// Create composite engine
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read_write",
		"db2": "read_write",
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Get edges between - preserve both databases.
	edges, err := composite.GetEdgesBetween(node1.ID, node2.ID)
	require.NoError(t, err)
	// Equal stored IDs in distinct databases identify two relationships.
	assert.Equal(t, 2, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}
