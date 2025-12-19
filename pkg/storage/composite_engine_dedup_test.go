package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeEngine_Deduplication_GetNodesByLabel(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create same node in both constituents (same ID)
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
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

	// Get nodes by label - should deduplicate
	nodes, err := composite.GetNodesByLabel("Person")
	require.NoError(t, err)
	// Should only return one node, not two
	assert.Equal(t, 1, len(nodes))
	assert.Equal(t, node1.ID, nodes[0].ID)
}

func TestCompositeEngine_Deduplication_GetEdgesByType(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("node2"), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID("edge1"), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
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

	// Get edges by type - should deduplicate
	edges, err := composite.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	// Should only return one edge, not two
	assert.Equal(t, 1, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_AllNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create same node in both constituents
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
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

	// Get all nodes - should deduplicate
	nodes, err := composite.AllNodes()
	require.NoError(t, err)
	// Should only return one node, not two
	assert.Equal(t, 1, len(nodes))
	assert.Equal(t, node1.ID, nodes[0].ID)
}

func TestCompositeEngine_Deduplication_AllEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("node2"), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID("edge1"), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
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

	// Get all edges - should deduplicate
	edges, err := composite.AllEdges()
	require.NoError(t, err)
	// Should only return one edge, not two
	assert.Equal(t, 1, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_GetOutgoingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("node2"), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID("edge1"), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
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

	// Get outgoing edges - should deduplicate
	edges, err := composite.GetOutgoingEdges(node1.ID)
	require.NoError(t, err)
	// Should only return one edge, not two
	assert.Equal(t, 1, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_GetIncomingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("node2"), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID("edge1"), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
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

	// Get incoming edges - should deduplicate
	edges, err := composite.GetIncomingEdges(node2.ID)
	require.NoError(t, err)
	// Should only return one edge, not two
	assert.Equal(t, 1, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_Deduplication_GetEdgesBetween(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and same edge in both constituents
	node1 := &Node{ID: NodeID("node1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("node2"), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID("edge1"), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
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

	// Get edges between - should deduplicate
	edges, err := composite.GetEdgesBetween(node1.ID, node2.ID)
	require.NoError(t, err)
	// Should only return one edge, not two
	assert.Equal(t, 1, len(edges))
	assert.Equal(t, edge1.ID, edges[0].ID)
}
