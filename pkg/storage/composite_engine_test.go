package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeEngine_CreateNode(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Create node
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)

	// Node should be in one of the constituents
	found := false
	for _, engine := range []Engine{engine1, engine2} {
		retrieved, err := engine.GetNode(node.ID)
		if err == nil {
			assert.Equal(t, node.ID, retrieved.ID)
			assert.Equal(t, node.Labels, retrieved.Labels)
			found = true
			break
		}
	}
	assert.True(t, found, "Node should be created in one of the constituents")
}

func TestCompositeEngine_GetNode(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in each constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Get node from first constituent
	retrieved, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved.ID)

	// Get node from second constituent
	retrieved, err = composite.GetNode(node2.ID)
	require.NoError(t, err)
	assert.Equal(t, node2.ID, retrieved.ID)

	// Get non-existent node
	_, err = composite.GetNode(NodeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_CreateEdge(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)

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

	// Create edge
	edge := &Edge{
		ID:         EdgeID(prefixTestID("edge1")),
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Type:       "KNOWS",
		Properties: map[string]interface{}{"since": 2020},
	}
	err := composite.CreateEdge(edge)
	require.NoError(t, err)

	// Verify edge exists in first constituent
	retrieved, err := engine1.GetEdge(edge.ID)
	require.NoError(t, err)
	assert.Equal(t, edge.ID, retrieved.ID)
	assert.Equal(t, edge.StartNode, retrieved.StartNode)
	assert.Equal(t, edge.EndNode, retrieved.EndNode)
}

func TestCompositeEngine_BulkCreateNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Create multiple nodes
	nodes := []*Node{
		{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}},
		{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}},
		{ID: NodeID(prefixTestID("node3")), Labels: []string{"Company"}},
	}
	err := composite.BulkCreateNodes(nodes)
	require.NoError(t, err)

	// Verify all nodes exist in one of the constituents
	for _, node := range nodes {
		found := false
		for _, engine := range []Engine{engine1, engine2} {
			_, err := engine.GetNode(node.ID)
			if err == nil {
				found = true
				break
			}
		}
		assert.True(t, found, "Node %s should exist in one of the constituents", node.ID)
	}
}

func TestCompositeEngine_GetSchema(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes with labels to populate schema
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Company"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Product"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)

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

	// Get merged schema
	mergedSchema := composite.GetSchema()

	// Verify schema exists (labels will be populated from nodes)
	assert.NotNil(t, mergedSchema)
}

func TestCompositeEngine_ReadOnlyConstituent(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)

	// Create composite engine with one read-only constituent
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read",       // Read-only
		"db2": "read_write", // Writable
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Can read from read-only constituent
	retrieved, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved.ID)

	// Cannot write to read-only constituent - should route to writable one
	newNode := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	_, err = composite.CreateNode(newNode)
	require.NoError(t, err)

	// New node should be in writable constituent
	_, err = engine2.GetNode(newNode.ID)
	assert.NoError(t, err)
}

func TestCompositeEngine_GetNodeFromMultipleConstituents(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in each constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Get nodes from both constituents
	retrieved1, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, node1.ID, retrieved1.ID)
	assert.Equal(t, "Alice", retrieved1.Properties["name"])

	retrieved2, err := composite.GetNode(node2.ID)
	require.NoError(t, err)
	assert.Equal(t, node2.ID, retrieved2.ID)
	assert.Equal(t, "Bob", retrieved2.Properties["name"])
}

func TestCompositeEngine_UpdateNode(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	engine1.CreateNode(node1)

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

	// Update node
	node1.Properties["age"] = 30
	err := composite.UpdateNode(node1)
	require.NoError(t, err)

	// Verify update
	retrieved, err := composite.GetNode(node1.ID)
	require.NoError(t, err)
	assert.Equal(t, 30, retrieved.Properties["age"])
}

func TestCompositeEngine_DeleteNode(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create node in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)

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

	// Delete node
	err := composite.DeleteNode(node1.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = composite.GetNode(node1.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetEdge(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

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

	// Get edge
	retrieved, err := composite.GetEdge(edge1.ID)
	require.NoError(t, err)
	assert.Equal(t, edge1.ID, retrieved.ID)
	assert.Equal(t, edge1.StartNode, retrieved.StartNode)
	assert.Equal(t, edge1.EndNode, retrieved.EndNode)
}

func TestCompositeEngine_UpdateEdge(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

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

	// Update edge
	edge1.Properties = map[string]interface{}{"since": 2020}
	err := composite.UpdateEdge(edge1)
	require.NoError(t, err)

	// Verify update
	retrieved, err := composite.GetEdge(edge1.ID)
	require.NoError(t, err)
	assert.Equal(t, 2020, retrieved.Properties["since"])
}

func TestCompositeEngine_DeleteEdge(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

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

	// Delete edge
	err := composite.DeleteEdge(edge1.ID)
	require.NoError(t, err)

	// Verify deletion
	_, err = composite.GetEdge(edge1.ID)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetNodesByLabel(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes with same label in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Get nodes by label
	nodes, err := composite.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 2)

	// Verify both nodes are in results
	nodeMap := make(map[NodeID]bool)
	for _, node := range nodes {
		nodeMap[node.ID] = true
	}
	assert.True(t, nodeMap[node1.ID])
	assert.True(t, nodeMap[node2.ID])
}

func TestCompositeEngine_GetFirstNodeByLabel(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes with same label in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Get first node by label
	node, err := composite.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "Person", node.Labels[0])
}

func TestCompositeEngine_GetOutgoingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node1.ID, EndNode: node3.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateEdge(edge2)

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

	// Get outgoing edges
	edges, err := composite.GetOutgoingEdges(node1.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestCompositeEngine_GetIncomingEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

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

	// Get incoming edges
	edges, err := composite.GetIncomingEdges(node2.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_GetEdgesBetween(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

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

	// Get edges between nodes
	edges, err := composite.GetEdgesBetween(node1.ID, node2.ID)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
	assert.Equal(t, edge1.ID, edges[0].ID)
}

func TestCompositeEngine_GetEdgeBetween(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edge
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)

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

	// Get edge between nodes
	edge := composite.GetEdgeBetween(node1.ID, node2.ID, "KNOWS")
	require.NotNil(t, edge)
	assert.Equal(t, edge1.ID, edge.ID)
}

func TestCompositeEngine_GetEdgesByType(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges with same type in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

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

	// Get edges by type
	edges, err := composite.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 2)

	// Verify both edges are in results
	edgeMap := make(map[EdgeID]bool)
	for _, edge := range edges {
		edgeMap[edge.ID] = true
	}
	assert.True(t, edgeMap[edge1.ID])
	assert.True(t, edgeMap[edge2.ID])
}

func TestCompositeEngine_AllNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Get all nodes
	nodes, err := composite.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 2)

	// Verify both nodes are in results
	nodeMap := make(map[NodeID]bool)
	for _, node := range nodes {
		nodeMap[node.ID] = true
	}
	assert.True(t, nodeMap[node1.ID])
	assert.True(t, nodeMap[node2.ID])
}

func TestCompositeEngine_AllEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

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

	// Get all edges
	edges, err := composite.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 2)

	// Verify both edges are in results
	edgeMap := make(map[EdgeID]bool)
	for _, edge := range edges {
		edgeMap[edge.ID] = true
	}
	assert.True(t, edgeMap[edge1.ID])
	assert.True(t, edgeMap[edge2.ID])
}

func TestCompositeEngine_GetAllNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Get all nodes (non-error version)
	nodes := composite.GetAllNodes()
	assert.GreaterOrEqual(t, len(nodes), 2)

	// Verify both nodes are in results
	nodeMap := make(map[NodeID]bool)
	for _, node := range nodes {
		nodeMap[node.ID] = true
	}
	assert.True(t, nodeMap[node1.ID])
	assert.True(t, nodeMap[node2.ID])
}

func TestCompositeEngine_GetInDegree(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges (both edges point to node2)
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node2.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	// For edge2, node3 is in engine2 but node2 is in engine1
	// We need to create node2 in engine2 as well, or create edge2 in engine1
	// Let's create edge2 in engine1 since node2 is there
	engine1.CreateNode(node3)
	engine1.CreateEdge(edge2)

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

	// Get in-degree (should count edges from both constituents)
	degree := composite.GetInDegree(node2.ID)
	assert.GreaterOrEqual(t, degree, 2)
}

func TestCompositeEngine_GetOutDegree(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node1.ID, EndNode: node3.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateEdge(edge2)

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

	// Get out-degree (should count edges from both constituents)
	degree := composite.GetOutDegree(node1.ID)
	assert.GreaterOrEqual(t, degree, 1)
}

func TestCompositeEngine_BulkCreateEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes - put node1 and node2 in engine1, node3 and node4 in engine2
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)

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

	// Create edges - edge1 within engine1, edge2 within engine2
	edges := []*Edge{
		{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"},
		{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"},
	}

	err := composite.BulkCreateEdges(edges)
	require.NoError(t, err)

	// Verify edges exist
	_, err = composite.GetEdge(edges[0].ID)
	require.NoError(t, err)
	_, err = composite.GetEdge(edges[1].ID)
	require.NoError(t, err)
}

func TestCompositeEngine_BulkDeleteNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Delete nodes
	err := composite.BulkDeleteNodes([]NodeID{node1.ID, node2.ID})
	require.NoError(t, err)

	// Verify nodes are deleted
	_, err = composite.GetNode(node1.ID)
	assert.Error(t, err)
	_, err = composite.GetNode(node2.ID)
	assert.Error(t, err)
}

func TestCompositeEngine_BulkDeleteEdges(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

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

	// Delete edges
	err := composite.BulkDeleteEdges([]EdgeID{edge1.ID, edge2.ID})
	require.NoError(t, err)

	// Verify edges are deleted
	_, err = composite.GetEdge(edge1.ID)
	assert.Error(t, err)
	_, err = composite.GetEdge(edge2.ID)
	assert.Error(t, err)
}

func TestCompositeEngine_routeWrite_PropertyBased(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with tenant_id property
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"tenant_id": "tenant_a"},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)

	// Node should be in one of the constituents
	found := false
	for _, engine := range []Engine{engine1, engine2} {
		_, err := engine.GetNode(node.ID)
		if err == nil {
			found = true
			break
		}
	}
	assert.True(t, found, "Node should be created in one of the constituents")
}

func TestCompositeEngine_routeWrite_LabelBased(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with labels only (no properties)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)

	// Node should be in one of the constituents
	found := false
	for _, engine := range []Engine{engine1, engine2} {
		_, err := engine.GetNode(node.ID)
		if err == nil {
			found = true
			break
		}
	}
	assert.True(t, found, "Node should be created in one of the constituents")
}

func TestCompositeEngine_routeWrite_PropertyBased_Int64(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with int64 tenant_id
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"tenant_id": int64(123)},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)
}

func TestCompositeEngine_routeWrite_PropertyBased_Int(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with int tenant_id
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"tenant_id": 456},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)
}

func TestCompositeEngine_routeWrite_PropertyBased_NegativeHash(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with negative int64 tenant_id (tests negative hash handling)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"tenant_id": int64(-123)},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)
}

func TestCompositeEngine_routeWrite_PropertyBased_NegativeInt(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with negative int tenant_id (tests negative hash handling)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"tenant_id": -456},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)
}

func TestCompositeEngine_routeWrite_LabelBased_NegativeHash(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with label that produces negative hash
	// Use a label that will produce a negative hash value
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Z"}, // Single char that might produce negative hash
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)
}

func TestCompositeEngine_routeWrite_NoLabelsNoProperties(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with no labels and no properties (should use default)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{},
		Properties: nil,
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)
}

func TestCompositeEngine_routeWrite_PropertiesWithoutTenantID(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Test routing with properties but no tenant_id (should use default)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := composite.CreateNode(node)
	require.NoError(t, err)
}

func TestCompositeEngine_CreateNode_NoWritableConstituents(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create composite engine with read-only constituents
	constituents := map[string]Engine{
		"db1": engine1,
		"db2": engine2,
	}
	constituentNames := map[string]string{
		"db1": "db1",
		"db2": "db2",
	}
	accessModes := map[string]string{
		"db1": "read", // Read-only
		"db2": "read", // Read-only
	}
	composite := NewCompositeEngine(constituents, constituentNames, accessModes)

	// Try to create node (should fail - no writable constituents)
	node := &Node{
		ID:         NodeID(prefixTestID("node1")),
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{},
	}
	_, err := composite.CreateNode(node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no writable constituents")
}

func TestCompositeEngine_UpdateNode_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Try to update non-existent node
	node := &Node{ID: NodeID(prefixTestID("nonexistent")), Labels: []string{"Person"}}
	err := composite.UpdateNode(node)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_DeleteNode_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Try to delete non-existent node
	err := composite.DeleteNode(NodeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetEdge_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Try to get non-existent edge
	_, err := composite.GetEdge(EdgeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_UpdateEdge_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Try to update non-existent edge
	edge := &Edge{ID: EdgeID(prefixTestID("nonexistent")), StartNode: NodeID(prefixTestID("node1")), EndNode: NodeID(prefixTestID("node2")), Type: "KNOWS"}
	err := composite.UpdateEdge(edge)
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_DeleteEdge_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Try to delete non-existent edge
	err := composite.DeleteEdge(EdgeID(prefixTestID("nonexistent")))
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
}

func TestCompositeEngine_GetFirstNodeByLabel_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Try to get first node with non-existent label
	// GetFirstNodeByLabel should return nil, ErrNotFound when not found
	node, err := composite.GetFirstNodeByLabel("NonExistent")
	// CompositeEngine should return ErrNotFound after checking all constituents
	assert.Error(t, err)
	assert.Equal(t, ErrNotFound, err)
	assert.Nil(t, node)
}

func TestCompositeEngine_GetEdgeBetween_NotFound(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Try to get edge between non-existent nodes
	edge := composite.GetEdgeBetween(NodeID(prefixTestID("node1")), NodeID(prefixTestID("node2")), "KNOWS")
	assert.Nil(t, edge)
}

func TestCompositeEngine_BulkCreateEdges_Unrouted(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in first constituent
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)

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

	// Create edge with non-existent start node (should route to first writable)
	// But edge creation will fail because nodes don't exist
	edge := &Edge{
		ID:        EdgeID(prefixTestID("edge1")),
		StartNode: NodeID(prefixTestID("nonexistent")),
		EndNode:   NodeID(prefixTestID("nonexistent2")),
		Type:      "KNOWS",
	}

	err := composite.BulkCreateEdges([]*Edge{edge})
	// Will fail because nodes don't exist, but tests the unrouted path
	assert.Error(t, err)
}

func TestCompositeEngine_BatchGetNodes(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Batch get nodes
	nodes, err := composite.BatchGetNodes([]NodeID{node1.ID, node2.ID, NodeID(prefixTestID("nonexistent"))})
	require.NoError(t, err)
	assert.Equal(t, 2, len(nodes))
	assert.NotNil(t, nodes[node1.ID])
	assert.NotNil(t, nodes[node2.ID])
	assert.Nil(t, nodes[NodeID(prefixTestID("nonexistent"))])
}

func TestCompositeEngine_Close(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

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

	// Close composite engine
	err := composite.Close()
	require.NoError(t, err)
}

func TestCompositeEngine_NodeCount(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	engine1.CreateNode(node1)
	engine2.CreateNode(node2)

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

	// Get node count (should sum from all constituents)
	count, err := composite.NodeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(2))
}

func TestCompositeEngine_EdgeCount(t *testing.T) {
	// Create constituent engines
	engine1 := NewMemoryEngine()
	engine2 := NewMemoryEngine()

	// Create nodes and edges in different constituents
	node1 := &Node{ID: NodeID(prefixTestID("node1")), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID(prefixTestID("node2")), Labels: []string{"Person"}}
	node3 := &Node{ID: NodeID(prefixTestID("node3")), Labels: []string{"Person"}}
	node4 := &Node{ID: NodeID(prefixTestID("node4")), Labels: []string{"Person"}}
	edge1 := &Edge{ID: EdgeID(prefixTestID("edge1")), StartNode: node1.ID, EndNode: node2.ID, Type: "KNOWS"}
	edge2 := &Edge{ID: EdgeID(prefixTestID("edge2")), StartNode: node3.ID, EndNode: node4.ID, Type: "KNOWS"}
	engine1.CreateNode(node1)
	engine1.CreateNode(node2)
	engine1.CreateEdge(edge1)
	engine2.CreateNode(node3)
	engine2.CreateNode(node4)
	engine2.CreateEdge(edge2)

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

	// Get edge count (should sum from all constituents)
	count, err := composite.EdgeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(2))
}
