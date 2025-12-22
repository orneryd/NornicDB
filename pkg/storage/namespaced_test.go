package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespacedEngine_BasicOperations(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	// Create namespaced engine for tenant_a
	tenantA := NewNamespacedEngine(inner, "tenant_a")
	assert.Equal(t, "tenant_a", tenantA.Namespace())

	// Create a node (NamespacedEngine receives unprefixed IDs)
	node := &Node{
		ID:     NodeID("node-1"),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name": "Alice",
		},
	}
	_, err := tenantA.CreateNode(node)
	require.NoError(t, err)

	// Get the node back (NamespacedEngine receives unprefixed IDs)
	retrieved, err := tenantA.GetNode(NodeID("node-1"))
	require.NoError(t, err)
	assert.Equal(t, "node-1", string(retrieved.ID))
	assert.Equal(t, "Alice", retrieved.Properties["name"])

	// Verify the underlying storage has the prefixed ID
	prefixedNode, err := inner.GetNode(NodeID("tenant_a:node-1"))
	require.NoError(t, err)
	assert.Equal(t, "tenant_a:node-1", string(prefixedNode.ID))
}

func TestNamespacedEngine_Isolation(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")
	tenantB := NewNamespacedEngine(inner, "tenant_b")

	// Create nodes in different tenants (NamespacedEngine receives unprefixed IDs)
	nodeA := &Node{
		ID:         NodeID("node-1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"tenant": "a"},
	}
	_, err := tenantA.CreateNode(nodeA)
	require.NoError(t, err)

	nodeB := &Node{
		ID:         NodeID("node-1"), // Same ID, different tenant
		Labels:     []string{"Person"},
		Properties: map[string]any{"tenant": "b"},
	}
	_, err = tenantB.CreateNode(nodeB)
	require.NoError(t, err)

	// Each tenant should only see their own nodes
	nodesA, err := tenantA.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodesA, 1)
	assert.Equal(t, "a", nodesA[0].Properties["tenant"])

	nodesB, err := tenantB.AllNodes()
	require.NoError(t, err)
	assert.Len(t, nodesB, 1)
	assert.Equal(t, "b", nodesB[0].Properties["tenant"])
}

func TestNamespacedEngine_Edges(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Create two nodes (NamespacedEngine receives unprefixed IDs)
	node1 := &Node{ID: NodeID("n1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("n2"), Labels: []string{"Person"}}
	_, err := tenantA.CreateNode(node1)
	require.NoError(t, err)
	_, err = tenantA.CreateNode(node2)
	require.NoError(t, err)

	// Create edge (NamespacedEngine receives unprefixed IDs)
	edge := &Edge{
		ID:        EdgeID("e1"),
		StartNode: NodeID("n1"),
		EndNode:   NodeID("n2"),
		Type:      "KNOWS",
		Properties: map[string]any{
			"since": "2020",
		},
	}
	err = tenantA.CreateEdge(edge)
	require.NoError(t, err)

	// Get edge back (NamespacedEngine receives unprefixed IDs)
	retrieved, err := tenantA.GetEdge(EdgeID("e1"))
	require.NoError(t, err)
	assert.Equal(t, "n1", string(retrieved.StartNode))
	assert.Equal(t, "n2", string(retrieved.EndNode))
	assert.Equal(t, "KNOWS", retrieved.Type)

	// Get outgoing edges (NamespacedEngine receives unprefixed IDs)
	outgoing, err := tenantA.GetOutgoingEdges(NodeID("n1"))
	require.NoError(t, err)
	assert.Len(t, outgoing, 1)
	assert.Equal(t, "e1", string(outgoing[0].ID))
}

func TestNamespacedEngine_QueryOperations(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")
	tenantB := NewNamespacedEngine(inner, "tenant_b")

	// Create nodes with same label in different tenants
	for i := 0; i < 3; i++ {
		node := &Node{
			ID:         NodeID("node-" + string(rune('a'+i))),
			Labels:     []string{"Person"},
			Properties: map[string]any{"id": i},
		}
		_, err := tenantA.CreateNode(node)
		require.NoError(t, err)
	}

	for i := 0; i < 2; i++ {
		node := &Node{
			ID:         NodeID("node-" + string(rune('x'+i))),
			Labels:     []string{"Person"},
			Properties: map[string]any{"id": i},
		}
		_, err := tenantB.CreateNode(node)
		require.NoError(t, err)
	}

	// Query by label - should only see tenant's nodes
	nodesA, err := tenantA.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Len(t, nodesA, 3)

	nodesB, err := tenantB.GetNodesByLabel("Person")
	require.NoError(t, err)
	assert.Len(t, nodesB, 2)
}

func TestNamespacedEngine_DeleteByPrefix(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Create some nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID("node-" + string(rune('0'+i))),
			Labels: []string{"Test"},
		}
		_, err := tenantA.CreateNode(node)
		require.NoError(t, err)
	}

	// DeleteByPrefix should not be supported on NamespacedEngine
	// (should be called on underlying engine)
	_, _, err := tenantA.DeleteByPrefix("tenant_a:")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported on NamespacedEngine")
}

func TestNamespacedEngine_Stats(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Create nodes and edges (NamespacedEngine receives unprefixed IDs)
	node1 := &Node{ID: NodeID("n1"), Labels: []string{"Person"}}
	node2 := &Node{ID: NodeID("n2"), Labels: []string{"Person"}}
	_, err := tenantA.CreateNode(node1)
	require.NoError(t, err)
	_, err = tenantA.CreateNode(node2)
	require.NoError(t, err)

	edge := &Edge{
		ID:        EdgeID("e1"),
		StartNode: NodeID("n1"),
		EndNode:   NodeID("n2"),
		Type:      "KNOWS",
	}
	err = tenantA.CreateEdge(edge)
	require.NoError(t, err)

	// Check counts
	nodeCount, err := tenantA.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(2), nodeCount)

	edgeCount, err := tenantA.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), edgeCount)
}

func TestNamespacedEngine_BulkOperations(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Bulk create nodes (NamespacedEngine receives unprefixed IDs)
	nodes := []*Node{
		{ID: NodeID("n1"), Labels: []string{"Person"}},
		{ID: NodeID("n2"), Labels: []string{"Person"}},
		{ID: NodeID("n3"), Labels: []string{"Person"}},
	}
	err := tenantA.BulkCreateNodes(nodes)
	require.NoError(t, err)

	// Verify all created
	allNodes, err := tenantA.AllNodes()
	require.NoError(t, err)
	assert.Len(t, allNodes, 3)

	// Bulk delete (NamespacedEngine receives unprefixed IDs)
	err = tenantA.BulkDeleteNodes([]NodeID{NodeID("n1"), NodeID("n2")})
	require.NoError(t, err)

	// Verify deleted
	allNodes, err = tenantA.AllNodes()
	require.NoError(t, err)
	assert.Len(t, allNodes, 1)
	assert.Equal(t, "n3", string(allNodes[0].ID))
}

func TestNamespacedEngine_Close(t *testing.T) {
	inner := NewMemoryEngine()
	defer inner.Close()

	tenantA := NewNamespacedEngine(inner, "tenant_a")

	// Close should not close underlying engine
	err := tenantA.Close()
	require.NoError(t, err)

	// Underlying engine should still work (direct access to inner engine needs prefixed IDs)
	node := &Node{ID: NodeID("test:test"), Labels: []string{"Test"}}
	_, err = inner.CreateNode(node)
	require.NoError(t, err)
}
