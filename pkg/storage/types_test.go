package storage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeID_String(t *testing.T) {
	// Test basic string conversion (not using prefixTestID since this tests the type itself)
	id := NodeID("node-123")
	assert.Equal(t, "node-123", string(id))
}

func TestEdgeID_String(t *testing.T) {
	// Test basic string conversion (not using prefixTestID since this tests the type itself)
	id := EdgeID("edge-456")
	assert.Equal(t, "edge-456", string(id))
}

func TestNode_MergeInternalProperties(t *testing.T) {
	now := time.Now()
	node := &Node{
		ID:         NodeID(prefixTestID("test-1")),
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice", "age": 30},
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	merged := node.mergeInternalProperties()

	// Original properties preserved
	assert.Equal(t, "Alice", merged["name"])
	assert.Equal(t, 30, merged["age"])

	// Internal properties added
	assert.Equal(t, now.Unix(), merged["_createdAt"])
	assert.Equal(t, now.Unix(), merged["_updatedAt"])
}

func TestNode_ExtractInternalProperties(t *testing.T) {
	now := time.Now()
	node := &Node{
		ID:     NodeID(prefixTestID("test-1")),
		Labels: []string{"Person"},
		Properties: map[string]any{
			"name":          "Alice",
			"age":           30,
			"_createdAt":    float64(now.Unix()),
			"_updatedAt":    float64(now.Unix()),
			"_decayScore":   0.85,
			"_lastAccessed": float64(now.Unix()),
			"_accessCount":  float64(5),
		},
	}

	node.ExtractInternalProperties()

	// Internal properties extracted into struct fields
	assert.Equal(t, now.Unix(), node.CreatedAt.Unix())
	assert.Equal(t, now.Unix(), node.UpdatedAt.Unix())

	// Internal properties removed from map (including legacy decay fields)
	_, hasCreatedAt := node.Properties["_createdAt"]
	assert.False(t, hasCreatedAt)
	_, hasDecayScore := node.Properties["_decayScore"]
	assert.False(t, hasDecayScore)
	_, hasLastAccessed := node.Properties["_lastAccessed"]
	assert.False(t, hasLastAccessed)
	_, hasAccessCount := node.Properties["_accessCount"]
	assert.False(t, hasAccessCount)

	// User properties preserved
	assert.Equal(t, "Alice", node.Properties["name"])
	assert.Equal(t, 30, node.Properties["age"])
}

func TestNode_MarshalNeo4jJSON(t *testing.T) {
	now := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	// Use unprefixed ID since MarshalNeo4jJSON should export unprefixed IDs for Neo4j compatibility
	node := &Node{
		ID:         NodeID("person-1"),
		Labels:     []string{"Person", "Employee"},
		Properties: map[string]any{"name": "Bob", "department": "Engineering"},
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	data, err := node.MarshalNeo4jJSON()
	require.NoError(t, err)

	var neo4jNode Neo4jNode
	err = json.Unmarshal(data, &neo4jNode)
	require.NoError(t, err)

	assert.Equal(t, "person-1", neo4jNode.ID)
	assert.Equal(t, []string{"Person", "Employee"}, neo4jNode.Labels)
	assert.Equal(t, "Bob", neo4jNode.Properties["name"])
	assert.Equal(t, "Engineering", neo4jNode.Properties["department"])
	assert.Equal(t, now.Unix(), int64(neo4jNode.Properties["_createdAt"].(float64)))
}

func TestToNeo4jExport(t *testing.T) {
	// Use unprefixed IDs since ToNeo4jExport should export unprefixed IDs for Neo4j compatibility
	nodes := []*Node{
		{ID: NodeID("n1"), Labels: []string{"A"}, Properties: map[string]any{"x": 1}},
		{ID: NodeID("n2"), Labels: []string{"B"}, Properties: map[string]any{"y": 2}},
	}
	edges := []*Edge{
		{ID: EdgeID("e1"), StartNode: NodeID("n1"), EndNode: NodeID("n2"), Type: "KNOWS", Properties: map[string]any{"since": 2020}},
	}

	export := ToNeo4jExport(nodes, edges)

	assert.Len(t, export.Nodes, 2)
	assert.Len(t, export.Relationships, 1)

	assert.Equal(t, "n1", export.Nodes[0].ID)
	assert.Equal(t, []string{"A"}, export.Nodes[0].Labels)

	assert.Equal(t, "e1", export.Relationships[0].ID)
	assert.Equal(t, "n1", export.Relationships[0].StartNode)
	assert.Equal(t, "n2", export.Relationships[0].EndNode)
	assert.Equal(t, "KNOWS", export.Relationships[0].Type)
}

func TestFromNeo4jExport(t *testing.T) {
	export := &Neo4jExport{
		Nodes: []Neo4jNode{
			{ID: "person-1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}},
			{ID: "person-2", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}},
		},
		Relationships: []Neo4jRelationship{
			{ID: "rel-1", StartNode: "person-1", EndNode: "person-2", Type: "FRIENDS", Properties: map[string]any{}},
		},
	}

	nodes, edges := FromNeo4jExport(export)

	require.Len(t, nodes, 2)
	require.Len(t, edges, 1)

	// FromNeo4jExport returns unprefixed IDs (prefixing happens when inserted into NamespacedEngine)
	assert.Equal(t, NodeID("person-1"), nodes[0].ID)
	assert.Equal(t, []string{"Person"}, nodes[0].Labels)
	assert.Equal(t, "Alice", nodes[0].Properties["name"])

	assert.Equal(t, EdgeID("rel-1"), edges[0].ID)
	assert.Equal(t, NodeID("person-1"), edges[0].StartNode)
	assert.Equal(t, NodeID("person-2"), edges[0].EndNode)
	assert.Equal(t, "FRIENDS", edges[0].Type)
}

func TestNeo4jExport_RoundTrip(t *testing.T) {
	// Create original nodes and edges
	original := &Neo4jExport{
		Nodes: []Neo4jNode{
			{ID: "n1", Labels: []string{"Memory"}, Properties: map[string]any{"content": "Test memory", "tier": "SEMANTIC"}},
		},
		Relationships: []Neo4jRelationship{
			{ID: "r1", StartNode: "n1", EndNode: "n1", Type: "RELATES_TO", Properties: map[string]any{"confidence": 0.9}},
		},
	}

	// Convert to internal format
	nodes, edges := FromNeo4jExport(original)

	// Convert back to Neo4j format
	exported := ToNeo4jExport(nodes, edges)

	// Verify data integrity
	assert.Equal(t, original.Nodes[0].ID, exported.Nodes[0].ID)
	assert.Equal(t, original.Nodes[0].Labels, exported.Nodes[0].Labels)
	assert.Equal(t, original.Nodes[0].Properties["content"], exported.Nodes[0].Properties["content"])

	assert.Equal(t, original.Relationships[0].ID, exported.Relationships[0].ID)
	assert.Equal(t, original.Relationships[0].Type, exported.Relationships[0].Type)
}

func TestExtractInternalProperties_NilProperties(t *testing.T) {
	node := &Node{ID: "test:n1", Labels: []string{"A"}}
	// Should not panic on nil Properties
	node.ExtractInternalProperties()
	assert.Nil(t, node.Properties)
}

func TestExtractInternalProperties_AllFields(t *testing.T) {
	now := time.Now()
	node := &Node{
		ID:     "test:n1",
		Labels: []string{"A"},
		Properties: map[string]interface{}{
			"name":          "Alice",
			"_createdAt":    float64(now.Unix()),
			"_updatedAt":    float64(now.Unix()),
			"_decayScore":   0.85,
			"_lastAccessed": float64(now.Unix()),
			"_accessCount":  float64(42),
		},
	}
	node.ExtractInternalProperties()

	assert.False(t, node.CreatedAt.IsZero())
	assert.False(t, node.UpdatedAt.IsZero())

	// Legacy internal properties should be removed
	_, hasCreatedAt := node.Properties["_createdAt"]
	assert.False(t, hasCreatedAt)
	_, hasDecay := node.Properties["_decayScore"]
	assert.False(t, hasDecay)
	// Regular properties should remain
	assert.Equal(t, "Alice", node.Properties["name"])
}

func TestCopyEdge_Nil(t *testing.T) {
	result := copyEdge(nil)
	assert.Nil(t, result)
}

func TestErrors(t *testing.T) {
	// Verify error messages are descriptive
	assert.Equal(t, "not found", ErrNotFound.Error())
	assert.Equal(t, "already exists", ErrAlreadyExists.Error())
	assert.Equal(t, "invalid id", ErrInvalidID.Error())
	assert.Equal(t, "invalid data", ErrInvalidData.Error())
	assert.Equal(t, "storage closed", ErrStorageClosed.Error())
}

func TestCollectLabels(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Person", "Manager"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)

	labels, err := CollectLabels(context.Background(), engine)
	require.NoError(t, err)
	assert.Len(t, labels, 3)
	assert.ElementsMatch(t, []string{"Person", "Employee", "Manager"}, labels)
}

func TestCollectEdgeTypes(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "KNOWS", Properties: map[string]interface{}{}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:n1", EndNode: "test:n2", Type: "LIKES", Properties: map[string]interface{}{}}))

	types, err := CollectEdgeTypes(context.Background(), engine)
	require.NoError(t, err)
	assert.Len(t, types, 2)
	assert.ElementsMatch(t, []string{"KNOWS", "LIKES"}, types)
}

func TestStreamNodesWithFallback(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)

	var count int
	err = StreamNodesWithFallback(context.Background(), engine, 10, func(node *Node) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestStreamEdgesWithFallback(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"A"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"B"}, Properties: map[string]interface{}{}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "R", Properties: map[string]interface{}{}}))

	var count int
	err = StreamEdgesWithFallback(context.Background(), engine, 10, func(edge *Edge) error {
		count++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
