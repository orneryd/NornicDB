package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMatchRelationshipWithWhereIdFunction tests MATCH with WHERE clause using id() function
// This is the pattern used by GraphQL resolvers: MATCH (n)-[r]->(m) WHERE id(n) = $nodeId RETURN r, id(n), id(m)
func TestMatchRelationshipWithWhereIdFunction(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes
	n1 := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	n2 := &storage.Node{
		ID:         "node-2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	n3 := &storage.Node{
		ID:         "node-3",
		Labels:     []string{"Company"},
		Properties: map[string]interface{}{"name": "TechCorp"},
	}
	require.NoError(t, store.CreateNode(n1))
	require.NoError(t, store.CreateNode(n2))
	require.NoError(t, store.CreateNode(n3))

	// Create relationships
	edge1 := &storage.Edge{
		ID:         "edge-1",
		Type:       "KNOWS",
		StartNode:  n1.ID,
		EndNode:    n2.ID,
		Properties: map[string]interface{}{},
	}
	edge2 := &storage.Edge{
		ID:         "edge-2",
		Type:       "WORKS_AT",
		StartNode:  n1.ID,
		EndNode:    n3.ID,
		Properties: map[string]interface{}{},
	}
	require.NoError(t, store.CreateEdge(edge1))
	require.NoError(t, store.CreateEdge(edge2))

	// Test: MATCH (n)-[r]->(m) WHERE id(n) = $nodeId RETURN r, id(n) as source, id(m) as target
	t.Run("outgoing edges with id() function in WHERE", func(t *testing.T) {
		query := "MATCH (n)-[r]->(m) WHERE id(n) = $nodeId RETURN r, id(n) as source, id(m) as target"
		params := map[string]interface{}{"nodeId": "node-1"}

		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should find 2 outgoing edges from node-1")

		// Verify first edge (KNOWS) - edges are returned as maps
		edge1Map, ok := result.Rows[0][0].(map[string]interface{})
		require.True(t, ok, "First row should contain edge map")
		assert.Equal(t, "edge-1", edge1Map["_edgeId"], "First edge ID should be edge-1")
		assert.Equal(t, "KNOWS", edge1Map["type"], "First edge type should be KNOWS")
		assert.Equal(t, "node-1", result.Rows[0][1], "Source should be node-1")
		assert.Equal(t, "node-2", result.Rows[0][2], "Target should be node-2")

		// Verify second edge (WORKS_AT)
		edge2Map, ok := result.Rows[1][0].(map[string]interface{})
		require.True(t, ok, "Second row should contain edge map")
		assert.Equal(t, "edge-2", edge2Map["_edgeId"], "Second edge ID should be edge-2")
		assert.Equal(t, "WORKS_AT", edge2Map["type"], "Second edge type should be WORKS_AT")
		assert.Equal(t, "node-1", result.Rows[1][1], "Source should be node-1")
		assert.Equal(t, "node-3", result.Rows[1][2], "Target should be node-3")
	})

	// Test: MATCH (n)<-[r]-(m) WHERE id(n) = $nodeId RETURN r, id(m) as source, id(n) as target
	t.Run("incoming edges with id() function in WHERE", func(t *testing.T) {
		query := "MATCH (n)<-[r]-(m) WHERE id(n) = $nodeId RETURN r, id(m) as source, id(n) as target"
		params := map[string]interface{}{"nodeId": "node-2"}

		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should find 1 incoming edge to node-2")

		// Verify edge - edges are returned as maps
		edge1Map, ok := result.Rows[0][0].(map[string]interface{})
		require.True(t, ok, "Row should contain edge map")
		assert.Equal(t, "edge-1", edge1Map["_edgeId"], "Edge ID should be edge-1")
		assert.Equal(t, "KNOWS", edge1Map["type"], "Edge type should be KNOWS")
		assert.Equal(t, "node-1", result.Rows[0][1], "Source should be node-1")
		assert.Equal(t, "node-2", result.Rows[0][2], "Target should be node-2")
	})

	// Test: MATCH (n)-[r]->(m) WHERE (id(n) = $nodeId OR n.id = $nodeId) RETURN r, id(n) as source, id(m) as target
	t.Run("outgoing edges with id() OR property in WHERE", func(t *testing.T) {
		// Add id property to node for testing OR condition
		n1.Properties["id"] = "node-1"
		require.NoError(t, store.UpdateNode(n1))

		query := "MATCH (n)-[r]->(m) WHERE (id(n) = $nodeId OR n.id = $nodeId) RETURN r, id(n) as source, id(m) as target"
		params := map[string]interface{}{"nodeId": "node-1"}

		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2, "Should find 2 outgoing edges from node-1")
	})

	// Test: No matches when nodeId doesn't match
	t.Run("no matches for non-existent node", func(t *testing.T) {
		query := "MATCH (n)-[r]->(m) WHERE id(n) = $nodeId RETURN r, id(n) as source, id(m) as target"
		params := map[string]interface{}{"nodeId": "node-999"}

		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		require.Len(t, result.Rows, 0, "Should find no edges for non-existent node")
	})
}

