package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createCommunityTestGraph(t *testing.T, engine storage.Engine) {
	// Create two communities: A-B-C (connected) and D-E-F (connected)
	// With a weak link between C and D

	// Community 1: A-B-C (triangle)
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}}))
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}}))
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}}))

	// Community 2: D-E-F (triangle)
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}}))
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "e", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "E"}}))
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "f", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "F"}}))

	// Dense connections within community 1
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e2", StartNode: "b", EndNode: "c", Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e3", StartNode: "c", EndNode: "a", Type: "CONNECTS"}))

	// Dense connections within community 2
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e4", StartNode: "d", EndNode: "e", Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e5", StartNode: "e", EndNode: "f", Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e6", StartNode: "f", EndNode: "d", Type: "CONNECTS"}))

	// Weak link between communities
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e7", StartNode: "c", EndNode: "d", Type: "CONNECTS"}))
}

func TestApocAlgoLouvain(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	createCommunityTestGraph(t, engine)

	t.Run("basic_louvain", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.louvain(['Node']) YIELD node, community", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should return 6 nodes
		assert.Len(t, result.Rows, 6)

		// Verify columns
		assert.Equal(t, []string{"node", "community"}, result.Columns)

		// Verify each row has node and community
		for _, row := range result.Rows {
			assert.Len(t, row, 2)
			assert.NotNil(t, row[0]) // node
			assert.NotNil(t, row[1]) // community ID
		}
	})

	t.Run("louvain_all_nodes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.louvain() YIELD node, community", nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 6)
	})
}

func TestApocAlgoLabelPropagation(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	createCommunityTestGraph(t, engine)

	t.Run("basic_label_propagation", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.labelPropagation(['Node']) YIELD node, community", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Should return 6 nodes
		assert.Len(t, result.Rows, 6)

		// Verify columns
		assert.Equal(t, []string{"node", "community"}, result.Columns)
	})

	t.Run("label_propagation_all_nodes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.labelPropagation() YIELD node, community", nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 6)
	})
}

func TestApocAlgoWCC(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create disconnected components
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "x", Labels: []string{"Node"}}))
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "y", Labels: []string{"Node"}}))
	require.NoError(t, engine.CreateNode(&storage.Node{ID: "z", Labels: []string{"Node"}}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "xy", StartNode: "x", EndNode: "y", Type: "CONNECTS"}))
	// z is isolated

	t.Run("basic_wcc", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.wcc(['Node']) YIELD node, componentId", nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Len(t, result.Rows, 3)
		assert.Equal(t, []string{"node", "componentId"}, result.Columns)

		// X and Y should be in the same component
		// Z should be in a different component
		componentMap := make(map[string]int)
		for _, row := range result.Rows {
			nodeMap := row[0].(map[string]interface{})
			nodeID := nodeMap["id"].(string)
			componentID := row[1].(int)
			componentMap[nodeID] = componentID
		}

		assert.Equal(t, componentMap["x"], componentMap["y"], "x and y should be in same component")
		assert.NotEqual(t, componentMap["x"], componentMap["z"], "z should be in different component")
	})
}

func TestApocAlgoWCC_SingleComponent(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	createCommunityTestGraph(t, engine)

	t.Run("connected_graph", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.wcc() YIELD node, componentId", nil)
		require.NoError(t, err)

		// All nodes should be in the same component since the graph is connected
		componentIDs := make(map[int]bool)
		for _, row := range result.Rows {
			componentIDs[row[1].(int)] = true
		}

		assert.Len(t, componentIDs, 1, "all nodes should be in the same component")
	})
}
