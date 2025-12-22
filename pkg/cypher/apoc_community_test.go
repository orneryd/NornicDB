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
	// When using NamespacedEngine, pass unprefixed IDs - the engine handles prefixing
	aNode := &storage.Node{ID: storage.NodeID("a"), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}}
	actualA, err := engine.CreateNode(aNode)
	require.NoError(t, err)
	
	bNode := &storage.Node{ID: storage.NodeID("b"), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}}
	actualB, err := engine.CreateNode(bNode)
	require.NoError(t, err)
	
	cNode := &storage.Node{ID: storage.NodeID("c"), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}}
	actualC, err := engine.CreateNode(cNode)
	require.NoError(t, err)

	// Community 2: D-E-F (triangle)
	dNode := &storage.Node{ID: storage.NodeID("d"), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}}
	actualD, err := engine.CreateNode(dNode)
	require.NoError(t, err)
	
	eNode := &storage.Node{ID: storage.NodeID("e"), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "E"}}
	actualE, err := engine.CreateNode(eNode)
	require.NoError(t, err)
	
	fNode := &storage.Node{ID: storage.NodeID("f"), Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "F"}}
	actualF, err := engine.CreateNode(fNode)
	require.NoError(t, err)

	// Dense connections within community 1
	// Use the actual IDs returned from CreateNode (which are unprefixed for NamespacedEngine)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID("e1"), StartNode: actualA, EndNode: actualB, Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID("e2"), StartNode: actualB, EndNode: actualC, Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID("e3"), StartNode: actualC, EndNode: actualA, Type: "CONNECTS"}))

	// Dense connections within community 2
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID("e4"), StartNode: actualD, EndNode: actualE, Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID("e5"), StartNode: actualE, EndNode: actualF, Type: "CONNECTS"}))
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID("e6"), StartNode: actualF, EndNode: actualD, Type: "CONNECTS"}))

	// Weak link between communities
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID("e7"), StartNode: actualC, EndNode: actualD, Type: "CONNECTS"}))
}

func TestApocAlgoLouvain(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
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
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
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
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create disconnected components
	_, err := engine.CreateNode(&storage.Node{ID: "x", Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "y", Labels: []string{"Node"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "z", Labels: []string{"Node"}})
	require.NoError(t, err)
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
			node := row[0].(*storage.Node)
			nodeID := string(node.ID)
			componentID := row[1].(int)
			componentMap[nodeID] = componentID
		}

		assert.Equal(t, componentMap["x"], componentMap["y"], "x and y should be in same component")
		assert.NotEqual(t, componentMap["x"], componentMap["z"], "z should be in different component")
	})
}

func TestApocAlgoWCC_SingleComponent(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create a fully connected graph using Cypher (ensures IDs are handled correctly)
	_, err := exec.Execute(ctx, `
		CREATE (a:Node {name: "A"}),
		       (b:Node {name: "B"}),
		       (c:Node {name: "C"}),
		       (d:Node {name: "D"}),
		       (e:Node {name: "E"}),
		       (f:Node {name: "F"}),
		       (a)-[:CONNECTS]->(b),
		       (b)-[:CONNECTS]->(c),
		       (c)-[:CONNECTS]->(a),
		       (d)-[:CONNECTS]->(e),
		       (e)-[:CONNECTS]->(f),
		       (f)-[:CONNECTS]->(d),
		       (c)-[:CONNECTS]->(d)
	`, nil)
	require.NoError(t, err)

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
