package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create test graph: A -> B -> C -> D
//                              \-> E -> F
func createTestGraph(t *testing.T) *StorageExecutor {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)

	// Create nodes
	nodes := []struct {
		id     string
		labels []string
		props  map[string]interface{}
	}{
		{"A", []string{"Node"}, map[string]interface{}{"name": "A", "lat": 0.0, "lon": 0.0}},
		{"B", []string{"Node"}, map[string]interface{}{"name": "B", "lat": 1.0, "lon": 0.0}},
		{"C", []string{"Node"}, map[string]interface{}{"name": "C", "lat": 2.0, "lon": 0.0}},
		{"D", []string{"Node"}, map[string]interface{}{"name": "D", "lat": 3.0, "lon": 0.0}},
		{"E", []string{"Node"}, map[string]interface{}{"name": "E", "lat": 1.0, "lon": 1.0}},
		{"F", []string{"Node"}, map[string]interface{}{"name": "F", "lat": 2.0, "lon": 1.0}},
	}

	for _, n := range nodes {
		err := engine.CreateNode(&storage.Node{
			ID:         storage.NodeID(n.id),
			Labels:     n.labels,
			Properties: n.props,
		})
		require.NoError(t, err)
	}

	// Create edges
	edges := []struct {
		from, to string
		weight   float64
	}{
		{"A", "B", 1.0},
		{"B", "C", 2.0},
		{"C", "D", 1.0},
		{"A", "E", 3.0},
		{"E", "F", 1.0},
	}

	for i, e := range edges {
		err := engine.CreateEdge(&storage.Edge{
			ID:         storage.EdgeID(e.from + "_" + e.to),
			StartNode:  storage.NodeID(e.from),
			EndNode:    storage.NodeID(e.to),
			Type:       "CONNECTS",
			Properties: map[string]interface{}{"weight": e.weight},
		})
		require.NoError(t, err, "Failed to create edge %d", i)
	}

	return exec
}

func TestApocAlgoDijkstra(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("shortest_path_A_to_D", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.dijkstra('A', 'D', 'CONNECTS', 'weight') YIELD path, weight", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should find one path")

		path := result.Rows[0][0].([]storage.NodeID)
		weight := result.Rows[0][1].(float64)

		assert.Equal(t, []storage.NodeID{"A", "B", "C", "D"}, path)
		assert.Equal(t, 4.0, weight) // 1 + 2 + 1
	})

	t.Run("no_path", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.dijkstra('D', 'A', 'CONNECTS', 'weight') YIELD path, weight", nil)
		require.NoError(t, err)
		assert.Empty(t, result.Rows, "Should find no path (no reverse edges)")
	})
}

func TestApocAlgoAStar(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("astar_A_to_D", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.aStar('A', 'D', 'CONNECTS', 'weight') YIELD path, weight", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "Should find one path")

		path := result.Rows[0][0].([]storage.NodeID)
		assert.Equal(t, []storage.NodeID{"A", "B", "C", "D"}, path)
	})
}

func TestApocAlgoAllSimplePaths(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("all_paths_A_to_D", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.allSimplePaths('A', 'D', 'CONNECTS', 10) YIELD path", nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1, "Should find one path A->B->C->D")
	})
}

func TestApocAlgoPageRank(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("pagerank_all_nodes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.pageRank() YIELD node, score", nil)
		require.NoError(t, err)
		assert.NotEmpty(t, result.Rows, "Should compute PageRank for all nodes")

		// Verify all nodes have scores
		assert.Len(t, result.Rows, 6, "Should have 6 nodes")

		// Check that scores are reasonable (all > 0)
		for _, row := range result.Rows {
			score := row[1].(float64)
			assert.Greater(t, score, 0.0, "All scores should be positive")
		}
	})

	t.Run("pagerank_with_label", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.pageRank('Node') YIELD node, score", nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 6)
	})
}

func TestApocAlgoBetweenness(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("betweenness_all_nodes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.betweenness() YIELD node, score", nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 6)

		// Find the betweenness scores
		scores := make(map[string]float64)
		for _, row := range result.Rows {
			nodeMap := row[0].(map[string]interface{})
			name := nodeMap["name"].(string)
			score := row[1].(float64)
			scores[name] = score
		}

		// B and C should have higher betweenness (on the main path)
		// A, D, E, F are endpoints or on side path
		t.Logf("Betweenness scores: %v", scores)
	})
}

func TestApocAlgoCloseness(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("closeness_all_nodes", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.algo.closeness() YIELD node, score", nil)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 6)

		// Verify all nodes have scores
		for _, row := range result.Rows {
			score := row[1].(float64)
			assert.GreaterOrEqual(t, score, 0.0, "All closeness scores should be >= 0")
		}
	})
}

func TestApocNeighborsTohop(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("neighbors_1_hop", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.neighbors.tohop('A', 'CONNECTS', 1) YIELD node", nil)
		require.NoError(t, err)
		// A connects to B and E
		assert.Len(t, result.Rows, 2, "A should have 2 direct neighbors")
	})

	t.Run("neighbors_2_hops", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.neighbors.tohop('A', 'CONNECTS', 2) YIELD node", nil)
		require.NoError(t, err)
		// A -> B, E (1 hop) + C, F (2 hops)
		assert.Len(t, result.Rows, 4, "A should reach 4 nodes in 2 hops")
	})
}

func TestApocNeighborsByhop(t *testing.T) {
	exec := createTestGraph(t)
	ctx := context.Background()

	t.Run("neighbors_grouped", func(t *testing.T) {
		result, err := exec.Execute(ctx, "CALL apoc.neighbors.byhop('A', 'CONNECTS', 3) YIELD nodes, depth", nil)
		require.NoError(t, err)
		assert.NotEmpty(t, result.Rows)

		// Verify we get depth buckets
		for _, row := range result.Rows {
			depth := row[1].(int)
			assert.Greater(t, depth, 0)
			assert.LessOrEqual(t, depth, 3)
		}
	})
}

func TestDijkstraWithWeights(t *testing.T) {
	engine := storage.NewMemoryEngine()
	exec := NewStorageExecutor(engine)

	// Create a graph where shorter hop count != shortest weighted path
	// A --1-- B --1-- C
	//  \            /
	//   \---10-----/
	nodes := []string{"A", "B", "C"}
	for _, n := range nodes {
		err := engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(n),
			Labels: []string{"Node"},
		})
		require.NoError(t, err)
	}

	// A->B (weight 1), B->C (weight 1), A->C (weight 10)
	edges := []struct {
		from, to string
		weight   float64
	}{
		{"A", "B", 1.0},
		{"B", "C", 1.0},
		{"A", "C", 10.0},
	}

	for _, e := range edges {
		err := engine.CreateEdge(&storage.Edge{
			ID:         storage.EdgeID(e.from + "_" + e.to),
			StartNode:  storage.NodeID(e.from),
			EndNode:    storage.NodeID(e.to),
			Type:       "ROAD",
			Properties: map[string]interface{}{"distance": e.weight},
		})
		require.NoError(t, err)
	}

	ctx := context.Background()
	result, err := exec.Execute(ctx, "CALL apoc.algo.dijkstra('A', 'C', 'ROAD', 'distance') YIELD path, weight", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	path := result.Rows[0][0].([]storage.NodeID)
	weight := result.Rows[0][1].(float64)

	// Should take A->B->C (weight 2) not A->C (weight 10)
	assert.Equal(t, []storage.NodeID{"A", "B", "C"}, path)
	assert.Equal(t, 2.0, weight)
}
