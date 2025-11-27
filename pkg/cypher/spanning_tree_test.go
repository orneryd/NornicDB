package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestApocPathSpanningTreeBasic tests basic spanning tree functionality
func TestApocPathSpanningTreeBasic(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a simple graph: A -> B -> C
	//                         A -> D
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "CONNECTS", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "CONNECTS", StartNode: "b", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "CONNECTS", StartNode: "a", EndNode: "d"})

	// Get spanning tree from node A (using node ID directly)
	query := `CALL apoc.path.spanningTree({id: 'a'}, {}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	// Should have 3 edges in the spanning tree (connecting 4 nodes)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 edges in spanning tree, got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeWithCycle tests spanning tree with cycles
func TestApocPathSpanningTreeWithCycle(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a graph with a cycle: A -> B -> C -> A
	//                                A -> D
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "CONNECTS", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "CONNECTS", StartNode: "b", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "CONNECTS", StartNode: "c", EndNode: "a"}) // Creates cycle
	store.CreateEdge(&storage.Edge{ID: "e4", Type: "CONNECTS", StartNode: "a", EndNode: "d"})

	query := `CALL apoc.path.spanningTree({id: 'a'}, {}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	// Should still have only 3 edges (spanning tree excludes cycle-creating edge)
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 edges in spanning tree (no cycles), got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeMaxLevel tests maxLevel configuration
func TestApocPathSpanningTreeMaxLevel(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a chain: A -> B -> C -> D
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "CONNECTS", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "CONNECTS", StartNode: "b", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "CONNECTS", StartNode: "c", EndNode: "d"})

	// Test with maxLevel: 2 (should get only first 2 edges)
	query := `CALL apoc.path.spanningTree({id: 'a'}, {maxLevel: 2}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 edges with maxLevel:2, got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeRelationshipFilter tests relationship type filtering
func TestApocPathSpanningTreeRelationshipFilter(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a graph with different relationship types
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "FRIEND", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "FRIEND", StartNode: "b", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "COLLEAGUE", StartNode: "a", EndNode: "d"})

	// Test with relationshipFilter for FRIEND only
	query := `CALL apoc.path.spanningTree({id: 'a'}, {relationshipFilter: 'FRIEND'}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	// Should have only 2 edges (FRIEND relationships)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 edges with FRIEND filter, got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeDFS tests depth-first search spanning tree
func TestApocPathSpanningTreeDFS(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a binary tree
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})
	store.CreateNode(&storage.Node{ID: "e", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "E"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "CONNECTS", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "CONNECTS", StartNode: "a", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "CONNECTS", StartNode: "b", EndNode: "d"})
	store.CreateEdge(&storage.Edge{ID: "e4", Type: "CONNECTS", StartNode: "b", EndNode: "e"})

	// Test with DFS
	query := `CALL apoc.path.spanningTree({id: 'a'}, {bfs: false}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree DFS query failed: %v", err)
	}

	// Should have 4 edges in the spanning tree
	if len(result.Rows) != 4 {
		t.Errorf("Expected 4 edges in DFS spanning tree, got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeLabelFilter tests label filtering
func TestApocPathSpanningTreeLabelFilter(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a graph with different labels
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Start"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Good"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Good"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Bad"}, Properties: map[string]interface{}{"name": "D"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "CONNECTS", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "CONNECTS", StartNode: "b", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "CONNECTS", StartNode: "a", EndNode: "d"})

	// Test with labelFilter to include only Good nodes
	query := `CALL apoc.path.spanningTree({id: 'a'}, {labelFilter: '+Good'}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	// Should have only 2 edges (excluding Bad labeled node)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 edges with Good label filter, got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeLimit tests limit configuration
func TestApocPathSpanningTreeLimit(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a star graph: A connected to B, C, D, E
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})
	store.CreateNode(&storage.Node{ID: "e", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "E"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "CONNECTS", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "CONNECTS", StartNode: "a", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "CONNECTS", StartNode: "a", EndNode: "d"})
	store.CreateEdge(&storage.Edge{ID: "e4", Type: "CONNECTS", StartNode: "a", EndNode: "e"})

	// Test with limit: 2
	query := `CALL apoc.path.spanningTree({id: 'a'}, {limit: 2}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 edges with limit:2, got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeDisconnectedGraph tests with disconnected components
func TestApocPathSpanningTreeDisconnectedGraph(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create two disconnected components: A-B and C-D
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "CONNECTS", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "CONNECTS", StartNode: "c", EndNode: "d"})

	// Get spanning tree from node A (should only include A-B component)
	query := `CALL apoc.path.spanningTree({id: 'a'}, {}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	// Should have only 1 edge (A-B), not reaching C-D
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 edge in disconnected graph spanning tree, got %d", len(result.Rows))
	}
}

// TestApocPathSpanningTreeDirection tests directional traversal
func TestApocPathSpanningTreeDirection(t *testing.T) {
	store := storage.NewMemoryEngine()
	e := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a directed graph: A -> B -> C
	//                           A <- D
	store.CreateNode(&storage.Node{ID: "a", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "A"}})
	store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "B"}})
	store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "C"}})
	store.CreateNode(&storage.Node{ID: "d", Labels: []string{"Node"}, Properties: map[string]interface{}{"name": "D"}})

	store.CreateEdge(&storage.Edge{ID: "e1", Type: "POINTS_TO", StartNode: "a", EndNode: "b"})
	store.CreateEdge(&storage.Edge{ID: "e2", Type: "POINTS_TO", StartNode: "b", EndNode: "c"})
	store.CreateEdge(&storage.Edge{ID: "e3", Type: "POINTS_TO", StartNode: "d", EndNode: "a"})

	// Test with outgoing relationships only
	query := `CALL apoc.path.spanningTree({id: 'a'}, {relationshipFilter: '>POINTS_TO'}) YIELD path RETURN path`

	result, err := e.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Spanning tree query failed: %v", err)
	}

	// Should have only 2 edges (A->B->C), not including D->A
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 edges with outgoing filter, got %d", len(result.Rows))
	}
}
