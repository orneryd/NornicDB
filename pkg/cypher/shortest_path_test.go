package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestShortestPathCypher(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test graph: A -> B -> C
	//                     A -> D -> C
	// Create nodes first
	exec.Execute(ctx, `CREATE (a:Person {name: 'Alice'})`, nil)
	exec.Execute(ctx, `CREATE (b:Person {name: 'Bob'})`, nil)
	exec.Execute(ctx, `CREATE (c:Person {name: 'Carol'})`, nil)
	exec.Execute(ctx, `CREATE (d:Person {name: 'Dave'})`, nil)

	// Create relationships using MATCH...CREATE
	exec.Execute(ctx, `MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)`, nil)
	exec.Execute(ctx, `MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)`, nil)
	exec.Execute(ctx, `MATCH (a:Person {name: 'Alice'}), (d:Person {name: 'Dave'}) CREATE (a)-[:KNOWS]->(d)`, nil)
	exec.Execute(ctx, `MATCH (d:Person {name: 'Dave'}), (c:Person {name: 'Carol'}) CREATE (d)-[:KNOWS]->(c)`, nil)

	t.Run("BasicShortestPath", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = shortestPath((start)-[:KNOWS*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected at least one path")
		}

		// Should find a path of length 2 (Alice -> Bob -> Carol or Alice -> Dave -> Carol)
		path := result.Rows[0][0].(map[string]interface{})
		length := path["length"].(int)
		if length != 2 {
			t.Errorf("Expected path length 2, got %d", length)
		}
	})

	t.Run("ShortestPathWithLength", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = shortestPath((start)-[:KNOWS*]->(end))
			RETURN length(p) AS pathLength
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected result")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2, got %d", length)
		}
	})

	t.Run("AllShortestPaths", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = allShortestPaths((start)-[:KNOWS*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should find 2 paths: Alice -> Bob -> Carol AND Alice -> Dave -> Carol
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 shortest paths, got %d", len(result.Rows))
		}

		// Both should be length 2
		for i, row := range result.Rows {
			path := row[0].(map[string]interface{})
			length := path["length"].(int)
			if length != 2 {
				t.Errorf("Path %d: Expected length 2, got %d", i, length)
			}
		}
	})

	t.Run("ShortestPathNodesAndRels", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Person {name: 'Alice'}), (end:Person {name: 'Carol'})
			MATCH p = shortestPath((start)-[:KNOWS*]->(end))
			RETURN nodes(p) AS nodeList, relationships(p) AS relList
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected result")
		}

		nodes := result.Rows[0][0].([]interface{})
		rels := result.Rows[0][1].([]interface{})

		// Path length 2 means 3 nodes and 2 relationships
		if len(nodes) != 3 {
			t.Errorf("Expected 3 nodes, got %d", len(nodes))
		}
		if len(rels) != 2 {
			t.Errorf("Expected 2 relationships, got %d", len(rels))
		}
	})
}

func TestShortestPathBidirectional(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create bidirectional graph
	exec.Execute(ctx, `CREATE (a:Node {id: 1})`, nil)
	exec.Execute(ctx, `CREATE (b:Node {id: 2})`, nil)
	exec.Execute(ctx, `CREATE (c:Node {id: 3})`, nil)
	exec.Execute(ctx, `MATCH (a:Node {id: 1}), (b:Node {id: 2}) CREATE (a)-[:REL]->(b)`, nil)
	exec.Execute(ctx, `MATCH (b:Node {id: 2}), (c:Node {id: 3}) CREATE (b)-[:REL]->(c)`, nil)

	t.Run("BidirectionalPath", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 3})
			MATCH p = shortestPath((start)-[:REL*]-(end))
			RETURN length(p) AS len
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected path")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2, got %d", length)
		}
	})
}

func TestShortestPathNoPath(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create disconnected nodes
	exec.Execute(ctx, "CREATE (a:Node {id: 1}), (b:Node {id: 2})", nil)

	t.Run("NoPathExists", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 2})
			MATCH p = shortestPath((start)-[:REL*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Should return empty result when no path exists
		if len(result.Rows) != 0 {
			t.Errorf("Expected no results, got %d", len(result.Rows))
		}
	})
}

func TestShortestPathWithMaxHops(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create chain: A -> B -> C -> D -> E
	exec.Execute(ctx, `CREATE (n1:Node {id: 1})`, nil)
	exec.Execute(ctx, `CREATE (n2:Node {id: 2})`, nil)
	exec.Execute(ctx, `CREATE (n3:Node {id: 3})`, nil)
	exec.Execute(ctx, `CREATE (n4:Node {id: 4})`, nil)
	exec.Execute(ctx, `CREATE (n5:Node {id: 5})`, nil)
	exec.Execute(ctx, `MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:NEXT]->(n2)`, nil)
	exec.Execute(ctx, `MATCH (n2:Node {id: 2}), (n3:Node {id: 3}) CREATE (n2)-[:NEXT]->(n3)`, nil)
	exec.Execute(ctx, `MATCH (n3:Node {id: 3}), (n4:Node {id: 4}) CREATE (n3)-[:NEXT]->(n4)`, nil)
	exec.Execute(ctx, `MATCH (n4:Node {id: 4}), (n5:Node {id: 5}) CREATE (n4)-[:NEXT]->(n5)`, nil)

	t.Run("WithinMaxHops", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 3})
			MATCH p = shortestPath((start)-[:NEXT*..5]->(end))
			RETURN length(p) AS len
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if len(result.Rows) == 0 {
			t.Fatal("Expected path")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2 (1->2->3), got %d", length)
		}
	})

	t.Run("BeyondMaxHops", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 1}), (end:Node {id: 5})
			MATCH p = shortestPath((start)-[:NEXT*..2]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Path requires 4 hops, max is 2, so no result
		if len(result.Rows) != 0 {
			t.Errorf("Expected no result (path too long), got %d rows", len(result.Rows))
		}
	})
}

func TestShortestPathDirectional(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create directed path: A -> B <- C
	exec.Execute(ctx, `CREATE (a:Node {id: 'A'})`, nil)
	exec.Execute(ctx, `CREATE (b:Node {id: 'B'})`, nil)
	exec.Execute(ctx, `CREATE (c:Node {id: 'C'})`, nil)
	exec.Execute(ctx, `MATCH (a:Node {id: 'A'}), (b:Node {id: 'B'}) CREATE (a)-[:TO]->(b)`, nil)
	exec.Execute(ctx, `MATCH (c:Node {id: 'C'}), (b:Node {id: 'B'}) CREATE (c)-[:TO]->(b)`, nil)

	t.Run("OutgoingOnly", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 'A'}), (end:Node {id: 'C'})
			MATCH p = shortestPath((start)-[:TO*]->(end))
			RETURN p
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// No outgoing path from A to C
		if len(result.Rows) != 0 {
			t.Errorf("Expected no path (wrong direction), got %d", len(result.Rows))
		}
	})

	t.Run("Undirected", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (start:Node {id: 'A'}), (end:Node {id: 'C'})
			MATCH p = shortestPath((start)-[:TO*]-(end))
			RETURN length(p) AS len
		`, nil)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Path exists via B when undirected: A-B-C
		if len(result.Rows) == 0 {
			t.Fatal("Expected path when undirected")
		}

		length := result.Rows[0][0].(int64)
		if length != 2 {
			t.Errorf("Expected length 2 (A-B-C), got %d", length)
		}
	})
}
