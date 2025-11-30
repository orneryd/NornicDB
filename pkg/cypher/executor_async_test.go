package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestExecuteImplicitAsync_CreateNode verifies that CREATE node queries
// execute correctly through the async path and data is persisted.
func TestExecuteImplicitAsync_CreateNode(t *testing.T) {
	engine := storage.NewMemoryEngine()
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Create a node
	result, err := executor.Execute(ctx, "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n.name", nil)
	if err != nil {
		t.Fatalf("CREATE failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected name 'Alice', got %v", result.Rows[0][0])
	}

	// Verify node is visible in subsequent query
	countResult, err := executor.Execute(ctx, "MATCH (n:Person) RETURN count(n) as c", nil)
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}

	if len(countResult.Rows) != 1 {
		t.Fatalf("Expected 1 row for count, got %d", len(countResult.Rows))
	}
	count, ok := countResult.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected int64 count, got %T: %v", countResult.Rows[0][0], countResult.Rows[0][0])
	}
	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}
}

// TestExecuteImplicitAsync_CreateRelationship verifies that relationship creation
// works correctly through the async path.
func TestExecuteImplicitAsync_CreateRelationship(t *testing.T) {
	engine := storage.NewMemoryEngine()
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Create two nodes
	_, err := executor.Execute(ctx, "CREATE (a:Person {name: 'Alice'})", nil)
	if err != nil {
		t.Fatalf("CREATE Alice failed: %v", err)
	}
	_, err = executor.Execute(ctx, "CREATE (b:Person {name: 'Bob'})", nil)
	if err != nil {
		t.Fatalf("CREATE Bob failed: %v", err)
	}

	// Create relationship using MATCH...CREATE
	_, err = executor.Execute(ctx, "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)", nil)
	if err != nil {
		t.Fatalf("CREATE relationship failed: %v", err)
	}

	// Verify relationship exists
	countResult, err := executor.Execute(ctx, "MATCH ()-[r:KNOWS]->() RETURN count(r) as c", nil)
	if err != nil {
		t.Fatalf("COUNT relationships failed: %v", err)
	}

	if len(countResult.Rows) != 1 {
		t.Fatalf("Expected 1 row for count, got %d", len(countResult.Rows))
	}
	count, ok := countResult.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected int64 count, got %T: %v", countResult.Rows[0][0], countResult.Rows[0][0])
	}
	if count != 1 {
		t.Errorf("Expected 1 relationship, got %d", count)
	}
}

// TestExecuteImplicitAsync_AggregationEmptyDB verifies that aggregation queries
// return correct results even when the database is empty.
func TestExecuteImplicitAsync_AggregationEmptyDB(t *testing.T) {
	engine := storage.NewMemoryEngine()
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Count nodes in empty database
	result, err := executor.Execute(ctx, "MATCH (n) RETURN count(n) as c", nil)
	if err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d rows", len(result.Rows))
	}

	count, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected int64, got %T: %v", result.Rows[0][0], result.Rows[0][0])
	}
	if count != 0 {
		t.Errorf("Expected count 0, got %d", count)
	}
}

// TestExecuteImplicitAsync_RelationshipCountEmptyDB verifies that relationship
// count returns 0 (not error) when no relationships exist.
func TestExecuteImplicitAsync_RelationshipCountEmptyDB(t *testing.T) {
	engine := storage.NewMemoryEngine()
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Count relationships in empty database
	result, err := executor.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) as edgeCount", nil)
	if err != nil {
		t.Fatalf("COUNT relationship query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d rows", len(result.Rows))
	}

	count, ok := result.Rows[0][0].(int64)
	if !ok {
		t.Fatalf("Expected int64, got %T: %v", result.Rows[0][0], result.Rows[0][0])
	}
	if count != 0 {
		t.Errorf("Expected count 0, got %d", count)
	}
}

// TestExecuteImplicitAsync_BulkCreateAndCount simulates the benchmark scenario:
// create many nodes/relationships, then count them.
func TestExecuteImplicitAsync_BulkCreateAndCount(t *testing.T) {
	engine := storage.NewMemoryEngine()
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Create 10 Person nodes
	for i := 0; i < 10; i++ {
		_, err := executor.Execute(ctx, "CREATE (n:Person {name: 'Person" + string(rune('A'+i)) + "'})", nil)
		if err != nil {
			t.Fatalf("CREATE node %d failed: %v", i, err)
		}
	}

	// Create relationships between consecutive nodes
	for i := 0; i < 9; i++ {
		query := "MATCH (a:Person {name: 'Person" + string(rune('A'+i)) + "'}), (b:Person {name: 'Person" + string(rune('A'+i+1)) + "'}) CREATE (a)-[:FOLLOWS]->(b)"
		_, err := executor.Execute(ctx, query, nil)
		if err != nil {
			t.Fatalf("CREATE relationship %d failed: %v", i, err)
		}
	}

	// Count nodes
	nodeResult, err := executor.Execute(ctx, "MATCH (n:Person) RETURN count(n) as c", nil)
	if err != nil {
		t.Fatalf("COUNT nodes failed: %v", err)
	}
	if len(nodeResult.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(nodeResult.Rows))
	}
	nodeCount, _ := nodeResult.Rows[0][0].(int64)
	if nodeCount != 10 {
		t.Errorf("Expected 10 nodes, got %d", nodeCount)
	}

	// Count relationships
	relResult, err := executor.Execute(ctx, "MATCH ()-[r:FOLLOWS]->() RETURN count(r) as c", nil)
	if err != nil {
		t.Fatalf("COUNT relationships failed: %v", err)
	}
	if len(relResult.Rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(relResult.Rows))
	}
	relCount, _ := relResult.Rows[0][0].(int64)
	if relCount != 9 {
		t.Errorf("Expected 9 relationships, got %d", relCount)
	}
}

// TestExecuteImplicitAsync_DeleteNode verifies DELETE works through async path.
func TestExecuteImplicitAsync_DeleteNode(t *testing.T) {
	engine := storage.NewMemoryEngine()
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Create and then delete a node
	_, err := executor.Execute(ctx, "CREATE (n:Temp {name: 'ToDelete'})", nil)
	if err != nil {
		t.Fatalf("CREATE failed: %v", err)
	}

	// Verify it exists
	countBefore, _ := executor.Execute(ctx, "MATCH (n:Temp) RETURN count(n) as c", nil)
	if countBefore.Rows[0][0].(int64) != 1 {
		t.Fatalf("Node should exist before delete")
	}

	// Delete it
	_, err = executor.Execute(ctx, "MATCH (n:Temp {name: 'ToDelete'}) DELETE n", nil)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Verify it's gone
	countAfter, _ := executor.Execute(ctx, "MATCH (n:Temp) RETURN count(n) as c", nil)
	if countAfter.Rows[0][0].(int64) != 0 {
		t.Errorf("Node should be deleted, but count is %v", countAfter.Rows[0][0])
	}
}

// TestExecuteImplicitAsync_CreateDeleteRelationship is the exact benchmark scenario.
func TestExecuteImplicitAsync_CreateDeleteRelationship(t *testing.T) {
	engine := storage.NewMemoryEngine()
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Create two permanent nodes
	_, err := executor.Execute(ctx, "CREATE (a:BenchNode {id: 1})", nil)
	if err != nil {
		t.Fatalf("CREATE node 1 failed: %v", err)
	}
	_, err = executor.Execute(ctx, "CREATE (b:BenchNode {id: 2})", nil)
	if err != nil {
		t.Fatalf("CREATE node 2 failed: %v", err)
	}

	// Run create-delete cycle multiple times (simulates benchmark)
	for i := 0; i < 5; i++ {
		// Create relationship
		_, err = executor.Execute(ctx, "MATCH (a:BenchNode {id: 1}), (b:BenchNode {id: 2}) CREATE (a)-[:BENCH_REL]->(b)", nil)
		if err != nil {
			t.Fatalf("Iteration %d: CREATE relationship failed: %v", i, err)
		}

		// Verify relationship exists
		countResult, err := executor.Execute(ctx, "MATCH ()-[r:BENCH_REL]->() RETURN count(r) as c", nil)
		if err != nil {
			t.Fatalf("Iteration %d: COUNT failed: %v", i, err)
		}
		if countResult.Rows[0][0].(int64) != 1 {
			t.Errorf("Iteration %d: Expected 1 relationship after create, got %v", i, countResult.Rows[0][0])
		}

		// Delete relationship
		_, err = executor.Execute(ctx, "MATCH ()-[r:BENCH_REL]->() DELETE r", nil)
		if err != nil {
			t.Fatalf("Iteration %d: DELETE relationship failed: %v", i, err)
		}

		// Verify relationship is gone
		countAfter, err := executor.Execute(ctx, "MATCH ()-[r:BENCH_REL]->() RETURN count(r) as c", nil)
		if err != nil {
			t.Fatalf("Iteration %d: COUNT after delete failed: %v", i, err)
		}
		if countAfter.Rows[0][0].(int64) != 0 {
			t.Errorf("Iteration %d: Expected 0 relationships after delete, got %v", i, countAfter.Rows[0][0])
		}
	}
}
