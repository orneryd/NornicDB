package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestCountSubqueryComparison tests COUNT { } subquery functionality
func TestCountSubquery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: Alice knows Bob, Charlie, and Dave
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (a)-[:KNOWS]->(d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT subquery with > comparison
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } > 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestCountSubqueryEquals tests COUNT { } = n syntax
func TestCountSubqueryEquals(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (d:Person {name: 'Dave'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT subquery with = comparison - find person with exactly 2 friends
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } = 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery with = failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestCountSubqueryZero tests COUNT { } = 0 syntax (no relationships)
func TestCountSubqueryZero(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: Alice knows Bob, Charlie has no relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT subquery = 0 - find people with no KNOWS relationships
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } = 0
		RETURN p.name ORDER BY p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery = 0 failed: %v", err)
	}

	// Bob and Charlie should match (neither have outgoing KNOWS)
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 results, got %d", len(result.Rows))
	}
}

// TestCountSubqueryGTE tests COUNT { } >= n syntax
func TestCountSubqueryGTE(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with varying relationships
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (a)-[:KNOWS]->(c:Person {name: 'Charlie'}),
		       (a)-[:KNOWS]->(d:Person {name: 'Dave'}),
		       (e:Person {name: 'Eve'})-[:KNOWS]->(f:Person {name: 'Frank'}),
		       (g:Person {name: 'Grace'})
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT { } >= 2 - find people with 2 or more friends
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)-[:KNOWS]->(other) } >= 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT subquery >= failed: %v", err)
	}

	// Only Alice has 3 friends (>= 2)
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Rows[0][0])
	}
}

// TestCountSubqueryIncoming tests COUNT with incoming relationships
func TestCountSubqueryIncoming(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data: Bob is known by Alice and Charlie
	_, err := exec.Execute(ctx, `
		CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}),
		       (c:Person {name: 'Charlie'})-[:KNOWS]->(b)
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Test COUNT with incoming relationships
	result, err := exec.Execute(ctx, `
		MATCH (p:Person)
		WHERE COUNT { MATCH (p)<-[:KNOWS]-(other) } >= 2
		RETURN p.name
	`, nil)
	if err != nil {
		t.Fatalf("COUNT incoming subquery failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Bob" {
		t.Errorf("Expected Bob, got %v", result.Rows[0][0])
	}
}
