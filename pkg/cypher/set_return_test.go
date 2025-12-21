package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SET with RETURN Tests - Bug Fix: "matched" vs RETURN alias
// ============================================================================
//
// These tests verify that MATCH...SET...RETURN queries properly respect the
// RETURN clause alias instead of returning hardcoded "matched" column.
//
// BUG DISCOVERED: executeSetMerge was returning "matched" instead of parsing
// RETURN clause (line 1717 of executor.go):
//   result.Columns = []string{"matched"}  // ❌ WRONG
//   result.Rows = [][]interface{}{{len(matchResult.Rows)}}
//
// EXPECTED: Parse RETURN clause and return requested columns with proper aliases.
//
// Test Coverage:
// - RETURN n (single node variable)
// - RETURN n, m (multiple node variables)
// - RETURN n AS alias (with alias)
// - RETURN n.property (property projection)
// - RETURN n.property AS alias (property with alias)
// - RETURN id(n) (function calls)
// - RETURN n, id(n) AS nodeId (mixed expressions)
// - Edge case: No RETURN clause (should return matched count)
// - SET n += {props} RETURN n (merge operator)
// - SET n.a = 1, n.b = 2 RETURN n (multiple properties)

// TestSetReturnSingleVariable tests MATCH...SET...RETURN n
func TestSetReturnSingleVariable(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": int64(30)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET...RETURN n should return column "n", not "matched"
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
		RETURN n
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 1, "Should have 1 column")
	assert.Equal(t, "n", result.Columns[0], "Column should be 'n', not 'matched'")
	require.Len(t, result.Rows, 1, "Should have 1 row")

	// Verify the returned node has the updated property
	returnedNode, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok, "Result should be a *storage.Node")
	assert.Equal(t, "active", returnedNode.Properties["status"], "Node should have updated status")
	assert.Equal(t, "Alice", returnedNode.Properties["name"], "Node should retain original properties")
}

// TestSetReturnMultipleVariables tests MATCH...SET...RETURN n, m
// Tests SET with multiple node variables in a relationship pattern
func TestSetReturnMultipleVariables(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test nodes with a relationship
	node1 := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	node2 := &storage.Node{
		ID:         "node-2",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Bob"},
	}
	_, err := store.CreateNode(node1)
	require.NoError(t, err)
	_, err = store.CreateNode(node2)
	require.NoError(t, err)

	// Create relationship: Alice KNOWS Bob
	edge := &storage.Edge{
		ID:         "edge-1",
		Type:       "KNOWS",
		StartNode:  "node-1",
		EndNode:    "node-2",
		Properties: map[string]interface{}{},
	}
	err = store.CreateEdge(edge)
	require.NoError(t, err)

	// MATCH (n:Person)-[:KNOWS]->(m:Person) SET n.status = 'active' RETURN n, m
	// This tests multiple variables (n and m) in the RETURN clause
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})-[:KNOWS]->(m:Person {name: 'Bob'})
		SET n.status = 'active'
		RETURN n, m
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 2, "Should have 2 columns")
	assert.Equal(t, "n", result.Columns[0], "First column should be 'n'")
	assert.Equal(t, "m", result.Columns[1], "Second column should be 'm'")
	require.Len(t, result.Rows, 1, "Should have 1 row")

	// Verify returned nodes
	returnedN, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok, "First result should be a *storage.Node")
	assert.Equal(t, "active", returnedN.Properties["status"], "Node n should have updated status")
	assert.Equal(t, "Alice", returnedN.Properties["name"], "Node n should be Alice")

	returnedM, ok := result.Rows[0][1].(*storage.Node)
	require.True(t, ok, "Second result should be a *storage.Node")
	assert.Equal(t, "Bob", returnedM.Properties["name"], "Node m should be Bob")
}

// TestSetReturnWithAlias tests MATCH...SET...RETURN n AS alias
func TestSetReturnWithAlias(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET...RETURN n AS person
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
		RETURN n AS person
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 1, "Should have 1 column")
	assert.Equal(t, "person", result.Columns[0], "Column should use alias 'person'")
	require.Len(t, result.Rows, 1, "Should have 1 row")
}

// TestSetReturnProperty tests MATCH...SET...RETURN n.property
func TestSetReturnProperty(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": int64(30)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET...RETURN n.name, n.status
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
		RETURN n.name, n.status
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 2, "Should have 2 columns")
	assert.Equal(t, "n.name", result.Columns[0], "First column should be 'n.name'")
	assert.Equal(t, "n.status", result.Columns[1], "Second column should be 'n.status'")
	require.Len(t, result.Rows, 1, "Should have 1 row")
	assert.Equal(t, "Alice", result.Rows[0][0], "Should return name property")
	assert.Equal(t, "active", result.Rows[0][1], "Should return updated status property")
}

// TestSetReturnPropertyWithAlias tests MATCH...SET...RETURN n.property AS alias
func TestSetReturnPropertyWithAlias(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET...RETURN n.name AS fullName, n.status AS currentStatus
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
		RETURN n.name AS fullName, n.status AS currentStatus
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 2, "Should have 2 columns")
	assert.Equal(t, "fullName", result.Columns[0], "First column should use alias 'fullName'")
	assert.Equal(t, "currentStatus", result.Columns[1], "Second column should use alias 'currentStatus'")
	require.Len(t, result.Rows, 1, "Should have 1 row")
	assert.Equal(t, "Alice", result.Rows[0][0], "Should return name value")
	assert.Equal(t, "active", result.Rows[0][1], "Should return status value")
}

// TestSetReturnFunction tests MATCH...SET...RETURN id(n)
func TestSetReturnFunction(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET...RETURN id(n)
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
		RETURN id(n)
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 1, "Should have 1 column")
	assert.Equal(t, "id(n)", result.Columns[0], "Column should be 'id(n)'")
	require.Len(t, result.Rows, 1, "Should have 1 row")
	assert.Equal(t, "node-1", result.Rows[0][0], "Should return node ID")
}

// TestSetReturnMixedExpressions tests MATCH...SET...RETURN n, id(n) AS nodeId
func TestSetReturnMixedExpressions(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET...RETURN n.name, id(n) AS nodeId, n
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
		RETURN n.name, id(n) AS nodeId, n
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 3, "Should have 3 columns")
	assert.Equal(t, "n.name", result.Columns[0], "First column should be 'n.name'")
	assert.Equal(t, "nodeId", result.Columns[1], "Second column should use alias 'nodeId'")
	assert.Equal(t, "n", result.Columns[2], "Third column should be 'n'")
	require.Len(t, result.Rows, 1, "Should have 1 row")
}

// TestSetNoReturn tests MATCH...SET without RETURN clause
func TestSetNoReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET without RETURN should return matched count
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
	`, nil)

	require.NoError(t, err)
	// When no RETURN clause, executor should return empty result or matched count
	// This test documents current behavior - may return nothing or stats
	t.Logf("No RETURN result: Columns=%v, Rows=%v", result.Columns, result.Rows)
}

// TestSetMergeOperatorReturn tests SET n += {props} RETURN n
func TestSetMergeOperatorReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": int64(30)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET n += {...} RETURN n
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n += {status: 'active', city: 'NYC'}
		RETURN n
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 1, "Should have 1 column")
	assert.Equal(t, "n", result.Columns[0], "Column should be 'n', not 'matched'")
	require.Len(t, result.Rows, 1, "Should have 1 row")

	// Verify the returned node has merged properties
	returnedNode, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok, "Result should be a *storage.Node")
	assert.Equal(t, "active", returnedNode.Properties["status"], "Node should have new status")
	assert.Equal(t, "NYC", returnedNode.Properties["city"], "Node should have new city")
	assert.Equal(t, "Alice", returnedNode.Properties["name"], "Node should retain original name")
	assert.Equal(t, int64(30), returnedNode.Properties["age"], "Node should retain original age")
}

// TestSetMultiplePropertiesReturn tests SET n.a = 1, n.b = 2 RETURN n
func TestSetMultiplePropertiesReturn(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET multiple properties...RETURN n
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active', n.verified = true, n.score = 100
		RETURN n
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 1, "Should have 1 column")
	assert.Equal(t, "n", result.Columns[0], "Column should be 'n', not 'matched'")
	require.Len(t, result.Rows, 1, "Should have 1 row")

	// Verify all properties were set
	returnedNode, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok, "Result should be a *storage.Node")
	assert.Equal(t, "active", returnedNode.Properties["status"])
	assert.Equal(t, true, returnedNode.Properties["verified"])
	assert.Equal(t, int64(100), returnedNode.Properties["score"])
}

// TestSetReturnMultipleMatches tests SET with RETURN when matching multiple nodes
func TestSetReturnMultipleMatches(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create multiple test nodes
	nodes := []*storage.Node{
		{
			ID:         "node-1",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Alice", "role": "user"},
		},
		{
			ID:         "node-2",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Bob", "role": "user"},
		},
		{
			ID:         "node-3",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Carol", "role": "user"},
		},
	}
	for _, n := range nodes {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	// MATCH all users...SET...RETURN names
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {role: 'user'})
		SET n.status = 'active'
		RETURN n.name, n.status
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 2, "Should have 2 columns")
	assert.Equal(t, "n.name", result.Columns[0], "First column should be 'n.name'")
	assert.Equal(t, "n.status", result.Columns[1], "Second column should be 'n.status'")
	require.Len(t, result.Rows, 3, "Should have 3 rows (one per matched node)")

	// Verify all rows have the updated status
	names := make([]string, 0, 3)
	for _, row := range result.Rows {
		names = append(names, row[0].(string))
		assert.Equal(t, "active", row[1], "All matched nodes should have status='active'")
	}
	assert.Contains(t, names, "Alice")
	assert.Contains(t, names, "Bob")
	assert.Contains(t, names, "Carol")
}

// TestSetReturnComplexQuery tests SET with RETURN in complex query
func TestSetReturnComplexQuery(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice", "age": int64(30), "score": int64(100)},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// Complex query with multiple SET operations and complex RETURN
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active', n.lastSeen = 'now'
		RETURN n.name AS name, n.age AS age, n.status AS status, id(n) AS id
	`, nil)

	require.NoError(t, err)
	require.Len(t, result.Columns, 4, "Should have 4 columns")
	assert.Equal(t, "name", result.Columns[0], "Column 0 should use alias 'name'")
	assert.Equal(t, "age", result.Columns[1], "Column 1 should use alias 'age'")
	assert.Equal(t, "status", result.Columns[2], "Column 2 should use alias 'status'")
	assert.Equal(t, "id", result.Columns[3], "Column 3 should use alias 'id'")
	require.Len(t, result.Rows, 1, "Should have 1 row")
	assert.Equal(t, "Alice", result.Rows[0][0])
	assert.Equal(t, int64(30), result.Rows[0][1])
	assert.Equal(t, "active", result.Rows[0][2])
	assert.Equal(t, "node-1", result.Rows[0][3])
}

// ============================================================================
// Regression Tests - Ensure fix doesn't break existing functionality
// ============================================================================

// TestSetReturnStarRegression tests MATCH...SET...RETURN * still works
func TestSetReturnStarRegression(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET...RETURN * should return all matched variables
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'Alice'})
		SET n.status = 'active'
		RETURN *
	`, nil)

	require.NoError(t, err)
	assert.NotEmpty(t, result.Columns, "Should have at least one column")
	assert.NotEmpty(t, result.Rows, "Should have at least one row")
}

// TestSetWithoutMatchRegression tests that SET still requires MATCH
func TestSetWithoutMatchRegression(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// SET without MATCH should error
	_, err := exec.Execute(ctx, `
		SET n.status = 'active'
		RETURN n
	`, nil)

	assert.Error(t, err, "SET without MATCH should return an error")
	assert.Contains(t, err.Error(), "syntax error", "Error should indicate syntax error")
}

// ============================================================================
// Parameter Substitution Tests
// ============================================================================

// TestSetReturnWithParameters tests SET with parameters in RETURN
func TestSetReturnWithParameters(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// MATCH...SET with parameters...RETURN
	params := map[string]interface{}{
		"name":   "Alice",
		"status": "active",
	}
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: $name})
		SET n.status = $status
		RETURN n.name, n.status
	`, params)

	require.NoError(t, err)
	require.Len(t, result.Columns, 2, "Should have 2 columns")
	assert.Equal(t, "n.name", result.Columns[0])
	assert.Equal(t, "n.status", result.Columns[1])
	require.Len(t, result.Rows, 1, "Should have 1 row")
	assert.Equal(t, "Alice", result.Rows[0][0])
	assert.Equal(t, "active", result.Rows[0][1])
}

// ============================================================================
// Edge Cases
// ============================================================================

// TestSetReturnEmptyMatch tests SET...RETURN when MATCH finds no nodes
func TestSetReturnEmptyMatch(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// MATCH with no results...SET...RETURN
	result, err := exec.Execute(ctx, `
		MATCH (n:Person {name: 'NonExistent'})
		SET n.status = 'active'
		RETURN n
	`, nil)

	require.NoError(t, err)
	assert.Empty(t, result.Rows, "Should return 0 rows when no nodes match")
}

// TestSetReturnWhitespaceVariations tests various whitespace in RETURN
func TestSetReturnWhitespaceVariations(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test node
	node := &storage.Node{
		ID:         "node-1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "Alice"},
	}
	_, err := store.CreateNode(node)
	require.NoError(t, err)

	// Test various whitespace patterns
	queries := []string{
		"MATCH (n:Person {name: 'Alice'}) SET n.status = 'active' RETURN n",
		"MATCH (n:Person {name: 'Alice'})\nSET n.status = 'active'\nRETURN n",
		"MATCH (n:Person {name: 'Alice'})\n\tSET n.status = 'active'\n\tRETURN n",
		"MATCH (n:Person {name: 'Alice'})\n  SET n.status = 'active'\n  RETURN n",
	}

	for i, query := range queries {
		t.Run(fmt.Sprintf("whitespace_variation_%d", i), func(t *testing.T) {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			assert.Equal(t, "n", result.Columns[0], "Column should be 'n' regardless of whitespace")
		})
	}
}
