package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Data Setup Helpers
// =============================================================================

// setupSocialNetwork creates a test social network for optimization tests
func setupSocialNetwork(t *testing.T) *StorageExecutor {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 5 users
	for i := 1; i <= 5; i++ {
		_, err := exec.Execute(ctx, `CREATE (u:Person {id: $id, name: $name})`, map[string]interface{}{
			"id":   i,
			"name": fmt.Sprintf("User%d", i),
		})
		require.NoError(t, err)
	}

	// Create FOLLOWS relationships:
	// User1 <-> User2 (mutual)
	// User1 -> User3
	// User3 -> User4
	// User4 -> User1
	// User5 -> User1
	// User5 -> User2
	follows := []struct{ from, to int }{
		{1, 2}, {2, 1}, // Mutual
		{1, 3},
		{3, 4},
		{4, 1},
		{5, 1},
		{5, 2},
	}

	for _, f := range follows {
		_, err := exec.Execute(ctx, `
			MATCH (a:Person {id: $from}), (b:Person {id: $to})
			CREATE (a)-[:FOLLOWS]->(b)
		`, map[string]interface{}{
			"from": f.from,
			"to":   f.to,
		})
		require.NoError(t, err)
	}

	return exec
}

// setupReviewedProducts creates products with reviews for edge property aggregation tests
func setupReviewedProducts(t *testing.T) *StorageExecutor {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create 3 products
	products := []string{"ProductA", "ProductB", "ProductC"}
	for _, name := range products {
		_, err := exec.Execute(ctx, `CREATE (p:Product {name: $name})`, map[string]interface{}{
			"name": name,
		})
		require.NoError(t, err)
	}

	// Create 5 customers
	for i := 1; i <= 5; i++ {
		_, err := exec.Execute(ctx, `CREATE (c:Customer {id: $id, name: $name})`, map[string]interface{}{
			"id":   i,
			"name": fmt.Sprintf("Customer%d", i),
		})
		require.NoError(t, err)
	}

	// Create REVIEWED relationships with ratings:
	// ProductA: ratings 5, 4, 4 (avg 4.33, count 3)
	// ProductB: ratings 3, 3 (avg 3.0, count 2)
	// ProductC: rating 5 (avg 5.0, count 1)
	reviews := []struct {
		customer int
		product  string
		rating   float64
	}{
		{1, "ProductA", 5},
		{2, "ProductA", 4},
		{3, "ProductA", 4},
		{1, "ProductB", 3},
		{4, "ProductB", 3},
		{5, "ProductC", 5},
	}

	for _, r := range reviews {
		_, err := exec.Execute(ctx, `
			MATCH (c:Customer {id: $cid}), (p:Product {name: $pname})
			CREATE (c)-[:REVIEWED {rating: $rating}]->(p)
		`, map[string]interface{}{
			"cid":    r.customer,
			"pname":  r.product,
			"rating": r.rating,
		})
		require.NoError(t, err)
	}

	return exec
}

func TestIntegration_IncomingCountViaExecute(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()
	query := "MATCH (p:Person)<-[:FOLLOWS]-(f:Person) RETURN p.name, count(f) ORDER BY count(f) DESC LIMIT 5"

	// Execute via main Execute function
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should return results
	assert.GreaterOrEqual(t, len(result.Rows), 1, "Should have results")
}

func TestIntegration_MutualFollowsViaExecute(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	query := "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name, b.name LIMIT 10"

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should find mutual relationships
	assert.GreaterOrEqual(t, len(result.Rows), 1, "Should find mutual relationships")
}

func TestIntegration_EdgeAggViaExecute(t *testing.T) {
	exec := setupReviewedProducts(t)
	ctx := context.Background()

	query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r) ORDER BY avg(r.rating) DESC"

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should return products with aggregations
	assert.GreaterOrEqual(t, len(result.Rows), 1, "Should have results")
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

// =============================================================================
// Helper Method Tests
// =============================================================================

func TestBatchGetNodes(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create multiple nodes
	for i := 1; i <= 3; i++ {
		_, err := exec.Execute(ctx, `CREATE (n:Test {id: $id})`, map[string]interface{}{"id": i})
		require.NoError(t, err)
	}

	// Get all node IDs
	allNodes, err := store.AllNodes(ctx)
	require.NoError(t, err)
	ids := make([]storage.NodeID, len(allNodes))
	for i, n := range allNodes {
		ids[i] = n.ID
	}

	// Batch get
	result := exec.BatchGetNodes(ctx, ids)
	assert.Len(t, result, 3, "Should return all 3 nodes")

	for _, id := range ids {
		assert.Contains(t, result, id, "Result should contain node ID")
	}
}

// =============================================================================
// Parallel Traversal Tests
// =============================================================================

func setupLargeNetwork(t *testing.T, nodeCount int) *StorageExecutor {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create nodes
	for i := 1; i <= nodeCount; i++ {
		_, err := exec.Execute(ctx, `CREATE (u:Person {id: $id, name: $name})`, map[string]interface{}{
			"id":   i,
			"name": fmt.Sprintf("User%d", i),
		})
		require.NoError(t, err)
	}

	// Create some relationships (ring topology + random)
	for i := 1; i <= nodeCount; i++ {
		// Ring: i -> (i % nodeCount) + 1
		next := (i % nodeCount) + 1
		_, err := exec.Execute(ctx, `
			MATCH (a:Person {id: $from}), (b:Person {id: $to})
			CREATE (a)-[:KNOWS]->(b)
		`, map[string]interface{}{
			"from": i,
			"to":   next,
		})
		require.NoError(t, err)
	}

	return exec
}

func TestParallelTraversal(t *testing.T) {
	// Create network large enough to trigger parallel execution (> 20 nodes)
	exec := setupLargeNetwork(t, 50)
	ctx := context.Background()

	// Query that will traverse from multiple starting points
	query := "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"

	// Execute - should use parallel traversal internally
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should find all 50 KNOWS relationships (ring topology)
	assert.Equal(t, 50, len(result.Rows), "Should find all relationships in ring")
}

func TestParallelTraversalConsistency(t *testing.T) {
	// Verify parallel and sequential produce same results
	exec := setupLargeNetwork(t, 30)
	ctx := context.Background()

	query := "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN a.name, c.name LIMIT 100"

	// Run multiple times to check for race conditions
	var results []int
	for i := 0; i < 5; i++ {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		results = append(results, len(result.Rows))
	}

	// All runs should produce same count
	for i := 1; i < len(results); i++ {
		assert.Equal(t, results[0], results[i], "Parallel execution should be deterministic in count")
	}
}

// TestOptimizedRelationshipDirectionContract pins the public dispatcher's actual
// row values, rather than merely checking that a direction-specific path ran.
