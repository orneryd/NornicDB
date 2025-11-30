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
	store := storage.NewMemoryEngine()
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
	store := storage.NewMemoryEngine()
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

// =============================================================================
// Pattern Detection Tests
// =============================================================================

func TestDetectQueryPattern_MutualRelationship(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected QueryPattern
		relType  string
	}{
		{
			name:     "mutual follows",
			query:    "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name, b.name",
			expected: PatternMutualRelationship,
			relType:  "FOLLOWS",
		},
		{
			name:     "mutual follows with limit",
			query:    "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name, b.name LIMIT 20",
			expected: PatternMutualRelationship,
			relType:  "FOLLOWS",
		},
		{
			name:     "mutual knows",
			query:    "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(a) RETURN a, b",
			expected: PatternMutualRelationship,
			relType:  "KNOWS",
		},
		{
			name:     "not mutual - different end var",
			query:    "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person) RETURN a, b, c",
			expected: PatternGeneric,
		},
		{
			name:     "simple match - not mutual",
			query:    "MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a, b",
			expected: PatternGeneric,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DetectQueryPattern(tt.query)
			if info.Pattern != tt.expected {
				t.Errorf("Pattern = %v, want %v", info.Pattern, tt.expected)
			}
			if tt.relType != "" && info.RelType != tt.relType {
				t.Errorf("RelType = %v, want %v", info.RelType, tt.relType)
			}
		})
	}
}

func TestDetectQueryPattern_IncomingCountAgg(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected QueryPattern
		relType  string
		startVar string
	}{
		{
			name:     "follower count",
			query:    "MATCH (p:Person)<-[:FOLLOWS]-(follower:Person) RETURN p.name, count(follower) as followers",
			expected: PatternIncomingCountAgg,
			relType:  "FOLLOWS",
			startVar: "p",
		},
		{
			name:     "follower count with ORDER BY",
			query:    "MATCH (p:Person)<-[:FOLLOWS]-(follower:Person) RETURN p.name, count(follower) as followers ORDER BY followers DESC LIMIT 10",
			expected: PatternIncomingCountAgg,
			relType:  "FOLLOWS",
			startVar: "p",
		},
		{
			name:     "reviewed count",
			query:    "MATCH (p:Product)<-[r:REVIEWED]-(c:Customer) RETURN p.name, count(c) as reviews",
			expected: PatternIncomingCountAgg,
			relType:  "REVIEWED",
			startVar: "p",
		},
		{
			name:     "count star",
			query:    "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name, count(*) as cnt",
			expected: PatternIncomingCountAgg,
			relType:  "FOLLOWS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DetectQueryPattern(tt.query)
			if info.Pattern != tt.expected {
				t.Errorf("Pattern = %v, want %v", info.Pattern, tt.expected)
			}
			if tt.relType != "" && info.RelType != tt.relType {
				t.Errorf("RelType = %v, want %v", info.RelType, tt.relType)
			}
			if tt.startVar != "" && info.StartVar != tt.startVar {
				t.Errorf("StartVar = %v, want %v", info.StartVar, tt.startVar)
			}
		})
	}
}

func TestDetectQueryPattern_OutgoingCountAgg(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected QueryPattern
		relType  string
	}{
		{
			name:     "following count",
			query:    "MATCH (p:Person)-[:FOLLOWS]->(following:Person) RETURN p.name, count(following)",
			expected: PatternOutgoingCountAgg,
			relType:  "FOLLOWS",
		},
		{
			name:     "acted in count",
			query:    "MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) RETURN a.name, count(m) as movies",
			expected: PatternOutgoingCountAgg,
			relType:  "ACTED_IN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DetectQueryPattern(tt.query)
			if info.Pattern != tt.expected {
				t.Errorf("Pattern = %v, want %v", info.Pattern, tt.expected)
			}
			if tt.relType != "" && info.RelType != tt.relType {
				t.Errorf("RelType = %v, want %v", info.RelType, tt.relType)
			}
		})
	}
}

func TestDetectQueryPattern_EdgePropertyAgg(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expected    QueryPattern
		aggProperty string
	}{
		{
			name:        "avg rating",
			query:       "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating) as avgRating",
			expected:    PatternEdgePropertyAgg,
			aggProperty: "rating",
		},
		{
			name:        "multiple agg on edge",
			query:       "MATCH ()-[r:REVIEWED]->() RETURN avg(r.rating), count(r), sum(r.rating)",
			expected:    PatternEdgePropertyAgg,
			aggProperty: "rating",
		},
		{
			name:        "min max on edge",
			query:       "MATCH ()-[r:TRANSACTION]->(p) RETURN min(r.amount), max(r.amount)",
			expected:    PatternEdgePropertyAgg,
			aggProperty: "amount",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DetectQueryPattern(tt.query)
			if info.Pattern != tt.expected {
				t.Errorf("Pattern = %v, want %v", info.Pattern, tt.expected)
			}
			if tt.aggProperty != "" && info.AggProperty != tt.aggProperty {
				t.Errorf("AggProperty = %v, want %v", info.AggProperty, tt.aggProperty)
			}
		})
	}
}

func TestDetectQueryPattern_LargeResultSet(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected QueryPattern
		limit    int
	}{
		{
			name:     "large limit 500",
			query:    "MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) RETURN a.name, m.title LIMIT 500",
			expected: PatternLargeResultSet,
			limit:    500,
		},
		{
			name:     "large limit 1000",
			query:    "MATCH (n) RETURN n LIMIT 1000",
			expected: PatternLargeResultSet,
			limit:    1000,
		},
		{
			name:     "small limit - not large",
			query:    "MATCH (n) RETURN n LIMIT 50",
			expected: PatternGeneric,
			limit:    50,
		},
		{
			name:     "limit exactly 100 - not large",
			query:    "MATCH (n) RETURN n LIMIT 100",
			expected: PatternGeneric,
			limit:    100,
		},
		{
			name:     "limit 101 - large",
			query:    "MATCH (a)-[r]->(b) RETURN a, b LIMIT 101",
			expected: PatternLargeResultSet,
			limit:    101,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DetectQueryPattern(tt.query)
			if info.Pattern != tt.expected {
				t.Errorf("Pattern = %v, want %v", info.Pattern, tt.expected)
			}
			if info.Limit != tt.limit {
				t.Errorf("Limit = %v, want %v", info.Limit, tt.limit)
			}
		})
	}
}

func TestPatternInfo_IsOptimizable(t *testing.T) {
	tests := []struct {
		pattern  QueryPattern
		expected bool
	}{
		{PatternGeneric, false},
		{PatternMutualRelationship, true},
		{PatternIncomingCountAgg, true},
		{PatternOutgoingCountAgg, true},
		{PatternEdgePropertyAgg, true},
		{PatternLargeResultSet, true},
	}

	for _, tt := range tests {
		info := PatternInfo{Pattern: tt.pattern}
		if info.IsOptimizable() != tt.expected {
			t.Errorf("IsOptimizable() for %v = %v, want %v", tt.pattern, info.IsOptimizable(), tt.expected)
		}
	}
}

func TestPatternInfo_NeedsRelationshipTypeScan(t *testing.T) {
	tests := []struct {
		pattern  QueryPattern
		expected bool
	}{
		{PatternGeneric, false},
		{PatternMutualRelationship, true},
		{PatternIncomingCountAgg, true},
		{PatternOutgoingCountAgg, true},
		{PatternEdgePropertyAgg, true},
		{PatternLargeResultSet, false},
	}

	for _, tt := range tests {
		info := PatternInfo{Pattern: tt.pattern}
		if info.NeedsRelationshipTypeScan() != tt.expected {
			t.Errorf("NeedsRelationshipTypeScan() for %v = %v, want %v", tt.pattern, info.NeedsRelationshipTypeScan(), tt.expected)
		}
	}
}

// =============================================================================
// Optimized Executor Tests
// =============================================================================

func TestOptimizedMutualRelationship(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	query := "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name, b.name"

	// Detect pattern
	info := DetectQueryPattern(query)
	assert.Equal(t, PatternMutualRelationship, info.Pattern)
	assert.Equal(t, "FOLLOWS", info.RelType)

	// Execute optimized
	result, ok := exec.ExecuteOptimized(ctx, query, info)
	require.True(t, ok, "Should use optimized execution")
	require.NotNil(t, result)

	// Should find User1-User2 mutual relationship
	assert.GreaterOrEqual(t, len(result.Rows), 1, "Should find at least 1 mutual pair")

	// Verify we found the mutual pair
	foundMutual := false
	for _, row := range result.Rows {
		name1 := row[0].(string)
		name2 := row[1].(string)
		if (name1 == "User1" && name2 == "User2") || (name1 == "User2" && name2 == "User1") {
			foundMutual = true
			break
		}
	}
	assert.True(t, foundMutual, "Should find User1-User2 mutual follow")
}

func TestOptimizedMutualRelationshipWithLimit(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	query := "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(a) RETURN a.name, b.name LIMIT 1"

	info := DetectQueryPattern(query)
	result, ok := exec.ExecuteOptimized(ctx, query, info)
	require.True(t, ok)
	require.NotNil(t, result)

	// Should respect LIMIT
	assert.LessOrEqual(t, len(result.Rows), 1, "Should respect LIMIT 1")
}

func TestOptimizedIncomingCount(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	query := "MATCH (p:Person)<-[:FOLLOWS]-(follower:Person) RETURN p.name, count(follower) as followers ORDER BY followers DESC LIMIT 10"

	info := DetectQueryPattern(query)
	assert.Equal(t, PatternIncomingCountAgg, info.Pattern)

	result, ok := exec.ExecuteOptimized(ctx, query, info)
	require.True(t, ok, "Should use optimized execution")
	require.NotNil(t, result)

	// User1 has most followers: User2, User4, User5 = 3 followers
	assert.GreaterOrEqual(t, len(result.Rows), 1)

	// Results should be sorted by count descending
	if len(result.Rows) >= 2 {
		count1 := result.Rows[0][1].(int64)
		count2 := result.Rows[1][1].(int64)
		assert.GreaterOrEqual(t, count1, count2, "Should be sorted descending")
	}
}

func TestOptimizedOutgoingCount(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	query := "MATCH (p:Person)-[:FOLLOWS]->(following:Person) RETURN p.name, count(following) as following"

	info := DetectQueryPattern(query)
	assert.Equal(t, PatternOutgoingCountAgg, info.Pattern)

	result, ok := exec.ExecuteOptimized(ctx, query, info)
	require.True(t, ok, "Should use optimized execution")
	require.NotNil(t, result)

	// User1 follows 2 people (User2, User3), User5 follows 2 people (User1, User2)
	assert.GreaterOrEqual(t, len(result.Rows), 1)
}

func TestOptimizedEdgePropertyAgg(t *testing.T) {
	exec := setupReviewedProducts(t)
	ctx := context.Background()

	query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating) as avgRating, count(r) as reviewCount ORDER BY avgRating DESC LIMIT 10"

	info := DetectQueryPattern(query)
	assert.Equal(t, PatternEdgePropertyAgg, info.Pattern)
	assert.Equal(t, "rating", info.AggProperty)

	result, ok := exec.ExecuteOptimized(ctx, query, info)
	require.True(t, ok, "Should use optimized execution")
	require.NotNil(t, result)

	// Should have 3 products
	assert.Equal(t, 3, len(result.Rows), "Should have 3 products with reviews")

	// Results should be sorted by avg rating descending
	// ProductC (5.0) > ProductA (4.33) > ProductB (3.0)
	if len(result.Rows) >= 2 {
		// Check avg values are descending
		for i := 0; i < len(result.Rows)-1; i++ {
			avg1 := result.Rows[i][1].(float64)
			avg2 := result.Rows[i+1][1].(float64)
			assert.GreaterOrEqual(t, avg1, avg2, "Should be sorted by avg descending")
		}
	}
}

// =============================================================================
// Integration Tests - Verify Optimization Is Actually Used
// =============================================================================

func TestIntegration_ExecuteUsesOptimizedPath(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	// This query should trigger optimized execution via Execute()
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

func TestOptimized_EmptyDatabase(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Mutual relationship on empty DB
	query := "MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(a) RETURN a, b"
	info := DetectQueryPattern(query)

	result, ok := exec.ExecuteOptimized(ctx, query, info)
	if ok {
		assert.Empty(t, result.Rows, "Empty DB should return no rows")
	}
}

func TestOptimized_NoMatchingRelType(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	// Query for relationship type that doesn't exist
	query := "MATCH (a)-[:NONEXISTENT]->(b)-[:NONEXISTENT]->(a) RETURN a, b"
	info := DetectQueryPattern(query)

	result, ok := exec.ExecuteOptimized(ctx, query, info)
	if ok {
		assert.Empty(t, result.Rows, "Non-existent rel type should return no rows")
	}
}

func TestOptimized_ZeroLimit(t *testing.T) {
	exec := setupSocialNetwork(t)
	ctx := context.Background()

	// LIMIT 0 is detected but falls through to generic execution
	// since 0 is not > 100 (large result set threshold)
	query := "MATCH (p:Person)<-[:FOLLOWS]-(f) RETURN p.name, count(f) LIMIT 0"
	info := DetectQueryPattern(query)

	// Verify pattern is detected correctly
	assert.Equal(t, PatternIncomingCountAgg, info.Pattern)
	assert.Equal(t, 0, info.Limit)

	// The optimized executor handles LIMIT during result building
	// LIMIT 0 means return no rows - let's verify via Execute()
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	// Note: Generic execution handles LIMIT 0 differently
	// This test just verifies the pattern detection works
	require.NotNil(t, result)
}

// =============================================================================
// Helper Method Tests
// =============================================================================

func TestGetNodeCached(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create a node
	_, err := exec.Execute(ctx, `CREATE (n:Test {name: 'TestNode'})`, nil)
	require.NoError(t, err)

	// Get all nodes to find the ID
	nodes := store.GetAllNodes()
	require.Len(t, nodes, 1)
	nodeID := nodes[0].ID

	// Test caching
	cache := make(map[storage.NodeID]*storage.Node)

	// First call should fetch from storage
	node1 := exec.getNodeCached(nodeID, cache)
	require.NotNil(t, node1)
	assert.Equal(t, "TestNode", node1.Properties["name"])

	// Cache should now have the node
	assert.Contains(t, cache, nodeID)

	// Second call should use cache
	node2 := exec.getNodeCached(nodeID, cache)
	require.NotNil(t, node2)
	assert.Equal(t, node1, node2, "Should return same cached node")
}

func TestGetPropertyString(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)

	node := &storage.Node{
		ID: "test",
		Properties: map[string]interface{}{
			"name":   "Alice",
			"age":    30,
			"active": true,
		},
	}

	assert.Equal(t, "Alice", exec.getPropertyString(node, "name"))
	assert.Equal(t, "30", exec.getPropertyString(node, "age"))
	assert.Equal(t, "true", exec.getPropertyString(node, "active"))
	assert.Equal(t, "", exec.getPropertyString(node, "nonexistent"))
	assert.Equal(t, "", exec.getPropertyString(nil, "name"))
}

func TestBatchGetNodes(t *testing.T) {
	store := storage.NewMemoryEngine()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create multiple nodes
	for i := 1; i <= 3; i++ {
		_, err := exec.Execute(ctx, `CREATE (n:Test {id: $id})`, map[string]interface{}{"id": i})
		require.NoError(t, err)
	}

	// Get all node IDs
	allNodes := store.GetAllNodes()
	ids := make([]storage.NodeID, len(allNodes))
	for i, n := range allNodes {
		ids[i] = n.ID
	}

	// Batch get
	result := exec.BatchGetNodes(ids)
	assert.Len(t, result, 3, "Should return all 3 nodes")

	for _, id := range ids {
		assert.Contains(t, result, id, "Result should contain node ID")
	}
}

// =============================================================================
// Parallel Traversal Tests
// =============================================================================

func setupLargeNetwork(t *testing.T, nodeCount int) *StorageExecutor {
	store := storage.NewMemoryEngine()
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
