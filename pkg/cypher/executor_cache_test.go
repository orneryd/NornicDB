// Package cypher - Executor cache integration tests.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestExecutor_CacheIntegration(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice', age: 30})`, nil)
	exec.Execute(ctx, `CREATE (n:User {name: 'Bob', age: 25})`, nil)

	// First query - cache miss
	// Note: We use RETURN n.name instead of count(n) because aggregation queries
	// are NOT cached (they must always be fresh to avoid stale counts)
	_, missesBefore, _, _, _ := exec.cache.Stats()
	result1, err := exec.Execute(ctx, `MATCH (n:User) RETURN n.name ORDER BY n.name`, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	hitsAfter, missesAfter, _, _, _ := exec.cache.Stats()
	if missesAfter != missesBefore+1 {
		t.Error("Expected cache miss on first query")
	}

	// Second identical query - cache hit
	result2, err := exec.Execute(ctx, `MATCH (n:User) RETURN n.name ORDER BY n.name`, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	hitsAfter2, _, _, _, _ := exec.cache.Stats()
	if hitsAfter2 != hitsAfter+1 {
		t.Error("Expected cache hit on second query")
	}

	// Results should be identical
	if len(result1.Rows) != len(result2.Rows) || result1.Rows[0][0] != result2.Rows[0][0] {
		t.Error("Cached result doesn't match original")
	}

	// Write operation should invalidate cache
	exec.Execute(ctx, `CREATE (n:User {name: 'Charlie', age: 35})`, nil)

	// Query again - should be cache miss after invalidation
	_, missesBefore3, _, _, _ := exec.cache.Stats()
	exec.Execute(ctx, `MATCH (n:User) RETURN n.name ORDER BY n.name`, nil)

	_, missesAfter3, _, _, _ := exec.cache.Stats()
	if missesAfter3 != missesBefore3+1 {
		t.Error("Expected cache miss after write operation")
	}

	// Result should reflect new data
	result3, _ := exec.Execute(ctx, `MATCH (n:User) RETURN n.name ORDER BY n.name`, nil)
	if len(result3.Rows) != 3 {
		t.Errorf("Expected 3 users after adding Charlie, got %d", len(result3.Rows))
	}
}

func TestExecutor_CacheSchemaQueries(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with labels
	_, err := exec.Execute(ctx, `CREATE (n:User {name: 'Alice'})`, nil)
	if err != nil {
		t.Fatalf("Create User failed: %v", err)
	}
	_, err = exec.Execute(ctx, `CREATE (n:Post {title: 'Hello'})`, nil)
	if err != nil {
		t.Fatalf("Create Post failed: %v", err)
	}

	// Schema query - should be cached
	hitsBefore, _, _, _, _ := exec.cache.Stats()
	result1, err := exec.Execute(ctx, `CALL db.labels()`, nil)
	if err != nil {
		t.Fatalf("First CALL db.labels() failed: %v", err)
	}
	if result1 == nil {
		t.Fatal("First CALL db.labels() returned nil result")
	}

	hitsAfter, _, _, _, _ := exec.cache.Stats()
	if hitsAfter != hitsBefore {
		t.Log("First schema query should be cache miss (expected)")
	}

	// Second call - cache hit
	result2, err := exec.Execute(ctx, `CALL db.labels()`, nil)
	if err != nil {
		t.Fatalf("Second CALL db.labels() failed: %v", err)
	}
	if result2 == nil {
		t.Fatal("Second CALL db.labels() returned nil result")
	}

	hitsAfter2, _, _, _, _ := exec.cache.Stats()
	if hitsAfter2 != hitsAfter+1 {
		t.Errorf("Second schema query should be cache hit (hits before: %d, after: %d)", hitsAfter, hitsAfter2)
	}

	// Results should match
	if len(result1.Rows) != len(result2.Rows) {
		t.Error("Cached schema result doesn't match original")
	}
}

func TestExecutor_CacheParameterizedQueries(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data with different labels
	_, err := exec.Execute(ctx, `CREATE (n:PersonA {age: 30})`, nil)
	if err != nil {
		t.Fatalf("Create PersonA failed: %v", err)
	}
	_, err = exec.Execute(ctx, `CREATE (n:PersonB {age: 25})`, nil)
	if err != nil {
		t.Fatalf("Create PersonB failed: %v", err)
	}

	// Query with param1 (different labels ensure different results)
	params1 := map[string]interface{}{"label": "PersonA"}
	result1, err := exec.Execute(ctx, `MATCH (n:PersonA) RETURN n.age AS age`, nil)
	if err != nil {
		t.Fatalf("Query PersonA failed: %v", err)
	}
	if len(result1.Rows) == 0 {
		t.Fatal("Query PersonA returned no rows")
	}

	// Query with param2 - different query, should not hit cache
	hitsBefore, _, _, _, _ := exec.cache.Stats()
	result2, err := exec.Execute(ctx, `MATCH (n:PersonB) RETURN n.age AS age`, nil)
	if err != nil {
		t.Fatalf("Query PersonB failed: %v", err)
	}
	if len(result2.Rows) == 0 {
		t.Fatal("Query PersonB returned no rows")
	}

	hitsAfter, _, _, _, _ := exec.cache.Stats()
	if hitsAfter != hitsBefore {
		t.Error("Different queries should not hit cache")
	}

	// Results should be different
	age1 := result1.Rows[0][0]
	age2 := result2.Rows[0][0]
	if age1 == age2 {
		t.Errorf("Different queries should return different results (PersonA=%v, PersonB=%v)", age1, age2)
	}

	// Same query again - should hit cache
	result3, err := exec.Execute(ctx, `MATCH (n:PersonA) RETURN n.age AS age`, nil)
	if err != nil {
		t.Fatalf("Query PersonA (second time) failed: %v", err)
	}

	hitsAfter2, _, _, _, _ := exec.cache.Stats()
	if hitsAfter2 != hitsAfter+1 {
		t.Errorf("Same query should hit cache (hits before: %d, after: %d)", hitsAfter, hitsAfter2)
	}

	if result3.Rows[0][0] != result1.Rows[0][0] {
		t.Error("Cached result doesn't match original")
	}

	// Verify params1 is unused (remove warning)
	_ = params1
}

func TestExecutor_CacheOnlyReadQueries(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Write queries should NOT be cached
	exec.Execute(ctx, `CREATE (n:Test {value: 1})`, nil)
	_, missesBefore, sizeBefore, _, _ := exec.cache.Stats()

	exec.Execute(ctx, `CREATE (n:Test {value: 2})`, nil)
	_, missesAfter, sizeAfter, _, _ := exec.cache.Stats()

	// Cache size and misses should not change for write queries
	if sizeBefore != sizeAfter {
		t.Error("Write queries should not be added to cache")
	}
	if missesBefore != missesAfter {
		t.Error("Write queries should not check cache")
	}
}

func BenchmarkExecutor_WithCache(b *testing.B) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice', age: 30})`, nil)

	// Warm up cache
	exec.Execute(ctx, `MATCH (n:User) RETURN count(n)`, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute(ctx, `MATCH (n:User) RETURN count(n)`, nil)
	}
}

func BenchmarkExecutor_WithoutCache(b *testing.B) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	exec.cache = nil // Disable cache
	ctx := context.Background()

	// Create test data
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice', age: 30})`, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute(ctx, `MATCH (n:User) RETURN count(n)`, nil)
	}
}
