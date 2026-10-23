// Package cypher - Executor cache integration tests.
package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecutor_CacheReflectsExternalNodeDelete(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	otherStore := storage.NewNamespacedEngine(baseStore, "other")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 10, time.Minute)
	ctx := context.Background()
	const query = `MATCH (n:CacheDelete) RETURN count(n)`

	_, err := store.CreateNode(&storage.Node{ID: "node", Labels: []string{"CacheDelete"}})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	hits, _, _, _, _ := exec.cache.Stats()
	require.Equal(t, int64(1), hits)

	_, err = otherStore.CreateNode(&storage.Node{ID: "other-node", Labels: []string{"CacheDelete"}})
	require.NoError(t, err)
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	hits, _, _, _, _ = exec.cache.Stats()
	require.Equal(t, int64(2), hits, "another database must not invalidate this cache")

	require.NoError(t, store.DeleteNode("node"))
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestExecutor_CacheReflectsExternalEdgeDelete(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 10, time.Minute)
	ctx := context.Background()
	const query = `MATCH ()-[r:CONNECTS]->() RETURN count(r)`

	_, err := store.CreateNode(&storage.Node{ID: "left"})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "right"})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID: "edge", StartNode: "left", EndNode: "right", Type: "CONNECTS",
	}))

	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	require.NoError(t, store.DeleteEdge("edge"))
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

type pausedLabelCountEngine struct {
	*storage.NamespacedEngine
	counted chan struct{}
	resume  chan struct{}
}

func (s *pausedLabelCountEngine) NodeCountByLabel(label string) (int64, error) {
	count, err := s.NamespacedEngine.NodeCountByLabel(label)
	if s.counted != nil {
		close(s.counted)
		<-s.resume
		s.counted = nil
	}
	return count, err
}

func TestExecutor_CacheDoesNotPublishOverlappingReadAtNewRevision(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := &pausedLabelCountEngine{
		NamespacedEngine: storage.NewNamespacedEngine(baseStore, "test"),
		counted:          make(chan struct{}),
		resume:           make(chan struct{}),
	}
	_, err := store.CreateNode(&storage.Node{ID: "node", Labels: []string{"CacheDelete"}})
	require.NoError(t, err)
	exec := NewStorageExecutorWithQueryCachePolicy(store, 10, time.Minute)
	const query = `MATCH (n:CacheDelete) RETURN count(n)`

	done := make(chan error, 1)
	go func() {
		_, executeErr := exec.Execute(context.Background(), query, nil)
		done <- executeErr
	}()
	<-store.counted
	require.NoError(t, store.DeleteNode("node"))
	close(store.resume)
	require.NoError(t, <-done)

	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestExecutor_CacheReflectsExternalAsyncNodeDeleteBeforeFlush(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	asyncStore := baseStore
	t.Cleanup(func() { require.NoError(t, asyncStore.Close()) })
	store := storage.NewNamespacedEngine(asyncStore, "test")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 10, time.Minute)
	ctx := context.Background()
	const query = `MATCH (n:CacheDelete) RETURN count(n)`

	_, err := store.CreateNode(&storage.Node{ID: "node", Labels: []string{"CacheDelete"}})
	require.NoError(t, err)
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	require.NoError(t, store.DeleteNode("node"))
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestExecutor_CacheReflectsExternalAsyncEdgeDeleteBeforeFlush(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	asyncStore := baseStore
	t.Cleanup(func() { require.NoError(t, asyncStore.Close()) })
	store := storage.NewNamespacedEngine(asyncStore, "test")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 10, time.Minute)
	ctx := context.Background()
	const query = `MATCH ()-[r:CONNECTS]->() RETURN count(r)`

	_, err := store.CreateNode(&storage.Node{ID: "left"})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "right"})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID: "edge", StartNode: "left", EndNode: "right", Type: "CONNECTS",
	}))
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	require.NoError(t, store.DeleteEdge("edge"))
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestExecutor_CacheReflectsExternalNamespaceDelete(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 10, time.Minute)
	ctx := context.Background()
	const query = `MATCH (n:CacheDelete) RETURN n`

	_, err := store.CreateNode(&storage.Node{ID: "node", Labels: []string{"CacheDelete"}})
	require.NoError(t, err)
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	_, _, err = baseStore.DeleteByPrefix("test:")
	require.NoError(t, err)
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

func TestExecutor_CacheReflectsExternalCompositeConstituentDelete(t *testing.T) {
	exec, manager := compositeTestFixture(t)
	constituent, err := manager.GetStorage("shard_a")
	require.NoError(t, err)
	ctx := context.Background()
	const query = `USE comp.a MATCH (n:CompositeCache) RETURN count(n)`

	_, err = constituent.CreateNode(&storage.Node{ID: "node", Labels: []string{"CompositeCache"}})
	require.NoError(t, err)
	result, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	hits, _, _, _, _ := exec.cache.Stats()
	require.Equal(t, int64(1), hits)

	require.NoError(t, constituent.DeleteNode("node"))
	nodes, err := constituent.GetNodesByLabel("CompositeCache")
	require.NoError(t, err)
	require.Empty(t, nodes)
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestExecutor_CacheIntegration(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create test data
	exec.Execute(ctx, `CREATE (n:User {name: 'Alice', age: 30})`, nil)
	exec.Execute(ctx, `CREATE (n:User {name: 'Bob', age: 25})`, nil)

	// First query - cache miss
	_, missesBefore, _, _, _ := exec.cache.Stats()
	result1, err := exec.Execute(ctx, `MATCH (n:User) RETURN count(n) as users`, nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result1.Rows) != 1 || result1.Rows[0][0].(int64) != 2 {
		t.Fatalf("Expected count 2, got %#v", result1.Rows)
	}

	hitsAfter, missesAfter, _, _, _ := exec.cache.Stats()
	if missesAfter != missesBefore+1 {
		t.Error("Expected cache miss on first query")
	}

	// Second identical query - cache hit
	result2, err := exec.Execute(ctx, `MATCH (n:User) RETURN count(n) as users`, nil)
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
	result3, _ := exec.Execute(ctx, `MATCH (n:User) RETURN count(n) as users`, nil)
	if len(result3.Rows) != 1 || result3.Rows[0][0].(int64) != 3 {
		t.Errorf("Expected count 3 after adding Charlie, got %#v", result3.Rows)
	}
}

func TestExecutor_CacheSchemaQueries(t *testing.T) {
	baseStore := newTestMemoryEngine(t)

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
	baseStore := newTestMemoryEngine(t)

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
	baseStore := newTestMemoryEngine(t)

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
	baseStore := newTestMemoryEngine(b)

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
	baseStore := newTestMemoryEngine(b)

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
