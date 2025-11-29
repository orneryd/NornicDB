// Package cypher - Query cache tests.
package cypher

import (
	"sync"
	"testing"
	"time"
)

func TestQueryCache_Basic(t *testing.T) {
	cache := NewQueryCache(10)

	result := &ExecuteResult{
		Columns: []string{"n"},
		Rows:    [][]interface{}{{"value"}},
	}

	// Cache miss
	_, found := cache.Get("MATCH (n) RETURN n", nil)
	if found {
		t.Error("Expected cache miss, got hit")
	}

	// Store result
	cache.Put("MATCH (n) RETURN n", nil, result, 1*time.Second)

	// Cache hit
	cached, found := cache.Get("MATCH (n) RETURN n", nil)
	if !found {
		t.Error("Expected cache hit, got miss")
	}

	if len(cached.Rows) != 1 || cached.Rows[0][0] != "value" {
		t.Error("Cached result doesn't match original")
	}
}

func TestQueryCache_TTL(t *testing.T) {
	cache := NewQueryCache(10)

	result := &ExecuteResult{
		Columns: []string{"n"},
		Rows:    [][]interface{}{{"value"}},
	}

	// Store with very short TTL
	cache.Put("MATCH (n) RETURN n", nil, result, 1*time.Millisecond)

	// Immediate hit
	_, found := cache.Get("MATCH (n) RETURN n", nil)
	if !found {
		t.Error("Expected cache hit immediately after put")
	}

	// Wait for TTL to expire
	time.Sleep(5 * time.Millisecond)

	// Should be expired
	_, found = cache.Get("MATCH (n) RETURN n", nil)
	if found {
		t.Error("Expected cache miss after TTL expiration")
	}
}

func TestQueryCache_LRUEviction(t *testing.T) {
	cache := NewQueryCache(3) // Small cache

	result := &ExecuteResult{Columns: []string{"n"}}

	// Fill cache
	cache.Put("query1", nil, result, 1*time.Minute)
	cache.Put("query2", nil, result, 1*time.Minute)
	cache.Put("query3", nil, result, 1*time.Minute)

	// All should be cached
	_, found := cache.Get("query1", nil)
	if !found {
		t.Error("query1 should be cached")
	}

	// Add fourth item - should evict query2 (least recently used)
	cache.Put("query4", nil, result, 1*time.Minute)

	// query2 should be evicted
	_, found = cache.Get("query2", nil)
	if found {
		t.Error("query2 should have been evicted")
	}

	// query1, query3, query4 should still be there
	_, found = cache.Get("query1", nil)
	if !found {
		t.Error("query1 should still be cached")
	}

	_, found = cache.Get("query3", nil)
	if !found {
		t.Error("query3 should still be cached")
	}

	_, found = cache.Get("query4", nil)
	if !found {
		t.Error("query4 should be cached")
	}
}

func TestQueryCache_ParameterizedQueries(t *testing.T) {
	cache := NewQueryCache(10)

	result1 := &ExecuteResult{Rows: [][]interface{}{{"Alice"}}}
	result2 := &ExecuteResult{Rows: [][]interface{}{{"Bob"}}}

	params1 := map[string]interface{}{"name": "Alice"}
	params2 := map[string]interface{}{"name": "Bob"}

	// Cache same query with different params
	cache.Put("MATCH (n {name: $name}) RETURN n", params1, result1, 1*time.Minute)
	cache.Put("MATCH (n {name: $name}) RETURN n", params2, result2, 1*time.Minute)

	// Should retrieve correct results for each param set
	cached1, found := cache.Get("MATCH (n {name: $name}) RETURN n", params1)
	if !found || cached1.Rows[0][0] != "Alice" {
		t.Error("Should retrieve Alice result")
	}

	cached2, found := cache.Get("MATCH (n {name: $name}) RETURN n", params2)
	if !found || cached2.Rows[0][0] != "Bob" {
		t.Error("Should retrieve Bob result")
	}
}

func TestQueryCache_Invalidate(t *testing.T) {
	cache := NewQueryCache(10)

	result := &ExecuteResult{Columns: []string{"n"}}

	// Cache multiple queries
	cache.Put("query1", nil, result, 1*time.Minute)
	cache.Put("query2", nil, result, 1*time.Minute)
	cache.Put("query3", nil, result, 1*time.Minute)

	// Verify cached
	_, found := cache.Get("query1", nil)
	if !found {
		t.Error("query1 should be cached before invalidation")
	}

	// Invalidate entire cache
	cache.Invalidate()

	// All should be gone
	_, found = cache.Get("query1", nil)
	if found {
		t.Error("query1 should be invalidated")
	}

	_, found = cache.Get("query2", nil)
	if found {
		t.Error("query2 should be invalidated")
	}

	_, found = cache.Get("query3", nil)
	if found {
		t.Error("query3 should be invalidated")
	}
}

func TestQueryCache_Stats(t *testing.T) {
	cache := NewQueryCache(10)

	result := &ExecuteResult{Columns: []string{"n"}}

	// Initial stats
	hits, misses, size := cache.Stats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Errorf("Initial stats wrong: hits=%d, misses=%d, size=%d", hits, misses, size)
	}

	// Cache miss
	cache.Get("query1", nil)
	hits, misses, size = cache.Stats()
	if hits != 0 || misses != 1 {
		t.Errorf("After miss: hits=%d, misses=%d", hits, misses)
	}

	// Add to cache
	cache.Put("query1", nil, result, 1*time.Minute)
	hits, misses, size = cache.Stats()
	if size != 1 {
		t.Errorf("Cache size should be 1, got %d", size)
	}

	// Cache hit
	cache.Get("query1", nil)
	hits, misses, size = cache.Stats()
	if hits != 1 || misses != 1 {
		t.Errorf("After hit: hits=%d, misses=%d", hits, misses)
	}
}

// =============================================================================
// SMART QUERY CACHE TESTS
// =============================================================================

func TestSmartQueryCache_Basic(t *testing.T) {
	cache := NewSmartQueryCache(10)

	result := &ExecuteResult{
		Columns: []string{"n"},
		Rows:    [][]interface{}{{"value"}},
	}

	// Cache miss
	_, found := cache.Get("MATCH (n:User) RETURN n", nil)
	if found {
		t.Error("Expected cache miss, got hit")
	}

	// Store result
	cache.Put("MATCH (n:User) RETURN n", nil, result, 1*time.Second)

	// Cache hit
	cached, found := cache.Get("MATCH (n:User) RETURN n", nil)
	if !found {
		t.Error("Expected cache hit, got miss")
	}

	if len(cached.Rows) != 1 || cached.Rows[0][0] != "value" {
		t.Error("Cached result doesn't match original")
	}
}

func TestSmartQueryCache_LabelExtraction(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "single label",
			query:    "MATCH (n:User) RETURN n",
			expected: []string{"User"},
		},
		{
			name:     "multiple labels",
			query:    "MATCH (n:User)-[:KNOWS]->(m:Person) RETURN n, m",
			expected: []string{"User", "KNOWS", "Person"},
		},
		{
			name:     "no labels",
			query:    "MATCH (n) RETURN n",
			expected: nil,
		},
		{
			name:     "complex query",
			query:    "MATCH (u:User)-[:FOLLOWS]->(p:Post)-[:HAS_TAG]->(t:Tag) WHERE u.name = 'Alice' RETURN p",
			expected: []string{"User", "FOLLOWS", "Post", "HAS_TAG", "Tag"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := extractLabelsFromQuery(tt.query)
			if len(labels) != len(tt.expected) {
				t.Errorf("Expected %d labels, got %d: %v", len(tt.expected), len(labels), labels)
				return
			}
			for i, label := range labels {
				if label != tt.expected[i] {
					t.Errorf("Expected label[%d]=%s, got %s", i, tt.expected[i], label)
				}
			}
		})
	}
}

func TestSmartQueryCache_SmartInvalidation(t *testing.T) {
	cache := NewSmartQueryCache(10)

	userResult := &ExecuteResult{Rows: [][]interface{}{{"user-data"}}}
	productResult := &ExecuteResult{Rows: [][]interface{}{{"product-data"}}}

	// Cache queries for different labels
	cache.PutWithLabels("MATCH (n:User) RETURN n", nil, userResult, 1*time.Minute, []string{"User"})
	cache.PutWithLabels("MATCH (n:Product) RETURN n", nil, productResult, 1*time.Minute, []string{"Product"})

	// Both should be cached
	_, found := cache.Get("MATCH (n:User) RETURN n", nil)
	if !found {
		t.Error("User query should be cached")
	}
	_, found = cache.Get("MATCH (n:Product) RETURN n", nil)
	if !found {
		t.Error("Product query should be cached")
	}

	// Invalidate only User label
	cache.InvalidateLabels([]string{"User"})

	// User query should be invalidated
	_, found = cache.Get("MATCH (n:User) RETURN n", nil)
	if found {
		t.Error("User query should be invalidated")
	}

	// Product query should still be cached
	cached, found := cache.Get("MATCH (n:Product) RETURN n", nil)
	if !found {
		t.Error("Product query should still be cached")
	}
	if cached.Rows[0][0] != "product-data" {
		t.Error("Product data should be preserved")
	}
}

func TestSmartQueryCache_FullInvalidation(t *testing.T) {
	cache := NewSmartQueryCache(10)

	result := &ExecuteResult{Columns: []string{"n"}}

	// Cache multiple queries
	cache.PutWithLabels("query1", nil, result, 1*time.Minute, []string{"Label1"})
	cache.PutWithLabels("query2", nil, result, 1*time.Minute, []string{"Label2"})
	cache.PutWithLabels("query3", nil, result, 1*time.Minute, []string{"Label3"})

	// Full invalidation
	cache.Invalidate()

	// All should be gone
	_, found := cache.Get("query1", nil)
	if found {
		t.Error("query1 should be invalidated")
	}
	_, found = cache.Get("query2", nil)
	if found {
		t.Error("query2 should be invalidated")
	}
	_, found = cache.Get("query3", nil)
	if found {
		t.Error("query3 should be invalidated")
	}
}

func TestSmartQueryCache_TTL(t *testing.T) {
	cache := NewSmartQueryCache(10)

	result := &ExecuteResult{Rows: [][]interface{}{{"value"}}}

	// Store with very short TTL
	cache.PutWithLabels("query", nil, result, 1*time.Millisecond, []string{"Test"})

	// Immediate hit
	_, found := cache.Get("query", nil)
	if !found {
		t.Error("Expected cache hit immediately after put")
	}

	// Wait for TTL to expire
	time.Sleep(5 * time.Millisecond)

	// Should be expired
	_, found = cache.Get("query", nil)
	if found {
		t.Error("Expected cache miss after TTL expiration")
	}
}

func TestSmartQueryCache_LRUEviction(t *testing.T) {
	cache := NewSmartQueryCache(3) // Small cache

	result := &ExecuteResult{Columns: []string{"n"}}

	// Fill cache
	cache.PutWithLabels("query1", nil, result, 1*time.Minute, []string{"L1"})
	cache.PutWithLabels("query2", nil, result, 1*time.Minute, []string{"L2"})
	cache.PutWithLabels("query3", nil, result, 1*time.Minute, []string{"L3"})

	// Access query1 to make it recently used
	cache.Get("query1", nil)

	// Add fourth item - should evict query2 (least recently used)
	cache.PutWithLabels("query4", nil, result, 1*time.Minute, []string{"L4"})

	// query2 should be evicted
	_, found := cache.Get("query2", nil)
	if found {
		t.Error("query2 should have been evicted")
	}

	// query1, query3, query4 should still be there
	_, found = cache.Get("query1", nil)
	if !found {
		t.Error("query1 should still be cached")
	}
}

func TestSmartQueryCache_Stats(t *testing.T) {
	cache := NewSmartQueryCache(10)

	result := &ExecuteResult{Columns: []string{"n"}}

	// Initial stats
	hits, misses, size, smartInvals, fullInvals := cache.Stats()
	if hits != 0 || misses != 0 || size != 0 || smartInvals != 0 || fullInvals != 0 {
		t.Errorf("Initial stats wrong")
	}

	// Cache miss
	cache.Get("query1", nil)
	hits, misses, size, _, _ = cache.Stats()
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}

	// Add and hit
	cache.PutWithLabels("query1", nil, result, 1*time.Minute, []string{"Label"})
	cache.Get("query1", nil)
	hits, _, size, _, _ = cache.Stats()
	if hits != 1 || size != 1 {
		t.Errorf("Expected 1 hit and size 1, got hits=%d, size=%d", hits, size)
	}

	// Smart invalidation
	cache.InvalidateLabels([]string{"Label"})
	_, _, _, smartInvals, fullInvals = cache.Stats()
	if smartInvals != 1 {
		t.Errorf("Expected 1 smart invalidation, got %d", smartInvals)
	}

	// Full invalidation
	cache.PutWithLabels("query2", nil, result, 1*time.Minute, []string{"Label"})
	cache.Invalidate()
	_, _, _, smartInvals, fullInvals = cache.Stats()
	if fullInvals != 1 {
		t.Errorf("Expected 1 full invalidation, got %d", fullInvals)
	}
}

func TestSmartQueryCache_Concurrent(t *testing.T) {
	cache := NewSmartQueryCache(100)
	result := &ExecuteResult{Columns: []string{"n"}}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := "query" + string(rune('A'+id))
				cache.Put(key, nil, result, 1*time.Minute)
				cache.Get(key, nil)
				if j%10 == 0 {
					cache.InvalidateLabels([]string{"User"})
				}
			}
		}(i)
	}
	wg.Wait()

	// Should not panic or deadlock
	hits, misses, size, _, _ := cache.Stats()
	t.Logf("Concurrent test: hits=%d, misses=%d, size=%d", hits, misses, size)
}

// =============================================================================
// QUERY PLAN CACHE TESTS
// =============================================================================

func TestQueryPlanCache_Basic(t *testing.T) {
	cache := NewQueryPlanCache(10)

	clauses := []Clause{&MatchClause{}}

	// Cache miss
	_, _, found := cache.Get("MATCH (n) RETURN n")
	if found {
		t.Error("Expected cache miss, got hit")
	}

	// Store plan
	cache.Put("MATCH (n) RETURN n", clauses, QueryMatch)

	// Cache hit
	cachedClauses, queryType, found := cache.Get("MATCH (n) RETURN n")
	if !found {
		t.Error("Expected cache hit, got miss")
	}

	if len(cachedClauses) != 1 {
		t.Errorf("Expected 1 clause, got %d", len(cachedClauses))
	}
	if queryType != QueryMatch {
		t.Errorf("Expected QueryMatch, got %v", queryType)
	}
}

func TestQueryPlanCache_Normalization(t *testing.T) {
	cache := NewQueryPlanCache(10)

	clauses := []Clause{&MatchClause{}}

	// Store with extra whitespace
	cache.Put("MATCH  (n)   RETURN  n", clauses, QueryMatch)

	// Should find with normalized query
	_, _, found := cache.Get("MATCH (n) RETURN n")
	if !found {
		t.Error("Normalized query should hit cache")
	}

	// Different whitespace patterns should all hit
	_, _, found = cache.Get("MATCH\n(n)\nRETURN\nn")
	if !found {
		t.Error("Query with newlines should hit cache")
	}
}

func TestQueryPlanCache_LRUEviction(t *testing.T) {
	cache := NewQueryPlanCache(3) // Small cache

	clauses := []Clause{&MatchClause{}}

	// Fill cache
	cache.Put("query1", clauses, QueryMatch)
	cache.Put("query2", clauses, QueryMatch)
	cache.Put("query3", clauses, QueryMatch)

	// Access query1 to make it recently used
	cache.Get("query1")

	// Add fourth item - should evict query2 (least recently used)
	cache.Put("query4", clauses, QueryMatch)

	// query2 should be evicted
	_, _, found := cache.Get("query2")
	if found {
		t.Error("query2 should have been evicted")
	}

	// query1 should still be there
	_, _, found = cache.Get("query1")
	if !found {
		t.Error("query1 should still be cached")
	}
}

func TestQueryPlanCache_Stats(t *testing.T) {
	cache := NewQueryPlanCache(10)

	clauses := []Clause{&MatchClause{}}

	// Initial stats
	hits, misses, size := cache.Stats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Errorf("Initial stats wrong: hits=%d, misses=%d, size=%d", hits, misses, size)
	}

	// Cache miss
	cache.Get("query1")
	hits, misses, size = cache.Stats()
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}

	// Add and hit
	cache.Put("query1", clauses, QueryMatch)
	cache.Get("query1")
	hits, misses, size = cache.Stats()
	if hits != 1 || size != 1 {
		t.Errorf("Expected 1 hit and size 1, got hits=%d, size=%d", hits, size)
	}
}

func TestQueryPlanCache_Clear(t *testing.T) {
	cache := NewQueryPlanCache(10)

	clauses := []Clause{&MatchClause{}}

	// Add some entries
	cache.Put("query1", clauses, QueryMatch)
	cache.Put("query2", clauses, QueryCreate)
	cache.Put("query3", clauses, QueryDelete)

	// Verify they're cached
	_, _, size := cache.Stats()
	if size != 3 {
		t.Errorf("Expected size 3, got %d", size)
	}

	// Clear
	cache.Clear()

	// All should be gone
	_, _, size = cache.Stats()
	if size != 0 {
		t.Errorf("Expected size 0 after clear, got %d", size)
	}

	_, _, found := cache.Get("query1")
	if found {
		t.Error("query1 should be cleared")
	}
}

func TestQueryPlanCache_Concurrent(t *testing.T) {
	cache := NewQueryPlanCache(100)
	clauses := []Clause{&MatchClause{}}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := "query" + string(rune('A'+id))
				cache.Put(key, clauses, QueryMatch)
				cache.Get(key)
			}
		}(i)
	}
	wg.Wait()

	// Should not panic or deadlock
	hits, misses, size := cache.Stats()
	t.Logf("Concurrent test: hits=%d, misses=%d, size=%d", hits, misses, size)
}

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"MATCH (n) RETURN n", "MATCH (n) RETURN n"},
		{"MATCH  (n)  RETURN  n", "MATCH (n) RETURN n"},
		{"MATCH\n(n)\nRETURN\nn", "MATCH (n) RETURN n"},
		{"  MATCH (n) RETURN n  ", "MATCH (n) RETURN n"},
		{"MATCH\t(n)\t\tRETURN n", "MATCH (n) RETURN n"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeQuery(tt.input)
			if result != tt.expected {
				t.Errorf("normalizeQuery(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

// =============================================================================
// BENCHMARKS
// =============================================================================

func BenchmarkQueryCache_Hit(b *testing.B) {
	cache := NewQueryCache(1000)
	result := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{"value"}}}
	cache.Put("query", nil, result, 1*time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("query", nil)
	}
}

func BenchmarkQueryCache_Miss(b *testing.B) {
	cache := NewQueryCache(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("nonexistent", nil)
	}
}

func BenchmarkSmartQueryCache_Hit(b *testing.B) {
	cache := NewSmartQueryCache(1000)
	result := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{"value"}}}
	cache.PutWithLabels("MATCH (n:User) RETURN n", nil, result, 1*time.Minute, []string{"User"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("MATCH (n:User) RETURN n", nil)
	}
}

func BenchmarkSmartQueryCache_SmartInvalidation(b *testing.B) {
	cache := NewSmartQueryCache(1000)
	result := &ExecuteResult{Columns: []string{"n"}}

	// Pre-populate with different labels
	for i := 0; i < 100; i++ {
		cache.PutWithLabels("query"+string(rune(i)), nil, result, 1*time.Minute, []string{"User"})
	}
	for i := 100; i < 200; i++ {
		cache.PutWithLabels("query"+string(rune(i)), nil, result, 1*time.Minute, []string{"Product"})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.InvalidateLabels([]string{"User"})
		// Re-add for next iteration
		for j := 0; j < 100; j++ {
			cache.PutWithLabels("query"+string(rune(j)), nil, result, 1*time.Minute, []string{"User"})
		}
	}
}

func BenchmarkQueryPlanCache_Hit(b *testing.B) {
	cache := NewQueryPlanCache(1000)
	clauses := []Clause{&MatchClause{}}
	cache.Put("MATCH (n:User) RETURN n", clauses, QueryMatch)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get("MATCH (n:User) RETURN n")
	}
}

func BenchmarkExtractLabelsFromQuery(b *testing.B) {
	query := "MATCH (u:User)-[:FOLLOWS]->(p:Post)-[:HAS_TAG]->(t:Tag) WHERE u.name = 'Alice' RETURN p"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractLabelsFromQuery(query)
	}
}
