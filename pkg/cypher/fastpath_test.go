package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestFastPath_MatchCreateDeleteRel tests the fast-path for MATCH...CREATE...DELETE patterns.
func TestFastPath_MatchCreateDeleteRel(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	defer asyncEngine.Close()

	// Setup: Create test nodes
	for i := 0; i < 10; i++ {
		asyncEngine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("actor%d", i)),
			Labels: []string{"Actor"},
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("Actor_%d", i),
			},
		})
		asyncEngine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("movie%d", i)),
			Labels: []string{"Movie"},
			Properties: map[string]interface{}{
				"title": fmt.Sprintf("Movie_%d", i),
			},
		})
	}

	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Test Pattern 1: WITH LIMIT pattern (benchmark style)
	query1 := "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r"

	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		result, err := executor.Execute(ctx, query1, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Stats.RelationshipsCreated != 1 || result.Stats.RelationshipsDeleted != 1 {
			t.Errorf("Expected 1 rel created and 1 deleted, got %+v", result.Stats)
		}
	}
	elapsed := time.Since(start)
	opsPerSec := float64(iterations) / elapsed.Seconds()

	t.Logf("Pattern 1 (WITH LIMIT): %.0f ops/sec", opsPerSec)

	// Should be at least 10K ops/sec with fast-path
	if opsPerSec < 10000 {
		t.Errorf("Fast-path too slow: %.0f ops/sec (expected >10000)", opsPerSec)
	}
}

// TestFastPath_LDBCPattern tests the LDBC-style pattern with property matching.
func TestFastPath_LDBCPattern(t *testing.T) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	defer asyncEngine.Close()

	// Setup: Create Person nodes with id properties (LDBC style)
	for i := 1; i <= 10; i++ {
		asyncEngine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("person%d", i)),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"id":   int64(i),
				"name": fmt.Sprintf("Person_%d", i),
			},
		})
	}

	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()

	// Test Pattern 2: LDBC style (property match, no WITH)
	query2 := "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r"

	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		result, err := executor.Execute(ctx, query2, nil)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if result.Stats.RelationshipsCreated != 1 || result.Stats.RelationshipsDeleted != 1 {
			t.Errorf("Expected 1 rel created and 1 deleted, got %+v", result.Stats)
		}
	}
	elapsed := time.Since(start)
	opsPerSec := float64(iterations) / elapsed.Seconds()

	t.Logf("Pattern 2 (LDBC property match): %.0f ops/sec", opsPerSec)

	// Should be at least 5K ops/sec with fast-path + caching
	// First iteration is slower due to cache miss, subsequent are fast
	if opsPerSec < 5000 {
		t.Errorf("Fast-path too slow: %.0f ops/sec (expected >5000)", opsPerSec)
	}
}

// TestFastPath_RegexMatching verifies the regex patterns match correctly.
func TestFastPath_RegexMatching(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		pattern string
		match   bool
	}{
		// Pattern 1: WITH LIMIT
		{
			name:    "benchmark pattern exact",
			query:   "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r",
			pattern: "withLimit",
			match:   true,
		},
		{
			name:    "benchmark pattern with spaces",
			query:   "MATCH (a:Actor),(m:Movie) WITH a,m LIMIT 1 CREATE (a)-[r:T]->(m) DELETE r",
			pattern: "withLimit",
			match:   true,
		},
		{
			name:    "benchmark pattern uppercase",
			query:   "MATCH (A:ACTOR), (M:MOVIE) WITH A, M LIMIT 1 CREATE (A)-[R:REL]->(M) DELETE R",
			pattern: "withLimit",
			match:   true,
		},
		// Pattern 2: LDBC property match
		{
			name:    "LDBC pattern exact",
			query:   "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r",
			pattern: "ldbc",
			match:   true,
		},
		{
			name:    "LDBC pattern with spaces",
			query:   "MATCH (p1:Person { id: 1 }), (p2:Person { id: 2 }) CREATE (p1)-[r:KNOWS]->(p2) DELETE r",
			pattern: "ldbc",
			match:   true,
		},
		// Non-matching patterns
		{
			name:    "LDBC without DELETE",
			query:   "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:KNOWS]->(p2)",
			pattern: "ldbc",
			match:   false,
		},
		{
			name:    "simple CREATE",
			query:   "CREATE (n:Test)",
			pattern: "withLimit",
			match:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var matched bool
			if tt.pattern == "withLimit" {
				matched = matchCreateDeleteRelPattern.MatchString(tt.query)
			} else {
				matched = matchPropCreateDeleteRelPattern.MatchString(tt.query)
			}

			if matched != tt.match {
				t.Errorf("Expected match=%v, got %v for query: %s", tt.match, matched, tt.query)
			}
		})
	}
}

// BenchmarkFastPath_WithLimit benchmarks the WITH LIMIT pattern.
func BenchmarkFastPath_WithLimit(b *testing.B) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	defer asyncEngine.Close()

	// Setup
	for i := 0; i < 10; i++ {
		asyncEngine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("actor%d", i)),
			Labels: []string{"Actor"},
		})
		asyncEngine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("movie%d", i)),
			Labels: []string{"Movie"},
		})
	}

	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()
	query := "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:T]->(m) DELETE r"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Execute(ctx, query, nil)
	}
}

// BenchmarkFastPath_LDBC benchmarks the LDBC property pattern.
func BenchmarkFastPath_LDBC(b *testing.B) {
	baseEngine := storage.NewMemoryEngine()

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	asyncEngine := storage.NewAsyncEngine(engine, nil)
	defer asyncEngine.Close()

	// Setup
	for i := 1; i <= 10; i++ {
		asyncEngine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("person%d", i)),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"id": int64(i),
			},
		})
	}

	executor := NewStorageExecutor(asyncEngine)
	ctx := context.Background()
	query := "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:KNOWS]->(p2) DELETE r"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Execute(ctx, query, nil)
	}
}
