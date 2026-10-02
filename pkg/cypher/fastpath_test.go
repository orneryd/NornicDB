package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFastPath_MatchCreateDeleteRel tests the fast-path for MATCH...CREATE...DELETE patterns.
func TestFastPath_MatchCreateDeleteRel(t *testing.T) {
	requirePerformanceWorkload(t)
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Setup: Create test nodes
	for i := 0; i < 10; i++ {
		engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("actor%d", i)),
			Labels: []string{"Actor"},
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("Actor_%d", i),
			},
		})
		engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("movie%d", i)),
			Labels: []string{"Movie"},
			Properties: map[string]interface{}{
				"title": fmt.Sprintf("Movie_%d", i),
			},
		})
	}

	executor := NewStorageExecutor(engine)
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
	require.True(t, executor.LastHotPathTrace().CompoundQueryFastPath)

	assertMinOpsPerSec(t, "Fast-path WITH LIMIT", opsPerSec, 10000)
}

// TestFastPath_LDBCPattern tests the LDBC-style pattern with property matching.
func TestFastPath_LDBCPattern(t *testing.T) {
	requirePerformanceWorkload(t)
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")

	// Setup: Create Person nodes with id properties (LDBC style)
	for i := 1; i <= 10; i++ {
		engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("person%d", i)),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"id":   int64(i),
				"name": fmt.Sprintf("Person_%d", i),
			},
		})
	}

	executor := NewStorageExecutor(engine)
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
	require.True(t, executor.LastHotPathTrace().CompoundQueryFastPath)

	// First iteration is slower due to cache miss, subsequent are fast.
	assertMinOpsPerSec(t, "Fast-path LDBC property match", opsPerSec, 5000)
}

// TestFastPath_CompoundQueryShapeMatching verifies the structured matcher matches correctly.
func TestFastPath_CompoundQueryShapeMatching(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantKind ShapeKind
	}{
		// Pattern 1: WITH LIMIT
		{
			name:     "benchmark pattern exact",
			query:    "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r",
			wantKind: shapeKindCompoundCreateDeleteRel,
		},
		{
			name:     "benchmark pattern with spaces",
			query:    "MATCH (a:Actor),(m:Movie) WITH a,m LIMIT 1 CREATE (a)-[r:T]->(m) DELETE r",
			wantKind: shapeKindCompoundCreateDeleteRel,
		},
		{
			name:     "benchmark pattern uppercase",
			query:    "MATCH (A:ACTOR), (M:MOVIE) WITH A, M LIMIT 1 CREATE (A)-[R:REL]->(M) DELETE R",
			wantKind: shapeKindCompoundCreateDeleteRel,
		},
		// Pattern 2: LDBC property match
		{
			name:     "LDBC pattern exact",
			query:    "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:TEMP_KNOWS]->(p2) DELETE r",
			wantKind: shapeKindCompoundPropCreateDeleteRel,
		},
		{
			name:     "LDBC pattern with spaces",
			query:    "MATCH (p1:Person { id: 1 }), (p2:Person { id: 2 }) CREATE (p1)-[r:KNOWS]->(p2) DELETE r",
			wantKind: shapeKindCompoundPropCreateDeleteRel,
		},
		// Non-matching patterns
		{
			name:     "LDBC without DELETE",
			query:    "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:KNOWS]->(p2)",
			wantKind: shapeKindUnknown,
		},
		{
			name:     "simple CREATE",
			query:    "CREATE (n:Test)",
			wantKind: shapeKindUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, ok := matchCompoundQueryShape(tt.query)
			if tt.wantKind == shapeKindUnknown {
				require.False(t, ok)
				require.Equal(t, shapeKindUnknown, match.Kind)
				return
			}

			require.True(t, ok)
			require.Equal(t, tt.wantKind, match.Kind)
		})
	}
}

func TestFastPath_CreateDeleteRelCount_HelperBranches(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)

	_, err := engine.CreateNode(&storage.Node{
		ID:     "p1",
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"id": int64(1),
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "p2",
		Labels: []string{"Person"},
		Properties: map[string]interface{}{
			"id": int64(2),
		},
	})
	require.NoError(t, err)

	okRes, ok := executor.executeFastPathCreateDeleteRelCount(
		"Person", "Person",
		"id", int64(1),
		"id", int64(2),
		"TEMP_REL", "r",
	)
	require.True(t, ok)
	require.NotNil(t, okRes)
	assert.Equal(t, []string{"count(r)"}, okRes.Columns)
	require.Len(t, okRes.Rows, 1)
	assert.Equal(t, int64(1), okRes.Rows[0][0])
	assert.Equal(t, 1, okRes.Stats.RelationshipsCreated)
	assert.Equal(t, 1, okRes.Stats.RelationshipsDeleted)

	// Missing property-matched endpoint returns not fast-path-applicable.
	missRes, missOK := executor.executeFastPathCreateDeleteRelCount(
		"Person", "Person",
		"id", int64(1),
		"id", int64(999),
		"TEMP_REL", "r",
	)
	assert.False(t, missOK)
	assert.Nil(t, missRes)

	// Label lookup branch when property filters are absent.
	labelRes, labelOK := executor.executeFastPathCreateDeleteRelCount(
		"Person", "Person",
		"", nil,
		"", nil,
		"TEMP_REL", "edgeRef",
	)
	require.True(t, labelOK)
	require.NotNil(t, labelRes)
	assert.Equal(t, []string{"count(edgeRef)"}, labelRes.Columns)

	// Missing label path returns false.
	noneRes, noneOK := executor.executeFastPathCreateDeleteRelCount(
		"MissingLabelA", "MissingLabelB",
		"", nil,
		"", nil,
		"TEMP_REL", "r",
	)
	assert.False(t, noneOK)
	assert.Nil(t, noneRes)
}

func TestFastPath_CreateDeleteRel_HelperBranches(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)

	_, err := engine.CreateNode(&storage.Node{
		ID:     "a1",
		Labels: []string{"Actor"},
		Properties: map[string]interface{}{
			"name": "A",
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "m1",
		Labels: []string{"Movie"},
		Properties: map[string]interface{}{
			"title": "M",
		},
	})
	require.NoError(t, err)

	okRes, ok := executor.executeFastPathCreateDeleteRel("Actor", "Movie", "", nil, "", nil, "TEMP")
	require.True(t, ok)
	require.NotNil(t, okRes)
	assert.Equal(t, 1, okRes.Stats.RelationshipsCreated)
	assert.Equal(t, 1, okRes.Stats.RelationshipsDeleted)

	// Property-miss branch.
	missRes, missOK := executor.executeFastPathCreateDeleteRel("Actor", "Movie", "name", "A", "title", "missing", "TEMP")
	assert.False(t, missOK)
	assert.Nil(t, missRes)

	// Missing-label branch.
	noneRes, noneOK := executor.executeFastPathCreateDeleteRel("NoLabelA", "NoLabelB", "", nil, "", nil, "TEMP")
	assert.False(t, noneOK)
	assert.Nil(t, noneRes)
}

// BenchmarkFastPath_WithLimit benchmarks the WITH LIMIT pattern.
func BenchmarkFastPath_WithLimit(b *testing.B) {
	baseEngine := newTestMemoryEngine(b)
	asyncBase := storage.NewAsyncEngine(baseEngine, nil)
	defer asyncBase.Close()
	engine := storage.NewNamespacedEngine(asyncBase, "test")

	// Setup
	for i := 0; i < 10; i++ {
		_, _ = engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("actor%d", i)),
			Labels: []string{"Actor"},
		})
		_, _ = engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("movie%d", i)),
			Labels: []string{"Movie"},
		})
	}

	executor := NewStorageExecutor(engine)
	ctx := context.Background()
	query := "MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:T]->(m) DELETE r"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Execute(ctx, query, nil)
	}
}

// BenchmarkFastPath_LDBC benchmarks the LDBC property pattern.
func BenchmarkFastPath_LDBC(b *testing.B) {
	baseEngine := newTestMemoryEngine(b)
	asyncBase := storage.NewAsyncEngine(baseEngine, nil)
	defer asyncBase.Close()
	engine := storage.NewNamespacedEngine(asyncBase, "test")

	// Setup
	for i := 1; i <= 10; i++ {
		_, _ = engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("person%d", i)),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"id": int64(i),
			},
		})
	}

	executor := NewStorageExecutor(engine)
	ctx := context.Background()
	query := "MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:KNOWS]->(p2) DELETE r"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		executor.Execute(ctx, query, nil)
	}
}
