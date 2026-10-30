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
	require.False(t, executor.LastHotPathTrace().CompoundQueryFastPath)

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
	require.False(t, executor.LastHotPathTrace().CompoundQueryFastPath)

	// First iteration is slower due to cache miss, subsequent are fast.
	assertMinOpsPerSec(t, "Fast-path LDBC property match", opsPerSec, 5000)
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

	okRes, err := executor.Execute(context.Background(), "MATCH (a:Person {id:1}), (b:Person {id:2}) CREATE (a)-[r:TEMP_REL]->(b) DELETE r RETURN count(r)", nil)
	require.NoError(t, err)
	require.NotNil(t, okRes)
	assert.Equal(t, []string{"count(r)"}, okRes.Columns)
	require.Len(t, okRes.Rows, 1)
	assert.Equal(t, int64(1), okRes.Rows[0][0])
	assert.Equal(t, 1, okRes.Stats.RelationshipsCreated)
	assert.Equal(t, 1, okRes.Stats.RelationshipsDeleted)

	missRes, err := executor.Execute(context.Background(), "MATCH (a:Person {id:1}), (b:Person {id:999}) CREATE (a)-[r:TEMP_REL]->(b) DELETE r RETURN count(r)", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(0)}}, missRes.Rows)
	assert.Zero(t, missRes.Stats.RelationshipsCreated)
	assert.Zero(t, missRes.Stats.RelationshipsDeleted)

	// Label lookup branch when property filters are absent.
	labelRes, err := executor.Execute(context.Background(), "MATCH (a:Person), (b:Person) CREATE (a)-[edgeRef:TEMP_REL]->(b) DELETE edgeRef RETURN count(edgeRef)", nil)
	require.NoError(t, err)
	require.NotNil(t, labelRes)
	assert.Equal(t, []string{"count(edgeRef)"}, labelRes.Columns)
	assert.Equal(t, [][]interface{}{{int64(4)}}, labelRes.Rows)
	assert.Equal(t, 4, labelRes.Stats.RelationshipsCreated)
	assert.Equal(t, 4, labelRes.Stats.RelationshipsDeleted)

	noneRes, err := executor.Execute(context.Background(), "MATCH (a:MissingLabelA), (b:MissingLabelB) CREATE (a)-[r:TEMP_REL]->(b) DELETE r RETURN count(r)", nil)
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int64(0)}}, noneRes.Rows)
	assert.Zero(t, noneRes.Stats.RelationshipsCreated)
	edges, err := engine.AllEdges()
	require.NoError(t, err)
	require.Empty(t, edges)
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

	okRes, err := executor.Execute(context.Background(), "MATCH (a:Actor), (m:Movie) CREATE (a)-[r:TEMP]->(m) DELETE r", nil)
	require.NoError(t, err)
	require.NotNil(t, okRes)
	assert.Equal(t, 1, okRes.Stats.RelationshipsCreated)
	assert.Equal(t, 1, okRes.Stats.RelationshipsDeleted)

	missRes, err := executor.Execute(context.Background(), "MATCH (a:Actor {name:'A'}), (m:Movie {title:'missing'}) CREATE (a)-[r:TEMP]->(m) DELETE r", nil)
	require.NoError(t, err)
	assert.Zero(t, missRes.Stats.RelationshipsCreated)
	assert.Zero(t, missRes.Stats.RelationshipsDeleted)

	noneRes, err := executor.Execute(context.Background(), "MATCH (a:NoLabelA), (b:NoLabelB) CREATE (a)-[r:TEMP]->(b) DELETE r", nil)
	require.NoError(t, err)
	assert.Zero(t, noneRes.Stats.RelationshipsCreated)
	assert.Zero(t, noneRes.Stats.RelationshipsDeleted)
	edges, err := engine.AllEdges()
	require.NoError(t, err)
	require.Empty(t, edges)
}

// BenchmarkFastPath_WithLimit benchmarks the WITH LIMIT pattern.
func BenchmarkFastPath_WithLimit(b *testing.B) {
	baseEngine := newTestMemoryEngine(b)
	asyncBase := baseEngine
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
		result, err := executor.Execute(ctx, query, nil)
		if err != nil {
			b.Fatal(err)
		}
		if result == nil || result.Stats == nil || result.Stats.RelationshipsCreated != 1 || result.Stats.RelationshipsDeleted != 1 {
			b.Fatalf("expected one relationship created and deleted, got %+v", result)
		}
	}
}

// BenchmarkFastPath_LDBC benchmarks the LDBC property pattern.
func BenchmarkFastPath_LDBC(b *testing.B) {
	baseEngine := newTestMemoryEngine(b)
	asyncBase := baseEngine
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
