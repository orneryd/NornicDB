package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestMultiMatchFiltersWithDisjunctiveRelationshipPatterns(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "test"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
		CREATE (first:TheLabel {id: 0}), (second:TheLabel {id: 1}), (third:TheLabel {id: 2})
		CREATE (first)-[:T]->(second), (second)-[:T]->(third)
	`, nil)
	if err != nil {
		t.Fatalf("create graph: %v", err)
	}

	result, err := exec.Execute(ctx, `
		MATCH (source), (target)
		WHERE source.id = 0
		  AND (source)-[:T]->(target:TheLabel)
		  OR (source)-[:T*]->(target:MissingLabel)
		RETURN DISTINCT target.id
	`, nil)
	if err != nil {
		t.Fatalf("execute pattern predicate query: %v", err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(1) {
		t.Fatalf("expected only the directly related labeled target, got %#v", result.Rows)
	}
}

func TestRelationshipPatternPredicatesRemainIsolatedBetweenExecutors(t *testing.T) {
	ctx := context.Background()
	query := `MATCH (source), (target) WHERE (source)-[:T]->(target) RETURN target.id`

	connectedStore := newTestMemoryEngine(t)
	connected := NewStorageExecutor(storage.NewNamespacedEngine(connectedStore, "connected"))
	_, err := connected.Execute(ctx, `CREATE (source {id: 0})-[:T]->(target {id: 1})`, nil)
	if err != nil {
		t.Fatalf("create connected graph: %v", err)
	}
	connectedResult, err := connected.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("query connected graph: %v", err)
	}
	if len(connectedResult.Rows) != 1 {
		t.Fatalf("expected one connected result, got %#v", connectedResult.Rows)
	}

	disconnectedStore := newTestMemoryEngine(t)
	disconnected := NewStorageExecutor(storage.NewNamespacedEngine(disconnectedStore, "disconnected"))
	_, err = disconnected.Execute(ctx, `CREATE ({id: 0}), ({id: 1})`, nil)
	if err != nil {
		t.Fatalf("create disconnected graph: %v", err)
	}
	disconnectedResult, err := disconnected.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("query disconnected graph: %v", err)
	}
	if len(disconnectedResult.Rows) != 0 {
		t.Fatalf("expected no disconnected results, got %#v", disconnectedResult.Rows)
	}
}

func BenchmarkBoundOneHopRelationshipPatternPredicate(b *testing.B) {
	baseStore := storage.NewMemoryEngine()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "benchmark"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, `CREATE (source)-[:T]->(target)`, nil)
	if err != nil {
		b.Fatalf("create graph: %v", err)
	}
	nodes, err := exec.Execute(ctx, `MATCH (source)-[:T]->(target) RETURN source, target`, nil)
	if err != nil || len(nodes.Rows) != 1 {
		b.Fatalf("load bindings: rows=%#v err=%v", nodes.Rows, err)
	}
	row := binding{
		"source": nodes.Rows[0][0].(*storage.Node),
		"target": nodes.Rows[0][1].(*storage.Node),
	}
	predicate := exec.getCompiledBindingWhere(ctx, `(source)-[:T]->(target)`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !predicate(row, nil) {
			b.Fatal("connected nodes did not satisfy relationship pattern")
		}
	}
}
