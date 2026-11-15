package storage

import (
	"fmt"
	"strings"
	"testing"
)

// A bound relationship MERGE rejects every candidate by one small identity
// property. Large unrelated properties must not be copied merely to reject it.
func BenchmarkTransactionBetweenPropertyRejection(b *testing.B) {
	engine := newTestMemoryEngine(b)
	ctx := b.Context()
	if err := engine.BulkCreateNodes(ctx, []*Node{{ID: "test:a"}, {ID: "test:b"}}); err != nil {
		b.Fatal(err)
	}
	edges := make([]*Edge, 1000)
	payload := strings.Repeat("x", 8<<10)
	for i := range edges {
		edges[i] = &Edge{ID: EdgeID(fmt.Sprintf("test:e%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"key": int64(i), "payload": payload}}
	}
	if err := engine.BulkCreateEdges(ctx, edges); err != nil {
		b.Fatal(err)
	}
	tx, err := engine.BeginTransaction(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tx.Rollback() })
	if err = tx.SetNamespace("test"); err != nil {
		b.Fatal(err)
	}
	match := func(edge *Edge) bool { return edge.Properties["key"] == int64(-1) }
	read := func() {
		rows, err := tx.MatchEdgesBetween(ctx, "test:a", "test:b", "LINK", []string{"key"}, match)
		if err != nil || len(rows) != 0 {
			b.Fatalf("rows=%d err=%v", len(rows), err)
		}
	}
	read()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		read()
	}
}
