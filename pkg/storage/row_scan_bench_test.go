package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkRowScans measures the committed-row scans over one fixture: 2,000
// nodes and 2,000 relationships, plus a write-behind overlay of 20 buffered
// rows that replace committed ones. It uses only APIs release also has, so
// the same file runs in both binaries of an A/B.
func BenchmarkRowScans(b *testing.B) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	const count = 2000
	nodes := make([]*Node, count)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:n%05d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "committed row text"}}
	}
	if err := engine.BulkCreateNodes(nodes); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if err := engine.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:e%05d", i)), StartNode: nodes[i].ID, EndNode: nodes[(i+1)%count].ID, Type: "REL", Properties: map[string]any{"note": "committed"}}); err != nil {
			b.Fatal(err)
		}
	}
	if err := engine.FlushWriteBehind(); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := engine.UpdateNode(&Node{ID: nodes[i*50].ID, Labels: []string{"Doc"}, Properties: map[string]any{"text": "buffered"}}); err != nil {
			b.Fatal(err)
		}
		if err := engine.UpdateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:e%05d", i*50)), StartNode: nodes[i*50].ID, EndNode: nodes[(i*50+1)%count].ID, Type: "REL", Properties: map[string]any{"note": "buffered"}}); err != nil {
			b.Fatal(err)
		}
	}
	ctx := context.Background()
	expect := func(b *testing.B, got int) {
		if got != count {
			b.Fatalf("scanned %d rows, want %d", got, count)
		}
	}
	b.Run("AllNodes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := engine.AllNodes()
			if err != nil {
				b.Fatal(err)
			}
			expect(b, len(got))
		}
	})
	b.Run("AllEdges", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			got, err := engine.AllEdges()
			if err != nil {
				b.Fatal(err)
			}
			expect(b, len(got))
		}
	})
	b.Run("StreamEdges", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			if err := engine.StreamEdges(ctx, func(*Edge) error { n++; return nil }); err != nil {
				b.Fatal(err)
			}
			expect(b, n)
		}
	})
	b.Run("StreamNodes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			if err := engine.StreamNodes(ctx, func(*Node) error { n++; return nil }); err != nil {
				b.Fatal(err)
			}
			expect(b, n)
		}
	})
	snapshot, err := engine.OpenReadSnapshot(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer snapshot.Close()
	b.Run("SnapshotIterateNodes", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			if err := snapshot.IterateNodes(func(*Node) bool { n++; return true }); err != nil {
				b.Fatal(err)
			}
			expect(b, n)
		}
	})
	b.Run("SnapshotStreamEdges", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n := 0
			if err := snapshot.StreamEdges(ctx, func(*Edge) error { n++; return nil }); err != nil {
				b.Fatal(err)
			}
			expect(b, n)
		}
	})
}
