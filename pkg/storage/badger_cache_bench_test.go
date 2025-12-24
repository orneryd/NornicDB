package storage

import (
	"testing"
)

func BenchmarkBadger_GetNode_CacheHit(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	node := testNode("n1")
	if _, err := engine.CreateNode(node); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.GetNode(node.ID); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBadger_GetEdgesByType_CacheHit(b *testing.B) {
	engine, err := NewBadgerEngineInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	n1 := testNode("n1")
	n2 := testNode("n2")
	if _, err := engine.CreateNode(n1); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.CreateNode(n2); err != nil {
		b.Fatal(err)
	}

	const edgeType = "KNOWS"
	for i := 0; i < 100; i++ {
		e := testEdge("e"+itoaBench(i), n1.ID, n2.ID, edgeType)
		if err := engine.CreateEdge(e); err != nil {
			b.Fatal(err)
		}
	}

	// Warm cache.
	edges, err := engine.GetEdgesByType(edgeType)
	if err != nil {
		b.Fatal(err)
	}
	if len(edges) == 0 {
		b.Fatal("expected edges")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edges, err := engine.GetEdgesByType(edgeType)
		if err != nil {
			b.Fatal(err)
		}
		if len(edges) != 100 {
			b.Fatalf("unexpected edge count: %d", len(edges))
		}
	}
}

func itoaBench(i int) string {
	// Avoid strconv in microbench setup; not on the measured path.
	if i == 0 {
		return "0"
	}
	var buf [16]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + (i % 10))
		i /= 10
	}
	return string(buf[pos:])
}
