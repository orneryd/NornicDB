package storage

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
)

// BenchmarkNOR56StreamingLargeRead compares full scans through direct and
// traced views. Every iteration verifies that the complete population arrived.
func BenchmarkNOR56StreamingLargeRead(b *testing.B) {
	base := fixtureowner.Own(b, fixtureowner.Track(b, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		_, err := base.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("bench:n%05d", i)), Labels: []string{"Item"}, Properties: map[string]interface{}{"text": "representative payload"}})
		if err != nil {
			b.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name   string
		engine Engine
	}{{"direct", base}, {"traced", NewTracedEngine(base)}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				count := 0
				err := StreamNodesWithFallback(ctx, tc.engine, 1000, func(*Node) error { count++; return nil })
				if err != nil || count != 10000 {
					b.Fatalf("count=%d err=%v", count, err)
				}
			}
		})
	}
}
