package storage

import (
	"context"
	"fmt"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
)

// BenchmarkSchemaRefreshNativePopulated measures rebuilding four UNIQUE caches
// from 128 real stored nodes; fixture writes are outside the timed region.
func BenchmarkSchemaRefreshNativePopulated(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngine(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	target := NewNamespacedEngine(engine, "test")
	for i := 0; i < 128; i++ {
		_, err = target.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprint(i)), Labels: []string{"Item"}, Properties: map[string]any{"a": i, "b": i, "c": i, "d": i}})
		if err != nil {
			b.Fatal(err)
		}
	}
	schema := target.GetSchema()
	for _, prop := range []string{"a", "b", "c", "d"} {
		if err := schema.AddUniqueConstraint("unique_"+prop, "Item", prop); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := RefreshUniqueConstraintValuesForEngine(context.Background(), target, schema); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// Verify the public cache contract outside the measured refresh loop.
	// Every stored claim must be complete and resolve to its actual node.
	for _, property := range []string{"a", "b", "c", "d"} {
		for value := 0; value < 128; value++ {
			id, found, exists, complete, err := schema.LookupUniqueConstraintValue(context.Background(), "Item", property, value)
			if err != nil || !found || !exists || !complete {
				b.Fatalf("claim %s=%d: found=%v exists=%v complete=%v err=%v", property, value, found, exists, complete, err)
			}
			node, err := target.GetNode(b.Context(), id)
			if err != nil || node == nil || node.Properties[property] != int64(value) && node.Properties[property] != value {
				b.Fatalf("claim %s=%d resolved to %v: %v", property, value, node, err)
			}
		}
	}
}
