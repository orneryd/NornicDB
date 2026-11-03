package storage

import (
	"fmt"
	"testing"
)

func BenchmarkScalarIndexMembershipMaintenance(b *testing.B) {
	for _, owner := range []string{"schema", "composite"} {
		b.Run(fmt.Sprintf("owner=%s", owner), func(b *testing.B) {
			sm := NewSchemaManager()
			if err := sm.AddPropertyIndex("scalar", "Item", []string{"value"}); err != nil {
				b.Fatal(err)
			}
			idx, _, err := sm.GetPropertyIndex(b.Context(), "Item", "value")
			if err != nil {
				b.Fatal(err)
			}
			props := map[string]interface{}{"value": int64(7)}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if owner == "schema" {
					if err := sm.PropertyIndexInsert("Item", "value", "test:n", int64(7)); err != nil {
						b.Fatal(err)
					}
					if err := sm.PropertyIndexDelete("Item", "value", "test:n", int64(7)); err != nil {
						b.Fatal(err)
					}
				} else {
					if err := idx.IndexNode(b.Context(), "test:n", props); err != nil {
						b.Fatal(err)
					}
					idx.RemoveNode("test:n", props)
				}
			}
			b.StopTimer()
			ids, err := idx.LookupFull(b.Context(), int64(7))
			if err != nil || len(ids) != 0 {
				b.Fatalf("maintenance did not restore population: %v %v", ids, err)
			}
		})
	}
}

// Both routes produce complete nonnull candidate IDs. The scan baseline
// actually scans accepted node values; no incorrect indexed omission is timed.
func BenchmarkNonNilCandidatesAgainstCorrectScan(b *testing.B) {
	store := newTestMemoryEngine(b)
	values := make(map[NodeID]interface{}, 1024)
	for i := range 1024 {
		id := NodeID(fmt.Sprintf("test:n%d", i))
		var value interface{}
		if i%8 == 0 {
			value = []byte{1, 2, 3, 4}
		}
		if _, err := store.CreateNode(b.Context(), &Node{ID: id, Labels: []string{"Item"}, Properties: map[string]interface{}{"value": value}}); err != nil {
			b.Fatal(err)
		}
		values[id] = value
	}
	sm := store.GetSchema()
	if err := sm.AddPropertyIndex("nonnull", "Item", []string{"value"}); err != nil {
		b.Fatal(err)
	}
	if err := sm.BackfillPropertyIndex("Item", "value", values); err != nil {
		b.Fatal(err)
	}
	ctx := b.Context()
	for _, kind := range []string{"correct-scan", "indexed"} {
		b.Run(kind, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var ids []NodeID
				if kind == "correct-scan" {
					nodes, err := store.GetNodesByLabel(ctx, "Item")
					if err != nil {
						b.Fatal(err)
					}
					for _, node := range nodes {
						if node.Properties["value"] != nil {
							ids = append(ids, node.ID)
						}
					}
				} else {
					var found bool
					var err error
					ids, found, err = sm.PropertyIndexNonNilCandidates(ctx, "Item", "value")
					if err != nil || !found {
						b.Fatalf("found=%v err=%v", found, err)
					}
				}
				if len(ids) != 128 {
					b.Fatalf("incomplete nonnull population %d", len(ids))
				}
			}
			b.ReportMetric(128, "ids/op")
		})
	}
}
