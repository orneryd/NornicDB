package storage

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
)

// Each pass deletes the same committed population and rolls back. The fixture
// stays bounded while exercising the transaction's committed chunk cleanup.
func BenchmarkSnapshotDeleteEmbeddingCleanup(b *testing.B) {
	for _, size := range []int{1, 64, 2000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = engine.Close() })
			ids := make([]NodeID, size)
			for i := range ids {
				ids[i] = NodeID(fmt.Sprintf("test:delete-empty-embedding-%d", i))
				if _, err := engine.CreateNode(b.Context(), &Node{ID: ids[i], Labels: []string{"Held"}, Properties: map[string]any{"n": i}}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := engine.BeginTransaction(b.Context(), OrdinaryBegin)
				if err != nil {
					b.Fatal(err)
				}
				if err := tx.SetNamespace("test"); err != nil {
					_ = tx.Rollback()
					b.Fatal(err)
				}
				for _, id := range ids {
					if err := tx.DeleteGraphNode(b.Context(), id, CascadeEdges); err != nil {
						_ = tx.Rollback()
						b.Fatal(err)
					}
				}
				if err := tx.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			for _, id := range []NodeID{ids[0], ids[len(ids)-1]} {
				if _, err := engine.GetStoredNode(b.Context(), id); err != nil {
					b.Fatal("rollback lost node", err)
				}
			}
		})
	}
}
