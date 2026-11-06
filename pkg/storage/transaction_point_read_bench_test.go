package storage

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
)

// BenchmarkNativeTransactionPointRead measures warm pinned point reads through
// each supported node hydration policy and the relationship decoder.
func BenchmarkNativeTransactionPointRead(b *testing.B) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	if _, err = engine.CreateNode(b.Context(), &Node{ID: "point:a", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "point"}}); err != nil {
		b.Fatal(err)
	}
	if err = engine.CreateEdge(b.Context(), &Edge{ID: "point:e", StartNode: "point:a", EndNode: "point:a", Type: "R"}); err != nil {
		b.Fatal(err)
	}
	tx, err := engine.BeginTransaction(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = tx.Rollback() })
	if err = tx.SetNamespace("point"); err != nil {
		b.Fatal(err)
	}
	properties := []string{"name"}
	for _, kind := range []string{"node", "query-node", "projected-node", "edge"} {
		b.Run(kind, func(b *testing.B) {
			read := func() error {
				if kind == "edge" {
					entity, err := tx.GetEdge(b.Context(), "point:e")
					if err == nil && entity == nil {
						b.Fatal("missing edge")
					}
					return err
				}
				var entity *Node
				var err error
				switch kind {
				case "node":
					entity, err = tx.GetNode(b.Context(), "point:a")
				case "query-node":
					entity, err = tx.GetQueryNode(b.Context(), "point:a")
				case "projected-node":
					entity, err = tx.GetNodeProjected(b.Context(), "point:a", properties)
				}
				if err == nil && entity == nil {
					b.Fatal("missing node")
				}
				return err
			}
			if err := read(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := read(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("cold-node", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			reader, err := engine.BeginTransaction(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			node, readErr := reader.GetNode(b.Context(), "point:a")
			closeErr := reader.Rollback()
			if readErr != nil || node == nil || node.Properties["name"] != "point" {
				b.Fatalf("cold point read: %v, %v", node, readErr)
			}
			if closeErr != nil {
				b.Fatal(closeErr)
			}
		}
	})

}
