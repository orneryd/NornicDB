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
	if _, err = engine.CreateNode(&Node{ID: "point:a", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "point"}}); err != nil {
		b.Fatal(err)
	}
	if err = engine.CreateEdge(&Edge{ID: "point:e", StartNode: "point:a", EndNode: "point:a", Type: "R"}); err != nil {
		b.Fatal(err)
	}
	tx, err := engine.BeginTransaction()
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
					entity, err := tx.GetEdge("point:e")
					if err == nil && entity == nil {
						b.Fatal("missing edge")
					}
					return err
				}
				var entity *Node
				var err error
				switch kind {
				case "node":
					entity, err = tx.GetNode("point:a")
				case "query-node":
					entity, err = tx.GetQueryNode("point:a")
				case "projected-node":
					entity, err = tx.GetNodeProjected("point:a", properties)
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
}
