package storage

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

var endpointBorrowSink *Node

func BenchmarkEndpointStoredNodeExistenceRead(b *testing.B) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	id := NodeID("test:product-12345")
	_, err = engine.CreateNode(&Node{ID: id, Labels: []string{"Product"}, Properties: map[string]any{"name": "product 12345", "category": "hardware", "sku": "SKU-12345", "price": 12.5, "stock": int64(25)}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := engine.GetStoredNode(id); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		endpointBorrowSink, err = engine.getNodeSnapshotForRead(context.Background(), id, false, storedRead, true)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEndpointStoredNodeExistenceReadBuffered(b *testing.B) {
	for _, pending := range []bool{false, true} {
		name := "persisted"
		if pending {
			name = "pending"
		}
		b.Run(name, func(b *testing.B) {
			engine := newWriteBehindTestEngine(b, time.Hour, writeBehindTestOptions{manualDrain: true})
			id := NodeID("test:product-12345")
			node := &Node{ID: id, Labels: []string{"Product"}, Properties: map[string]any{"name": "product 12345", "category": "hardware", "sku": "SKU-12345", "price": 12.5, "stock": int64(25)}}
			if pending {
				tx, err := engine.BeginTransaction()
				if err != nil {
					b.Fatal(err)
				}
				if err = tx.SetNamespace("test"); err != nil {
					b.Fatal(err)
				}
				if err = tx.SetImplicit(true); err != nil {
					b.Fatal(err)
				}
				if _, err = tx.CreateNode(node); err != nil {
					b.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					b.Fatal(err)
				}
			} else if _, err := engine.CreateNode(node); err != nil {
				b.Fatal(err)
			}
			if _, err := engine.GetStoredNode(id); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				endpointBorrowSink, err = engine.getNodeSnapshotForRead(context.Background(), id, false, storedRead, true)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
