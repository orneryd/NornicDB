package storage

import (
	"context"
	"testing"
)

// BenchmarkPropertyUpdateVectorJournal measures reads, encoding and complete
// flag updates on a node with 1 MiB of managed vectors. Setup is untimed.
const propertyUpdateVectorBenchChunks = 1024
const propertyUpdateVectorBenchBytes = propertyUpdateVectorBenchChunks * propertyUpdateVectorDims * 4

func BenchmarkPropertyUpdateVectorJournal(b *testing.B) {
	engine, w := newPropertyUpdateVectorEngine(b, WALRecordsLarge)
	const id NodeID = "test:bench"
	seedPropertyUpdateVectorNode(b, engine, w, id, propertyUpdateVectorBenchChunks)

	b.Run("GetNode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(propertyUpdateVectorBenchBytes)
		for i := 0; i < b.N; i++ {
			if _, err := engine.GetNode(context.Background(), id); err != nil {
				b.Fatal(err)
			}
		}
	})

	image, err := engine.GetNode(context.Background(), id)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("EncodeEntityCommit", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(propertyUpdateVectorBenchBytes)
		operations := []Operation{{Type: OpUpdateNode, Node: image}}
		for i := 0; i < b.N; i++ {
			payload, err := EncodeEntityTransaction(EntityTransactionRequest{
				Namespace:  "test",
				LogicalID:  "bench-encode",
				Operations: operations,
				Mode:       CommitModeDirect,
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(payload) == 0 {
				b.Fatal("empty entity commit encoding")
			}
		}
	})

	b.Run("FlagUpdate", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(propertyUpdateVectorBenchBytes)
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			node, err := engine.GetNode(ctx, id)
			if err != nil {
				b.Fatal(err)
			}
			node.Properties["flag"] = i%2 == 0
			if err := w.UpdateNode(ctx, NodeUpdateRequest{After: node}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
