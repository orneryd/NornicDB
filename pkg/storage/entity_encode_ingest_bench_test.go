package storage

import (
	"fmt"
	"testing"
)

var entityEncodeSink []byte

// Model the relationship-heavy 2,000-row commits in the Northwind seeder.
func BenchmarkEncodeEntityTransactionNorthwind(b *testing.B) {
	ops := make([]Operation, 2000)
	for i := range ops {
		edge := &Edge{
			ID: EdgeID(fmt.Sprintf("test:edge-%d", i)), StartNode: "test:order", EndNode: NodeID(fmt.Sprintf("test:product-%d", i)),
			Type: "ORDERED", Properties: map[string]interface{}{"quantity": int64(i % 10), "price": float64(i) / 10},
		}
		ops[i] = Operation{Type: OpCreateEdge, Edge: edge, EdgeID: edge.ID}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		entityEncodeSink, err = EncodeEntityTransaction(EntityTransactionRequest{Namespace: "test", LogicalID: "batch", Operations: ops, Mode: CommitModeImplicit})
		if err != nil {
			b.Fatal(err)
		}
	}
}
