package storage

import (
	"bytes"
	"fmt"
	"testing"
)

// BenchmarkSemanticWALEncodingNorthwind compares the historical gob semantic
// transaction with the current MessagePack transaction and a typed-operation
// representation. It does not exercise commit, replay, replication, or disk.
func BenchmarkSemanticWALEncodingNorthwind(b *testing.B) {
	ops := make([]Operation, 2000)
	for i := range ops {
		id := EdgeID(fmt.Sprintf("test:edge-%d", i))
		ops[i] = Operation{Type: OpCreateEdge, EdgeID: id, Edge: &Edge{
			ID: id, StartNode: NodeID(fmt.Sprintf("test:order-%d", i)),
			EndNode: NodeID(fmt.Sprintf("test:product-%d", i)), Type: "ORDERS",
			Properties: map[string]any{"quantity": int64(i % 10), "price": float64(i) / 10},
		}}
	}
	for _, codec := range []struct {
		name   string
		encode func(string, string, []Operation, ...EntityCommitMode) ([]byte, error)
	}{
		{"gob-transaction", encodeV4GobEntityTransaction},
		{"msgpack-transaction", EncodeEntityTransaction},
	} {
		b.Run(codec.name, func(b *testing.B) {
			var buffer bytes.Buffer
			var wireBytes int
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				payload, err := codec.encode("test", "batch", ops, CommitModeImplicit)
				if err != nil {
					b.Fatal(err)
				}
				encoded, err := marshalWALRecoveryPayload(&buffer, walEntityCommitData{Payload: payload})
				if err != nil {
					b.Fatal(err)
				}
				wireBytes = len(encoded)
			}
			b.ReportMetric(float64(wireBytes), "wire-B/op")
		})
	}
	b.Run("typed-operations", func(b *testing.B) {
		var buffer bytes.Buffer
		value := &WALBufferedCommitData{Namespace: "test", LogicalTxID: "batch", Ops: ops}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			encoded, err := marshalWALRecoveryPayload(&buffer, value)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(len(encoded)), "wire-B/op")
		}
	})
	b.Run("typed-operations-zstd", func(b *testing.B) {
		var buffer bytes.Buffer
		value := &WALBufferedCommitData{Namespace: "test", LogicalTxID: "batch", Ops: ops}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			encoded, err := marshalWALRecoveryPayload(&buffer, value)
			if err != nil {
				b.Fatal(err)
			}
			compressed := compressWALBufferedCommitPayload(encoded)
			b.ReportMetric(float64(len(compressed)), "wire-B/op")
		}
	})
}
