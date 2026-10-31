package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

var benchmarkMVCCAdjacencyKey []byte

// BenchmarkMVCCAdjacencyKeyString measures the existing numeric-ID hit path
// for writing both directions of versioned adjacency.
func BenchmarkMVCCAdjacencyKeyString(b *testing.B) {
	for _, direction := range []string{"outgoing", "incoming"} {
		b.Run(direction, func(b *testing.B) {
			engine := newTestMemoryEngine(b)
			txn, readTs, err := engine.db.beginTxn(true)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { txn.Discard(); engine.db.endRead(readTs) })
			version := MVCCVersion{CommitTimestamp: time.Unix(1700000000, 0).UTC(), CommitSequence: 1}
			if _, err := engine.mvccAdjacencyKeyString(mvccAdjacencyOutgoing, txn, "bench:node", "bench:edge", version); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if direction == "outgoing" {
					benchmarkMVCCAdjacencyKey, err = engine.mvccAdjacencyKeyString(mvccAdjacencyOutgoing, txn, "bench:node", "bench:edge", version)
				} else {
					benchmarkMVCCAdjacencyKey, err = engine.mvccAdjacencyKeyString(mvccAdjacencyIncoming, txn, "bench:node", "bench:edge", version)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMVCCAdjacencyDelta compares the create, delete and endpoint-change
// write paths with the same already-resolved IDs and bounded transactions.
func BenchmarkMVCCAdjacencyDelta(b *testing.B) {
	for _, mode := range []string{"create", "delete", "update"} {
		b.Run(mode, func(b *testing.B) {
			engine := newTestMemoryEngine(b)
			edge := &Edge{ID: "bench:edge", StartNode: "bench:start", EndNode: "bench:end"}
			version := MVCCVersion{CommitTimestamp: time.Unix(1700000000, 0).UTC(), CommitSequence: 1}
			if err := engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
				return engine.writeEdgeAdjacencyDeltaInTxn(txn, nil, edge, version)
			}); err != nil {
				b.Fatal(err)
			}
			var oldEdge, newEdge *Edge
			if mode != "create" {
				oldEdge = edge
			}
			if mode != "delete" {
				newEdge = edge
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				txn, readTs, err := engine.db.beginTxn(true)
				if err != nil {
					b.Fatal(err)
				}
				for range 64 {
					if err := engine.writeEdgeAdjacencyDeltaInTxn(txn, oldEdge, newEdge, version); err != nil {
						b.Fatal(err)
					}
				}
				txn.Discard()
				engine.db.endRead(readTs)
			}
		})
	}
}
