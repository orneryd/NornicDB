package storage

import "testing"

// BenchmarkIDDictionaryResolveHit measures the common cached numeric-ID
// resolution path separately for nodes and relationships.
func BenchmarkIDDictionaryResolveHit(b *testing.B) {
	for _, kind := range []string{"node", "edge"} {
		b.Run(kind, func(b *testing.B) {
			e := newTestMemoryEngine(b)
			d := e.BadgerEngine.idDict
			txn, readTs, err := e.db.beginTxn(true)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { txn.Discard(); e.db.endRead(readTs) })
			resolve := func() (uint64, error) {
				if kind == "node" {
					return resolveOrAllocateNumericIDInTxn(d, txn, NodeID("bench:node"))
				}
				return resolveOrAllocateNumericIDInTxn(d, txn, EdgeID("bench:edge"))
			}
			if _, err := resolve(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := resolve(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
