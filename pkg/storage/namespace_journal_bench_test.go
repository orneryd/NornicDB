package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

// Measure successful WAL publications through two namespaces, including the
// admission boundary, journal encoding and the ordinary native commit owner.
func BenchmarkNamespaceJournalWALCreate(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		b.Run(fmt.Sprintf("parallel=%v", parallel), func(b *testing.B) {
			b.Cleanup(config.WithWALEnabled())
			wal, err := NewWAL(b.TempDir(), &WALConfig{SyncMode: "none"})
			if err != nil {
				b.Fatal(err)
			}
			engine := NewWALEngine(NewMemoryEngine(), wal)
			b.Cleanup(func() { _ = engine.Close() })
			var sequence atomic.Uint64
			write := func() {
				i := sequence.Add(1)
				_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("wal_bench_%d:n%d", i%2, i)), Labels: []string{"JournalBench"}})
				if err != nil {
					b.Error(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						write()
					}
				})
			} else {
				for i := 0; i < b.N; i++ {
					write()
				}
			}
		})
	}
}
