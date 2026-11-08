package nornicdb

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkMergeKeyWait times MERGE writers of a uniquely constrained key
// through a database opened with the default configuration, whose
// transaction timeout and slow-query threshold bound and log MERGE key waits
// (NOR-284). "uncontended" runs one writer at a time, so no MERGE waits;
// "contended" runs parallel writers of four shared keys, so writers wait for
// each other; waits/op reports how often one blocked.
func BenchmarkMergeKeyWait(b *testing.B) {
	config := DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := Open(b.TempDir(), config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, err := db.ExecuteCypher(ctx, "CREATE CONSTRAINT doc_id FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil); err != nil {
		b.Fatal(err)
	}
	const query = "MERGE (n:Doc {id: $k}) SET n.v = $v"

	b.Run("uncontended", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := db.ExecuteCypher(ctx, query, map[string]interface{}{"k": int64(i % 1024), "v": int64(i)}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("contended", func(b *testing.B) {
		b.ReportAllocs()
		var next, failures, waits atomic.Int64
		defer storage.SetUniqueKeyLockWaitHookForTesting(func() { waits.Add(1) })()
		b.SetParallelism(2)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := next.Add(1)
				if _, err := db.ExecuteCypher(ctx, query, map[string]interface{}{"k": 1_000_000 + i%4, "v": i}); err != nil {
					failures.Add(1)
				}
			}
		})
		if n := failures.Load(); n > 0 {
			b.Fatalf("%d contended MERGE writers failed", n)
		}
		b.ReportMetric(float64(waits.Load())/float64(b.N), "waits/op")
	})
}
