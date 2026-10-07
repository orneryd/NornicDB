package cypher

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkTxlogEntries reads WAL entries of database "test" through
// db.txlog.entries and db.txlog.byTxId, over a WAL of 20,000 entries split
// between two databases (#953). The result cache is off.
func BenchmarkTxlogEntries(b *testing.B) {
	dir := b.TempDir()
	badger, err := storage.NewBadgerEngine(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer badger.Close()
	wal, err := storage.NewWAL(filepath.Join(dir, "wal"), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer wal.Close()
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(storage.NewWALEngine(badger, wal), "test"), 0, 0)
	for i := 0; i < 10000; i++ {
		database := "test"
		if i%2 == 1 {
			database = "other"
		}
		if _, err := wal.AppendTxBegin(database, "tx-bench", nil); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 10000; i++ {
		if _, err := wal.AppendTxCommit("other", "tx-other", 1); err != nil {
			b.Fatal(err)
		}
	}
	if err := wal.Sync(); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for _, query := range []struct{ name, cypher string }{
		{"entries/range", "CALL db.txlog.entries(1, 0)"},
		{"entries/tail", "CALL db.txlog.entries(19000, 0)"},
		{"byTxId/limit=10", "CALL db.txlog.byTxId('tx-bench', 10)"},
	} {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			rows := 0
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(ctx, query.cypher, nil)
				if err != nil {
					b.Fatal(err)
				}
				rows = len(result.Rows)
			}
			b.ReportMetric(float64(rows), "rows/op")
		})
	}
}
