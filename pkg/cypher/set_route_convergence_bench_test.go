package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkSetCreateRoutes measures the SET / CREATE routes that share the
// SET applicator and the CREATE core, through Execute as a client reaches
// them. create_async runs on the async server stack, where auto-commit
// node-only CREATE takes the bulk fast path.
func BenchmarkSetCreateRoutes(b *testing.B) {
	ctx := context.Background()
	memory := func(b *testing.B) *StorageExecutor {
		exec, _ := newTestExecutor(b)
		return exec
	}
	asyncStack := func(b *testing.B) *StorageExecutor {
		dir := b.TempDir()
		badger, err := storage.NewBadgerEngine(dir)
		require.NoError(b, err)
		wal, err := storage.NewWAL(dir+"/wal", nil)
		require.NoError(b, err)
		async := storage.NewAsyncEngine(storage.NewWALEngine(badger, wal), nil)
		b.Cleanup(func() {
			_ = async.Close()
			_ = wal.Close()
			_ = badger.Close()
		})
		return NewStorageExecutor(storage.NewNamespacedEngine(async, "test"))
	}
	for _, bc := range []struct {
		name  string
		build func(b *testing.B) *StorageExecutor
		setup []string
		query string
	}{
		{name: "create_set", build: memory, query: "CREATE (n:BenchCS {id: $i}) SET n.x = 1, n.y = 'y', n:Tagged RETURN n.x AS x"},
		{name: "create_ref", build: memory, query: "CREATE (a:BenchA {name: 'x'}), (b:BenchB {name: a.name}) RETURN b.name AS n"},
		{name: "create_async", build: asyncStack, query: "CREATE (a:BenchA {id: $i}), (b:BenchB {id: $i})"},
		{name: "match_set_multi", build: memory, setup: []string{"CREATE (:BenchMS {id: 1, a: 0})"}, query: "MATCH (n:BenchMS {id: 1}) SET n.a = n.a + 1, n.b = 'x'"},
		{name: "match_set_map", build: memory, setup: []string{"CREATE (:BenchMS {id: 1, a: 0})"}, query: "MATCH (n:BenchMS {id: 1}) SET n += {a: $i, c: null}"},
		{name: "merge_set", build: memory, query: "MERGE (n:BenchMG {id: 1}) SET n.a = $i, n:Tagged"},
		{name: "match_create_set", build: memory, setup: []string{"CREATE (:BenchMC {id: 1, name: 'm'})"}, query: "MATCH (a:BenchMC {id: 1}) CREATE (b:BenchMC2 {name: a.name}) SET b.x = $i"},
	} {
		b.Run(bc.name, func(b *testing.B) {
			exec := bc.build(b)
			for _, q := range bc.setup {
				_, err := exec.Execute(ctx, q, nil)
				require.NoError(b, err)
			}
			params := map[string]interface{}{"i": int64(0)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				params["i"] = int64(i)
				if _, err := exec.Execute(ctx, bc.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
