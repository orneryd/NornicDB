package cypher

// BenchmarkZZAsyncStrip compares identical write workloads across storage
// stacks now that every write takes the single transactional route.
// EXPERIMENT (async-strip, uncommitted): measurement-only helper, delete
// with the experiment.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkZZAsyncStrip(b *testing.B) {
	ctx := context.Background()

	buildStack := func(b *testing.B) *StorageExecutor {
		dir := b.TempDir()
		badger, err := storage.NewBadgerEngine(dir)
		if err != nil {
			b.Fatal(err)
		}
		wal, err := storage.NewWAL(dir+"/wal", nil)
		if err != nil {
			b.Fatal(err)
		}
		engine := storage.NewWALEngine(badger, wal)
		b.Cleanup(func() {
			_ = wal.Close()
			_ = badger.Close()
		})
		return NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
	}

	stacks := []struct {
		name string
		exec *StorageExecutor
	}{
		{"wal", buildStack(b)},
	}
	mem, _ := newTestExecutor(b)
	stacks = append(stacks, struct {
		name string
		exec *StorageExecutor
	}{"memory", mem})

	queries := []struct {
		name  string
		query string
	}{
		{"create1", "CREATE (a:ZZA {id: $i})"},
		{"create2", "CREATE (a:ZZA {id: $i}), (b:ZZB {id: $i})"},
		{"create_rel", "CREATE (a:ZZA {id: $i})-[:ZZR {w: $i}]->(b:ZZB {id: $i})"},
		{"unwind100", "UNWIND range($i, $i + 99) AS x CREATE (n:ZZU {id: x})"},
		{"merge_set", "MERGE (n:ZZM {id: $i}) SET n.x = $i"},
	}

	for _, st := range stacks {
		for _, q := range queries {
			b.Run(st.name+"/"+q.name, func(b *testing.B) {
				params := map[string]interface{}{"i": int64(0)}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					params["i"] = int64(i)
					if _, err := st.exec.Execute(ctx, q.query, params); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
