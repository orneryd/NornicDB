package cypher

// BenchmarkZZAsyncStrip compares identical write workloads across storage
// stacks. The wal_writebehind stack enables BadgerEngine's rotating
// write-behind commit buffer (the proper-layer async), so the two WAL
// stacks measure buffered vs synchronous commit throughput on the same
// transactional route.

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func BenchmarkZZAsyncStrip(b *testing.B) {
	ctx := context.Background()

	buildStack := func(b *testing.B, writeBehind bool) *StorageExecutor {
		dir := b.TempDir()
		interval := 50 * time.Millisecond
		if v := os.Getenv("ZZWB_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				interval = d
			}
		}
		maxOps := 200000
		if v := os.Getenv("ZZWB_MAXOPS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				maxOps = n
			}
		}
		badger, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
			DataDir:             dir,
			WriteBehind:         writeBehind,
			WriteBehindInterval: interval,
			WriteBehindMaxOps:   maxOps,
		})
		if err != nil {
			b.Fatal(err)
		}
		// Write-behind already accepts losing the unflushed generation on
		// crash, so the WAL mirrors it: appends stop paying a write(2)
		// syscall per marker and the background sync drains them instead.
		walCfg := storage.DefaultWALConfig()
		walCfg.BatchSyncInterval = 50 * time.Millisecond
		walCfg.DeferAppendFlush = writeBehind
		wal, err := storage.NewWAL(dir+"/wal", walCfg)
		if err != nil {
			b.Fatal(err)
		}
		engine := storage.NewWALEngine(badger, wal)
		b.Cleanup(func() {
			if err := badger.FlushWriteBehind(); err != nil {
				b.Logf("flush on cleanup: %v", err)
			}
			_ = wal.Close()
			_ = badger.Close()
		})
		return NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
	}

	stacks := []struct {
		name string
		exec *StorageExecutor
	}{
		{"wal", buildStack(b, false)},
		{"wal_writebehind", buildStack(b, true)},
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
