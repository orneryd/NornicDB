package server

// Close-out lane L8, round 5 (2026-11-22): L-H5-001 diagnostic stress. The
// round-2 replay of the lead's scenario reproduced one miss on the ANTLR
// parser (k1-3280: index-seek read and its immediate re-read 0 while client 0
// was dropping the index; a scan read 1; the seek read 1 a second later). This
// run widens the window (31 writers, 10 ms between DDL cycles) and, at a miss,
// records what the seek path could use at that moment: whether the index
// exists, whether its entries hold the node (LookupFull), the seekable index
// list, a re-read with different statement text (outside the result cache),
// and the DDL timestamps around the write and the read.
// L8_H5_CACHE=off runs with the query cache off. Test-only, not for merge.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestL8_H5_001_StressDiag(t *testing.T) {
	seconds := huntEnvInt("HUNT_H5_SECONDS", 900)
	only := os.Getenv("L8_H5_STRESS_PARSER")
	cache := os.Getenv("L8_H5_CACHE") != "off"
	l8ForParsers(t, func(t *testing.T) {
		if only != "" && only != nornicConfig.GetParserType() {
			t.Skip("other parser")
		}
		h := huntOpen(t, filepath.Join(t.TempDir(), "store"), huntOptions{QueryCache: cache})
		defer h.Close(t)
		l8Q(t, h, "L-H5-001", "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil)
		schema := storage.NewNamespacedEngine(h.db.GetBaseStorageForManager(), "nornic").GetSchema()
		epoch := time.Now()
		since := func() int64 { return time.Since(epoch).Microseconds() }
		var phase, acked, misses, cycles int64
		var dropStart, dropEnd, createStart, createEnd atomic.Int64
		var mu sync.Mutex
		details := []map[string]any{}
		errs := &huntH5Errors{}
		stop := time.Now().Add(time.Duration(seconds) * time.Second)
		var wg sync.WaitGroup
		for c := 0; c < 32; c++ {
			wg.Add(1)
			go func(client int) {
				defer wg.Done()
				for i := 0; time.Now().Before(stop); i++ {
					if client == 0 {
						atomic.StoreInt64(&phase, 1)
						dropStart.Store(since())
						h.Run(routeBoltAuto, "DROP INDEX h5_rb IF EXISTS", nil, time.Minute)
						dropEnd.Store(since())
						atomic.StoreInt64(&phase, 2)
						createStart.Store(since())
						h.Run(routeBoltAuto, "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil, time.Minute)
						createEnd.Store(since())
						atomic.StoreInt64(&phase, 0)
						atomic.AddInt64(&cycles, 1)
						time.Sleep(10 * time.Millisecond)
						continue
					}
					key := fmt.Sprintf("k%d-%d", client, i)
					writeStart := since()
					phaseWrite := atomic.LoadInt64(&phase)
					err := huntTx(h, errs, "readback write", func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error {
						_, err := huntRun(ctx, tx, "CREATE (:H5RB {key: $k})", map[string]any{"k": key})
						return err
					})
					if err != nil {
						continue
					}
					writeEnd := since()
					atomic.AddInt64(&acked, 1)
					params := map[string]any{"k": key}
					readStart := since()
					phaseRead := atomic.LoadInt64(&phase)
					o := h.Run(routeBoltAuto, "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", params, time.Minute)
					readEnd := since()
					if o.Failed() || !strings.Contains(l8One(o), "Integer:1") {
						atomic.AddInt64(&misses, 1)
						// What the seek path holds right now.
						idx, exists := schema.GetCompositeIndex("h5_rb")
						lookup := "no index"
						if exists && idx != nil {
							ids, lerr := idx.LookupFull(context.Background(), key)
							lookup = fmt.Sprintf("%d ids err=%v", len(ids), lerr)
						}
						seekable := []string{}
						if list, err := schema.SeekableCompositeIndexesForLabel(context.Background(), "H5RB"); err == nil {
							for _, s := range list {
								seekable = append(seekable, s.Name)
							}
						}
						again := h.Run(routeBoltAuto, "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", params, time.Minute)
						otherText := h.Run(routeBoltAuto, "MATCH (n:H5RB {key: $k})  RETURN count(n) AS c2", params, time.Minute)
						scan := h.Run(routeBoltAuto, "MATCH (n:H5RB) WHERE n.key + '' = $k RETURN count(n) AS c", params, time.Minute)
						mu.Lock()
						if len(details) < 25 {
							details = append(details, map[string]any{"key": key, "first": l8One(o), "first_code": o.Code,
								"phase_write": phaseWrite, "phase_read": phaseRead, "phase_after": atomic.LoadInt64(&phase),
								"write_us": []int64{writeStart, writeEnd}, "read_us": []int64{readStart, readEnd},
								"last_drop_us": []int64{dropStart.Load(), dropEnd.Load()}, "last_create_us": []int64{createStart.Load(), createEnd.Load()},
								"index_exists_now": exists, "index_lookup_now": lookup, "seekable_now": seekable,
								"reread_same_text": l8One(again), "reread_other_text": l8One(otherText), "scan": l8One(scan)})
						}
						mu.Unlock()
					}
				}
			}(c)
		}
		wg.Wait()
		final := l8Q(t, h, "L-H5-001", "MATCH (n:H5RB) RETURN count(n) AS c", nil)
		l8Log(t, "L8_H5_DIAG", map[string]any{"seconds": seconds, "query_cache": cache, "acknowledged": acked, "misses": misses, "ddl_cycles": cycles,
			"nodes_at_end": l8One(final), "miss_details": details})
		if misses > 0 {
			t.Errorf("%d of %d acknowledged writes not found by the next read", misses, acked)
		}
	})
}
