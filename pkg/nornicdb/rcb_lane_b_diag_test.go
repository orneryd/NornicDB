package nornicdb_test

// Root-cause diagnostics, v1.5.2 verification campaign, lane B (test-only).
//
//   TestRCBLifecycleTombstoneCompaction  G-lifecycle-start (E41): the
//     create-and-delete cycle of the E41 probe, then the census and the empty
//     label scan before and after a close/reopen and before and after a forced
//     Badger compaction (Flatten); then the same cycle with the history
//     settings that enable the MVCC lifecycle manager.
//   TestRCBHARecordPolicy  G-replication-leader-command (E10): the journal's
//     record capacity and the oversized write under the high-availability
//     primary mode and under standalone mode.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func rcbConfig() *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchCursorMax = 128
	cfg.Database.AsyncWritesEnabled = false
	return cfg
}

func rcbExec(t *testing.T, db *nornicdb.DB, q string, params map[string]interface{}) (float64, string) {
	t.Helper()
	start := time.Now()
	res, err := db.ExecuteCypher(context.Background(), q, params)
	ms := float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if res == nil {
		return ms, "<nil>"
	}
	return ms, fmt.Sprint(res.Rows)
}

func rcbEmptyScan(t *testing.T, db *nornicdb.DB, tag string) {
	t.Helper()
	var times []float64
	var rows string
	for i := 0; i < 5; i++ {
		ms, r := rcbExec(t, db, "MATCH (n:ZZDel) WHERE n.i >= 0 RETURN count(n)", nil)
		times = append(times, ms)
		rows = r
	}
	sort.Float64s(times)
	fmt.Printf("RCBE41 SCAN %s empty-label scan median_ms=%.2f rows=%s\n", tag, times[2], rows)
}

func rcbCycles(t *testing.T, db *nornicdb.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rcbExec(t, db, "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]interface{}{"n": int64(2000)})
		rcbExec(t, db, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted", nil)
		rcbExec(t, db, "MATCH (n:ZZDel) DETACH DELETE n", nil)
	}
}

func TestRCBLifecycleTombstoneCompaction(t *testing.T) {
	const cycles = 60
	for _, variant := range []struct {
		name        string
		maxVersions int
		interval    time.Duration
	}{
		{"default-history", -1, 0},
		{"lifecycle-enabled-maxversions2-interval1s", 2, time.Second},
	} {
		t.Run(variant.name, func(t *testing.T) {
			root := t.TempDir()
			cfg := rcbConfig()
			if variant.maxVersions >= 0 {
				cfg.Database.MVCCRetentionMaxVersions = variant.maxVersions
				cfg.Database.MVCCLifecycleCycleInterval = variant.interval
			}
			fmt.Printf("RCBE41 VARIANT %s MVCCRetentionMaxVersions=%d MVCCRetentionTTL=%v MVCCLifecycleEnabled=%v interval=%v\n", variant.name,
				cfg.Database.MVCCRetentionMaxVersions, cfg.Database.MVCCRetentionTTL, cfg.Database.MVCCLifecycleEnabled, cfg.Database.MVCCLifecycleCycleInterval)
			db, err := nornicdb.Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			rcbCycles(t, db, cycles)
			if variant.maxVersions >= 0 {
				time.Sleep(4 * variant.interval)
			}
			tag := variant.name + "-c" + fmt.Sprint(cycles)
			zzEI1dCensus(tag, zzEI1dBadger(db))
			rcbEmptyScan(t, db, tag)
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			db, err = nornicdb.Open(root, cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = db.Close() }()
			rcbExec(t, db, "CREATE (:RCBTick {at: timestamp()})", nil)
			zzEI1dCensus(variant.name+"-reopen", zzEI1dBadger(db))
			rcbEmptyScan(t, db, variant.name+"-reopen")
			bdb := zzEI1dBadger(db)
			if bdb == nil {
				t.Fatal("no badger handle")
			}
			fmt.Printf("RCBE41 BADGER %s maxVersion=%d before Flatten\n", variant.name, bdb.MaxVersion())
			start := time.Now()
			flattenErr := bdb.Flatten(2)
			fmt.Printf("RCBE41 FLATTEN %s err=%v seconds=%.2f\n", variant.name, flattenErr, time.Since(start).Seconds())
			zzEI1dCensus(variant.name+"-flatten", bdb)
			rcbEmptyScan(t, db, variant.name+"-flatten")
		})
	}
}

func rcbUnwrapWAL(engine storage.Engine) (*storage.WALEngine, string) {
	var chain []string
	for i := 0; i < 10 && engine != nil; i++ {
		chain = append(chain, fmt.Sprintf("%T", engine))
		if w, ok := engine.(*storage.WALEngine); ok {
			return w, strings.Join(chain, " > ")
		}
		inner, ok := engine.(interface{ GetInnerEngine() storage.Engine })
		if !ok {
			break
		}
		engine = inner.GetInnerEngine()
	}
	return nil, strings.Join(chain, " > ")
}

func TestRCBHARecordPolicy(t *testing.T) {
	over := strings.Repeat("a", 17<<20)
	for _, mode := range []string{"ha_standby-primary", "standalone"} {
		t.Run(mode, func(t *testing.T) {
			if mode != "standalone" {
				t.Setenv("NORNICDB_CLUSTER_MODE", "ha_standby")
				t.Setenv("NORNICDB_CLUSTER_HA_ROLE", "primary")
				t.Setenv("NORNICDB_CLUSTER_HA_PEER_ADDR", "127.0.0.1:1")
			}
			root := t.TempDir()
			cfg := rcbConfig()
			db, err := nornicdb.Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			wal, chain := rcbUnwrapWAL(db.GetBaseStorageForManager())
			capacityErr := fmt.Errorf("no WAL engine found")
			if wal != nil {
				capacityErr = wal.GetWAL().CheckRecordCapacity(storage.OpCreateNode, len(over), "nornic")
			}
			fmt.Printf("RCBE10 POLICY mode=%s chain=[%s] CheckRecordCapacity(create_node, %d bytes)=%v\n", mode, chain, len(over), capacityErr)
			_, err = db.ExecuteCypher(context.Background(), "CREATE (:RCBBig {id:'big', blob:$b})", map[string]interface{}{"b": over})
			fmt.Printf("RCBE10 WRITE mode=%s over-frame %d chars err=%v\n", mode, len(over), err)
			res, countErr := db.ExecuteCypher(context.Background(), "MATCH (n:RCBBig) RETURN count(n)", nil)
			var rows interface{}
			if res != nil {
				rows = res.Rows
			}
			fmt.Printf("RCBE10 STORED mode=%s rows=%v err=%v\n", mode, rows, countErr)
			if err := db.Close(); err != nil {
				fmt.Printf("RCBE10 CLOSE mode=%s err=%v\n", mode, err)
			}
		})
	}
}
