package nornicdb_test

// Root-cause diagnostic round 2, campaign lane B (test-only).
// G-lifecycle-start (E41): the empty-label scan measured without the result
// cache (a new parameter value per run), before and after a test-side Badger
// compaction of the closed store (the bypass: dead versions below the
// discard timestamp are dropped), against a plain close/reopen control.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func rcbUncachedEmptyScan(t *testing.T, db *nornicdb.DB, tag string, salt int) {
	t.Helper()
	var parts []string
	for rep := 0; rep < 3; rep++ {
		ms, rows := rcbExec(t, db, "MATCH (n:ZZDel) WHERE n.i >= $k RETURN count(n)", map[string]interface{}{"k": int64(-1000 - salt*10 - rep)})
		parts = append(parts, fmt.Sprintf("%.2f(%s)", ms, rows))
	}
	fmt.Printf("RCBE41R2 SCAN %s uncached empty-label scan ms: %v\n", tag, parts)
}

func TestL9LifecycleScanCompactionBypass(t *testing.T) {
	for _, retention := range []int{0, 2, 100} {
		t.Run(fmt.Sprintf("retention_%d", retention), func(t *testing.T) {
			rcbLifecycleScanCompactionBypass(t, retention)
		})
	}
}

func rcbLifecycleScanCompactionBypass(t *testing.T, retention int) {
	for _, bypass := range []bool{false, true} {
		name := "control-reopen-only"
		if bypass {
			name = "bypass-offline-compaction"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			cfg := rcbConfig()
			cfg.Database.MVCCRetentionMaxVersions = retention
			db, err := nornicdb.Open(root, cfg)
			if err != nil {
				t.Fatal(err)
			}
			rcbUncachedEmptyScan(t, db, name+"-c0", 0)
			rcbCycles(t, db, 60)
			zzEI1dCensus(name+"-c60", zzEI1dBadger(db))
			rcbUncachedEmptyScan(t, db, name+"-c60", 1)
			bdb := zzEI1dBadger(db)
			if bdb == nil {
				t.Fatal("no badger handle")
			}
			dir, valueDir := bdb.Opts().Dir, bdb.Opts().ValueDir
			if err := db.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if bypass {
				opts := badger.DefaultOptions(dir).WithValueDir(valueDir).WithNumLevelZeroTables(1).WithCompactL0OnClose(true).WithLogger(nil)
				start := time.Now()
				b2, err := badger.OpenManaged(opts)
				if err != nil {
					t.Fatalf("open managed for compaction: %v", err)
				}
				maxVersion := b2.MaxVersion()
				b2.SetDiscardTs(maxVersion)
				flattenErr := b2.Flatten(1)
				closeErr := b2.Close()
				if flattenErr != nil || closeErr != nil {
					t.Fatalf("compaction: %v; close: %v", flattenErr, closeErr)
				}
				fmt.Printf("RCBE41R2 BYPASS dir=%s maxVersion=%d discardTs=%d flattenErr=%v closeErr=%v seconds=%.2f\n", dir, maxVersion, maxVersion, flattenErr, closeErr, time.Since(start).Seconds())
			}
			db, err = nornicdb.Open(root, cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer func() { _ = db.Close() }()
			zzEI1dCensus(name+"-reopened", zzEI1dBadger(db))
			rcbUncachedEmptyScan(t, db, name+"-reopened", 2)
			_, rows := rcbExec(t, db, "MATCH (n:ZZDel) RETURN count(n)", nil)
			if rows != "[[0]]" {
				t.Fatalf("live label population changed: %s", rows)
			}
			fmt.Printf("RCBE41R2 CHECK %s live ZZDel after reopen rows=%s\n", name, rows)
		})
	}
}

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

func rcbCycles(t *testing.T, db *nornicdb.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rcbExec(t, db, "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]interface{}{"n": int64(2000)})
		rcbExec(t, db, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted", nil)
		rcbExec(t, db, "MATCH (n:ZZDel) DETACH DELETE n", nil)
	}
}

