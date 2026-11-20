package nornicdb_test

// Root-cause diagnostic round 2, campaign lane B (test-only).
// G-lifecycle-start (E41): the empty-label scan measured without the result
// cache (a new parameter value per run), before and after a test-side Badger
// compaction of the closed store (the bypass: dead versions below the
// discard timestamp are dropped), against a plain close/reopen control.

import (
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

func TestRCBLifecycleScanCompactionBypass(t *testing.T) {
	for _, bypass := range []bool{false, true} {
		name := "control-reopen-only"
		if bypass {
			name = "bypass-offline-compaction"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			cfg := rcbConfig()
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
			fmt.Printf("RCBE41R2 CHECK %s live ZZDel after reopen rows=%s\n", name, rows)
		})
	}
}
