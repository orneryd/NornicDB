package nornicdb_test

// Report gap S31 (v1.5.2 verification campaign, test-side only): which
// level-0 / compaction setting of the server store profile keeps the dead
// label-index versions that slow the empty-label scan.
//
// The server store uses the high-performance Badger profile
// (pkg/storage/badger.go:989-1002, selected at pkg/nornicdb/db.go:1510):
// 128 MiB memtables x5, NumLevelZeroTables 10, CompactL0OnClose false.
// Workload as the earlier bypass run: 60 cycles of creating and deleting
// 2,000 :ZZDel nodes, then the uncached empty-label scan.
//
// LIVE: the workload on the high-performance profile (the default) and on
// the low-memory profile (Storage.Mode "low": 8 MiB memtable, one level-0
// table before compaction, level-0 compaction on close); LSM layout, label
// prefix versions and scan times live and after a reopen.
// OFFLINE: copies of the closed high-performance store, each touched by Badger
// opened directly with the high-performance settings and ONE change, then
// reopened by nornicdb and scanned:
//   O0 no touch (reopen only);
//   O1 the same settings, discard timestamp at the newest version (control);
//   O2 O1 + CompactL0OnClose(true);
//   O3 O1 + NumLevelZeroTables(1), waiting for level 0 to drain;
//   O4 O2 without the discard timestamp;
//   O5 O3 without the discard timestamp.
// Diagnostic: fails only on harness errors.

import (
	"context"
	"fmt"
	"math"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func gapS31Config(mode string) *nornicdb.Config {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchCursorMax = 128
	cfg.Database.AsyncWritesEnabled = false
	if mode != "" {
		cfg.Storage.Mode = mode
	}
	return cfg
}

func gapS31Exec(t *testing.T, db *nornicdb.DB, q string, params map[string]interface{}) (float64, string) {
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

var gapS31Salt int64

func gapS31Scan(t *testing.T, db *nornicdb.DB) string {
	var times []float64
	rows := ""
	for rep := 0; rep < 5; rep++ {
		gapS31Salt++
		ms, r := gapS31Exec(t, db, "MATCH (n:ZZDel) WHERE n.i >= $k RETURN count(n)", map[string]interface{}{"k": -1000 - gapS31Salt})
		times = append(times, ms)
		rows = r
	}
	sort.Float64s(times)
	return fmt.Sprintf("scan_median_ms=%.2f min=%.2f rows=%s", times[2], times[0], rows)
}

func gapS31Cycles(t *testing.T, db *nornicdb.DB, n int) {
	for i := 0; i < n; i++ {
		gapS31Exec(t, db, "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]interface{}{"n": int64(2000)})
		gapS31Exec(t, db, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted", nil)
		gapS31Exec(t, db, "MATCH (n:ZZDel) DETACH DELETE n", nil)
	}
}

func gapS31Badger(db *nornicdb.DB) *badger.DB {
	var e storage.Engine = db.GetBaseStorageForManager()
	for i := 0; i < 10 && e != nil; i++ {
		if b, ok := e.(interface{ DB() *badger.DB }); ok {
			return b.DB()
		}
		if w, ok := e.(interface{ GetInnerEngine() storage.Engine }); ok {
			e = w.GetInnerEngine()
			continue
		}
		return nil
	}
	return nil
}

// gapS31Layout: tables per level, label-index (0x03) versions and deletion
// markers, and a raw iteration of the 0x03 prefix at the newest timestamp.
func gapS31Layout(bdb *badger.DB) string {
	if bdb == nil {
		return "no badger handle"
	}
	lv := map[int][2]uint64{}
	for _, tb := range bdb.Tables() {
		x := lv[tb.Level]
		x[0] += uint64(tb.KeyCount)
		x[1]++
		lv[tb.Level] = x
	}
	var lp []string
	for l := 0; l < 8; l++ {
		if x, ok := lv[l]; ok {
			lp = append(lp, fmt.Sprintf("L%d{tables=%d keys=%d}", l, x[1], x[0]))
		}
	}
	if len(lp) == 0 {
		lp = append(lp, "no-tables(all-in-memtables)")
	}
	versions, deleted := 0, 0
	txn := bdb.NewTransactionAt(math.MaxUint64, false)
	it := txn.NewIterator(badger.IteratorOptions{AllVersions: true, PrefetchValues: false, Prefix: []byte{0x03}})
	for it.Rewind(); it.Valid(); it.Next() {
		versions++
		if it.Item().IsDeletedOrExpired() {
			deleted++
		}
	}
	it.Close()
	start := time.Now()
	live := 0
	it = txn.NewIterator(badger.IteratorOptions{PrefetchValues: false, Prefix: []byte{0x03}})
	for it.Rewind(); it.Valid(); it.Next() {
		live++
	}
	it.Close()
	raw := float64(time.Since(start).Microseconds()) / 1000
	txn.Discard()
	return fmt.Sprintf("lsm=[%s] label_index_0x03{versions=%d deletion_markers=%d live=%d raw_prefix_scan_ms=%.2f}", strings.Join(lp, " "), versions, deleted, live, raw)
}

// gapS31HPOptions are the high-performance profile's Badger settings
// (pkg/storage/badger.go:951 and 989-1002 at e8cd436e5).
func gapS31HPOptions(dir, valueDir string) badger.Options {
	return badger.DefaultOptions(dir).WithValueDir(valueDir).WithDetectConflicts(true).WithLogger(nil).
		WithMemTableSize(128 << 20).
		WithValueLogFileSize(256 << 20).
		WithNumMemtables(5).
		WithNumLevelZeroTables(10).
		WithNumLevelZeroTablesStall(20).
		WithValueThreshold(1 << 20).
		WithBlockCacheSize(256 << 20).
		WithIndexCacheSize(128 << 20).
		WithNumCompactors(4).
		WithCompactL0OnClose(false)
}

func gapS31L0(b *badger.DB) int {
	n := 0
	for _, tb := range b.Tables() {
		if tb.Level == 0 {
			n++
		}
	}
	return n
}

func TestGapS31LevelZeroSetting(t *testing.T) {
	const cycles = 60
	var hpRoot, badgerDir, valueDir string
	for _, profile := range []string{"high-performance", "low"} {
		root := t.TempDir()
		mode := ""
		if profile == "low" {
			mode = "low"
		}
		db, err := nornicdb.Open(root, gapS31Config(mode))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("GAPS31 LIVE profile=%s c0 %s %s\n", profile, gapS31Scan(t, db), gapS31Layout(gapS31Badger(db)))
		start := time.Now()
		gapS31Cycles(t, db, cycles)
		fmt.Printf("GAPS31 LIVE profile=%s c%d cycles_s=%.1f %s %s\n", profile, cycles, time.Since(start).Seconds(), gapS31Scan(t, db), gapS31Layout(gapS31Badger(db)))
		bdb := gapS31Badger(db)
		if bdb == nil {
			t.Fatal("no badger handle")
		}
		dir, vdir := bdb.Opts().Dir, bdb.Opts().ValueDir
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		if profile == "high-performance" {
			hpRoot, badgerDir, valueDir = root, dir, vdir
			continue
		}
		db, err = nornicdb.Open(root, gapS31Config(mode))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("GAPS31 LIVE profile=%s reopened %s %s\n", profile, gapS31Scan(t, db), gapS31Layout(gapS31Badger(db)))
		_ = db.Close()
	}
	relDir, err := filepath.Rel(hpRoot, badgerDir)
	if err != nil {
		t.Fatal(err)
	}
	relValue, err := filepath.Rel(hpRoot, valueDir)
	if err != nil {
		t.Fatal(err)
	}
	type variant struct {
		name    string
		touch   bool
		discard bool
		change  func(badger.Options) badger.Options
		drainL0 bool
	}
	variants := []variant{
		{name: "O0-reopen-only"},
		{name: "O1-hp-settings-discard", touch: true, discard: true, change: func(o badger.Options) badger.Options { return o }},
		{name: "O2-CompactL0OnClose-true", touch: true, discard: true, change: func(o badger.Options) badger.Options { return o.WithCompactL0OnClose(true) }},
		{name: "O3-NumLevelZeroTables-1", touch: true, discard: true, drainL0: true, change: func(o badger.Options) badger.Options { return o.WithNumLevelZeroTables(1) }},
		{name: "O4-CompactL0OnClose-true-no-discard", touch: true, change: func(o badger.Options) badger.Options { return o.WithCompactL0OnClose(true) }},
		{name: "O5-NumLevelZeroTables-1-no-discard", touch: true, drainL0: true, change: func(o badger.Options) badger.Options { return o.WithNumLevelZeroTables(1) }},
	}
	for _, v := range variants {
		copyRoot := filepath.Join(t.TempDir(), "store")
		if out, err := exec.Command("cp", "-a", hpRoot, copyRoot).CombinedOutput(); err != nil {
			t.Fatalf("copy: %v %s", err, out)
		}
		touch := "none"
		if v.touch {
			opts := v.change(gapS31HPOptions(filepath.Join(copyRoot, relDir), filepath.Join(copyRoot, relValue)))
			start := time.Now()
			b2, err := badger.OpenManaged(opts)
			if err != nil {
				t.Fatalf("%s: open managed: %v", v.name, err)
			}
			maxVersion := b2.MaxVersion()
			if v.discard {
				b2.SetDiscardTs(maxVersion)
			}
			l0Before := gapS31L0(b2)
			l0After := l0Before
			if v.drainL0 {
				deadline := time.Now().Add(60 * time.Second)
				for gapS31L0(b2) > 0 && time.Now().Before(deadline) {
					time.Sleep(100 * time.Millisecond)
				}
				l0After = gapS31L0(b2)
			}
			closeErr := b2.Close()
			touch = fmt.Sprintf("maxVersion=%d discardTs=%v L0_tables_at_open=%d L0_tables_before_close=%d NumLevelZeroTables=%d CompactL0OnClose=%t closeErr=%v seconds=%.2f",
				maxVersion, map[bool]string{true: fmt.Sprint(maxVersion), false: "0(not set)"}[v.discard], l0Before, l0After, opts.NumLevelZeroTables, opts.CompactL0OnClose, closeErr, time.Since(start).Seconds())
		}
		db, err := nornicdb.Open(copyRoot, gapS31Config(""))
		if err != nil {
			t.Fatalf("%s: reopen: %v", v.name, err)
		}
		fmt.Printf("GAPS31 OFFLINE %s touch={%s} reopened %s %s\n", v.name, touch, gapS31Scan(t, db), gapS31Layout(gapS31Badger(db)))
		_ = db.Close()
	}
}
