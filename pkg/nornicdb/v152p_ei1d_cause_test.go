package nornicdb_test

// Untracked probe (batch 1b, fourth request: the cause of the repeated-delete
// slow-down). Identical on both builds. The cycle of the earlier aging runs
// (DB.ExecuteCypher; seed 2,000 :ZZDel nodes; timed MATCH (n:ZZDel) DELETE n
// RETURN count(*); DETACH DELETE clean-up), 240 cycles, clean close and reopen
// of the same store after cycle 150, history setting at DefaultConfig's value.
// Settings as the aging runs: decay and auto-links off, SearchCursorMax 128,
// asynchronous writes off, embedding off.
//
// Per cycle it also times: the seed; a read-only label scan of the 2,000 live
// nodes before the delete (MATCH (n:ZZDel) WHERE n.i >= 0 RETURN count(n));
// the same scan on the empty label after clean-up; and the same scan of a
// control label :ZZCtl holding 10 nodes created once at the start.
// At sampled cycles it counts every stored Badger version by key prefix
// (all versions, deletion markers, latest live keys, keys containing ZZDel)
// and prints Badger's table statistics, and times a raw Badger iteration of
// the label-index prefix (0x03) at the latest read timestamp.
// CPU profiles: the delete statement only (pprof label phase=delete), cycles
// 1-15, 136-150 and 226-240, printed with go tool pprof -top.

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func zzEI1dMedian(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func zzEI1dBadger(db *nornicdb.DB) *badger.DB {
	var e storage.Engine = db.GetBaseStorageForManager()
	for i := 0; i < 10 && e != nil; i++ {
		if b, ok := e.(interface{ DB() *badger.DB }); ok {
			return b.DB()
		}
		if w, ok := e.(interface{ GetInnerEngine() storage.Engine }); ok {
			e = w.GetInnerEngine()
			continue
		}
		fmt.Printf("ZZEI1D unwrap stopped at %T\n", e)
		return nil
	}
	return nil
}

func zzEI1dCensus(tag string, bdb *badger.DB) {
	if bdb == nil {
		fmt.Printf("ZZEI1D census %s: no badger handle\n", tag)
		return
	}
	type agg struct{ versions, deleted, latestLive, zzVersions, zzDeleted, zzLatestLive int }
	m := map[byte]*agg{}
	get := func(p byte) *agg {
		if m[p] == nil {
			m[p] = &agg{}
		}
		return m[p]
	}
	start := time.Now()
	txn := bdb.NewTransactionAt(math.MaxUint64, false)
	it := txn.NewIterator(badger.IteratorOptions{AllVersions: true, PrefetchValues: false})
	var lastKey []byte
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		if len(k) == 0 {
			continue
		}
		a := get(k[0])
		zz := strings.Contains(string(k), "ZZDel")
		a.versions++
		if zz {
			a.zzVersions++
		}
		del := item.IsDeletedOrExpired()
		if del {
			a.deleted++
			if zz {
				a.zzDeleted++
			}
		}
		first := lastKey == nil || string(lastKey) != string(k)
		if first {
			lastKey = append(lastKey[:0], k...)
			if !del {
				a.latestLive++
				if zz {
					a.zzLatestLive++
				}
			}
		}
	}
	it.Close()
	txn.Discard()
	var keys []int
	for p := range m {
		keys = append(keys, int(p))
	}
	sort.Ints(keys)
	var parts []string
	for _, p := range keys {
		a := m[byte(p)]
		parts = append(parts, fmt.Sprintf("0x%02X{ver=%d del=%d live=%d zzVer=%d zzDel=%d zzLive=%d}", p, a.versions, a.deleted, a.latestLive, a.zzVersions, a.zzDeleted, a.zzLatestLive))
	}
	fmt.Printf("ZZEI1D census %s (%.0f ms): %s\n", tag, float64(time.Since(start).Microseconds())/1000, strings.Join(parts, " "))
	lv := map[int][2]uint64{}
	for _, t := range bdb.Tables() {
		x := lv[t.Level]
		x[0] += uint64(t.KeyCount)
		x[1]++
		lv[t.Level] = x
	}
	var lp []string
	for l := 0; l < 8; l++ {
		if x, ok := lv[l]; ok {
			lp = append(lp, fmt.Sprintf("L%d{tables=%d keys=%d}", l, x[1], x[0]))
		}
	}
	lsm, vlog := bdb.Size()
	fmt.Printf("ZZEI1D lsm %s: %s lsmBytes=%d vlogBytes=%d\n", tag, strings.Join(lp, " "), lsm, vlog)
	// Raw iteration of the label-index prefix at the latest read timestamp
	// (deleted keys are skipped by Badger, not returned).
	start = time.Now()
	txn = bdb.NewTransactionAt(math.MaxUint64, false)
	it = txn.NewIterator(badger.IteratorOptions{Prefix: []byte{0x03}, PrefetchValues: false})
	n := 0
	for it.Rewind(); it.ValidForPrefix([]byte{0x03}); it.Next() {
		n++
	}
	it.Close()
	txn.Discard()
	fmt.Printf("ZZEI1D rawscan %s: prefix 0x03 returned %d keys in %.2f ms\n", tag, n, float64(time.Since(start).Microseconds())/1000)
}

func zzEI1dPprofTop(tag, file string) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		goBin = filepath.Join(runtime.GOROOT(), "bin", "go")
	}
	for _, mode := range [][]string{{"-tagfocus=phase=delete", "-top", "-nodecount=30"}, {"-tagfocus=phase=delete", "-top", "-cum", "-nodecount=45"}, {"-top", "-cum", "-nodecount=30"}} {
		args := append([]string{"tool", "pprof"}, mode...)
		args = append(args, file)
		out, err := exec.Command(goBin, args...).CombinedOutput()
		fmt.Printf("ZZEI1D pprof %s %v (err=%v):\n%s\nZZEI1D pprof end\n", tag, mode, err, out)
	}
}

func TestZZEI1dDeleteSlowdownCause(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	profDir := t.TempDir()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Memory.SearchCursorMax = 128
	cfg.Database.AsyncWritesEnabled = false
	fmt.Printf("ZZEI1D build go=%s MVCCRetentionMaxVersions=%d MVCCRetentionTTL=%v MVCCLifecycleEnabled=%v\n", runtime.Version(), cfg.Database.MVCCRetentionMaxVersions, cfg.Database.MVCCRetentionTTL, cfg.Database.MVCCLifecycleEnabled)

	exec1 := func(db *nornicdb.DB, q string, p map[string]interface{}) (float64, string) {
		s := time.Now()
		res, err := db.ExecuteCypher(ctx, q, p)
		d := float64(time.Since(s).Microseconds()) / 1000
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if res == nil {
			return d, "<nil>"
		}
		return d, fmt.Sprint(res.Rows)
	}
	db, err := nornicdb.Open(root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	exec1(db, "UNWIND range(1, 10) AS i CREATE (:ZZCtl {i: i})", nil)
	zzEI1dCensus("c0", zzEI1dBadger(db))

	var seed, liveScan, del, clean, emptyScan, ctlScan []float64
	var prof *os.File
	profWin := map[int]string{1: "c1-15", 136: "c136-150", 226: "c226-240"}
	sample := map[int]bool{1: true, 15: true, 30: true, 60: true, 90: true, 120: true, 150: true, 165: true, 180: true, 210: true, 240: true}
	for i := 1; i <= 240; i++ {
		if i == 151 {
			if err := db.Close(); err != nil {
				fmt.Printf("ZZEI1D close error: %v\n", err)
			}
			db, err = nornicdb.Open(root, cfg)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			zzEI1dCensus("after-reopen", zzEI1dBadger(db))
			d, r := exec1(db, "MATCH (n:ZZDel) WHERE n.i >= 0 RETURN count(n)", nil)
			fmt.Printf("ZZEI1D after-reopen empty scan %.1f ms %s\n", d, r)
		}
		if w, ok := profWin[i]; ok {
			prof, err = os.Create(filepath.Join(profDir, w+".pprof"))
			if err != nil {
				t.Fatal(err)
			}
			if err := pprof.StartCPUProfile(prof); err != nil {
				t.Fatal(err)
			}
		}
		d, _ := exec1(db, "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]interface{}{"n": int64(2000)})
		seed = append(seed, d)
		d, r := exec1(db, "MATCH (n:ZZDel) WHERE n.i >= 0 RETURN count(n)", nil)
		liveScan = append(liveScan, d)
		if r != "[[2000]]" {
			fmt.Printf("ZZEI1D cycle %d live scan %s\n", i, r)
		}
		var delRes string
		pprof.Do(ctx, pprof.Labels("phase", "delete"), func(context.Context) {
			d, delRes = exec1(db, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted", nil)
		})
		del = append(del, d)
		if delRes != "[[2000]]" {
			fmt.Printf("ZZEI1D cycle %d delete %s\n", i, delRes)
		}
		d, _ = exec1(db, "MATCH (n:ZZDel) DETACH DELETE n", nil)
		clean = append(clean, d)
		d, r = exec1(db, "MATCH (n:ZZDel) WHERE n.i >= 0 RETURN count(n)", nil)
		emptyScan = append(emptyScan, d)
		if r != "[[0]]" {
			fmt.Printf("ZZEI1D cycle %d empty scan %s\n", i, r)
		}
		d, r = exec1(db, "MATCH (n:ZZCtl) WHERE n.i >= 0 RETURN count(n)", nil)
		ctlScan = append(ctlScan, d)
		if r != "[[10]]" {
			fmt.Printf("ZZEI1D cycle %d ctl scan %s\n", i, r)
		}
		if w, ok := profWin[i-14]; ok {
			pprof.StopCPUProfile()
			prof.Close()
			_ = w
		}
		if sample[i] {
			zzEI1dCensus(fmt.Sprintf("c%d", i), zzEI1dBadger(db))
		}
	}
	_ = db.Close()
	for _, w := range []string{"c1-15", "c136-150", "c226-240"} {
		zzEI1dPprofTop(w, filepath.Join(profDir, w+".pprof"))
	}
	series := []struct {
		name string
		v    []float64
	}{{"seed", seed}, {"livescan", liveScan}, {"delete", del}, {"cleanup", clean}, {"emptyscan", emptyScan}, {"ctlscan", ctlScan}}
	for _, s := range series {
		var parts []string
		for i := 0; i < len(s.v); i += 15 {
			parts = append(parts, fmt.Sprintf("%d-%d:%.1f", i+1, i+15, zzEI1dMedian(s.v[i:i+15])))
		}
		fmt.Printf("ZZEI1D medians %s (ms per 15 cycles, reopen after 150): %s\n", s.name, strings.Join(parts, " "))
	}
	var parts []string
	for i, v := range del {
		parts = append(parts, fmt.Sprintf("%.0f", v))
		_ = i
	}
	fmt.Printf("ZZEI1D delete durations ms: %s\n", strings.Join(parts, " "))
}
