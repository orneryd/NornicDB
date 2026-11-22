package nornicdb_test

// Untracked probe (batch 1b, fourth request: what resets or avoids the
// repeated-delete slow-down). Identical on both builds; uses the helpers of
// zz_ei1d_cause_test.go. Three variants, each on a fresh store with the
// aging-run settings: 120 cycles of {seed 2,000 :ZZDel; MATCH (n:ZZDel)
// DELETE n RETURN count(*); DETACH DELETE clean-up}, then an action, then 15
// more cycles. Each variant prints the census, the raw label-index scan and
// the medians of delete, live scan and empty-label scan for cycles 106-120
// and for the 15 cycles after the action.
//   backup-restore: DB.Backup to a file, then DB.Restore of that file into the
//     same open database (the calls behind /admin/backup and /admin/restore).
//   flatten: Badger's Flatten on the store's handle (mechanism check only; no
//     user-facing command does this).
//   hist2-prune: history setting 2 (enables the lifecycle manager), then
//     DB.TriggerLifecyclePrune.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func TestZZEI1dRemedy(t *testing.T) {
	ctx := context.Background()
	for _, variant := range []string{"backup-restore", "flatten", "hist2-prune"} {
		root := t.TempDir()
		cfg := nornicdb.DefaultConfig()
		cfg.Memory.DecayEnabled = false
		cfg.Memory.AutoLinksEnabled = false
		cfg.Memory.EmbeddingEnabled = false
		cfg.Memory.SearchCursorMax = 128
		cfg.Database.AsyncWritesEnabled = false
		if variant == "hist2-prune" {
			cfg.Database.MVCCRetentionMaxVersions = 2
		}
		fmt.Printf("ZZEI1E %s: MVCCRetentionMaxVersions=%d\n", variant, cfg.Database.MVCCRetentionMaxVersions)
		db, err := nornicdb.Open(root, cfg)
		if err != nil {
			t.Fatal(err)
		}
		run := func(q string, p map[string]interface{}) (float64, string) {
			s := time.Now()
			res, err := db.ExecuteCypher(ctx, q, p)
			d := float64(time.Since(s).Microseconds()) / 1000
			if err != nil {
				t.Fatalf("%s %s: %v", variant, q, err)
			}
			if res == nil {
				return d, "<nil>"
			}
			return d, fmt.Sprint(res.Rows)
		}
		run("UNWIND range(1, 10) AS i CREATE (:ZZCtl {i: i})", nil)
		var del, live, empty []float64
		cycle := func(i int) {
			run("UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]interface{}{"n": int64(2000)})
			d, r := run("MATCH (n:ZZDel) WHERE n.i >= 0 RETURN count(n)", nil)
			live = append(live, d)
			if r != "[[2000]]" {
				fmt.Printf("ZZEI1E %s cycle %d live %s\n", variant, i, r)
			}
			d, r = run("MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted", nil)
			del = append(del, d)
			if r != "[[2000]]" {
				fmt.Printf("ZZEI1E %s cycle %d delete %s\n", variant, i, r)
			}
			run("MATCH (n:ZZDel) DETACH DELETE n", nil)
			d, r = run("MATCH (n:ZZDel) WHERE n.i >= 0 RETURN count(n)", nil)
			empty = append(empty, d)
			if r != "[[0]]" {
				fmt.Printf("ZZEI1E %s cycle %d empty %s\n", variant, i, r)
			}
		}
		report := func(tag string, from int) {
			fmt.Printf("ZZEI1E %s medians %s: delete %.1f ms, livescan %.1f ms, emptyscan %.1f ms\n", variant, tag,
				zzEI1dMedian(del[from:from+15]), zzEI1dMedian(live[from:from+15]), zzEI1dMedian(empty[from:from+15]))
		}
		for i := 1; i <= 15; i++ {
			cycle(i)
		}
		report("c1-15", 0)
		for i := 16; i <= 120; i++ {
			cycle(i)
		}
		report("c106-120", 105)
		zzEI1dCensus(variant+"-before", zzEI1dBadger(db))
		start := time.Now()
		switch variant {
		case "backup-restore":
			file := filepath.Join(t.TempDir(), "zz.backup")
			if err := db.Backup(ctx, file); err != nil {
				t.Fatalf("backup: %v", err)
			}
			st, _ := os.Stat(file)
			fmt.Printf("ZZEI1E backup-restore: backup %d bytes in %.0f ms\n", st.Size(), float64(time.Since(start).Microseconds())/1000)
			s2 := time.Now()
			if err := db.Restore(ctx, file); err != nil {
				t.Fatalf("restore: %v", err)
			}
			fmt.Printf("ZZEI1E backup-restore: restore in %.0f ms\n", float64(time.Since(s2).Microseconds())/1000)
		case "flatten":
			bdb := zzEI1dBadger(db)
			if err := bdb.Flatten(4); err != nil {
				fmt.Printf("ZZEI1E flatten error: %v\n", err)
			}
			fmt.Printf("ZZEI1E flatten in %.0f ms\n", float64(time.Since(start).Microseconds())/1000)
		case "hist2-prune":
			st, _ := db.LifecycleStatus()
			fmt.Printf("ZZEI1E hist2-prune lifecycle status before: %v\n", st)
			if err := db.TriggerLifecyclePrune(ctx); err != nil {
				fmt.Printf("ZZEI1E hist2-prune error: %v\n", err)
			}
			st, _ = db.LifecycleStatus()
			fmt.Printf("ZZEI1E hist2-prune in %.0f ms; status after: %v\n", float64(time.Since(start).Microseconds())/1000, st)
		}
		zzEI1dCensus(variant+"-after", zzEI1dBadger(db))
		_, r := run("MATCH (n:ZZCtl) WHERE n.i >= 0 RETURN count(n)", nil)
		fmt.Printf("ZZEI1E %s control nodes after action: %s\n", variant, r)
		for i := 121; i <= 135; i++ {
			cycle(i)
		}
		report("c121-135 (after action)", 120)
		_ = db.Close()
	}
}
