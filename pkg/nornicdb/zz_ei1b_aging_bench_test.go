package nornicdb_test

// Untracked probe (batch 1b, follow-up runs 1 and 2), one self-contained file
// applied identically to both arms of the lab's paired A/B mode (same worker).
// The repeated create-and-delete cycle of item 9's aging test, through the
// database's own Cypher entry point (DB.ExecuteCypher, same signature on both
// builds), on a database opened with nornicdb.Open over a known directory so
// that the store can be reopened.
//
// Each sub-benchmark: 150 cycles of {seed 2,000 nodes; timed
// MATCH (n:ZZDel) DELETE n RETURN count(*); MATCH (n:ZZDel) DETACH DELETE n},
// a clean close and a reopen of the same store, then 60 more cycles.
// hist100: Database.MVCCRetentionMaxVersions = 100 (the server tests' setting
// that item 9 ran with); hist-default: left at DefaultConfig's value. Other
// settings as the server test fixture: decay and auto-links off,
// SearchCursorMax 128, asynchronous writes off; embedding off.
// Reports the median delete time of cycles 1-15, 136-150 (before the reopen),
// 151-165 (first after the reopen) and 196-210; every duration is printed.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

func zzEI1bAgingMedian(ms []float64) float64 {
	s := append([]float64(nil), ms...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func BenchmarkZZEI1bDeleteAging(b *testing.B) {
	ctx := context.Background()
	for _, hist := range []string{"hist100", "hist-default"} {
		b.Run(hist, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				root, err := os.MkdirTemp("", "zz-ei1b-aging-*")
				if err != nil {
					b.Fatal(err)
				}
				cfg := nornicdb.DefaultConfig()
				cfg.Memory.DecayEnabled = false
				cfg.Memory.AutoLinksEnabled = false
				cfg.Memory.EmbeddingEnabled = false
				cfg.Memory.SearchCursorMax = 128
				cfg.Database.AsyncWritesEnabled = false
				defaultHist := cfg.Database.MVCCRetentionMaxVersions
				if hist == "hist100" {
					cfg.Database.MVCCRetentionMaxVersions = 100
				}
				fmt.Printf("ZZEI1B aging %s: MVCCRetentionMaxVersions=%d (DefaultConfig value %d)\n", hist, cfg.Database.MVCCRetentionMaxVersions, defaultHist)
				var all []float64
				cycle := func(db *nornicdb.DB, i int) {
					if _, err := db.ExecuteCypher(ctx, "UNWIND range(1, $n) AS i CREATE (:ZZDel {i: i})", map[string]interface{}{"n": int64(2000)}); err != nil {
						b.Fatalf("seed %d: %v", i, err)
					}
					start := time.Now()
					res, err := db.ExecuteCypher(ctx, "MATCH (n:ZZDel) DELETE n RETURN count(*) AS deleted", nil)
					d := time.Since(start)
					if err != nil {
						b.Fatalf("delete %d: %v", i, err)
					}
					if res == nil || fmt.Sprint(res.Rows) != "[[2000]]" {
						fmt.Printf("ZZEI1B aging %s cycle %d delete result %v\n", hist, i, res)
					}
					if _, err := db.ExecuteCypher(ctx, "MATCH (n:ZZDel) DETACH DELETE n", nil); err != nil {
						b.Fatalf("clean-up %d: %v", i, err)
					}
					all = append(all, float64(d.Microseconds())/1000)
				}
				db, err := nornicdb.Open(root, cfg)
				if err != nil {
					b.Fatal(err)
				}
				for i := 1; i <= 150; i++ {
					cycle(db, i)
				}
				if err := db.Close(); err != nil {
					fmt.Printf("ZZEI1B aging %s close error: %v\n", hist, err)
				}
				db, err = nornicdb.Open(root, cfg)
				if err != nil {
					b.Fatalf("reopen: %v", err)
				}
				for i := 151; i <= 210; i++ {
					cycle(db, i)
				}
				_ = db.Close()
				_ = os.RemoveAll(root)
				var parts []string
				for i := 0; i < len(all); i += 15 {
					end := i + 15
					var blk []string
					for _, v := range all[i:end] {
						blk = append(blk, fmt.Sprintf("%.0f", v))
					}
					parts = append(parts, fmt.Sprintf("%d-%d:[%s]", i+1, end, strings.Join(blk, " ")))
				}
				fmt.Printf("ZZEI1B aging %s durations ms (reopen after cycle 150): %s\n", hist, strings.Join(parts, " "))
				b.ReportMetric(zzEI1bAgingMedian(all[0:15]), "ms_c1-15")
				b.ReportMetric(zzEI1bAgingMedian(all[135:150]), "ms_c136-150")
				b.ReportMetric(zzEI1bAgingMedian(all[150:165]), "ms_c151-165")
				b.ReportMetric(zzEI1bAgingMedian(all[195:210]), "ms_c196-210")
			}
		})
	}
}
