package cypher

// Scratch investigation (pd-w161-4), round 3, not for merge. Each condition
// (quiet, two CPU spinners, two writers) runs on its own freshly built
// fixture, so no condition inherits another condition's writes. Logged with
// the prefix PDW1614R3.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func pdw1614CumOf(w pdw1614Window, needle string) time.Duration {
	var best int64
	for name, ns := range w.cum {
		if strings.Contains(name, needle) && ns > best {
			best = ns
		}
	}
	return time.Duration(best)
}

func (w pdw1614Window) logSeconds(t *testing.T, name string, statements int) {
	t.Helper()
	per := func(d time.Duration) time.Duration { return (d / time.Duration(max(statements, 1))).Round(time.Millisecond) }
	stmt := time.Duration(w.stmtNs)
	bucket := func(b string) time.Duration { return time.Duration(w.buckets[b]) }
	textScan, rowText, count, mvcc := bucket("textScan"), bucket("evaluateRowValueText"), bucket("countStore"), bucket("mvcc")
	snapshot := pdw1614CumOf(w, "(*BadgerTransaction).withSnapshotViewLocked")
	t.Logf("PDW1614R3 seconds %s per statement: wall=%v cpu=%v textScan=%v evaluateRowValueText=%v countStore=%v mvccBucket=%v withSnapshotViewLocked=%v cpuOutsideMvccBucket=%v cpuOutsideTextScan=%v schedMean=%.3fms schedP99=%.3fms cpuByLabel=%v",
		name, per(w.wall), per(stmt), per(textScan), per(rowText), per(count), per(mvcc), per(snapshot), per(stmt-mvcc), per(stmt-textScan),
		w.schedMean*1000, w.schedP99*1000, w.cpu)
}

func TestPDW1614FreshConditions(t *testing.T) {
	for _, condition := range []string{"quiet", "spinners", "writers"} {
		t.Run(condition, func(t *testing.T) {
			f := pdw1614Build(t)
			f.applyEvaluationInputs(t)
			// Warm both statements once on the quiet fixture.
			if run := pdw1614RunStatement(f.store, f.search, f.searchParams(f.sizes.matches, false), false); run.err != nil {
				t.Fatalf("SEARCH: %v", run.err)
			}
			pdw1614RunStatement(f.store, f.evaluate, f.evaluateParams(), true)
			var load *pdw1614Load
			switch condition {
			case "spinners":
				load = pdw1614StartBurn(2)
			case "writers":
				load = f.startWriters(2, true, "r3")
			}
			if load != nil {
				time.Sleep(5 * time.Second)
			}
			search := f.searchParams(f.sizes.matches, false)
			var runs []pdw1614Run
			w := pdw1614Measure(t, func() {
				for i := 0; i < 3; i++ {
					runs = append(runs, pdw1614RunStatement(f.store, f.search, search, false))
				}
			})
			walls := []string{}
			for _, run := range runs {
				if run.err != nil {
					t.Fatalf("SEARCH: %v", run.err)
				}
				walls = append(walls, run.wall.Round(time.Millisecond).String())
			}
			t.Logf("PDW1614R3 runs %s search ranked-only walls=%v", condition, walls)
			w.logSeconds(t, condition+" search ranked-only", len(runs))
			for _, tx := range []bool{true, false} {
				runs = nil
				n := 3
				if !tx {
					n = 1
				}
				params := f.evaluateParams()
				w = pdw1614Measure(t, func() {
					for i := 0; i < n; i++ {
						runs = append(runs, pdw1614RunStatement(f.store, f.evaluate, params, tx))
					}
				})
				parts := []string{}
				for _, run := range runs {
					if run.err != nil {
						t.Fatalf("EVALUATE: %v", run.err)
					}
					parts = append(parts, fmt.Sprintf("%v %s", run.wall.Round(time.Millisecond), run.summary))
				}
				t.Logf("PDW1614R3 runs %s evaluate tx=%v [%s]", condition, tx, strings.Join(parts, " | "))
				w.logSeconds(t, fmt.Sprintf("%s evaluate tx=%v", condition, tx), len(runs))
			}
			load.halt(t, "r3-"+condition)
			f.close()
		})
	}
}
