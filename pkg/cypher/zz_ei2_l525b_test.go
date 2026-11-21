package cypher

// Batch 2 L525 remaining cases (USE body, decay property view). Research only, untracked. Same text on both builds.

import (
	"context"
	"math"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func zzei2L525Alloc(statement func()) uint64 {
	statement()
	least := uint64(math.MaxUint64)
	for i := 0; i < 3; i++ {
		runtime.GC()
		var start, end runtime.MemStats
		runtime.ReadMemStats(&start)
		statement()
		runtime.ReadMemStats(&end)
		least = min(least, end.TotalAlloc-start.TotalAlloc)
	}
	return least
}

func TestZZEI2L525UseAndDecay(t *testing.T) {
	engine, home, other, vectorBytes := zzei2L525Fixture(t)
	mgr := &standardDBManager{engines: map[string]storage.Engine{"nornic": home, "other": other}, aliases: map[string]string{}}
	homeExec := zzei2L525Exec(home)
	homeExec.SetDatabaseManager(mgr)
	otherExec := zzei2L525Exec(other)
	otherExec.SetDatabaseManager(mgr)
	run := func(name string, exec *StorageExecutor, q string) {
		res, err := exec.Execute(context.Background(), q, nil)
		if err != nil {
			t.Logf("ZZEI2 L525b %s: %q ERROR %v", name, q, err)
			return
		}
		bytes := zzei2L525Alloc(func() { _, _ = exec.Execute(context.Background(), q, nil) })
		t.Logf("ZZEI2 L525b %s: %q rows=%d first=%v auto-commit %.2f MiB; stored chunk vectors %.2f MiB; decay=%v propertyViewClone=%v",
			name, q, len(res.Rows), firstRow(res), float64(bytes)/(1<<20), float64(vectorBytes)/(1<<20), engine.IsDecayEnabled(), zzei2L525PropertyView(exec, q))
	}
	q := "MATCH (s:Source) RETURN s.title AS t"
	qc := "MATCH (s:Source) WHERE s.title IS NOT NULL RETURN count(s) AS c"
	run("use/control-direct", otherExec, q)
	run("use/control-direct-count", otherExec, qc)
	run("use/use-clause", homeExec, "USE other "+q)
	run("use/use-clause-count", homeExec, "USE other "+qc)
	run("use/call-use-body", homeExec, "CALL { USE other MATCH (s:Source) RETURN s.title AS t } RETURN t")
	run("decay/off-control", otherExec, q)
	run("decay/off-count", otherExec, qc)
	engine.SetDecayEnabled(true)
	run("decay/on", otherExec, q)
	run("decay/on-count", otherExec, qc)
	run("decay/on-use-clause", homeExec, "USE other "+q)
	engine.SetDecayEnabled(false)
	run("decay/off-again", otherExec, q)
}

func firstRow(res *ExecuteResult) any {
	if res == nil || len(res.Rows) == 0 {
		return nil
	}
	return res.Rows[0]
}
