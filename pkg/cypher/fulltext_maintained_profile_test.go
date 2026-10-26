// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"
)

// This opt-in diagnostic runs only through sig/tnrun.sh. It prints profiles
// into the durable lab output instead of leaving evidence in a worker's /tmp.
func BenchmarkNOR148FulltextProfile(t *testing.B) {
	executor, _ := fulltextMaintainedFixture(t, 6000)
	ctx := context.Background()
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN count(node) AS n"
	params := map[string]any{"query": "hotel torrey pines services"}
	_, err := executor.Execute(ctx, query, params)
	require.NoError(t, err)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	dir := t.TempDir()
	cpuPath := filepath.Join(dir, "cpu.pprof")
	cpu, err := os.Create(cpuPath)
	require.NoError(t, err)
	require.NoError(t, pprof.StartCPUProfile(cpu))
	for call := 0; call < 12; call++ {
		_, err = executor.Execute(ctx, query, params)
		require.NoError(t, err)
	}
	pprof.StopCPUProfile()
	require.NoError(t, cpu.Close())
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("12 consumed count queries: bytes=%d allocations=%d live-heap-before=%d live-heap-after=%d", after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, before.HeapAlloc, after.HeapAlloc)
	heapPath := filepath.Join(dir, "allocs.pprof")
	heap, err := os.Create(heapPath)
	require.NoError(t, err)
	require.NoError(t, pprof.Lookup("allocs").WriteTo(heap, 0))
	require.NoError(t, heap.Close())
	for _, profile := range []string{cpuPath, heapPath} {
		args := []string{"tool", "pprof", "-top", "-nodecount=18", os.Args[0], profile}
		out, profileErr := exec.Command("go", args...).CombinedOutput()
		t.Logf("go tool pprof %s:\n%s", profile, out)
		require.NoError(t, profileErr)
	}
}
