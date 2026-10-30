// Package memorypressure supplies a deterministic uncollected-heap fixture for
// application admission tests. It is used only by tests and runs serially.
package memorypressure

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
)

// New disables automatic GC and returns a policy whose physical-usage stand-in
// includes committed heap pages, including idle pages not returned to the OS.
// A recovery must collect and scavenge; lowering a fake sample cannot pass.
func New(t *testing.T) *memory.Policy {
	return NewWithClock(t, time.Now)
}

// NewWithClock uses an injected policy clock to exercise recovery cooldowns
// without waiting for wall time. Heap usage and reclamation remain real.
func NewWithClock(t *testing.T, now func() time.Time) *memory.Policy {
	t.Helper()
	previousGC := debug.SetGCPercent(-1)
	previousLimit := debug.SetMemoryLimit(1<<63 - 1)
	t.Cleanup(func() {
		debug.SetGCPercent(previousGC)
		debug.SetMemoryLimit(previousLimit)
		runtime.GC()
	})
	debug.FreeOSMemory()
	used := func() int64 {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return int64(stats.HeapSys - stats.HeapReleased)
	}
	return memory.NewPolicy(used()+16<<20, 0, time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: used()}, nil
	}, now)
}

// Garbage makes completed work's allocation unreachable without collecting it.
// KeepAlive and noinline prevent compiler lifetime/elision from replacing it.
//
//go:noinline
func Garbage() {
	data := make([]byte, 32<<20)
	for i := 0; i < len(data); i += 4096 {
		data[i] = 1
	}
	runtime.KeepAlive(data)
}
