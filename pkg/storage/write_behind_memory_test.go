package storage

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWriteBehind_MemoryComesDownAfterBurst pins the write-behind buffer's
// memory behavior around a write burst: while the burst runs, buffered
// generations hold the unflushed state (bounded by the size cap); once the
// burst ends and the flusher drains, the buffer must return to empty and
// release what it held — including the pending-node index the schema
// attaches, whose maps Go never shrinks.
func TestWriteBehind_MemoryComesDownAfterBurst(t *testing.T) {
	engine := newWriteBehindTestEngine(t, 50*time.Millisecond)

	heapMB := func() float64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return float64(m.HeapAlloc) / (1 << 20)
	}

	base := heapMB()

	const n = 50000
	for i := 0; i < n; i++ {
		bufferedCreate(t, engine, &Node{
			ID:         NodeID(fmt.Sprintf("test:n%d", i)),
			Labels:     []string{"L"},
			Properties: map[string]any{"v": int64(i)},
		})
	}

	// Attach a schema with an indexed pair so the pending-node index is
	// built for the rest of the burst, exactly as a Pokec-style load does.
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddPropertyIndex("l_v_idx", "L", []string{"v"}))
	for i := 0; i < n/2; i++ {
		bufferedCreate(t, engine, &Node{
			ID:         NodeID(fmt.Sprintf("test:m%d", i)),
			Labels:     []string{"L"},
			Properties: map[string]any{"v": int64(i)},
		})
	}

	// Wait for retirement, not just publication: PendingOps can reach zero
	// before removeDrained releases the generation and pending-node index.
	require.Eventually(t, func() bool {
		engine.writeBehind.mu.RLock()
		defer engine.writeBehind.mu.RUnlock()
		return engine.writeBehind.opCount == 0 && len(engine.writeBehind.draining) == 0
	}, 60*time.Second, time.Millisecond, "buffer must retire every generation after the burst")
	require.Zero(t, engine.writeBehind.PendingOps(), "buffer must drain after the burst")

	// The active generation rotates on the flush cadence, so what the burst
	// appended is gone from the buffer shortly after it ends.
	engine.writeBehind.mu.RLock()
	activeCommits := len(engine.writeBehind.active.commits)
	activeNodes := len(engine.writeBehind.active.nodes)
	pendingIndex := engine.writeBehind.pendingIndex
	draining := len(engine.writeBehind.draining)
	targetOps := engine.writeBehind.targetOps
	engine.writeBehind.mu.RUnlock()
	require.Zero(t, draining, "no draining generations may remain")
	require.True(t, activeCommits == 0 && activeNodes == 0,
		"the active generation must have rotated past the burst (commits=%d nodes=%d)", activeCommits, activeNodes)
	require.Nil(t, pendingIndex,
		"the pending-node index must be released once every generation has retired")

	// The target tracks measured replay throughput, not live memory. It may
	// remain at the cap while idle: newCommitBuffer does not preallocate to
	// that target, and empty ticks do not change the last throughput sample.
	// Assert released state above and retained heap below instead.

	// Heap returns toward baseline plus the database itself (Badger's
	// in-memory tables and caches hold the committed nodes). Budget
	// generously per node so this stays stable across Go runtimes: the
	// point is that burst-sized buffered state is gone, not that the store
	// is free.
	settled := heapMB()
	t.Logf("retired burst: target=%d active_commits=%d active_nodes=%d draining=%d pending_index_released=%t heap_growth=%.1fMB",
		targetOps, activeCommits, activeNodes, draining, pendingIndex == nil, settled-base)
	const perNodeBudgetMB = float64(n) * 0.002 // ~2KB/node of stored data
	if settled-base > perNodeBudgetMB {
		t.Fatalf("heap did not come down after the burst: baseline=%.1fMB settled=%.1fMB growth=%.1fMB > budget=%.1fMB",
			base, settled, settled-base, perNodeBudgetMB)
	}
}
