package storage

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826: what one projected label scan of a vector
// property (the read that populates a vector index, and the read an exact
// similarity query makes) leaves behind in the node body cache.
func TestR1PR1826VectorLabelScanRetention(t *testing.T) {
	b := createTestBadgerEngine(t)
	ctx := t.Context()
	const count, dims = 6000, 1536
	for start := 0; start < count; start += 250 {
		var nodes []*Node
		for i := start; i < start+250; i++ {
			vec := make([]float64, dims)
			for d := range vec {
				vec[d] = float64(i*d%997) / 997
			}
			nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("test:vec-%05d", i)), Labels: []string{"Entity"}, Properties: map[string]any{"uuid": fmt.Sprint(i), "vec": vec}})
		}
		require.NoError(t, b.BulkCreateNodes(ctx, nodes))
	}
	for i := 0; i < count; i++ {
		b.cacheDeleteNode(NodeID(fmt.Sprintf("test:vec-%05d", i)), nodeCacheAllForms)
	}
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return stats.HeapAlloc
	}
	before := heap()
	for scan := 1; scan <= 2; scan++ {
		seen := 0
		require.NoError(t, b.StreamNodesByLabelProjected(ctx, "Entity", []string{"vec"}, func(*Node) error { seen++; return nil }))
		require.Equal(t, count, seen)
		b.nodeBodyCacheMu.RLock()
		entries, bytes := len(b.nodeBodyCache), b.nodeBodyCacheBytes
		b.nodeBodyCacheMu.RUnlock()
		t.Logf("R1RETAIN scan=%d nodes=%d dims=%d bodyCacheEntries=%d bodyCacheBytes=%d heapGrowth=%d", scan, count, dims, entries, bytes, int64(heap())-int64(before))
	}
}
