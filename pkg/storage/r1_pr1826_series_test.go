package storage

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// Review lane R1 of PR #1826: the node body cache's byte budget against what one
// projected label scan of a numeric list property leaves in it.
func TestR1PR1826SeriesLabelScanRetention(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	ctx := t.Context()
	const count, length = 1500, 50000
	for start := 0; start < count; start += 10 {
		var nodes []*Node
		for i := start; i < start+10; i++ {
			readings := make([]float64, length)
			for d := range readings {
				readings[d] = float64((i*d)%9973) / 9973
			}
			nodes = append(nodes, &Node{ID: NodeID(fmt.Sprintf("test:series-%05d", i)), Labels: []string{"Sensor"}, Properties: map[string]any{"uuid": fmt.Sprint(i), "readings": readings}})
		}
		require.NoError(t, b.BulkCreateNodes(ctx, nodes))
	}
	for i := 0; i < count; i++ {
		b.cacheDeleteNode(NodeID(fmt.Sprintf("test:series-%05d", i)), nodeCacheAllForms)
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
		require.NoError(t, b.StreamNodesByLabelProjected(ctx, "Sensor", []string{"readings"}, func(*Node) error { seen++; return nil }))
		require.Equal(t, count, seen)
		b.nodeBodyCacheMu.RLock()
		entries, bytes, limit := len(b.nodeBodyCache), b.nodeBodyCacheBytes, b.nodeBodyCacheMaxBytes
		b.nodeBodyCacheMu.RUnlock()
		t.Logf("R1SERIES scan=%d nodes=%d numbers=%d entries=%d countedBytes=%d budgetBytes=%d heapGrowth=%d", scan, count, length, entries, bytes, limit, int64(heap())-int64(before))
	}
	// The same nodes read whole, one by one (the read the base caches too).
	for i := 0; i < count; i++ {
		b.cacheDeleteNode(NodeID(fmt.Sprintf("test:series-%05d", i)), nodeCacheAllForms)
	}
	afterClear := heap()
	for i := 0; i < count; i++ {
		_, err := b.GetNode(ctx, NodeID(fmt.Sprintf("test:series-%05d", i)))
		require.NoError(t, err)
	}
	b.nodeBodyCacheMu.RLock()
	entries, bytes, limit := len(b.nodeBodyCache), b.nodeBodyCacheBytes, b.nodeBodyCacheMaxBytes
	b.nodeBodyCacheMu.RUnlock()
	b.nodeCacheMu.RLock()
	nodeEntries, nodeBytes := len(b.nodeCache), b.nodeCacheBytes
	b.nodeCacheMu.RUnlock()
	t.Logf("R1SERIES whole point reads: bodyEntries=%d bodyCountedBytes=%d budgetBytes=%d nodeEntries=%d nodeCountedBytes=%d heapGrowth=%d", entries, bytes, limit, nodeEntries, nodeBytes, int64(heap())-int64(afterClear))
}
