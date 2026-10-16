package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The full-node cache and the query-node cache share the configured entry
// limit: reads of disjoint nodes through both cannot hold twice the budget.
func TestNodeCachesShareOneEntryLimit(t *testing.T) {
	const limit = 8
	b, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, NodeCacheMaxEntries: limit})
	require.NoError(t, err)
	defer b.Close()
	for i := 0; i < 4*limit; i++ {
		_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%03d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"i": int64(i)}})
		require.NoError(t, err)
	}
	b.invalidateCachesAfterRestore()
	most := 0
	for i := 0; i < 2*limit; i++ {
		_, err := b.GetNode(NodeID(fmt.Sprintf("test:n%03d", i)))
		require.NoError(t, err)
		_, err = b.GetQueryNode(NodeID(fmt.Sprintf("test:n%03d", 2*limit+i)))
		require.NoError(t, err)
		b.nodeCacheMu.RLock()
		if held := len(b.nodeCache) + len(b.queryNodeCache); held > most {
			most = held
		}
		b.nodeCacheMu.RUnlock()
	}
	t.Logf("NODE_CACHES limit=%d most_held=%d", limit, most)
	require.LessOrEqual(t, most, limit+1, "both caches together stay within the configured limit")
}

// A query-node cache hit is a get operation like a node-cache hit or a miss.
func TestStorageOp_ObservesQueryNodeCacheHit(t *testing.T) {
	be, reg, _, _ := setupAttachedEngine(t)
	id := NodeID("test:query-hit")
	_, err := be.CreateNode(&Node{ID: id, Labels: []string{"Doc"}})
	require.NoError(t, err)
	be.invalidateCachesAfterRestore()
	before := findOpDurationSampleCount(t, reg, "get")
	for read := 0; read < 3; read++ {
		_, err := be.GetQueryNode(id)
		require.NoError(t, err)
	}
	be.nodeCacheMu.RLock()
	_, cached := be.queryNodeCache[id]
	be.nodeCacheMu.RUnlock()
	require.True(t, cached, "the later reads are query-node cache hits")
	require.EqualValues(t, 3, findOpDurationSampleCount(t, reg, "get")-before)
}
