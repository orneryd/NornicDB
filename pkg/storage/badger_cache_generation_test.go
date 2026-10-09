package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A read that captured a cache's generation before a write can't put back
// what it read once the write has written to that cache (#1024).
func TestCacheFillsDropReadsThatRacedAWrite(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer engine.Close()

	gen := engine.nodeCacheGen.current()
	engine.cacheStoreNode(&Node{ID: "gen:n", Properties: map[string]any{"v": int64(2)}})
	engine.cacheFillNode(gen, &Node{ID: "gen:n", Properties: map[string]any{"v": int64(1)}})
	require.EqualValues(t, 2, engine.nodeCache["gen:n"].Properties["v"])
	gen = engine.nodeCacheGen.current()
	engine.cacheDeleteNode("gen:n")
	engine.cacheFillNode(gen, &Node{ID: "gen:n"})
	require.NotContains(t, engine.nodeCache, NodeID("gen:n"), "a deleted node isn't put back")
	engine.cacheFillNode(engine.nodeCacheGen.current(), &Node{ID: "gen:n"})
	require.Contains(t, engine.nodeCache, NodeID("gen:n"), "a read that raced nothing fills")
	engine.cacheFillNode(0, nil)

	gen = engine.edgeCacheGen.current()
	engine.cacheDeleteEdge("gen:e")
	engine.cacheFillEdge(gen, &Edge{ID: "gen:e"})
	require.NotContains(t, engine.edgeCache, EdgeID("gen:e"))
	engine.cacheFillEdge(engine.edgeCacheGen.current(), &Edge{ID: "gen:e"})
	require.Contains(t, engine.edgeCache, EdgeID("gen:e"))
	engine.cacheFillEdge(0, nil)

	gen = engine.adjCacheGen.current()
	engine.adjCacheInvalidateForEdge(&Edge{StartNode: "gen:a", EndNode: "gen:b"})
	engine.adjCacheStoreOutgoing(gen, "gen:a", []EdgeID{"gen:e"})
	engine.adjCacheStoreIncoming(gen, "gen:b", []EdgeID{"gen:e"})
	require.NotContains(t, engine.outgoingAdjCache, NodeID("gen:a"))
	require.NotContains(t, engine.incomingAdjCache, NodeID("gen:b"))

	gen = engine.labelFirstCacheGen.current()
	engine.labelCacheInvalidateForNodeLabels([]string{"L"}, "gen:n")
	engine.labelCacheSetFirst(gen, "L", "gen:n")
	require.NotContains(t, engine.labelFirstNodeCache, "L")

	_, err = engine.GetEdgesByType("T")
	require.NoError(t, err)
	require.Contains(t, engine.edgeTypeCache, "T")
	engine.InvalidateEdgeTypeCacheForType("T")
	require.NotContains(t, engine.edgeTypeCache, "T")
	gen = engine.edgeTypeCacheGen.current()
	engine.InvalidateEdgeTypeCache()
	engine.edgeTypeCacheFill(gen, "T", nil)
	require.NotContains(t, engine.edgeTypeCache, "T")
}

// Readers that miss the node cache while a node is updated never leave an
// older version cached once the writes are done (#1024). Before the fix,
// 31 of 200 rounds left a stale version.
func TestNodeCacheNotStaleAfterConcurrentUpdates(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer engine.Close()
	engine.nodeCacheMaxEntries = 4 // so reads keep missing
	for i := 0; i < 16; i++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("race:n%d", i)), Labels: []string{"L"}, Properties: map[string]interface{}{"v": int64(0)}})
		require.NoError(t, err)
	}
	for round := 0; round < 200; round++ {
		var stop atomic.Bool
		var wg sync.WaitGroup
		for r := 0; r < 8; r++ {
			r := r
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; !stop.Load(); i++ {
					_, _ = engine.GetNode(NodeID(fmt.Sprintf("race:n%d", (i+r)%16)))
				}
			}()
		}
		want := int64(round*100 + 50)
		for v := int64(1); v <= 50; v++ {
			require.NoError(t, engine.UpdateNode(&Node{ID: "race:n0", Labels: []string{"L"}, Properties: map[string]interface{}{"v": int64(round*100) + v}}))
		}
		stop.Store(true)
		wg.Wait()
		var stored *Node
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			item, err := txn.Get(nodeKey("race:n0"))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				stored, err = engine.decodeNodeWithEmbeddings(txn, val, "race:n0")
				return err
			})
		}))
		require.EqualValues(t, want, stored.Properties["v"])
		got, err := engine.GetNode("race:n0")
		require.NoError(t, err)
		require.EqualValues(t, want, got.Properties["v"], "round %d", round)
	}
}
