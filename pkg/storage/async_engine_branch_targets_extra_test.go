package storage

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type asyncDeleteHookEngine struct {
	*MemoryEngine
	onGetNode func()
}

// asyncNonStreamingEngine intentionally exposes only Engine so AsyncEngine
// falls back to AllNodes/AllEdges paths instead of optional streaming helpers.
type asyncNonStreamingEngine struct {
	Engine
}

func (e *asyncDeleteHookEngine) GetNode(id NodeID) (*Node, error) {
	if e.onGetNode != nil {
		e.onGetNode()
	}
	return e.MemoryEngine.GetNode(id)
}

func TestAsyncEngine_DeleteNode_RecheckBranches(t *testing.T) {
	makeEngine := func(t *testing.T, hook func(ae *AsyncEngine, id NodeID)) *AsyncEngine {
		t.Helper()
		inner := &asyncDeleteHookEngine{MemoryEngine: NewMemoryEngine()}
		id := NodeID("test:recheck")
		_, err := inner.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
		ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: 1_000_000})
		inner.onGetNode = func() { hook(ae, id) }
		t.Cleanup(func() { _ = ae.Close() })
		return ae
	}

	t.Run("already_deleted_on_recheck", func(t *testing.T) {
		ae := makeEngine(t, func(ae *AsyncEngine, id NodeID) {
			ae.mu.Lock()
			ae.deleteNodes[id] = true
			ae.mu.Unlock()
		})
		require.NoError(t, ae.DeleteNode("test:recheck"))
	})

	t.Run("cache_reappears_on_recheck", func(t *testing.T) {
		ae := makeEngine(t, func(ae *AsyncEngine, id NodeID) {
			ae.mu.Lock()
			ae.nodeCache[id] = &Node{ID: id, Labels: []string{"N"}}
			ae.mu.Unlock()
		})
		require.NoError(t, ae.DeleteNode("test:recheck"))
	})

	t.Run("inflight_on_recheck", func(t *testing.T) {
		ae := makeEngine(t, func(ae *AsyncEngine, id NodeID) {
			ae.mu.Lock()
			ae.inFlightNodes[id] = true
			ae.mu.Unlock()
		})
		require.NoError(t, ae.DeleteNode("test:recheck"))
		ae.mu.RLock()
		require.True(t, ae.deleteNodes["test:recheck"])
		ae.mu.RUnlock()
	})
}

func TestAsyncEngine_DeleteNode_CachedInflightBranch(t *testing.T) {
	inner := NewMemoryEngine()
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: 1_000_000})
	defer ae.Close()

	id := NodeID("test:cached-inflight")
	ae.mu.Lock()
	ae.nodeCache[id] = &Node{ID: id, Labels: []string{"N"}}
	ae.inFlightNodes[id] = true
	ae.pendingWrites = 0
	ae.mu.Unlock()

	require.NoError(t, ae.DeleteNode(id))
	ae.mu.RLock()
	require.True(t, ae.deleteNodes[id])
	require.EqualValues(t, 1, ae.pendingWrites)
	ae.mu.RUnlock()
}

func TestAsyncEngine_FlushWithResult_BulkDeleteFallbackMixedOutcomes(t *testing.T) {
	errEngine := newErrorEngine()
	errEngine.failBulkDeletes = true

	_, err := errEngine.CreateNode(&Node{ID: "test:del-ok-node", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = errEngine.CreateNode(&Node{ID: "test:del-a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = errEngine.CreateNode(&Node{ID: "test:del-b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, errEngine.CreateEdge(&Edge{ID: "test:del-ok-edge", StartNode: "test:del-a", EndNode: "test:del-b", Type: "REL"}))

	ae := NewAsyncEngine(errEngine, &AsyncEngineConfig{FlushInterval: 1_000_000})
	defer ae.Close()

	ae.mu.Lock()
	ae.deleteNodes["test:del-ok-node"] = true
	ae.deleteNodes["test:del-missing-node"] = true
	ae.deleteEdges["test:del-ok-edge"] = true
	ae.deleteEdges["test:del-missing-edge"] = true
	ae.pendingWrites = 4
	ae.mu.Unlock()

	result := ae.FlushWithResult()
	require.GreaterOrEqual(t, result.NodesDeleted, 1)
	require.GreaterOrEqual(t, result.EdgesDeleted, 1)
	require.GreaterOrEqual(t, result.DeletesFailed, 1)
	require.NotEmpty(t, result.FirstDeleteError)
}

func TestAsyncEngine_MergeFallbackAndCachePaths(t *testing.T) {
	inner := NewMemoryEngine()
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: 1_000_000})
	defer ae.Close()

	_, err := inner.CreateNode(&Node{ID: "test:engine-node", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = inner.CreateNode(&Node{ID: "test:n1", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = inner.CreateNode(&Node{ID: "test:n2", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, inner.CreateEdge(&Edge{ID: "test:engine-edge", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}))

	ae.mu.Lock()
	ae.nodeCache["test:cache-node"] = &Node{ID: "test:cache-node", Labels: []string{"Person"}}
	ae.edgeCache["test:cache-edge"] = &Edge{ID: "test:cache-edge", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}
	ae.mu.Unlock()

	// Exercise fallback path (engine.GetNodesByLabel) with visit short-circuit.
	var calls int32
	err = ae.ForEachNodeIDByLabel("Person", func(NodeID) bool {
		atomic.AddInt32(&calls, 1)
		return atomic.LoadInt32(&calls) < 2
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1))

	nodes, err := ae.AllNodes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(nodes), 2)

	edges, err := ae.AllEdges()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(edges), 2)
}

func TestAsyncEngine_StreamFallbackBranches(t *testing.T) {
	base := NewMemoryEngine()
	inner := &asyncNonStreamingEngine{Engine: base}
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: 1_000_000})
	defer ae.Close()

	_, err := base.CreateNode(&Node{ID: "tenant:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "tenant:b", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "other:c", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&Edge{ID: "edge:a", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"}))
	require.NoError(t, base.CreateEdge(&Edge{ID: "edge:b", StartNode: "tenant:b", EndNode: "tenant:a", Type: "REL"}))

	ae.mu.Lock()
	ae.nodeCache["tenant:cached"] = &Node{ID: "tenant:cached", Labels: []string{"N"}}
	ae.nodeCache["tenant:deleted-cached"] = &Node{ID: "tenant:deleted-cached", Labels: []string{"N"}}
	ae.deleteNodes["tenant:deleted-cached"] = true
	ae.deleteNodes["tenant:b"] = true
	ae.edgeCache["edge:cached"] = &Edge{ID: "edge:cached", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"}
	ae.edgeCache["edge:deleted-cached"] = &Edge{ID: "edge:deleted-cached", StartNode: "tenant:a", EndNode: "tenant:b", Type: "REL"}
	ae.deleteEdges["edge:deleted-cached"] = true
	ae.deleteEdges["edge:b"] = true
	ae.mu.Unlock()

	// The Engine contract carries the streaming kernel, so even an inner that
	// only embeds Engine streams through its promoted kernel: cached pending
	// nodes merge with the engine rows, deleted entries are skipped.
	var streamed []NodeID
	err = ae.StreamNodes(context.Background(), func(node *Node) error {
		streamed = append(streamed, node.ID)
		return nil
	})
	require.NoError(t, err)
	require.Contains(t, streamed, NodeID("tenant:cached"))
	require.NotContains(t, streamed, NodeID("tenant:deleted-cached"))
	require.NotContains(t, streamed, NodeID("tenant:b"))

	var prefixStreamed []NodeID
	err = ae.StreamNodesByPrefix(context.Background(), "tenant:", func(node *Node) error {
		prefixStreamed = append(prefixStreamed, node.ID)
		return nil
	})
	require.NoError(t, err)
	require.Contains(t, prefixStreamed, NodeID("tenant:cached"))
	require.NotContains(t, prefixStreamed, NodeID("tenant:deleted-cached"))
	require.NotContains(t, prefixStreamed, NodeID("other:c"))

	// StreamEdges fallback: skip deleted cached + deleted underlying IDs.
	var streamedEdges []EdgeID
	err = ae.StreamEdges(context.Background(), func(edge *Edge) error {
		streamedEdges = append(streamedEdges, edge.ID)
		return nil
	})
	require.NoError(t, err)
	require.NotContains(t, streamedEdges, EdgeID("edge:deleted-cached"))
	require.NotContains(t, streamedEdges, EdgeID("edge:b"))
}
