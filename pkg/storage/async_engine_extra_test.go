package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type prefixCountErrorEngine struct {
	*MemoryEngine
	streamNodeErr error
	streamEdgeErr error
	allNodesErr   error
	allEdgesErr   error
}

type labelQueryErrorEngine struct {
	*MemoryEngine
	getNodesByLabelErr error
}

type nonStreamingCountEngine struct {
	Engine
	allNodesErr error
	allEdgesErr error
}

type nilSchemaProviderEngine struct {
	*MemoryEngine
}

type finderEngine struct {
	Engine
	node *Node
}

type edgeQueryErrorEngine struct {
	*MemoryEngine
	getEdgesByTypeErr error
	getOutgoingErr    error
}

type firstLabelEngine struct {
	Engine
	node *Node
	err  error
}

type batchGetErrorEngine struct {
	*MemoryEngine
	err error
}

type nodeLabelErrorEngine struct {
	Engine
	err   error
	nodes []*Node
}

type countErrorEngine struct {
	*MemoryEngine
	nodeCountErr error
	edgeCountErr error
}

func (e *prefixCountErrorEngine) StreamNodes(_ context.Context, _ func(*Node) error) error {
	return e.streamNodeErr
}

func (e *prefixCountErrorEngine) StreamEdges(_ context.Context, _ func(*Edge) error) error {
	return e.streamEdgeErr
}

func (e *prefixCountErrorEngine) AllNodes() ([]*Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.MemoryEngine.AllNodes()
}

func (e *prefixCountErrorEngine) AllEdges() ([]*Edge, error) {
	if e.allEdgesErr != nil {
		return nil, e.allEdgesErr
	}
	return e.MemoryEngine.AllEdges()
}

func (e *labelQueryErrorEngine) GetNodesByLabel(label string) ([]*Node, error) {
	if e.getNodesByLabelErr != nil {
		return nil, e.getNodesByLabelErr
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

func (e *nonStreamingCountEngine) AllNodes() ([]*Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.Engine.AllNodes()
}

func (e *nonStreamingCountEngine) AllEdges() ([]*Edge, error) {
	if e.allEdgesErr != nil {
		return nil, e.allEdgesErr
	}
	return e.Engine.AllEdges()
}

func (e *nilSchemaProviderEngine) GetSchemaForNamespace(_ string) *SchemaManager {
	return nil
}

func (e *finderEngine) FindNodeNeedingEmbedding() *Node {
	return CopyNode(e.node)
}

func (e *edgeQueryErrorEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	if e.getEdgesByTypeErr != nil {
		return nil, e.getEdgesByTypeErr
	}
	return e.MemoryEngine.GetEdgesByType(edgeType)
}

func (e *edgeQueryErrorEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if e.getOutgoingErr != nil {
		return nil, e.getOutgoingErr
	}
	return e.MemoryEngine.GetOutgoingEdges(nodeID)
}

func (e *firstLabelEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return CopyNode(e.node), nil
}

func (e *batchGetErrorEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.MemoryEngine.BatchGetNodes(ids)
}

func (e *nodeLabelErrorEngine) GetNodesByLabel(label string) ([]*Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	out := make([]*Node, len(e.nodes))
	copy(out, e.nodes)
	return out, nil
}

func (e *countErrorEngine) NodeCount() (int64, error) {
	if e.nodeCountErr != nil {
		return 0, e.nodeCountErr
	}
	return e.MemoryEngine.NodeCount()
}

func (e *countErrorEngine) EdgeCount() (int64, error) {
	if e.edgeCountErr != nil {
		return 0, e.edgeCountErr
	}
	return e.MemoryEngine.EdgeCount()
}

func newAsyncTestEngine(t *testing.T) *AsyncEngine {
	t.Helper()
	inner := createTestBadgerEngine(t)
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{
		FlushInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { ae.Close() })
	return ae
}

func makeNode(id string) *Node {
	return &Node{
		ID:         NodeID(prefixTestID(id)),
		Labels:     []string{"TestLabel"},
		Properties: map[string]interface{}{"name": id},
	}
}

func makeEdge(id, from, to string) *Edge {
	return &Edge{
		ID:         EdgeID(prefixTestID(id)),
		StartNode:  NodeID(prefixTestID(from)),
		EndNode:    NodeID(prefixTestID(to)),
		Type:       "RELATED",
		Properties: map[string]interface{}{},
	}
}

// ============================================================================
// AllNodes / AllEdges
// ============================================================================

func TestAsyncEngine_AllNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	nodes, err := ae.AllNodes()
	require.NoError(t, err)
	assert.Empty(t, nodes)
}

func TestAsyncEngine_AllNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, err := ae.CreateNode(makeNode("n1"))
	require.NoError(t, err)
	_, err = ae.CreateNode(makeNode("n2"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	nodes, err := ae.AllNodes()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(nodes), 2)

	t.Run("cache only on engine error", func(t *testing.T) {
		engine := &prefixCountErrorEngine{
			MemoryEngine: NewMemoryEngine(),
			allNodesErr:  errors.New("all nodes failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := ae.CreateNode(makeNode("cache-only-node"))
		require.NoError(t, err)
		ae.mu.Lock()
		ae.deleteNodes[NodeID(prefixTestID("deleted-node"))] = true
		ae.nodeCache[NodeID(prefixTestID("deleted-node"))] = makeNode("deleted-node")
		ae.mu.Unlock()

		nodes, err := ae.AllNodes()
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, NodeID(prefixTestID("cache-only-node")), nodes[0].ID)
	})
}

func TestAsyncEngine_AllEdges_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edges, err := ae.AllEdges()
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestAsyncEngine_AllEdges_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, err := ae.CreateNode(makeNode("n1"))
	require.NoError(t, err)
	_, err = ae.CreateNode(makeNode("n2"))
	require.NoError(t, err)
	require.NoError(t, ae.CreateEdge(makeEdge("e1", "n1", "n2")))
	require.NoError(t, ae.Flush())

	edges, err := ae.AllEdges()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)

	t.Run("cache only on engine error", func(t *testing.T) {
		engine := &prefixCountErrorEngine{
			MemoryEngine: NewMemoryEngine(),
			allEdgesErr:  errors.New("all edges failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		cached := makeEdge("cache-edge", "n1", "n2")
		ae.mu.Lock()
		ae.edgeCache[cached.ID] = cached
		deleted := makeEdge("deleted-edge", "n1", "n2")
		ae.edgeCache[deleted.ID] = deleted
		ae.deleteEdges[deleted.ID] = true
		ae.mu.Unlock()

		edges, err := ae.AllEdges()
		require.NoError(t, err)
		require.Len(t, edges, 1)
		assert.Equal(t, cached.ID, edges[0].ID)
	})
}

// ============================================================================
// BatchGetNodes
// ============================================================================

func TestAsyncEngine_BatchGetNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	result, err := ae.BatchGetNodes([]NodeID{})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestAsyncEngine_BatchGetNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)

	id1, err := ae.CreateNode(makeNode("batchn1"))
	require.NoError(t, err)
	id2, err := ae.CreateNode(makeNode("batchn2"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	result, err := ae.BatchGetNodes([]NodeID{id1, id2})
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestAsyncEngine_BatchGetNodes_SomeMissing(t *testing.T) {
	ae := newAsyncTestEngine(t)
	result, err := ae.BatchGetNodes([]NodeID{NodeID(prefixTestID("missing"))})
	require.NoError(t, err)
	assert.Empty(t, result)

	t.Run("skips blank and deleted ids and returns cached plus engine nodes", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := engine.CreateNode(&Node{ID: "test:engine-batch", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "engine"}})
		require.NoError(t, err)
		ae.mu.Lock()
		ae.nodeCache["test:cache-batch"] = &Node{ID: "test:cache-batch", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "cache"}}
		ae.deleteNodes["test:deleted-batch"] = true
		ae.mu.Unlock()
		_, err = engine.CreateNode(&Node{ID: "test:deleted-batch", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "deleted"}})
		require.NoError(t, err)

		result, err := ae.BatchGetNodes([]NodeID{"", "test:cache-batch", "test:engine-batch", "test:deleted-batch"})
		require.NoError(t, err)
		assert.Len(t, result, 2)
		assert.Contains(t, result, NodeID("test:cache-batch"))
		assert.Contains(t, result, NodeID("test:engine-batch"))
		assert.NotContains(t, result, NodeID("test:deleted-batch"))
	})

	t.Run("returns cached results when engine batch lookup fails", func(t *testing.T) {
		engine := &batchGetErrorEngine{
			MemoryEngine: NewMemoryEngine(),
			err:          errors.New("batch get failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		ae.mu.Lock()
		ae.nodeCache["test:cache-only-batch"] = &Node{ID: "test:cache-only-batch", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "cache"}}
		ae.mu.Unlock()

		result, err := ae.BatchGetNodes([]NodeID{"test:cache-only-batch", "test:missing-from-engine"})
		require.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Contains(t, result, NodeID("test:cache-only-batch"))
	})
}

// ============================================================================
// BulkCreateNodes / BulkCreateEdges
// ============================================================================

func TestAsyncEngine_BulkCreateNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkCreateNodes([]*Node{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkCreateNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	nodes := []*Node{makeNode("bulk-n1"), makeNode("bulk-n2"), makeNode("bulk-n3")}
	err := ae.BulkCreateNodes(nodes)
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	count, err := ae.NodeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(3))
}

func TestAsyncEngine_BulkCreateEdges_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkCreateEdges([]*Edge{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkCreateEdges_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("be1"))
	_, _ = ae.CreateNode(makeNode("be2"))
	require.NoError(t, ae.Flush())

	edges := []*Edge{makeEdge("bulk-e1", "be1", "be2")}
	err := ae.BulkCreateEdges(edges)
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	count, err := ae.EdgeCount()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(1))
}

// ============================================================================
// BulkDeleteNodes / BulkDeleteEdges
// ============================================================================

func TestAsyncEngine_BulkDeleteNodes_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkDeleteNodes([]NodeID{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkDeleteNodes_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id1, _ := ae.CreateNode(makeNode("del-n1"))
	id2, _ := ae.CreateNode(makeNode("del-n2"))
	require.NoError(t, ae.Flush())

	err := ae.BulkDeleteNodes([]NodeID{id1, id2})
	require.NoError(t, err)
}

func TestAsyncEngine_BulkDeleteEdges_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	err := ae.BulkDeleteEdges([]EdgeID{})
	assert.NoError(t, err)
}

func TestAsyncEngine_BulkDeleteEdges_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("de1"))
	_, _ = ae.CreateNode(makeNode("de2"))
	_ = ae.CreateEdge(makeEdge("del-e1", "de1", "de2"))
	require.NoError(t, ae.Flush())

	err := ae.BulkDeleteEdges([]EdgeID{EdgeID(prefixTestID("del-e1"))})
	require.NoError(t, err)
}

// ============================================================================
// GetEdgesBetween / GetEdgeBetween / GetAllNodes / Degree
// ============================================================================

func TestAsyncEngine_GetEdgesBetween(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("gb1"))
	_, _ = ae.CreateNode(makeNode("gb2"))
	_ = ae.CreateEdge(makeEdge("gb-e1", "gb1", "gb2"))
	require.NoError(t, ae.Flush())

	edges, err := ae.GetEdgesBetween(NodeID(prefixTestID("gb1")), NodeID(prefixTestID("gb2")))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestAsyncEngine_GetEdgesBetween_ReadYourOwnWritesBeforeFlush(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("pending-gb1"))
	_, _ = ae.CreateNode(makeNode("pending-gb2"))
	pending := makeEdge("pending-gb-e1", "pending-gb1", "pending-gb2")
	require.NoError(t, ae.CreateEdge(pending))

	edges, err := ae.GetEdgesBetween(NodeID(prefixTestID("pending-gb1")), NodeID(prefixTestID("pending-gb2")))
	require.NoError(t, err)
	require.Len(t, edges, 1)
	assert.Equal(t, pending.ID, edges[0].ID)

	edge := ae.GetEdgeBetween(NodeID(prefixTestID("pending-gb1")), NodeID(prefixTestID("pending-gb2")), pending.Type)
	require.NotNil(t, edge)
	assert.Equal(t, pending.ID, edge.ID)
}

func TestAsyncEngine_GetEdgesBetween_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edges, err := ae.GetEdgesBetween(NodeID(prefixTestID("x")), NodeID(prefixTestID("y")))
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestAsyncEngine_GetEdgeBetween_NotFound(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edge := ae.GetEdgeBetween(NodeID(prefixTestID("x")), NodeID(prefixTestID("y")), "NOTYPE")
	assert.Nil(t, edge)
}

func TestAsyncEngine_GetAllNodes(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("gan1"))
	_, _ = ae.CreateNode(makeNode("gan2"))
	require.NoError(t, ae.Flush())

	nodes := ae.GetAllNodes()
	assert.GreaterOrEqual(t, len(nodes), 2)
}

func TestAsyncEngine_GetInOutDegree(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("deg1"))
	_, _ = ae.CreateNode(makeNode("deg2"))
	_ = ae.CreateEdge(makeEdge("deg-e1", "deg1", "deg2"))
	require.NoError(t, ae.Flush())

	in := ae.GetInDegree(NodeID(prefixTestID("deg2")))
	out := ae.GetOutDegree(NodeID(prefixTestID("deg1")))
	assert.GreaterOrEqual(t, in, 0)
	assert.GreaterOrEqual(t, out, 0)
}

func TestAsyncEngine_GetInOutDegree_ReadYourOwnWritesBeforeFlush(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("pending-deg1"))
	_, _ = ae.CreateNode(makeNode("pending-deg2"))
	require.NoError(t, ae.CreateEdge(makeEdge("pending-deg-e1", "pending-deg1", "pending-deg2")))

	assert.Equal(t, 1, ae.GetOutDegree(NodeID(prefixTestID("pending-deg1"))))
	assert.Equal(t, 1, ae.GetInDegree(NodeID(prefixTestID("pending-deg2"))))
}

// ============================================================================
// NodeCountByPrefix / EdgeCountByPrefix
// ============================================================================

func TestAsyncEngine_NodeCountByPrefix(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("pfx-a1"))
	_, _ = ae.CreateNode(makeNode("pfx-a2"))
	require.NoError(t, ae.Flush())

	count, err := ae.NodeCountByPrefix(prefixTestID("pfx-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

func TestAsyncEngine_EdgeCountByPrefix(t *testing.T) {
	ae := newAsyncTestEngine(t)
	count, err := ae.EdgeCountByPrefix(prefixTestID("epfx-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))

	ae.mu.Lock()
	ae.deleteEdges[EdgeID(prefixTestID("epfx-negative"))] = true
	ae.mu.Unlock()
	count, err = ae.EdgeCountByPrefix(prefixTestID("epfx-negative"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestAsyncEngine_Counts_WithUpdatesInflightAndPrefixFiltering(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })

	_, err := engine.CreateNode(&Node{ID: "test:update-node", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "update"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:deleted-node", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "delete"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:update-edge", StartNode: "test:update-node", EndNode: "test:deleted-node", Type: "REL"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:deleted-edge", StartNode: "test:deleted-node", EndNode: "test:update-node", Type: "REL"}))

	ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })

	ae.mu.Lock()
	ae.nodeCache["test:update-node"] = &Node{ID: "test:update-node", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "updated"}}
	ae.updateNodes["test:update-node"] = true
	ae.nodeCache["test:new-node"] = &Node{ID: "test:new-node", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "new"}}
	ae.nodeCache["test:inflight-node"] = &Node{ID: "test:inflight-node", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "inflight"}}
	ae.inFlightNodes["test:inflight-node"] = true
	ae.nodeCache["other:ignored-node"] = &Node{ID: "other:ignored-node", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "other"}}
	ae.deleteNodes["test:deleted-node"] = true

	ae.edgeCache["test:update-edge"] = &Edge{ID: "test:update-edge", StartNode: "test:update-node", EndNode: "test:deleted-node", Type: "REL"}
	ae.updateEdges["test:update-edge"] = true
	ae.edgeCache["test:new-edge"] = &Edge{ID: "test:new-edge", StartNode: "test:update-node", EndNode: "test:new-node", Type: "REL"}
	ae.edgeCache["test:inflight-edge"] = &Edge{ID: "test:inflight-edge", StartNode: "test:new-node", EndNode: "test:update-node", Type: "REL"}
	ae.inFlightEdges["test:inflight-edge"] = true
	ae.edgeCache["other:ignored-edge"] = &Edge{ID: "other:ignored-edge", StartNode: "other:ignored-node", EndNode: "test:update-node", Type: "REL"}
	ae.deleteEdges["test:deleted-edge"] = true
	ae.mu.Unlock()

	nodeCount, err := ae.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(4), nodeCount)

	edgeCount, err := ae.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(4), edgeCount)

	nodeCount, err = ae.NodeCountByPrefix("test:")
	require.NoError(t, err)
	assert.Equal(t, int64(3), nodeCount)

	edgeCount, err = ae.EdgeCountByPrefix("test:")
	require.NoError(t, err)
	assert.Equal(t, int64(3), edgeCount)
}

func TestAsyncEngine_DeleteHelpers_CachedInflightAndIdempotent(t *testing.T) {
	t.Run("delete cached node notifies only for pending create", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		createdID := NodeID("test:delete-created")
		updatedID := NodeID("test:delete-updated")
		ae.mu.Lock()
		ae.nodeCache[createdID] = &Node{ID: createdID, Labels: []string{"Temp"}, Properties: map[string]interface{}{"name": "created"}}
		ae.nodeCache[updatedID] = &Node{ID: updatedID, Labels: []string{"Temp"}, Properties: map[string]interface{}{"name": "updated"}}
		ae.updateNodes[updatedID] = true
		ae.labelIndex["temp"] = map[NodeID]bool{createdID: true, updatedID: true}
		ae.mu.Unlock()

		var deleted []NodeID
		ae.OnNodeDeleted(func(id NodeID) {
			deleted = append(deleted, id)
		})

		require.NoError(t, ae.DeleteNode(createdID))
		require.NoError(t, ae.DeleteNode(updatedID))

		ae.mu.RLock()
		_, createdInCache := ae.nodeCache[createdID]
		_, updatedInCache := ae.nodeCache[updatedID]
		_, createdInIndex := ae.labelIndex["temp"][createdID]
		_, updatedInIndex := ae.labelIndex["temp"][updatedID]
		ae.mu.RUnlock()

		assert.False(t, createdInCache)
		assert.False(t, updatedInCache)
		assert.False(t, createdInIndex)
		assert.False(t, updatedInIndex)
		assert.Equal(t, []NodeID{createdID}, deleted)
	})

	t.Run("delete node handles inflight and idempotent paths", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		id := NodeID("test:inflight-node-delete")
		ae.mu.Lock()
		ae.inFlightNodes[id] = true
		ae.mu.Unlock()

		require.NoError(t, ae.DeleteNode(id))
		ae.mu.RLock()
		assert.True(t, ae.deleteNodes[id])
		pendingWrites := ae.pendingWrites
		ae.mu.RUnlock()
		assert.Equal(t, int64(1), pendingWrites)

		require.NoError(t, ae.DeleteNode(id))
		ae.mu.RLock()
		assert.Equal(t, int64(1), ae.pendingWrites)
		ae.mu.RUnlock()
	})

	t.Run("delete edge handles cached inflight underlying and idempotent paths", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		cachedID := EdgeID("test:cached-inflight-edge")
		ae.mu.Lock()
		ae.edgeCache[cachedID] = &Edge{ID: cachedID, StartNode: "test:a", EndNode: "test:b", Type: "REL"}
		ae.inFlightEdges[cachedID] = true
		ae.mu.Unlock()

		require.NoError(t, ae.DeleteEdge(cachedID))
		ae.mu.RLock()
		_, stillCached := ae.edgeCache[cachedID]
		assert.False(t, stillCached)
		assert.True(t, ae.deleteEdges[cachedID])
		ae.mu.RUnlock()

		underID := EdgeID("test:under-inflight-edge")
		ae.mu.Lock()
		ae.inFlightEdges[underID] = true
		ae.mu.Unlock()
		require.NoError(t, ae.DeleteEdge(underID))
		ae.mu.RLock()
		assert.True(t, ae.deleteEdges[underID])
		ae.mu.RUnlock()

		require.NoError(t, ae.DeleteEdge(underID))
		ae.mu.RLock()
		assert.True(t, ae.deleteEdges[underID])
		ae.mu.RUnlock()
	})
}

func TestAsyncEngine_CreateAndUpdateNodeHelpers(t *testing.T) {
	t.Run("create rejects nil and invalid properties", func(t *testing.T) {
		ae := newAsyncTestEngine(t)

		_, err := ae.CreateNode(nil)
		assert.ErrorIs(t, err, ErrInvalidData)

		_, err = ae.CreateNode(&Node{
			ID:         NodeID(prefixTestID("bad-props")),
			Labels:     []string{"Bad"},
			Properties: map[string]interface{}{"bad": []byte("unsupported")},
		})
		require.Error(t, err)
	})

	t.Run("create clears pending delete and marks updates when recreating", func(t *testing.T) {
		ae := newAsyncTestEngine(t)
		id := NodeID(prefixTestID("recreate"))

		ae.mu.Lock()
		ae.deleteNodes[id] = true
		ae.mu.Unlock()

		_, err := ae.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Recreated"},
			Properties: map[string]interface{}{"name": "first"},
		})
		require.NoError(t, err)

		ae.mu.RLock()
		assert.False(t, ae.deleteNodes[id])
		assert.True(t, ae.updateNodes[id])
		assert.True(t, ae.labelIndex["recreated"][id])
		ae.mu.RUnlock()

		_, err = ae.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Recreated"},
			Properties: map[string]interface{}{"name": "second"},
		})
		require.NoError(t, err)

		ae.mu.RLock()
		require.NotNil(t, ae.nodeCache[id])
		assert.Equal(t, "second", ae.nodeCache[id].Properties["name"])
		assert.True(t, ae.updateNodes[id])
		ae.mu.RUnlock()
	})

	t.Run("update captures engine baseline once and allows missing baseline", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		_, err := engine.CreateNode(&Node{
			ID:         "test:baseline-node",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"name": "before"},
		})
		require.NoError(t, err)

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		err = ae.UpdateNode(&Node{
			ID:         "test:baseline-node",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"name": "after-1"},
		})
		require.NoError(t, err)

		ae.mu.RLock()
		require.Contains(t, ae.nodeUpdateBaseline, NodeID("test:baseline-node"))
		require.NotNil(t, ae.nodeUpdateBaseline["test:baseline-node"])
		assert.Equal(t, "before", ae.nodeUpdateBaseline["test:baseline-node"].Properties["name"])
		ae.mu.RUnlock()

		err = ae.UpdateNode(&Node{
			ID:         "test:baseline-node",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"name": "after-2"},
		})
		require.NoError(t, err)

		ae.mu.RLock()
		require.NotNil(t, ae.nodeUpdateBaseline["test:baseline-node"])
		assert.Equal(t, "before", ae.nodeUpdateBaseline["test:baseline-node"].Properties["name"])
		ae.mu.RUnlock()

		err = ae.UpdateNode(&Node{
			ID:         "test:missing-baseline",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"name": "missing"},
		})
		require.NoError(t, err)

		ae.mu.RLock()
		baseline, exists := ae.nodeUpdateBaseline["test:missing-baseline"]
		ae.mu.RUnlock()
		assert.True(t, exists)
		assert.Nil(t, baseline)
	})
}

func TestAsyncEngine_NodeAndEdgeCount_ErrorAndClamp(t *testing.T) {
	t.Run("count methods propagate engine errors", func(t *testing.T) {
		engine := &countErrorEngine{
			MemoryEngine: NewMemoryEngine(),
			nodeCountErr: errors.New("node count failed"),
			edgeCountErr: errors.New("edge count failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := ae.NodeCount()
		require.ErrorContains(t, err, "node count failed")

		_, err = ae.EdgeCount()
		require.ErrorContains(t, err, "edge count failed")
	})

	t.Run("count methods clamp negative totals", func(t *testing.T) {
		ae := newAsyncTestEngine(t)

		ae.mu.Lock()
		ae.deleteNodes["test:neg-node"] = true
		ae.deleteEdges["test:neg-edge"] = true
		ae.mu.Unlock()

		nodeCount, err := ae.NodeCount()
		require.NoError(t, err)
		assert.Zero(t, nodeCount)

		edgeCount, err := ae.EdgeCount()
		require.NoError(t, err)
		assert.Zero(t, edgeCount)
	})
}

// ============================================================================
// Pending embeddings
// ============================================================================

func TestAsyncEngine_AddToPendingEmbeddings(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id, _ := ae.CreateNode(makeNode("emb1"))
	// Should not panic
	ae.AddToPendingEmbeddings(id)
	assert.Equal(t, 1, ae.PendingEmbeddingsCount())
}

func TestAsyncEngine_FindNodeNeedingEmbedding(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id, _ := ae.CreateNode(makeNode("emb2"))
	ae.AddToPendingEmbeddings(id)

	node := ae.FindNodeNeedingEmbedding()
	// May be nil depending on flush state
	_ = node

	t.Run("cache node is returned before engine", func(t *testing.T) {
		ae := newAsyncTestEngine(t)
		id, err := ae.CreateNode(&Node{
			ID:         NodeID(prefixTestID("emb-cache")),
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"content": "needs embedding"},
		})
		require.NoError(t, err)

		node := ae.FindNodeNeedingEmbedding()
		require.NotNil(t, node)
		require.Equal(t, id, node.ID)
	})

	t.Run("pending engine node is skipped when cache already has embedding", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		id := NodeID(prefixTestID("emb-skip"))
		_, err := engine.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"content": "engine pending"},
		})
		require.NoError(t, err)
		engine.AddToPendingEmbeddings(id)

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		ae.mu.Lock()
		ae.nodeCache[id] = &Node{
			ID:              id,
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"content": "cached embedded"},
			ChunkEmbeddings: [][]float32{{1, 2, 3}},
		}
		ae.mu.Unlock()

		require.Nil(t, ae.FindNodeNeedingEmbedding())
	})

	t.Run("deleted pending engine node is cleared", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		id := NodeID(prefixTestID("emb-deleted"))
		_, err := engine.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"content": "deleted pending"},
		})
		require.NoError(t, err)
		engine.AddToPendingEmbeddings(id)

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		ae.mu.Lock()
		ae.deleteNodes[id] = true
		ae.mu.Unlock()

		require.Nil(t, ae.FindNodeNeedingEmbedding())
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
	})

	t.Run("exportable engine fallback finds first matching node", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		engine.SetEmbeddingsEnabled(true)
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := engine.CreateNode(&Node{
			ID:              "mem:embedded",
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2}},
			Properties:      map[string]interface{}{"text": "already embedded"},
		})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{
			ID:         "mem:needs-embedding",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"text": "embed me"},
		})
		require.NoError(t, err)

		got := ae.FindNodeNeedingEmbedding()
		require.NotNil(t, got)
		assert.Equal(t, NodeID("mem:needs-embedding"), got.ID)
	})

	t.Run("dedicated finder returns engine node when not shadowed", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &finderEngine{
			Engine: base,
			node: &Node{
				ID:         "mem:finder-hit",
				Labels:     []string{"Doc"},
				Properties: map[string]interface{}{"text": "from finder"},
			},
		}
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		got := ae.FindNodeNeedingEmbedding()
		require.NotNil(t, got)
		assert.Equal(t, NodeID("mem:finder-hit"), got.ID)
	})

	t.Run("dedicated finder returns nil when engine reports none", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		ae := NewAsyncEngine(&finderEngine{Engine: base, node: nil}, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })
		assert.Nil(t, ae.FindNodeNeedingEmbedding())
	})

	t.Run("dedicated finder is suppressed by cached embedding for same id", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &finderEngine{
			Engine: base,
			node: &Node{
				ID:         "mem:finder-shadowed",
				Labels:     []string{"Doc"},
				Properties: map[string]interface{}{"text": "from finder"},
			},
		}
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })
		ae.mu.Lock()
		ae.nodeCache["mem:finder-shadowed"] = &Node{
			ID:              "mem:finder-shadowed",
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"text": "cached embedded"},
			ChunkEmbeddings: [][]float32{{1, 2}},
		}
		ae.mu.Unlock()

		assert.Nil(t, ae.FindNodeNeedingEmbedding())
	})

	t.Run("exportable fallback returns nil on all-nodes error", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &nonStreamingCountEngine{
			Engine:      base,
			allNodesErr: errors.New("all nodes failed"),
		}
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		assert.Nil(t, ae.FindNodeNeedingEmbedding())
	})

	t.Run("exportable fallback skips deleted and embedded engine nodes", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &nonStreamingCountEngine{Engine: base}
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := base.CreateNode(&Node{
			ID:              "mem:embedded-skip",
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{{0.1}},
			Properties:      map[string]interface{}{"text": "embedded"},
		})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{
			ID:         "mem:deleted-skip",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"text": "deleted"},
		})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{
			ID:         "mem:eligible",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"text": "eligible"},
		})
		require.NoError(t, err)

		ae.mu.Lock()
		ae.deleteNodes["mem:deleted-skip"] = true
		ae.nodeCache["mem:embedded-skip"] = &Node{
			ID:              "mem:embedded-skip",
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"text": "cached embedded"},
			ChunkEmbeddings: [][]float32{{9, 9}},
		}
		ae.mu.Unlock()

		got := ae.FindNodeNeedingEmbedding()
		require.NotNil(t, got)
		assert.Equal(t, NodeID("mem:eligible"), got.ID)
	})
}

func TestAsyncEngine_MarkNodeEmbedded(t *testing.T) {
	ae := newAsyncTestEngine(t)
	id, _ := ae.CreateNode(makeNode("emb3"))
	ae.AddToPendingEmbeddings(id)
	// Should not panic
	ae.MarkNodeEmbedded(id)

	t.Run("fallback no-op when engine lacks pending manager", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		ae := NewAsyncEngine(&nonStreamingCountEngine{Engine: base}, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })
		ae.MarkNodeEmbedded("test:no-manager")
	})
}

func TestAsyncEngine_RefreshPendingEmbeddingsIndex(t *testing.T) {
	ae := newAsyncTestEngine(t)
	count := ae.RefreshPendingEmbeddingsIndex()
	assert.GreaterOrEqual(t, count, 0)

	t.Run("fallback zero when engine lacks pending manager", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		ae := NewAsyncEngine(&nonStreamingCountEngine{Engine: base}, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })
		assert.Equal(t, 0, ae.RefreshPendingEmbeddingsIndex())
		assert.Equal(t, 0, ae.PendingEmbeddingsCount())
		assert.Nil(t, ae.ListNamespaces())
	})
}

func TestAsyncEngine_AdaptiveFlushInterval_Branches(t *testing.T) {
	ae := newAsyncTestEngine(t)

	ae.minFlushInterval = 10 * time.Millisecond
	ae.maxFlushInterval = 100 * time.Millisecond
	ae.targetFlushSize = 100

	t.Run("pending non-positive returns max interval", func(t *testing.T) {
		assert.Equal(t, 100*time.Millisecond, ae.adaptiveFlushInterval(0))
	})

	t.Run("target non-positive returns max interval", func(t *testing.T) {
		ae.targetFlushSize = 0
		assert.Equal(t, 100*time.Millisecond, ae.adaptiveFlushInterval(10))
		ae.targetFlushSize = 100
	})

	t.Run("max less than min returns min interval", func(t *testing.T) {
		ae.minFlushInterval = 50 * time.Millisecond
		ae.maxFlushInterval = 40 * time.Millisecond
		assert.Equal(t, 50*time.Millisecond, ae.adaptiveFlushInterval(10))
		ae.minFlushInterval = 10 * time.Millisecond
		ae.maxFlushInterval = 100 * time.Millisecond
	})

	t.Run("ratio capped at one for large pending", func(t *testing.T) {
		assert.Equal(t, 100*time.Millisecond, ae.adaptiveFlushInterval(1000))
	})

	t.Run("interpolates between min and max", func(t *testing.T) {
		// pending=50 => ratio=0.5 => 10ms + 0.5*(90ms) = 55ms
		assert.Equal(t, 55*time.Millisecond, ae.adaptiveFlushInterval(50))
	})
}

// ============================================================================
// GetSchema / GetSchemaForNamespace / GetEngine
// ============================================================================

func TestAsyncEngine_GetSchema(t *testing.T) {
	ae := newAsyncTestEngine(t)
	schema := ae.GetSchema()
	assert.NotNil(t, schema)
}

func TestAsyncEngine_GetSchemaForNamespace(t *testing.T) {
	ae := newAsyncTestEngine(t)
	schema := ae.GetSchemaForNamespace("test")
	assert.NotNil(t, schema)
}

func TestAsyncEngine_GetEngine(t *testing.T) {
	ae := newAsyncTestEngine(t)
	eng := ae.GetEngine()
	assert.NotNil(t, eng)
}

// ============================================================================
// GetEdgesByType
// ============================================================================

func TestAsyncEngine_GetEdgesByType_Empty(t *testing.T) {
	ae := newAsyncTestEngine(t)
	edges, err := ae.GetEdgesByType("NONEXISTENT")
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestAsyncEngine_GetEdgesByType_WithData(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("et1"))
	_, _ = ae.CreateNode(makeNode("et2"))
	_ = ae.CreateEdge(makeEdge("et-e1", "et1", "et2"))
	require.NoError(t, ae.Flush())

	edges, err := ae.GetEdgesByType("RELATED")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(edges), 1)

	t.Run("empty type delegates to all edges", func(t *testing.T) {
		edges, err := ae.GetEdgesByType("")
		require.NoError(t, err)
		assert.NotEmpty(t, edges)
	})

	t.Run("returns cache on engine error", func(t *testing.T) {
		engine := &edgeQueryErrorEngine{
			MemoryEngine:      NewMemoryEngine(),
			getEdgesByTypeErr: errors.New("edge type failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		require.NoError(t, ae.CreateEdge(&Edge{ID: "test:cached-related", StartNode: "test:a", EndNode: "test:b", Type: "RELATED"}))
		edges, err := ae.GetEdgesByType("related")
		require.NoError(t, err)
		require.Len(t, edges, 1)
		assert.Equal(t, EdgeID("test:cached-related"), edges[0].ID)
	})

	t.Run("merge skips duplicates and deleted engine edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "a"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "b"}})
		require.NoError(t, err)

		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:engine-related", StartNode: "test:a", EndNode: "test:b", Type: "RELATED"}))
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:deleted-related", StartNode: "test:b", EndNode: "test:a", Type: "RELATED"}))

		require.NoError(t, ae.CreateEdge(&Edge{ID: "test:engine-related", StartNode: "test:a", EndNode: "test:b", Type: "RELATED"}))
		ae.mu.Lock()
		ae.deleteEdges["test:deleted-related"] = true
		ae.mu.Unlock()

		edges, err := ae.GetEdgesByType("RELATED")
		require.NoError(t, err)
		assert.Equal(t, 1, len(edges))
		assert.Equal(t, EdgeID("test:engine-related"), edges[0].ID)
	})
}

// ============================================================================
// Label lookup / iteration / prefix delete delegates
// ============================================================================

func TestAsyncEngine_ForEachNodeIDByLabel_MergesCacheAndEngine(t *testing.T) {
	ae := newAsyncTestEngine(t)

	// Engine-backed node
	engineNode := makeNode("label-engine")
	_, err := ae.GetEngine().CreateNode(engineNode)
	require.NoError(t, err)

	// Cache-backed node (not flushed yet)
	cacheNode := makeNode("label-cache")
	_, err = ae.CreateNode(cacheNode)
	require.NoError(t, err)

	seen := map[NodeID]bool{}
	err = ae.ForEachNodeIDByLabel("testlabel", func(id NodeID) bool {
		seen[id] = true
		return true
	})
	require.NoError(t, err)
	assert.True(t, seen[engineNode.ID], "engine node should be visited")
	assert.True(t, seen[cacheNode.ID], "cached node should be visited")

	// Nil callback is a no-op path.
	require.NoError(t, ae.ForEachNodeIDByLabel("testlabel", nil))

	t.Run("stops early when callback returns false", func(t *testing.T) {
		engine := &lookupEngine{
			Engine: NewMemoryEngine(),
			ids:    []NodeID{"test:engine-1", "test:engine-2"},
		}
		t.Cleanup(func() { _ = engine.Close() })

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := ae.CreateNode(&Node{ID: "test:cache-1", Labels: []string{"StopLabel"}, Properties: map[string]interface{}{"name": "cache"}})
		require.NoError(t, err)

		visited := 0
		err = ae.ForEachNodeIDByLabel("stoplabel", func(id NodeID) bool {
			visited++
			return false
		})
		require.NoError(t, err)
		assert.Equal(t, 1, visited)
	})

	t.Run("propagates lookup engine error", func(t *testing.T) {
		engine := &lookupEngine{
			Engine: NewMemoryEngine(),
			err:    errors.New("lookup failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		err := ae.ForEachNodeIDByLabel("testlabel", func(id NodeID) bool { return true })
		require.ErrorContains(t, err, "lookup failed")
	})

	t.Run("fallback merges GetNodesByLabel results", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		ae := NewAsyncEngine(&nonStreamingCountEngine{Engine: base}, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := base.CreateNode(&Node{ID: "test:engine-a", Labels: []string{"Merge"}, Properties: map[string]interface{}{"name": "engine"}})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{ID: "test:engine-b", Labels: []string{"Merge"}, Properties: map[string]interface{}{"name": "deleted"}})
		require.NoError(t, err)

		_, err = ae.CreateNode(&Node{ID: "test:cache-a", Labels: []string{"Merge"}, Properties: map[string]interface{}{"name": "cache"}})
		require.NoError(t, err)
		ae.mu.Lock()
		ae.deleteNodes["test:engine-b"] = true
		ae.nodeCache["test:engine-a"] = &Node{ID: "test:engine-a", Labels: []string{"Merge"}, Properties: map[string]interface{}{"name": "cached-dup"}}
		ae.labelIndex["merge"]["test:engine-a"] = true
		ae.mu.Unlock()

		var seen []NodeID
		err = ae.ForEachNodeIDByLabel("merge", func(id NodeID) bool {
			seen = append(seen, id)
			return true
		})
		require.NoError(t, err)
		assert.ElementsMatch(t, []NodeID{"test:cache-a", "test:engine-a"}, seen)
	})
}

func TestAsyncEngine_GetFirstAndGetNodesByLabel_CaseInsensitive(t *testing.T) {
	ae := newAsyncTestEngine(t)

	// Cached first-hit path.
	cacheNode := &Node{
		ID:         NodeID(prefixTestID("first-cache")),
		Labels:     []string{"MiXeDCaSe"},
		Properties: map[string]interface{}{"name": "cached"},
	}
	_, err := ae.CreateNode(cacheNode)
	require.NoError(t, err)

	first, err := ae.GetFirstNodeByLabel("mixedcase")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, cacheNode.ID, first.ID)

	// Engine fallback path.
	require.NoError(t, ae.Flush())
	engineOnly := &Node{
		ID:         NodeID(prefixTestID("first-engine")),
		Labels:     []string{"EngineOnly"},
		Properties: map[string]interface{}{"name": "engine"},
	}
	_, err = ae.GetEngine().CreateNode(engineOnly)
	require.NoError(t, err)

	first, err = ae.GetFirstNodeByLabel("engineonly")
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, engineOnly.ID, first.ID)

	nodes, err := ae.GetNodesByLabel("mixedcase")
	require.NoError(t, err)
	assert.NotEmpty(t, nodes)

	t.Run("get first falls back to GetNodesByLabel and empty results", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		ae := NewAsyncEngine(&nonStreamingCountEngine{Engine: base}, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		first, err := ae.GetFirstNodeByLabel("missing")
		require.NoError(t, err)
		assert.Nil(t, first)
	})

	t.Run("get first uses engine getter success and error paths", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &firstLabelEngine{
			Engine: base,
			node: &Node{
				ID:         "test:first-from-engine",
				Labels:     []string{"Getter"},
				Properties: map[string]interface{}{"name": "engine"},
			},
		}
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		first, err := ae.GetFirstNodeByLabel("getter")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, NodeID("test:first-from-engine"), first.ID)

		engine.err = errors.New("getter failed")
		first, err = ae.GetFirstNodeByLabel("getter")
		require.ErrorContains(t, err, "getter failed")
		assert.Nil(t, first)
	})

	t.Run("stale cache label index falls through to engine", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &firstLabelEngine{
			Engine: base,
			node: &Node{
				ID:         "test:first-fallback",
				Labels:     []string{"Stale"},
				Properties: map[string]interface{}{"name": "engine"},
			},
		}
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		ae.mu.Lock()
		ae.labelIndex["stale"] = map[NodeID]bool{
			"test:deleted-stale": true,
			"test:missing-stale": true,
		}
		ae.deleteNodes["test:deleted-stale"] = true
		ae.mu.Unlock()

		first, err := ae.GetFirstNodeByLabel("stale")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, NodeID("test:first-fallback"), first.ID)
	})

	t.Run("get nodes returns cache on engine error", func(t *testing.T) {
		engine := &labelQueryErrorEngine{
			MemoryEngine:       NewMemoryEngine(),
			getNodesByLabelErr: errors.New("label query failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := ae.CreateNode(&Node{
			ID:         NodeID(prefixTestID("label-cache-only")),
			Labels:     []string{"CacheOnly"},
			Properties: map[string]interface{}{"name": "cached"},
		})
		require.NoError(t, err)

		nodes, err := ae.GetNodesByLabel("cacheonly")
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		assert.Equal(t, NodeID(prefixTestID("label-cache-only")), nodes[0].ID)
	})

	t.Run("pending relabel hides stale persisted label view", func(t *testing.T) {
		ae := newAsyncTestEngine(t)
		node := &Node{
			ID:         NodeID(prefixTestID("relabel-node")),
			Labels:     []string{"OldLabel"},
			Properties: map[string]interface{}{"name": "before"},
		}
		_, err := ae.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, ae.Flush())

		updated := CopyNode(node)
		updated.Labels = []string{"NewLabel"}
		updated.Properties = map[string]interface{}{"name": "after"}
		require.NoError(t, ae.UpdateNode(updated))

		oldNodes, err := ae.GetNodesByLabel("oldlabel")
		require.NoError(t, err)
		assert.Empty(t, oldNodes)

		newNodes, err := ae.GetNodesByLabel("newlabel")
		require.NoError(t, err)
		require.Len(t, newNodes, 1)
		assert.Equal(t, updated.ID, newNodes[0].ID)

		first, err := ae.GetFirstNodeByLabel("newlabel")
		require.NoError(t, err)
		require.NotNil(t, first)
		assert.Equal(t, updated.ID, first.ID)
	})

	t.Run("for-each fallback propagates get-nodes error", func(t *testing.T) {
		engine := &nodeLabelErrorEngine{
			Engine: &nonStreamingCountEngine{Engine: NewMemoryEngine()},
			err:    errors.New("get nodes failed"),
		}
		t.Cleanup(func() {
			if closer, ok := engine.Engine.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
		})
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		err := ae.ForEachNodeIDByLabel("missing", func(id NodeID) bool { return true })
		require.ErrorContains(t, err, "get nodes failed")
	})
}

func TestAsyncEngine_GetIncomingEdges_MergesCacheAndEngine(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, _ = ae.CreateNode(makeNode("in-n1"))
	_, _ = ae.CreateNode(makeNode("in-n2"))
	_, _ = ae.CreateNode(makeNode("in-n3"))
	require.NoError(t, ae.Flush())

	// Engine edge.
	require.NoError(t, ae.GetEngine().CreateEdge(makeEdge("in-engine", "in-n1", "in-n2")))
	// Cache edge.
	require.NoError(t, ae.CreateEdge(makeEdge("in-cache", "in-n3", "in-n2")))

	incoming, err := ae.GetIncomingEdges(NodeID(prefixTestID("in-n2")))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(incoming), 2)
}

func TestAsyncEngine_GetOutgoingEdges_MergeAndErrorPaths(t *testing.T) {
	t.Run("returns cache on engine error", func(t *testing.T) {
		engine := &edgeQueryErrorEngine{
			MemoryEngine:   NewMemoryEngine(),
			getOutgoingErr: errors.New("outgoing failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		require.NoError(t, ae.CreateEdge(&Edge{ID: "test:cached-out", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
		edges, err := ae.GetOutgoingEdges("test:a")
		require.NoError(t, err)
		require.Len(t, edges, 1)
		assert.Equal(t, EdgeID("test:cached-out"), edges[0].ID)
	})

	t.Run("merge skips duplicates and deleted engine edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "a"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "b"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:c", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "c"}})
		require.NoError(t, err)

		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:dup-out", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:deleted-out", StartNode: "test:a", EndNode: "test:c", Type: "REL"}))

		require.NoError(t, ae.CreateEdge(&Edge{ID: "test:dup-out", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
		ae.mu.Lock()
		ae.deleteEdges["test:deleted-out"] = true
		ae.mu.Unlock()

		edges, err := ae.GetOutgoingEdges("test:a")
		require.NoError(t, err)
		require.Len(t, edges, 1)
		assert.Equal(t, EdgeID("test:dup-out"), edges[0].ID)
	})
}

func TestAsyncEngine_IterateNodes_DeleteByPrefix_LastWriteTime(t *testing.T) {
	ae := newAsyncTestEngine(t)

	_, err := ae.CreateNode(makeNode("iter-a1"))
	require.NoError(t, err)
	_, err = ae.CreateNode(makeNode("iter-a2"))
	require.NoError(t, err)
	require.NoError(t, ae.Flush())

	visited := 0
	err = ae.IterateNodes(func(node *Node) bool {
		if node != nil {
			visited++
		}
		return true
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, visited, 2)

	// Cover LastWriteTime() delegate/fallback path.
	_ = ae.LastWriteTime()

	// Delete all test nodes by prefix and verify they're gone.
	nodesDeleted, _, err := ae.DeleteByPrefix(prefixTestID("iter-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, nodesDeleted, int64(2))

	remaining, err := ae.NodeCountByPrefix(prefixTestID("iter-"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining)

	t.Run("cache copy and early stop without iterator backend", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })

		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		node := &Node{
			ID:         NodeID(prefixTestID("iter-copy")),
			Labels:     []string{"Iter"},
			Properties: map[string]interface{}{"name": "original"},
		}
		_, err := ae.CreateNode(node)
		require.NoError(t, err)

		visited := 0
		err = ae.IterateNodes(func(n *Node) bool {
			visited++
			n.Properties["name"] = "mutated"
			return false
		})
		require.NoError(t, err)
		assert.Equal(t, 1, visited)

		stored, err := ae.GetNode(node.ID)
		require.NoError(t, err)
		assert.Equal(t, "original", stored.Properties["name"])
	})
}

func TestAsyncEngine_CountByPrefixHelpers(t *testing.T) {
	t.Run("streaming engine path", func(t *testing.T) {
		ae := newAsyncTestEngine(t)
		_, err := ae.CreateNode(makeNode("prefix-a"))
		require.NoError(t, err)
		_, err = ae.CreateNode(makeNode("other-b"))
		require.NoError(t, err)
		require.NoError(t, ae.CreateEdge(makeEdge("prefix-e", "prefix-a", "other-b")))
		require.NoError(t, ae.Flush())

		nodes, err := countNodesInEngineByPrefix(ae, prefixTestID("prefix"))
		require.NoError(t, err)
		assert.Equal(t, int64(1), nodes)

		edges, err := countEdgesInEngineByPrefix(ae, prefixTestID("prefix"))
		require.NoError(t, err)
		assert.Equal(t, int64(1), edges)
	})

	t.Run("allnodes fallback path", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &nonStreamingCountEngine{Engine: base}
		_, err := base.CreateNode(&Node{ID: "test:n1", Labels: []string{"Test"}})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{ID: "other:n2", Labels: []string{"Test"}})
		require.NoError(t, err)
		require.NoError(t, base.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "other:n2", Type: "REL"}))

		nodes, err := countNodesInEngineByPrefix(engine, "test:")
		require.NoError(t, err)
		assert.Equal(t, int64(1), nodes)

		edges, err := countEdgesInEngineByPrefix(engine, "test:")
		require.NoError(t, err)
		assert.Equal(t, int64(1), edges)
	})

	t.Run("allnodes fallback error path", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		engine := &nonStreamingCountEngine{
			Engine:      base,
			allNodesErr: errors.New("all nodes failed"),
			allEdgesErr: errors.New("all edges failed"),
		}

		nodes, err := countNodesInEngineByPrefix(engine, "test:")
		require.ErrorContains(t, err, "all nodes failed")
		assert.Equal(t, int64(0), nodes)

		edges, err := countEdgesInEngineByPrefix(engine, "test:")
		require.ErrorContains(t, err, "all edges failed")
		assert.Equal(t, int64(0), edges)
	})

	t.Run("streaming error path", func(t *testing.T) {
		engine := &prefixCountErrorEngine{
			MemoryEngine:  NewMemoryEngine(),
			streamNodeErr: errors.New("stream nodes failed"),
			streamEdgeErr: errors.New("stream edges failed"),
		}
		t.Cleanup(func() { _ = engine.Close() })

		nodes, err := countNodesInEngineByPrefix(engine, "test:")
		require.ErrorContains(t, err, "stream nodes failed")
		assert.Equal(t, int64(0), nodes)

		edges, err := countEdgesInEngineByPrefix(engine, "test:")
		require.ErrorContains(t, err, "stream edges failed")
		assert.Equal(t, int64(0), edges)
	})
}

func TestAsyncEngine_StreamFallbackAndErrors(t *testing.T) {
	t.Run("stream nodes callback error and context cancellation", func(t *testing.T) {
		ae := newAsyncTestEngine(t)
		_, err := ae.CreateNode(makeNode("stream-node-a"))
		require.NoError(t, err)

		errBoom := errors.New("node callback failed")
		err = ae.StreamNodes(context.Background(), func(node *Node) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = ae.StreamNodes(ctx, func(node *Node) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("stream edges callback error and context cancellation", func(t *testing.T) {
		ae := newAsyncTestEngine(t)
		_, _ = ae.CreateNode(makeNode("stream-edge-a"))
		_, _ = ae.CreateNode(makeNode("stream-edge-b"))
		require.NoError(t, ae.Flush())
		require.NoError(t, ae.CreateEdge(makeEdge("stream-edge-1", "stream-edge-a", "stream-edge-b")))

		errBoom := errors.New("edge callback failed")
		err := ae.StreamEdges(context.Background(), func(edge *Edge) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = ae.StreamEdges(ctx, func(edge *Edge) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("stream fallback with memory engine", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := ae.CreateNode(&Node{ID: "mem:n1", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "n1"}})
		require.NoError(t, err)
		_, err = ae.CreateNode(&Node{ID: "mem:n2", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "n2"}})
		require.NoError(t, err)
		require.NoError(t, ae.CreateEdge(&Edge{ID: "mem:e1", StartNode: "mem:n1", EndNode: "mem:n2", Type: "REL"}))

		var nodeCount, edgeCount int
		require.NoError(t, ae.StreamNodes(context.Background(), func(node *Node) error {
			nodeCount++
			return nil
		}))
		require.NoError(t, ae.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeCount++
			return nil
		}))
		assert.Equal(t, 2, nodeCount)
		assert.Equal(t, 1, edgeCount)
	})

	t.Run("fallback skips deleted and duplicate nodes and edges", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		_, err := engine.CreateNode(&Node{ID: "mem:n1", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "engine"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "mem:n2", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "delete-me"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: "mem:e1", StartNode: "mem:n1", EndNode: "mem:n2", Type: "REL"}))
		require.NoError(t, engine.CreateEdge(&Edge{ID: "mem:e2", StartNode: "mem:n2", EndNode: "mem:n1", Type: "REL"}))

		ae.mu.Lock()
		ae.nodeCache["mem:n1"] = &Node{ID: "mem:n1", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "cached"}}
		ae.deleteNodes["mem:n2"] = true
		ae.edgeCache["mem:e1"] = &Edge{ID: "mem:e1", StartNode: "mem:n1", EndNode: "mem:n2", Type: "REL"}
		ae.deleteEdges["mem:e2"] = true
		ae.mu.Unlock()

		var nodeIDs []NodeID
		require.NoError(t, ae.StreamNodes(context.Background(), func(node *Node) error {
			nodeIDs = append(nodeIDs, node.ID)
			return nil
		}))
		assert.Equal(t, []NodeID{"mem:n1"}, nodeIDs)

		var edgeIDs []EdgeID
		require.NoError(t, ae.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeIDs = append(edgeIDs, edge.ID)
			return nil
		}))
		assert.Equal(t, []EdgeID{"mem:e1"}, edgeIDs)
	})

	t.Run("badger stream skips deleted and duplicate engine entries", func(t *testing.T) {
		ae := newAsyncTestEngine(t)

		engineNode := makeNode("stream-engine")
		_, err := ae.GetEngine().CreateNode(engineNode)
		require.NoError(t, err)
		deletedNode := makeNode("stream-deleted")
		_, err = ae.GetEngine().CreateNode(deletedNode)
		require.NoError(t, err)

		_, err = ae.CreateNode(&Node{ID: engineNode.ID, Labels: []string{"TestLabel"}, Properties: map[string]interface{}{"name": "cached"}})
		require.NoError(t, err)
		ae.mu.Lock()
		ae.deleteNodes[deletedNode.ID] = true
		ae.mu.Unlock()

		var nodeIDs []NodeID
		require.NoError(t, ae.StreamNodes(context.Background(), func(node *Node) error {
			nodeIDs = append(nodeIDs, node.ID)
			return nil
		}))
		assert.Equal(t, []NodeID{engineNode.ID}, nodeIDs)

		require.NoError(t, ae.GetEngine().CreateEdge(makeEdge("stream-engine-edge", "stream-engine", "stream-deleted")))
		require.NoError(t, ae.GetEngine().CreateEdge(makeEdge("stream-deleted-edge", "stream-deleted", "stream-engine")))
		require.NoError(t, ae.CreateEdge(&Edge{ID: EdgeID(prefixTestID("stream-engine-edge")), StartNode: engineNode.ID, EndNode: deletedNode.ID, Type: "RELATED"}))
		ae.mu.Lock()
		ae.deleteEdges[EdgeID(prefixTestID("stream-deleted-edge"))] = true
		ae.mu.Unlock()

		var edgeIDs []EdgeID
		require.NoError(t, ae.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeIDs = append(edgeIDs, edge.ID)
			return nil
		}))
		assert.Equal(t, []EdgeID{EdgeID(prefixTestID("stream-engine-edge"))}, edgeIDs)
	})

	t.Run("underlying streamer handles callback error and early stop", func(t *testing.T) {
		ae := newAsyncTestEngine(t)
		_, err := ae.CreateNode(makeNode("stream-under-a"))
		require.NoError(t, err)
		_, err = ae.CreateNode(makeNode("stream-under-b"))
		require.NoError(t, err)
		require.NoError(t, ae.Flush())

		errBoom := errors.New("underlying node callback failed")
		err = ae.StreamNodes(context.Background(), func(node *Node) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)

		nodeCount := 0
		require.NoError(t, ae.StreamNodes(context.Background(), func(node *Node) error {
			nodeCount++
			return ErrIterationStopped
		}))
		assert.Equal(t, 1, nodeCount)

		require.NoError(t, ae.CreateEdge(makeEdge("stream-under-edge", "stream-under-a", "stream-under-b")))
		require.NoError(t, ae.Flush())

		errBoom = errors.New("underlying edge callback failed")
		err = ae.StreamEdges(context.Background(), func(edge *Edge) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)

		edgeCount := 0
		require.NoError(t, ae.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeCount++
			return ErrIterationStopped
		}))
		assert.Equal(t, 1, edgeCount)
	})

	t.Run("fallback stream handles allnodes errors and early stop", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		_, err := base.CreateNode(&Node{ID: "mem:fallback-node-1", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "n1"}})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{ID: "mem:fallback-node-2", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "n2"}})
		require.NoError(t, err)
		_, err = base.CreateNode(&Node{ID: "mem:fallback-node-3", Labels: []string{"T"}, Properties: map[string]interface{}{"name": "n3"}})
		require.NoError(t, err)
		require.NoError(t, base.CreateEdge(&Edge{ID: "mem:fallback-edge-1", StartNode: "mem:fallback-node-1", EndNode: "mem:fallback-node-2", Type: "REL"}))
		require.NoError(t, base.CreateEdge(&Edge{ID: "mem:fallback-edge-2", StartNode: "mem:fallback-node-2", EndNode: "mem:fallback-node-3", Type: "REL"}))

		engine := &nonStreamingCountEngine{Engine: base}
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		nodeCount := 0
		require.NoError(t, ae.StreamNodes(context.Background(), func(node *Node) error {
			nodeCount++
			return ErrIterationStopped
		}))
		assert.Equal(t, 1, nodeCount)

		edgeCount := 0
		require.NoError(t, ae.StreamEdges(context.Background(), func(edge *Edge) error {
			edgeCount++
			return ErrIterationStopped
		}))
		assert.Equal(t, 1, edgeCount)

		engine.allNodesErr = errors.New("all nodes failed")
		err = ae.StreamNodes(context.Background(), func(node *Node) error { return nil })
		require.ErrorContains(t, err, "all nodes failed")

		engine.allEdgesErr = errors.New("all edges failed")
		err = ae.StreamEdges(context.Background(), func(edge *Edge) error { return nil })
		require.ErrorContains(t, err, "all edges failed")

		engine.allNodesErr = nil
		errBoom := errors.New("fallback node callback failed")
		err = ae.StreamNodes(context.Background(), func(node *Node) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)

		engine.allEdgesErr = nil
		errBoom = errors.New("fallback edge callback failed")
		err = ae.StreamEdges(context.Background(), func(edge *Edge) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("stream node chunks returns callback error on chunk", func(t *testing.T) {
		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae.Close() })

		for i := 0; i < 3; i++ {
			_, err := ae.CreateNode(&Node{
				ID:         NodeID(prefixTestID(fmt.Sprintf("chunk-node-%d", i))),
				Labels:     []string{"Chunk"},
				Properties: map[string]interface{}{"name": i},
			})
			require.NoError(t, err)
		}

		errBoom := errors.New("chunk callback failed")
		err := ae.StreamNodeChunks(context.Background(), 2, func(nodes []*Node) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)
	})
}

func TestAsyncEngine_GetEdge_CacheDeleteAndEnginePaths(t *testing.T) {
	ae := newAsyncTestEngine(t)
	_, _ = ae.CreateNode(makeNode("edge-get-1"))
	_, _ = ae.CreateNode(makeNode("edge-get-2"))

	cached := makeEdge("edge-cached", "edge-get-1", "edge-get-2")
	require.NoError(t, ae.CreateEdge(cached))

	got, err := ae.GetEdge(cached.ID)
	require.NoError(t, err)
	require.Equal(t, cached.ID, got.ID)

	ae.mu.Lock()
	ae.deleteEdges[cached.ID] = true
	ae.mu.Unlock()
	_, err = ae.GetEdge(cached.ID)
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, ae.Flush())
	engineEdge := makeEdge("edge-engine", "edge-get-1", "edge-get-2")
	require.NoError(t, ae.GetEngine().CreateEdge(engineEdge))

	got, err = ae.GetEdge(engineEdge.ID)
	require.NoError(t, err)
	require.Equal(t, engineEdge.ID, got.ID)
}

func TestAsyncEngine_NodeCountByPrefix_NegativeClamp(t *testing.T) {
	ae := newAsyncTestEngine(t)
	ae.mu.Lock()
	ae.deleteNodes[NodeID(prefixTestID("neg-prefix-node"))] = true
	ae.mu.Unlock()

	count, err := ae.NodeCountByPrefix(prefixTestID("neg-prefix"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestAsyncEngine_LastWriteTime_NilAndFallback(t *testing.T) {
	var nilAE *AsyncEngine
	assert.True(t, nilAE.LastWriteTime().IsZero())

	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	ae := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })

	assert.True(t, ae.LastWriteTime().IsZero())
}

func TestAsyncEngine_ConstraintValidationHelpers(t *testing.T) {
	ae := newAsyncTestEngine(t)
	schema := ae.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("user_email", "User", "email"))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "user_key", Type: ConstraintNodeKey, Label: "User", Properties: []string{"tenant", "username"}}))
	require.NoError(t, schema.AddConstraint(Constraint{Name: "user_name_exists", Type: ConstraintExists, Label: "User", Properties: []string{"name"}}))
	require.NoError(t, schema.AddPropertyTypeConstraint("user_age_type", "User", "age", PropertyTypeInteger))

	t.Run("bulk duplicate unique in batch", func(t *testing.T) {
		err := ae.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u1")), Labels: []string{"User"}, Properties: map[string]interface{}{"email": "dup@example.com"}},
			{ID: NodeID(prefixTestID("u2")), Labels: []string{"User"}, Properties: map[string]interface{}{"email": "dup@example.com"}},
		})
		require.Error(t, err)
	})

	t.Run("bulk duplicate node key in batch", func(t *testing.T) {
		err := ae.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u3")), Labels: []string{"User"}, Properties: map[string]interface{}{"tenant": "t1", "username": "alice"}},
			{ID: NodeID(prefixTestID("u4")), Labels: []string{"User"}, Properties: map[string]interface{}{"tenant": "t1", "username": "alice"}},
		})
		require.Error(t, err)
	})

	t.Run("bulk nil and missing node key property", func(t *testing.T) {
		require.ErrorIs(t, ae.validateBulkNodeConstraints([]*Node{nil}), ErrInvalidData)
		err := ae.validateBulkNodeConstraints([]*Node{
			{ID: NodeID(prefixTestID("u5")), Labels: []string{"User"}, Properties: map[string]interface{}{"tenant": "t1"}},
		})
		require.Error(t, err)
	})

	t.Run("bulk constraint branches that should be ignored", func(t *testing.T) {
		ignoreSchema := ae.GetSchemaForNamespace("ignore")
		require.NoError(t, ignoreSchema.AddConstraint(Constraint{
			Name:       "multi_unique_ignored",
			Type:       ConstraintUnique,
			Label:      "Multi",
			Properties: []string{"a", "b"},
		}))
		require.NoError(t, ignoreSchema.AddConstraint(Constraint{
			Name:       "nil_unique_ignored",
			Type:       ConstraintUnique,
			Label:      "Multi",
			Properties: []string{"a"},
		}))

		err := ae.validateBulkNodeConstraints([]*Node{
			{ID: "ignore:m1", Labels: []string{"Multi"}, Properties: map[string]interface{}{"b": "only-second"}},
			{ID: "ignore:m2", Labels: []string{"NoSchema"}, Properties: map[string]interface{}{"x": 1}},
		})
		require.NoError(t, err)
	})

	t.Run("bulk succeeds with namespaced engine, nil unique value, and nil schema provider", func(t *testing.T) {
		base := NewMemoryEngine()
		t.Cleanup(func() { _ = base.Close() })
		namespaced := NewNamespacedEngine(base, "tenantbulk")
		ae2 := NewAsyncEngine(namespaced, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae2.Close() })

		schema := ae2.GetSchemaForNamespace("tenantbulk")
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "tenantbulk_email_unique",
			Type:       ConstraintUnique,
			Label:      "User",
			Properties: []string{"email"},
		}))
		require.NoError(t, schema.AddConstraint(Constraint{
			Name:       "tenantbulk_user_key",
			Type:       ConstraintNodeKey,
			Label:      "User",
			Properties: []string{"tenant", "username"},
		}))

		err := ae2.validateBulkNodeConstraints([]*Node{
			{ID: "user-1", Labels: []string{"User"}, Properties: map[string]interface{}{"tenant": "t1", "username": "alice"}},
			{ID: "user-2", Labels: []string{"User"}, Properties: map[string]interface{}{"tenant": "t1", "username": "bob", "email": "bob@example.com"}},
		})
		require.NoError(t, err)

		nilSchemaEngine := &nilSchemaProviderEngine{MemoryEngine: NewMemoryEngine()}
		t.Cleanup(func() { _ = nilSchemaEngine.Close() })
		ae3 := NewAsyncEngine(nilSchemaEngine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae3.Close() })

		err = ae3.validateBulkNodeConstraints([]*Node{
			{ID: "test:nil-schema", Labels: []string{"User"}, Properties: map[string]interface{}{"name": "ok"}},
		})
		require.NoError(t, err)
	})

	t.Run("unique constraint cache and engine paths", func(t *testing.T) {
		cached := &Node{ID: NodeID(prefixTestID("cache-user")), Labels: []string{"User"}, Properties: map[string]interface{}{"email": "cache@example.com", "name": "Alice", "tenant": "t1", "username": "alice", "age": int64(30)}}
		_, err := ae.CreateNode(cached)
		require.NoError(t, err)
		err = ae.validateNodeConstraints(&Node{ID: NodeID(prefixTestID("cache-user-2")), Labels: []string{"User"}, Properties: map[string]interface{}{"email": "cache@example.com", "name": "Bob", "tenant": "t2", "username": "bob", "age": int64(31)}})
		require.Error(t, err)

		require.NoError(t, ae.Flush())
		err = ae.validateNodeConstraints(&Node{ID: NodeID(prefixTestID("engine-user")), Labels: []string{"User"}, Properties: map[string]interface{}{"email": "cache@example.com", "name": "Carol", "tenant": "t3", "username": "carol", "age": int64(32)}})
		require.Error(t, err)
	})

	t.Run("node key, existence, property type and namespace errors", func(t *testing.T) {
		err := ae.validateNodeConstraints(&Node{ID: NodeID(prefixTestID("nk1")), Labels: []string{"User"}, Properties: map[string]interface{}{"tenant": "t1"}})
		require.Error(t, err)

		err = ae.validateNodeConstraintsWithNamespace(&Node{ID: NodeID(prefixTestID("exists1")), Labels: []string{"User"}, Properties: map[string]interface{}{"tenant": "t1", "username": "bob"}}, "test", true)
		require.Error(t, err)

		err = ae.validateNodeConstraints(&Node{ID: NodeID(prefixTestID("ptype1")), Labels: []string{"User"}, Properties: map[string]interface{}{"name": "Alice", "tenant": "t1", "username": "alice", "age": "old"}})
		require.Error(t, err)

		_, _, err = ae.resolveNamespace("missing-prefix")
		require.Error(t, err)
		require.ErrorIs(t, ae.validateNodeConstraints(nil), ErrInvalidData)
	})

	t.Run("direct node key helper branches", func(t *testing.T) {
		c := Constraint{Name: "key", Type: ConstraintNodeKey, Label: "User", Properties: []string{"tenant", "username"}}
		require.NoError(t, ae.checkNodeKeyConstraint(&Node{ID: NodeID(prefixTestID("no-key-props")), Labels: []string{"User"}}, Constraint{Label: "User"}, "test", true))

		err := ae.checkNodeKeyConstraint(&Node{ID: NodeID(prefixTestID("nil-props")), Labels: []string{"User"}}, c, "test", true)
		require.Error(t, err)

		ae.mu.Lock()
		ae.nodeCache[NodeID(prefixTestID("cache-key"))] = &Node{
			ID:         NodeID(prefixTestID("cache-key")),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t1", "username": "alice"},
		}
		ae.deleteNodes[NodeID(prefixTestID("cache-deleted"))] = true
		ae.nodeCache[NodeID(prefixTestID("cache-deleted"))] = &Node{
			ID:         NodeID(prefixTestID("cache-deleted")),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t1", "username": "ghost"},
		}
		ae.mu.Unlock()

		err = ae.checkNodeKeyConstraint(&Node{
			ID:         NodeID(prefixTestID("cache-key-2")),
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t1", "username": "alice"},
		}, c, "test", true)
		require.Error(t, err)

		engine := NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		_, err = engine.CreateNode(&Node{
			ID:         "other:key-user",
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t1", "username": "shared"},
		})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{
			ID:         "test:key-user",
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t1", "username": "shared"},
		})
		require.NoError(t, err)

		ae2 := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae2.Close() })

		err = ae2.checkNodeKeyConstraint(&Node{
			ID:         "test:key-user-2",
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t1", "username": "shared"},
		}, c, "test", true)
		require.Error(t, err)

		err = ae2.checkNodeKeyConstraint(&Node{
			ID:         "test:key-user-3",
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t9", "username": "unique"},
		}, c, "test", true)
		require.NoError(t, err)

		errEngine := &labelQueryErrorEngine{
			MemoryEngine:       NewMemoryEngine(),
			getNodesByLabelErr: errors.New("label query failed"),
		}
		t.Cleanup(func() { _ = errEngine.Close() })
		ae3 := NewAsyncEngine(errEngine, &AsyncEngineConfig{FlushInterval: time.Hour})
		t.Cleanup(func() { _ = ae3.Close() })

		err = ae3.checkNodeKeyConstraint(&Node{
			ID:         "test:key-user-4",
			Labels:     []string{"User"},
			Properties: map[string]interface{}{"tenant": "t1", "username": "resilient"},
		}, c, "test", true)
		require.NoError(t, err)
	})

	t.Run("direct existence helper branches", func(t *testing.T) {
		require.NoError(t, ae.checkExistenceConstraint(&Node{ID: NodeID(prefixTestID("exists-ok"))}, Constraint{Label: "User"}))
		err := ae.checkExistenceConstraint(&Node{ID: NodeID(prefixTestID("exists-nil"))}, Constraint{Label: "User", Properties: []string{"name"}})
		require.Error(t, err)
	})
}
