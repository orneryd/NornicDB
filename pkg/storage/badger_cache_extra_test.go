package storage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerCache_LabelCacheLifecycle_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	nid := NodeID(prefixTestID("node-1"))

	// set/get
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "Person", nid, b.db.oracle.published.Load())
	got, ok := b.labelCacheGetFirst("Person")
	assert.True(t, ok)
	assert.Equal(t, nid, got)

	// invalidate exact node+label
	_, ok = b.labelCacheGetFirst("person")
	assert.False(t, ok, "labels are case-sensitive (#862)")
	b.labelCacheInvalidateForNodeLabels([]string{"Person"}, nid)
	_, ok = b.labelCacheGetFirst("Person")
	assert.False(t, ok)

	// re-add and invalidate removed labels
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "Employee", nid, b.db.oracle.published.Load())
	b.labelCacheInvalidateForRemovedLabels([]string{"Employee", "Person"}, []string{"Person"}, nid)
	_, ok = b.labelCacheGetFirst("Employee")
	assert.False(t, ok)
}

func TestBadgerCache_NodeCreateUpdateDelete_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := &Node{ID: NodeID(prefixTestID("ncache-1")), Labels: []string{"Person", "Legacy"}, Properties: map[string]interface{}{"name": "a"}}
	_, err := b.CreateNode(n)
	require.NoError(t, err)
	assert.EqualValues(t, 1, b.nodeCount.Load())
	_, ok := b.nodeCache[n.ID]
	assert.True(t, ok)
	require.NoError(t, b.ForEachNodeIDByLabel("Legacy", func(NodeID) bool { return true }))
	n2 := &Node{ID: n.ID, Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "b"}}
	require.NoError(t, b.UpdateNode(NodeUpdateRequest{After: n2}))
	assert.Equal(t, "b", b.nodeCache[n.ID].Properties["name"])
	_, ok = b.labelCacheGetFirst("Legacy")
	assert.False(t, ok)
	require.NoError(t, b.DeleteNode(n.ID))
	assert.EqualValues(t, 0, b.nodeCount.Load())
	_, ok = b.nodeCache[n.ID]
	assert.False(t, ok)

	id := NodeID(prefixTestID("tenant_cache:n2"))
	_, err = b.CreateNode(&Node{ID: id, Labels: []string{"Person"}})
	require.NoError(t, err)
	for _, edgeID := range []EdgeID{EdgeID(prefixTestID("cascade-1")), EdgeID(prefixTestID("cascade-2"))} {
		require.NoError(t, b.CreateEdge(&Edge{ID: edgeID, StartNode: id, EndNode: id, Type: "KNOWS"}))
	}
	_, err = b.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	beforeEdges := b.edgeCount.Load()
	require.NoError(t, b.DeleteNode(id))
	assert.EqualValues(t, beforeEdges-2, b.edgeCount.Load())
	_, ok = b.edgeTypeCache["KNOWS"]
	assert.False(t, ok)
}

func TestBadgerCache_EdgeCreateUpdateDelete_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	source, target := NodeID(prefixTestID("cache-source")), NodeID(prefixTestID("cache-target"))
	for _, id := range []NodeID{source, target} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	eid := EdgeID(prefixTestID("edge-1"))
	e := &Edge{ID: eid, StartNode: source, EndNode: target, Type: "KNOWS"}
	_, err := b.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.NoError(t, b.CreateEdge(e))
	assert.EqualValues(t, 1, b.edgeCount.Load())
	_, ok := b.edgeTypeCache["KNOWS"]
	assert.False(t, ok)

	e.Type = "LIKES"
	require.NoError(t, b.UpdateEdge(EdgeUpdateRequest{After: e}))
	_, err = b.GetEdgesByType("LIKES")
	require.NoError(t, err)
	_, err = b.GetEdgesByType("HATES")
	require.NoError(t, err)
	e.Type = "HATES"
	require.NoError(t, b.UpdateEdge(EdgeUpdateRequest{After: e}))
	_, ok = b.edgeTypeCache["LIKES"]
	assert.False(t, ok)
	_, ok = b.edgeTypeCache["HATES"]
	assert.False(t, ok)
	require.NoError(t, b.DeleteEdge(eid))
	assert.EqualValues(t, 0, b.edgeCount.Load())
}

func TestBadgerCache_BulkCacheHooks_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)

	for _, id := range []NodeID{NodeID(prefixTestID("bn-1")), NodeID(prefixTestID("bn-2"))} {
		b.cacheOnNodeCreated(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]interface{}{}})
	}
	assert.EqualValues(t, 2, b.nodeCount.Load())
	for _, id := range []EdgeID{EdgeID(prefixTestID("be-1")), EdgeID(prefixTestID("be-2")), EdgeID(prefixTestID("be-3"))} {
		b.cacheOnEdgeCreated(&Edge{ID: id, Type: "REL"})
	}
	assert.EqualValues(t, 3, b.edgeCount.Load())

	b.cacheOnEdgesDeleted([]EdgeID{EdgeID(prefixTestID("be-1")), EdgeID(prefixTestID("be-2"))})
	assert.EqualValues(t, 1, b.edgeCount.Load())

	b.cacheOnNodesDeleted([]NodeID{NodeID(prefixTestID("bn-1")), NodeID(prefixTestID("bn-2"))}, 2, 1)
	assert.EqualValues(t, 0, b.nodeCount.Load())
	assert.EqualValues(t, 0, b.edgeCount.Load())
}

func TestBadgerCache_NoopBranches_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)

	// nil / empty guards
	b.cacheStoreNode(nil, b.db.oracle.published.Load())
	b.cacheDeleteNode("", nodeCacheAllForms)
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "", NodeID("x"), b.db.oracle.published.Load())
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "L", "", b.db.oracle.published.Load())
	b.labelCacheInvalidateForNodeLabels(nil, NodeID("x"))
	b.labelCacheInvalidateForRemovedLabels(nil, nil, NodeID("x"))
	b.cacheOnEdgeCreated(nil)
	b.cacheOnEdgeUpdated("", nil)
	b.cacheOnEdgesDeleted(nil)
	b.cacheOnNodesDeleted(nil, 0, 0)

	assert.EqualValues(t, 0, b.nodeCount.Load())
	assert.EqualValues(t, 0, b.edgeCount.Load())
}

func TestBadgerCache_AdjacencyInvalidateAll_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	b.adjCacheMu.Lock()
	b.outgoingAdjCache[NodeID(prefixTestID("a"))] = []EdgeID{EdgeID(prefixTestID("e"))}
	b.adjCacheMu.Unlock()
	b.adjCacheInvalidate()
	assert.Empty(t, b.outgoingAdjCache)
	assert.Empty(t, b.incomingAdjCache)

	// An empty cache is kept, not re-allocated.
	before := reflect.ValueOf(b.outgoingAdjCache).Pointer()
	b.adjCacheInvalidate()
	assert.Equal(t, before, reflect.ValueOf(b.outgoingAdjCache).Pointer())
}
