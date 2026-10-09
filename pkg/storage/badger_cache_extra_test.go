package storage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBadgerCache_LabelCacheLifecycle_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	nid := NodeID(prefixTestID("node-1"))

	// set/get
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "Person", nid)
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
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "Employee", nid)
	b.labelCacheInvalidateForRemovedLabels([]string{"Employee", "Person"}, []string{"Person"}, nid)
	_, ok = b.labelCacheGetFirst("Employee")
	assert.False(t, ok)
}

func TestBadgerCache_NodeCreateUpdateDelete_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	n := &Node{ID: NodeID(prefixTestID("ncache-1")), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "a"}}

	b.cacheOnNodeCreated(n)
	assert.EqualValues(t, 1, b.nodeCount.Load())
	_, ok := b.nodeCache[n.ID]
	assert.True(t, ok)

	n2 := &Node{ID: n.ID, Labels: []string{"Person", "Employee"}, Properties: map[string]interface{}{"name": "b"}}
	b.cacheOnNodeUpdated(n2)
	assert.Equal(t, "b", b.nodeCache[n.ID].Properties["name"])

	old := &Node{ID: n.ID, Labels: []string{"Person", "Legacy"}, Properties: map[string]interface{}{}}
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "Legacy", n.ID)
	b.cacheOnNodeUpdatedWithOldNode(n2, old)
	_, ok = b.labelCacheGetFirst("Legacy")
	assert.False(t, ok)

	b.cacheOnNodeDeleted(n.ID, 0)
	assert.EqualValues(t, 0, b.nodeCount.Load())
	_, ok = b.nodeCache[n.ID]
	assert.False(t, ok)

	b.edgeCount.Store(3)
	b.edgeTypeCache["KNOWS"] = []*Edge{{ID: EdgeID(prefixTestID("edge-del")), Type: "KNOWS"}}
	b.cacheOnNodeCreated(&Node{ID: NodeID(prefixTestID("tenant_cache:n2")), Labels: []string{"Person"}, Properties: map[string]interface{}{}})
	beforeEdges := b.edgeCount.Load()
	b.cacheOnNodeDeleted(NodeID(prefixTestID("tenant_cache:n2")), 2)
	assert.EqualValues(t, beforeEdges-2, b.edgeCount.Load())
	_, ok = b.edgeTypeCache["KNOWS"]
	assert.False(t, ok)
}

func TestBadgerCache_EdgeCreateUpdateDelete_Extra(t *testing.T) {
	b := createTestBadgerEngine(t)
	eid := EdgeID(prefixTestID("edge-1"))
	e := &Edge{ID: eid, Type: "KNOWS"}

	b.edgeTypeCache["KNOWS"] = []*Edge{{ID: eid, Type: "KNOWS"}}
	b.cacheOnEdgeCreated(e)
	assert.EqualValues(t, 1, b.edgeCount.Load())
	_, ok := b.edgeTypeCache["KNOWS"]
	assert.False(t, ok)

	b.edgeTypeCache["LIKES"] = []*Edge{{ID: eid, Type: "LIKES"}}
	b.edgeTypeCache["HATES"] = []*Edge{{ID: eid, Type: "HATES"}}
	b.cacheOnEdgeUpdated("LIKES", &Edge{ID: eid, Type: "HATES"})
	_, ok = b.edgeTypeCache["LIKES"]
	assert.False(t, ok)
	_, ok = b.edgeTypeCache["HATES"]
	assert.False(t, ok)

	b.cacheOnEdgeDeleted(eid, "KNOWS")
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
	b.cacheStoreNode(nil)
	b.cacheDeleteNode("")
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "", NodeID("x"))
	b.labelCacheSetFirst(b.labelFirstCacheGen.current(), "L", "")
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
	b.adjCacheInvalidateAll()
	assert.Empty(t, b.outgoingAdjCache)
	assert.Empty(t, b.incomingAdjCache)

	// An empty cache is kept, not re-allocated.
	before := reflect.ValueOf(b.outgoingAdjCache).Pointer()
	b.adjCacheInvalidateAll()
	assert.Equal(t, before, reflect.ValueOf(b.outgoingAdjCache).Pointer())
}
