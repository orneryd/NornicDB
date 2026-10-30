package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_NodeCacheStoresDeepCopies(t *testing.T) {
	engine := createTestBadgerEngine(t)

	node := testNode("n1")
	node.Properties["name"] = "Alice"
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	// Mutate the original node without persisting. If the cache stores pointers
	// directly, this would corrupt cached state.
	node.Properties["name"] = "Eve"

	got, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, "Alice", got.Properties["name"])
}

func TestBadgerEngine_AllNodesPopulatesNodeCache(t *testing.T) {
	engine := createTestBadgerEngine(t)

	node := testNode("allnodes-cache")
	node.Properties["name"] = "Alice"
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	engine.nodeCacheMu.Lock()
	engine.nodeCache = make(map[NodeID]*Node, engine.nodeCacheMaxEntries)
	engine.nodeCacheMu.Unlock()

	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, node.ID, nodes[0].ID)

	engine.nodeCacheMu.RLock()
	cached, ok := engine.nodeCache[node.ID]
	engine.nodeCacheMu.RUnlock()
	require.True(t, ok, "AllNodes should populate cache for repeated scan workloads")
	require.NotSame(t, nodes[0], cached)
	require.Equal(t, "Alice", cached.Properties["name"])
}

func TestBadgerEngine_NodeCacheNormalizesHomogeneousPropertyArrays(t *testing.T) {
	engine := createTestBadgerEngine(t)

	node := testNode("ncache-array")
	node.Properties["embedding"] = []interface{}{0.7, 0.2, 0.05, 0.05}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	nodes, err := engine.AllNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.IsType(t, []float64{}, nodes[0].Properties["embedding"])
	require.Equal(t, []float64{0.7, 0.2, 0.05, 0.05}, nodes[0].Properties["embedding"])
}

func TestBadgerEngine_UpdateEdge_UpdatesTypeIndexAndInvalidatesCache(t *testing.T) {
	engine := createTestBadgerEngine(t)

	n1 := testNode("n1")
	n2 := testNode("n2")
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	_, err = engine.CreateNode(n2)
	require.NoError(t, err)

	edge := testEdge("e1", n1.ID, n2.ID, "KNOWS")
	err = engine.CreateEdge(edge)
	require.NoError(t, err)

	// Warm the edge-type cache for KNOWS.
	knows, err := engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Len(t, knows, 1)
	require.Equal(t, edge.ID, knows[0].ID)

	edge.Type = "LIKES"
	err = engine.UpdateEdge(edge)
	require.NoError(t, err)

	// Old type should be empty (both index updated + cache invalidated).
	knows, err = engine.GetEdgesByType("KNOWS")
	require.NoError(t, err)
	require.Len(t, knows, 0)

	likes, err := engine.GetEdgesByType("LIKES")
	require.NoError(t, err)
	require.Len(t, likes, 1)
	require.Equal(t, edge.ID, likes[0].ID)
}
