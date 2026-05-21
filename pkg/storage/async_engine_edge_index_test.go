package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// edgeIDsTyped returns just the IDs of edges for set-equality assertions.
func edgeIDsTyped(edges []*Edge) []EdgeID {
	out := make([]EdgeID, 0, len(edges))
	for _, e := range edges {
		out = append(out, e.ID)
	}
	return out
}

// indexedStartIDs returns the EdgeID set tracked by cacheEdgesByStart for n.
// Caller is responsible for taking the read lock; we copy under it.
func indexedStartIDs(ae *AsyncEngine, n NodeID) []EdgeID {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	set := ae.cacheEdgesByStart[n]
	out := make([]EdgeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

func indexedEndIDs(ae *AsyncEngine, n NodeID) []EdgeID {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	set := ae.cacheEdgesByEnd[n]
	out := make([]EdgeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

func mustCreateNode(t *testing.T, eng Engine, id string) NodeID {
	t.Helper()
	n := &Node{ID: NodeID(prefixTestID(id)), Labels: []string{"N"}}
	_, err := eng.CreateNode(n)
	require.NoError(t, err)
	return n.ID
}

func TestAsyncEngine_EdgeIndex_CreateThenLookup(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNode(t, engine, "ix-create-a")
	b := mustCreateNode(t, engine, "ix-create-b")
	c := mustCreateNode(t, engine, "ix-create-c")

	e1 := EdgeID(prefixTestID("ix-create-e1"))
	e2 := EdgeID(prefixTestID("ix-create-e2"))
	e3 := EdgeID(prefixTestID("ix-create-e3"))

	require.NoError(t, async.CreateEdge(&Edge{ID: e1, StartNode: a, EndNode: b, Type: "T"}))
	require.NoError(t, async.CreateEdge(&Edge{ID: e2, StartNode: a, EndNode: c, Type: "T"}))
	require.NoError(t, async.CreateEdge(&Edge{ID: e3, StartNode: b, EndNode: c, Type: "T"}))

	assert.ElementsMatch(t, []EdgeID{e1, e2}, indexedStartIDs(async, a))
	assert.ElementsMatch(t, []EdgeID{e3}, indexedStartIDs(async, b))
	assert.Empty(t, indexedStartIDs(async, c))

	assert.ElementsMatch(t, []EdgeID{e1}, indexedEndIDs(async, b))
	assert.ElementsMatch(t, []EdgeID{e2, e3}, indexedEndIDs(async, c))
	assert.Empty(t, indexedEndIDs(async, a))

	out, err := async.GetOutgoingEdges(a)
	require.NoError(t, err)
	assert.ElementsMatch(t, []EdgeID{e1, e2}, edgeIDsTyped(out))

	in, err := async.GetIncomingEdges(c)
	require.NoError(t, err)
	assert.ElementsMatch(t, []EdgeID{e2, e3}, edgeIDsTyped(in))
}

func TestAsyncEngine_EdgeIndex_UpdateSameEndpoints(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNode(t, engine, "ix-upd-a")
	b := mustCreateNode(t, engine, "ix-upd-b")
	id := EdgeID(prefixTestID("ix-upd-e"))

	require.NoError(t, async.CreateEdge(&Edge{ID: id, StartNode: a, EndNode: b, Type: "T", Properties: map[string]any{"v": 1}}))
	require.NoError(t, async.UpdateEdge(&Edge{ID: id, StartNode: a, EndNode: b, Type: "T", Properties: map[string]any{"v": 2}}))

	// One entry per direction, not duplicated.
	assert.Equal(t, []EdgeID{id}, indexedStartIDs(async, a))
	assert.Equal(t, []EdgeID{id}, indexedEndIDs(async, b))

	out, err := async.GetOutgoingEdges(a)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 2, out[0].Properties["v"])
}

func TestAsyncEngine_EdgeIndex_UpdateChangedEndpoints(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNode(t, engine, "ix-mv-a")
	b := mustCreateNode(t, engine, "ix-mv-b")
	c := mustCreateNode(t, engine, "ix-mv-c")
	d := mustCreateNode(t, engine, "ix-mv-d")
	id := EdgeID(prefixTestID("ix-mv-e"))

	require.NoError(t, async.CreateEdge(&Edge{ID: id, StartNode: a, EndNode: b, Type: "T"}))
	// Move the edge to a→c, then to d→c. Index for a (start), b (end), and
	// the previous c (end) should be drained at each step.
	require.NoError(t, async.UpdateEdge(&Edge{ID: id, StartNode: a, EndNode: c, Type: "T"}))
	assert.Equal(t, []EdgeID{id}, indexedStartIDs(async, a))
	assert.Empty(t, indexedEndIDs(async, b))
	assert.Equal(t, []EdgeID{id}, indexedEndIDs(async, c))

	require.NoError(t, async.UpdateEdge(&Edge{ID: id, StartNode: d, EndNode: c, Type: "T"}))
	assert.Empty(t, indexedStartIDs(async, a))
	assert.Equal(t, []EdgeID{id}, indexedStartIDs(async, d))
	assert.Equal(t, []EdgeID{id}, indexedEndIDs(async, c))

	out, err := async.GetOutgoingEdges(d)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, c, out[0].EndNode)

	// a no longer has any outgoing — and crucially the lookup must NOT
	// surface the relocated edge.
	out, err = async.GetOutgoingEdges(a)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestAsyncEngine_EdgeIndex_DeleteCacheOnlyEdge(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNode(t, engine, "ix-del-a")
	b := mustCreateNode(t, engine, "ix-del-b")
	id := EdgeID(prefixTestID("ix-del-e"))

	require.NoError(t, async.CreateEdge(&Edge{ID: id, StartNode: a, EndNode: b, Type: "T"}))
	require.Equal(t, []EdgeID{id}, indexedStartIDs(async, a))

	require.NoError(t, async.DeleteEdge(id))
	assert.Empty(t, indexedStartIDs(async, a))
	assert.Empty(t, indexedEndIDs(async, b))

	out, err := async.GetOutgoingEdges(a)
	require.NoError(t, err)
	assert.Empty(t, out)
}

func TestAsyncEngine_EdgeIndex_BulkCreateAndDelete(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNode(t, engine, "ix-bulk-a")
	b := mustCreateNode(t, engine, "ix-bulk-b")
	c := mustCreateNode(t, engine, "ix-bulk-c")

	edges := []*Edge{
		{ID: EdgeID(prefixTestID("ix-bulk-e1")), StartNode: a, EndNode: b, Type: "T"},
		{ID: EdgeID(prefixTestID("ix-bulk-e2")), StartNode: a, EndNode: c, Type: "T"},
		{ID: EdgeID(prefixTestID("ix-bulk-e3")), StartNode: b, EndNode: c, Type: "T"},
	}
	require.NoError(t, async.BulkCreateEdges(edges))

	assert.ElementsMatch(t, []EdgeID{edges[0].ID, edges[1].ID}, indexedStartIDs(async, a))
	assert.ElementsMatch(t, []EdgeID{edges[2].ID}, indexedStartIDs(async, b))
	assert.ElementsMatch(t, []EdgeID{edges[1].ID, edges[2].ID}, indexedEndIDs(async, c))

	// Bulk delete two of three.
	require.NoError(t, async.BulkDeleteEdges([]EdgeID{edges[0].ID, edges[2].ID}))
	assert.ElementsMatch(t, []EdgeID{edges[1].ID}, indexedStartIDs(async, a))
	assert.Empty(t, indexedStartIDs(async, b))
	assert.ElementsMatch(t, []EdgeID{edges[1].ID}, indexedEndIDs(async, c))
	assert.Empty(t, indexedEndIDs(async, b))

	out, err := async.GetOutgoingEdges(a)
	require.NoError(t, err)
	assert.ElementsMatch(t, []EdgeID{edges[1].ID}, edgeIDsTyped(out))
}

func TestAsyncEngine_EdgeIndex_FlushEvictsFromIndex(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNode(t, engine, "ix-flush-a")
	b := mustCreateNode(t, engine, "ix-flush-b")
	id := EdgeID(prefixTestID("ix-flush-e"))

	require.NoError(t, async.CreateEdge(&Edge{ID: id, StartNode: a, EndNode: b, Type: "T"}))
	require.Equal(t, []EdgeID{id}, indexedStartIDs(async, a))

	require.NoError(t, async.Flush())

	// After flush the cache (and its index) should be empty for this edge.
	assert.Empty(t, indexedStartIDs(async, a))
	assert.Empty(t, indexedEndIDs(async, b))

	// And the merged read still returns the edge from the underlying engine.
	out, err := async.GetOutgoingEdges(a)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, id, out[0].ID)
}

func TestAsyncEngine_EdgeIndex_MergeWithEngineEdges(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	a := mustCreateNode(t, engine, "ix-merge-a")
	b := mustCreateNode(t, engine, "ix-merge-b")
	c := mustCreateNode(t, engine, "ix-merge-c")

	// One edge lives only in the engine.
	engineOnly := EdgeID(prefixTestID("ix-merge-engine"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: engineOnly, StartNode: a, EndNode: b, Type: "T"}))

	// One edge is created via async (cache-only until flush).
	cacheOnly := EdgeID(prefixTestID("ix-merge-cache"))
	require.NoError(t, async.CreateEdge(&Edge{ID: cacheOnly, StartNode: a, EndNode: c, Type: "T"}))

	// One edge exists in the engine and is then UPDATED via async — the
	// async cache override should win and not be duplicated.
	overridden := EdgeID(prefixTestID("ix-merge-override"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: overridden, StartNode: a, EndNode: b, Type: "T", Properties: map[string]any{"v": 1}}))
	require.NoError(t, async.UpdateEdge(&Edge{ID: overridden, StartNode: a, EndNode: b, Type: "T", Properties: map[string]any{"v": 2}}))

	out, err := async.GetOutgoingEdges(a)
	require.NoError(t, err)
	assert.ElementsMatch(t, []EdgeID{engineOnly, cacheOnly, overridden}, edgeIDsTyped(out))

	// The override must reflect the async-cached property value, not the engine's.
	for _, e := range out {
		if e.ID == overridden {
			assert.Equal(t, 2, e.Properties["v"], "cache override should win over engine value")
		}
	}

	// Now mark the engine-only edge for deletion via async — it must NOT
	// appear in the merged result.
	require.NoError(t, async.DeleteEdge(engineOnly))
	out, err = async.GetOutgoingEdges(a)
	require.NoError(t, err)
	assert.NotContains(t, edgeIDsTyped(out), engineOnly)
}

// TestAsyncEngine_EdgeIndex_LookupCostIsLocal exercises the per-node lookup
// in a regime where, before the inverted index, GetOutgoingEdges scanned
// every cached edge. It does not assert wall-clock cost (flaky on CI), but
// it does assert correctness at scale: only the queried node's edges come
// back, with no false positives from the other 999 cached edges.
func TestAsyncEngine_EdgeIndex_LookupCostIsLocal(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()
	async := NewAsyncEngine(engine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer async.Close()

	const nodeCount = 100
	const edgesPerNode = 10

	nodeIDs := make([]NodeID, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeIDs[i] = mustCreateNode(t, engine, fmt.Sprintf("ix-scale-n%d", i))
	}
	for i := 0; i < nodeCount; i++ {
		for k := 0; k < edgesPerNode; k++ {
			endIdx := (i + k + 1) % nodeCount
			id := EdgeID(prefixTestID(fmt.Sprintf("ix-scale-e%d-%d", i, k)))
			require.NoError(t, async.CreateEdge(&Edge{
				ID:        id,
				StartNode: nodeIDs[i],
				EndNode:   nodeIDs[endIdx],
				Type:      "T",
			}))
		}
	}

	// Pick a node mid-list and ensure only its own outgoing edges come back.
	target := nodeIDs[42]
	out, err := async.GetOutgoingEdges(target)
	require.NoError(t, err)
	assert.Len(t, out, edgesPerNode)
	for _, e := range out {
		assert.Equal(t, target, e.StartNode)
	}

	// Index size for that node should be exactly edgesPerNode — proves we
	// aren't accidentally storing every edge under every node.
	assert.Len(t, indexedStartIDs(async, target), edgesPerNode)
}
