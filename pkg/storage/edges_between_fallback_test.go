package storage

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The legacy fallback (a pair the edge-between index doesn't list) finds the
// edge whichever endpoint has the larger degree: it walks the start node's
// outgoing and the end node's incoming adjacency in lockstep and uses the one
// exhausted first.
func TestEdgesBetweenFallback_FindsEdgeFromEitherSide(t *testing.T) {
	engine := createTestBadgerEngine(t)
	hub := testNode("fb-hub")
	leaf := testNode("fb-leaf")
	_, err := engine.CreateNode(hub)
	require.NoError(t, err)
	_, err = engine.CreateNode(leaf)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		other := testNode(fmt.Sprintf("fb-other-%d", i))
		_, err := engine.CreateNode(other)
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(testEdge(fmt.Sprintf("fb-out-%d", i), hub.ID, other.ID, "HAS")))
		require.NoError(t, engine.CreateEdge(testEdge(fmt.Sprintf("fb-in-%d", i), other.ID, leaf.ID, "HAS")))
	}
	// hub -> leaf: the hub has 51 outgoing edges and the leaf 51 incoming ones.
	edge := testEdge("fb-target", hub.ID, leaf.ID, "HAS")
	require.NoError(t, engine.CreateEdge(edge))
	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		if err := txn.Delete(engine.edgeBetweenHeadKeyFromStringIDs(hub.ID, leaf.ID, "HAS")); err != nil {
			return err
		}
		return txn.Delete(engine.edgeBetweenIndexKeyFromStringIDs(hub.ID, leaf.ID, "HAS", edge.ID))
	}))

	got, err := engine.edgesBetweenFromLegacyOutgoingIndex(hub.ID, leaf.ID, "HAS")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, edge.ID, got[0].ID)
	none, err := engine.edgesBetweenFromLegacyOutgoingIndex(leaf.ID, hub.ID, "HAS")
	require.NoError(t, err)
	assert.Empty(t, none, "direction matters")
	other, err := engine.edgesBetweenFromLegacyOutgoingIndex(hub.ID, leaf.ID, "OTHER")
	require.NoError(t, err)
	assert.Empty(t, other, "type matters")
}

// A miss from a high-degree hub to a fresh node must not materialize the hub's
// adjacency: MERGE of new relationships out of a hub was quadratic. Measure
// allocated work rather than elapsed time so concurrent package tests and GC
// cannot turn scheduler contention into a complexity regression. The old hub
// scan copies every outgoing key and therefore fails this bound.
func TestEdgesBetweenFallback_CostIsTheSmallerDegree(t *testing.T) {
	run := func(degree int) float64 {
		engine := createTestBadgerEngine(t)
		hub := testNode("deg-hub")
		_, err := engine.CreateNode(hub)
		require.NoError(t, err)
		edges := make([]*Edge, 0, degree)
		for i := 0; i < degree; i++ {
			n := testNode(fmt.Sprintf("deg-n-%d", i))
			_, err := engine.CreateNode(n)
			require.NoError(t, err)
			edges = append(edges, testEdge(fmt.Sprintf("deg-e-%d", i), hub.ID, n.ID, "HAS"))
		}
		require.NoError(t, engine.BulkCreateEdges(edges))
		fresh := testNode("deg-fresh")
		_, err = engine.CreateNode(fresh)
		require.NoError(t, err)
		return testing.AllocsPerRun(20, func() {
			got, err := engine.edgesBetweenFromLegacyOutgoingIndex(hub.ID, fresh.ID, "HAS")
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
	small, large := run(1000), run(16000)
	require.Positive(t, small, "the real Badger lookup must be measured")
	ratio := large / small
	t.Logf("allocations per miss: degree 1000: %.0f, 16000: %.0f, ratio %.1f (hub scan ~16)", small, large, ratio)
	require.Less(t, ratio, 4.0, "fallback must not allocate the high-degree hub's adjacency")
}
