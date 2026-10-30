package storage

import (
	"fmt"
	"testing"
	"time"

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
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
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

// A miss from a high-degree hub to a fresh node costs the fresh node's degree,
// not the hub's: MERGE of new relationships out of a hub was quadratic.
func TestEdgesBetweenFallback_CostIsTheSmallerDegree(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	run := func(degree int) func(int) time.Duration {
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
		return func(iterations int) time.Duration {
			start := time.Now()
			for i := 0; i < iterations; i++ {
				got, err := engine.edgesBetweenFromLegacyOutgoingIndex(hub.ID, fresh.ID, "HAS")
				require.NoError(t, err)
				require.Empty(t, got)
			}
			return time.Since(start)
		}
	}
	smallRun, largeRun := run(1000), run(16000)
	// Use equal work above clock resolution; a 200-operation sample can
	// round to zero on Windows and cannot establish the scaling ratio.
	iterations := 200
	for smallRun(iterations) < 20*time.Millisecond {
		iterations *= 2
	}
	small, large := smallRun(iterations), largeRun(iterations)
	ratio := float64(large) / float64(small)
	t.Logf("%d misses from a hub of degree 1000: %v, 16000: %v, ratio %.1f (hub-degree cost ~16)", iterations, small, large, ratio)
	require.Less(t, ratio, 4.0)
}
