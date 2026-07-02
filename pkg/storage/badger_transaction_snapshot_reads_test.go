package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedSnapshotAdjacencyGraph creates a start node with a single outgoing edge
// to target, plus unrelatedEdges edges wired between throwaway node pairs that
// are NOT adjacent to start. It returns start, target, and the ID of the one
// edge start actually owns. The unrelated edges inflate the total edge count so
// a tx-snapshot adjacency read that scans all edges (O(E)) is distinguishable
// from one that scans only start's adjacency (O(deg(start))).
func seedSnapshotAdjacencyGraph(tb testing.TB, engine *BadgerEngine, unrelatedEdges int) (NodeID, NodeID, EdgeID) {
	tb.Helper()

	start := NodeID(prefixTestID("snap-adj-start"))
	target := NodeID(prefixTestID("snap-adj-target"))
	edgeID := EdgeID(prefixTestID("snap-adj-edge"))

	_, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Node"}})
	require.NoError(tb, err)
	_, err = engine.CreateNode(&Node{ID: target, Labels: []string{"Node"}})
	require.NoError(tb, err)
	require.NoError(tb, engine.CreateEdge(&Edge{ID: edgeID, StartNode: start, EndNode: target, Type: "LINKS"}))

	for i := 0; i < unrelatedEdges; i++ {
		from := NodeID(prefixTestID(fmt.Sprintf("snap-adj-noise-from-%d", i)))
		to := NodeID(prefixTestID(fmt.Sprintf("snap-adj-noise-to-%d", i)))
		_, err := engine.CreateNode(&Node{ID: from, Labels: []string{"Noise"}})
		require.NoError(tb, err)
		_, err = engine.CreateNode(&Node{ID: to, Labels: []string{"Noise"}})
		require.NoError(tb, err)
		require.NoError(tb, engine.CreateEdge(&Edge{
			ID:        EdgeID(prefixTestID(fmt.Sprintf("snap-adj-noise-edge-%d", i))),
			StartNode: from,
			EndNode:   to,
			Type:      "NOISE",
		}))
	}
	return start, target, edgeID
}

// TestTransaction_SnapshotAdjacencyIsNodeLocalAndConsistent proves the Badger
// transaction snapshot adjacency read returns exactly the bound node's edges
// (not the whole graph's) and holds the snapshot across a concurrent commit.
// This exercises getCommittedAdjacentEdgesLocked on the visible-at directional
// adjacency path.
func TestTransaction_SnapshotAdjacencyIsNodeLocalAndConsistent(t *testing.T) {
	engine := createTestBadgerEngine(t)

	start, target, edgeID := seedSnapshotAdjacencyGraph(t, engine, 200)

	txRead, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txRead.Rollback()
	require.False(t, txRead.readTS.IsZero(), "read tx must pin a snapshot version")

	outBefore, err := txRead.GetOutgoingEdges(start)
	require.NoError(t, err)
	require.Equal(t, []string{string(edgeID)}, sortedEdgeIDStrings(outBefore),
		"outgoing adjacency must return only start's own edge, not the 200 unrelated edges")

	inBefore, err := txRead.GetIncomingEdges(target)
	require.NoError(t, err)
	require.Equal(t, []string{string(edgeID)}, sortedEdgeIDStrings(inBefore))

	// Commit a second edge from start in a concurrent transaction.
	laterTarget := NodeID(prefixTestID("snap-adj-target-later"))
	laterEdgeID := EdgeID(prefixTestID("snap-adj-edge-later"))
	txInsert, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = txInsert.CreateNode(&Node{ID: laterTarget, Labels: []string{"Node"}})
	require.NoError(t, err)
	require.NoError(t, txInsert.CreateEdge(&Edge{ID: laterEdgeID, StartNode: start, EndNode: laterTarget, Type: "LINKS"}))
	require.NoError(t, txInsert.Commit())

	// The read snapshot must not observe the post-begin edge.
	outAfter, err := txRead.GetOutgoingEdges(start)
	require.NoError(t, err)
	require.Equal(t, sortedEdgeIDStrings(outBefore), sortedEdgeIDStrings(outAfter),
		"snapshot read must be stable across a concurrent commit")

	// A fresh transaction sees both edges.
	txFresh, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer txFresh.Rollback()
	freshOut, err := txFresh.GetOutgoingEdges(start)
	require.NoError(t, err)
	require.Equal(t, []string{string(edgeID), string(laterEdgeID)}, sortedEdgeIDStrings(freshOut))
}

// TestTransaction_SnapshotAdjacencyRejectsUnknownDirection covers the defensive
// guard in the snapshot branch: an EdgeDirection outside Outgoing/Incoming
// returns ErrInvalidData rather than silently resolving. The callers only pass
// Outgoing/Incoming, so this guard is reachable only from within the package.
func TestTransaction_SnapshotAdjacencyRejectsUnknownDirection(t *testing.T) {
	engine := createTestBadgerEngine(t)
	start, _, _ := seedSnapshotAdjacencyGraph(t, engine, 1)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.False(t, tx.readTS.IsZero(), "read tx must pin a snapshot version")

	tx.mu.Lock()
	_, err = tx.getCommittedAdjacentEdgesLocked(start, EdgeDirection(99))
	tx.mu.Unlock()
	require.ErrorIs(t, err, ErrInvalidData)
}

// BenchmarkTransaction_SnapshotAdjacency measures tx-snapshot outgoing adjacency
// cost as the total edge count grows while the bound node's degree stays at 1.
// A node-local read stays flat across edge counts; an all-edges scan degrades
// linearly with the total edge count.
func BenchmarkTransaction_SnapshotAdjacency(b *testing.B) {
	for _, unrelatedEdges := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("edges=%d", unrelatedEdges), func(b *testing.B) {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(b, err)
			b.Cleanup(func() { engine.Close() })

			start, _, _ := seedSnapshotAdjacencyGraph(b, engine, unrelatedEdges)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := engine.BeginTransaction()
				if err != nil {
					b.Fatal(err)
				}
				edges, err := tx.GetOutgoingEdges(start)
				if err != nil {
					b.Fatal(err)
				}
				if len(edges) != 1 {
					b.Fatalf("expected 1 outgoing edge, got %d", len(edges))
				}
				tx.Rollback()
			}
		})
	}
}
