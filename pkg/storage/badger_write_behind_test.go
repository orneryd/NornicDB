package storage

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newWriteBehindTestEngine(t *testing.T, interval time.Duration) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: interval,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func bufferedCreate(t *testing.T, engine *BadgerEngine, node *Node) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestWriteBehind_BufferedCommitVisibleBeforeFlush(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	require.Greater(t, engine.writeBehind.PendingOps(), 0, "commit must stay buffered before flush")

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["v"], "acknowledged write visible through the overlay")
}

func TestWriteBehind_FlushLandsInBadger(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Properties["v"])

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestWriteBehind_MultipleCommitsApplyInOrder(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})

	// Updating the same node in a later buffered commit must shadow the
	// earlier version (a second create of the same ID now correctly
	// conflicts via the overlay).
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	err = tx.UpdateNode(&Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"], "newest commit shadows the older one")

	require.NoError(t, engine.FlushWriteBehind())
	got, err = engine.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])
}

func TestWriteBehind_BufferedDeleteHidesAndPersists(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})
	require.NoError(t, engine.FlushWriteBehind())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:n1"))
	require.NoError(t, tx.Commit())

	_, err = engine.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound, "buffered delete hides the node")

	require.NoError(t, engine.FlushWriteBehind())
	_, err = engine.GetNode("test:n1")
	require.ErrorIs(t, err, ErrNotFound, "delete persists after flush")
}

// TestWriteBehind_ScansSeeBufferedWrites reproduces the Northwind seed
// verification failure: label scans, label streams and edge-type reads must
// see acknowledged-but-unflushed writes through the overlay immediately, not
// only after the background flush lands them.
func TestWriteBehind_ScansSeeBufferedWrites(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	for i := 0; i < 5; i++ {
		bufferedCreate(t, engine, &Node{
			ID:         NodeID("test:o" + strconv.Itoa(i)),
			Labels:     []string{"Order"},
			Properties: map[string]any{"v": int64(i)},
		})
	}
	for i := 0; i < 3; i++ {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		err = tx.CreateEdge(&Edge{
			ID:        EdgeID("test:p" + strconv.Itoa(i)),
			StartNode: "test:o0",
			EndNode:   "test:o1",
			Type:      "PURCHASED",
		})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	// Nothing has flushed: every read path below must see the buffered data.
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	nodes, err := engine.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, nodes, 5, "GetNodesByLabel must merge the overlay")

	// The derived-count fast paths must include buffered deltas too.
	labelCount, err := engine.NodeCountByLabel("Order")
	require.NoError(t, err)
	require.Equal(t, int64(5), labelCount, "NodeCountByLabel must include buffered deltas")
	nsLabelCount, err := engine.NodeCountByLabelInNamespace("test", "Order")
	require.NoError(t, err)
	require.Equal(t, int64(5), nsLabelCount, "NodeCountByLabelInNamespace must include buffered deltas")

	allNodes, err := engine.AllNodes()
	require.NoError(t, err)
	foundOrder := 0
	for _, n := range allNodes {
		for _, label := range n.Labels {
			if label == "Order" {
				foundOrder++
			}
		}
	}
	require.Equal(t, 5, foundOrder, "AllNodes must merge the overlay")

	var streamed int
	err = engine.StreamNodesByLabelProjected("Order", nil, func(n *Node) error {
		streamed++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 5, streamed, "StreamNodesByLabelProjected must merge the overlay")

	edges, err := engine.GetEdgesByType("PURCHASED")
	require.NoError(t, err)
	require.Len(t, edges, 3, "GetEdgesByType must merge the overlay")

	edgeCount, err := engine.EdgeCountByType("PURCHASED")
	require.NoError(t, err)
	require.Equal(t, int64(3), edgeCount, "EdgeCountByType must include buffered deltas")

	allEdges, err := engine.AllEdges()
	require.NoError(t, err)
	purchased := 0
	for _, e := range allEdges {
		if e.Type == "PURCHASED" {
			purchased++
		}
	}
	require.Equal(t, 3, purchased, "AllEdges must merge the overlay")

	var streamedEdges int
	err = engine.StreamEdgesByType(context.Background(), "PURCHASED", func(e *Edge) error {
		streamedEdges++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, streamedEdges, "StreamEdgesByType must merge the overlay")

	// A buffered delete of a node also hides it from label scans.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:o0"))
	require.NoError(t, tx.Commit())

	nodes, err = engine.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, nodes, 4, "buffered delete must hide the node from label scans")

	require.NoError(t, engine.FlushWriteBehind())
	nodes, err = engine.GetNodesByLabel("Order")
	require.NoError(t, err)
	require.Len(t, nodes, 4, "delete persists through the flush")
}

func TestWriteBehind_ExplicitBeginDrainsBufferedWrites(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)

	// The storage layer's BeginTransaction does not drain (autocommit uses it);
	// the explicit-transaction admission path drains via FlushWriteBehind.
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.PendingOps())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode("test:n1")
	require.NoError(t, err)
	require.NotNil(t, node)
	require.NoError(t, tx.Rollback())
}

func TestWriteBehind_CloseDrainsAndPersists(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             dir,
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	})
	require.NoError(t, err)

	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}, Properties: map[string]any{"v": int64(7)}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)
	require.NoError(t, engine.Close())

	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(7), got.Properties["v"], "Close must persist acknowledged buffered writes")
}

// TestWriteBehind_DetachDeleteAcrossBatchesDeletesEdgesOnce reproduces the
// Northwind wipe/reseed corruption: a bounded DETACH DELETE wipe spans many
// statements, and each relationship is reachable from BOTH endpoints'
// adjacency indexes. Without the write-behind tombstone overlay, the second
// endpoint's batch re-deletes relationships the first batch already deleted
// (and re-applies their derived-count deltas), and the replay later grinds
// through tombstoned-head fallback iterators.
func TestWriteBehind_DetachDeleteAcrossBatchesDeletesEdgesOnce(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	// a -> b and b -> c: deleting a and b in separate buffered statements
	// reaches the a->b relationship from both endpoints.
	bufferedCreate(t, engine, &Node{ID: "test:a", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:b", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:c", Labels: []string{"N"}})
	for _, e := range []*Edge{
		{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"},
		{ID: "test:bc", StartNode: "test:b", EndNode: "test:c", Type: "R"},
	} {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		require.NoError(t, tx.CreateEdge(e))
		require.NoError(t, tx.Commit())
	}
	// Land the creates so the delete path reads committed records, exactly
	// like a wipe of previously persisted data.
	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, int64(2), mustEdgeCount(t, engine))

	// Wipe batch 1: delete a, which detaches a->b.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:a"))
	require.NoError(t, tx.Commit())

	// The buffered delete must hide the relationship immediately.
	require.True(t, engine.writeBehind.EdgeDeleted("test:ab"))
	require.Equal(t, int64(-1), engine.writeBehind.EdgeTypeCountDelta("R"))
	edges, err := engine.GetEdgesByType("R")
	require.NoError(t, err)
	require.Len(t, edges, 1, "buffered detach delete must hide the relationship from type scans")

	// Wipe batch 2: delete b. b->c goes, but a->b was already deleted and
	// must not be counted or replayed again.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:b"))
	require.NoError(t, tx.Commit())

	require.Equal(t, int64(-2), engine.writeBehind.EdgeTypeCountDelta("R"),
		"each relationship must be deleted exactly once across wipe batches")
	require.True(t, engine.writeBehind.EdgeDeleted("test:ab"))
	require.True(t, engine.writeBehind.EdgeDeleted("test:bc"))

	edges, err = engine.GetEdgesByType("R")
	require.NoError(t, err)
	require.Empty(t, edges, "both relationships hidden before the flush")

	// The flush lands both batches: counts must not underflow and the
	// type is empty.
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.LastErr())
	require.Zero(t, mustEdgeCount(t, engine), "wipe must not double-delete the cached total edge count")
	count, err := engine.EdgeCountByType("R")
	require.NoError(t, err)
	require.Zero(t, count, "wipe must not double-delete derived edge-type counts")
	edges, err = engine.GetEdgesByType("R")
	require.NoError(t, err)
	require.Empty(t, edges)

	// Reseed and confirm the round trip converges.
	bufferedCreate(t, engine, &Node{ID: "test:a2", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:b2", Labels: []string{"N"}})
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:a2b2", StartNode: "test:a2", EndNode: "test:b2", Type: "R"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, int64(1), mustEdgeCount(t, engine), "reseeded relationship counted exactly once")
	count, err = engine.EdgeCountByType("R")
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "reseeded relationship counted exactly once")
}

// mustEdgeCount returns the engine's cached total edge count.
func mustEdgeCount(t *testing.T, engine *BadgerEngine) int64 {
	t.Helper()
	n, err := engine.EdgeCount()
	require.NoError(t, err)
	return n
}

// TestWriteBehind_DetachDeleteAfterFlushStillDeletesOnce covers the
// interleaving the Northwind wipe hits when the flusher lands a wipe batch's
// generation before the next bounded batch runs: the second batch must see
// the already-applied relationship as gone through Badger, not re-delete it.
func TestWriteBehind_DetachDeleteAfterFlushStillDeletesOnce(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	bufferedCreate(t, engine, &Node{ID: "test:a", Labels: []string{"N"}})
	bufferedCreate(t, engine, &Node{ID: "test:b", Labels: []string{"N"}})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind())

	// Wipe batch 1: delete a (detaches a->b). Flush immediately: the
	// generation is applied and removed from the buffer's overlay.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:a"))
	require.NoError(t, tx.Commit())
	require.NoError(t, engine.FlushWriteBehind())

	// Wipe batch 2: delete b. The relationship is already tombstoned in
	// Badger; the batch must not delete it again.
	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:b"))
	require.NoError(t, tx.Commit())
	require.Equal(t, int64(0), engine.writeBehind.EdgeTypeCountDelta("R"),
		"already-applied relationship must not be deleted twice")
	require.NoError(t, engine.FlushWriteBehind())
	require.Zero(t, engine.writeBehind.LastErr())
	require.Zero(t, mustEdgeCount(t, engine))
	count, err := engine.EdgeCountByType("R")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestWriteBehind_DisabledCommitIsSynchronous(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	require.Nil(t, engine.writeBehind)
	bufferedCreate(t, engine, &Node{ID: "test:n1", Labels: []string{"L"}})

	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "disabled engine commits synchronously")
}
