// Unit tests for the edge-update snapshot conflict classification bug.
//
// BUG: BadgerTransaction.UpdateEdge resolved the target edge through the
// BEGIN-time snapshot (getCommittedEdgeLocked -> GetEdgeVisibleAt) and mapped
// ErrNotVisibleAtSnapshot to ErrNotFound. When a peer transaction committed
// the same edge AFTER this transaction began, the edge was live at
// latest-committed state but invisible at the snapshot, so the update failed
// with a hard "not found" instead of a retryable write-write conflict.
// Neo4j never surfaces this interleaving as "not found": MERGE takes
// relationship locks and re-reads after locking, so the second writer blocks
// and then succeeds. The closest NornicDB equivalent is the existing
// commit-time classification (checkEdgeWriteConflict), which already returns
// ErrConflict with the pinned "edge %s changed after transaction start"
// wire shape. UpdateEdge must classify the same way.
package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// beginPinnedTransaction begins a transaction and pins it to the test
// namespace immediately so tx.readTS is bound to the BEGIN-time snapshot
// before any peer activity. Pinning happens lazily on the first ID-bearing
// operation; reading a known-absent node is a cheap way to force it.
func beginPinnedTransaction(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.GetNode("test:pin-probe")
	require.ErrorIs(t, err, ErrNotFound)
	return tx
}

func createEdgeEndpoints(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
}

// TestBug_UpdateEdgePeerCommittedAfterBeginIsConflictNotNotFound is the core
// regression: a peer commits an edge after tx1 begins; tx1's UpdateEdge on
// that edge must fail with the retryable ErrConflict ("edge ... changed after
// transaction start"), NOT ErrNotFound.
func TestBug_UpdateEdgePeerCommittedAfterBeginIsConflictNotNotFound(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	createEdgeEndpoints(t, engine)

	tx1 := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx1.Rollback() })

	// Peer commits the edge AFTER tx1's begin snapshot.
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
		Properties: map[string]interface{}{"x": "peer"},
	}))

	// tx1 found the edge via a latest-committed lookup (the MERGE
	// relationship-resolution path) and now tries to update it.
	err = tx1.UpdateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
		Properties: map[string]interface{}{"x": "tx1"},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict,
		"snapshot-invisible but live edge must be a retryable conflict, got: %v", err)
	require.NotErrorIs(t, err, ErrNotFound)
	require.Contains(t, err.Error(), "changed after transaction start",
		"conflict must carry the pinned wire-contract substring")
}

// TestUpdateEdgePeerDeletedEdgeStillNotFound guards the genuine-delete case:
// when the peer created AND deleted the edge after tx1 began, the edge is
// absent (tombstoned) at latest-committed state, so ErrNotFound remains the
// correct answer — the reclassification must be tombstone-aware.
func TestUpdateEdgePeerDeletedEdgeStillNotFound(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	createEdgeEndpoints(t, engine)

	tx1 := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx1.Rollback() })

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}))
	require.NoError(t, engine.DeleteEdge("test:e1"))

	// Latest-committed state is a tombstone: not a conflict, just gone.
	err = tx1.UpdateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
		Properties: map[string]interface{}{"x": "tx1"},
	})
	require.ErrorIs(t, err, ErrNotFound)
	require.NotErrorIs(t, err, ErrConflict)
}

// TestUpdateEdgeMissingEdgeStillNotFound pins the trivial case: an edge that
// never existed anywhere keeps returning ErrNotFound.
func TestUpdateEdgeMissingEdgeStillNotFound(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	createEdgeEndpoints(t, engine)

	tx1 := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx1.Rollback() })

	err = tx1.UpdateEdge(&Edge{
		ID: "test:never-existed", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	})
	require.ErrorIs(t, err, ErrNotFound)
}

// TestGetEdgeSnapshotReadStaysNotFound pins the READ path: GetEdge must keep
// hiding peer commits that landed after this transaction began (snapshot
// isolation). Only the write path reclassifies; reads are untouched.
func TestGetEdgeSnapshotReadStaysNotFound(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	createEdgeEndpoints(t, engine)

	tx1 := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx1.Rollback() })

	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}))

	_, err = tx1.GetEdge("test:e1")
	require.ErrorIs(t, err, ErrNotFound,
		"snapshot reads must not observe peer commits made after begin")
	require.NotErrorIs(t, err, ErrConflict)
}

// TestGetCommittedEdgeForUpdateLockedZeroReadTSDelegates pins the legacy
// pre-pin path: with no namespace pinned yet (readTS zero) the write-side
// resolver must delegate to the plain committed read and see the edge.
func TestGetCommittedEdgeForUpdateLockedZeroReadTSDelegates(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	createEdgeEndpoints(t, engine)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL",
	}))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// Force the legacy zero-readTS state (pre-MVCC transactions and
	// manually constructed transactions in older codepaths).
	tx.readTS = MVCCVersion{}
	edge, err := tx.getCommittedEdgeForUpdateLocked("test:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e1"), edge.ID)
}

// BenchmarkTransaction_UpdateEdge measures the UpdateEdge success path (edge
// visible at the transaction's snapshot). The conflict reclassification only
// runs on the previously-failing ErrNotVisibleAtSnapshot path, so this
// benchmark pins the no-regression claim for the hot path.
func BenchmarkTransaction_UpdateEdge(b *testing.B) {
	engine, err := NewBadgerEngine(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"test:a", "test:b"} {
		if _, err := engine.CreateNode(&Node{ID: id, Labels: []string{"L"}}); err != nil {
			b.Fatal(err)
		}
	}
	if err := engine.CreateEdge(&Edge{
		ID: "test:bench-edge", StartNode: "test:a", EndNode: "test:b", Type: "REL",
		Properties: map[string]interface{}{"x": 0},
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := engine.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.UpdateEdge(&Edge{
			ID: "test:bench-edge", StartNode: "test:a", EndNode: "test:b", Type: "REL",
			Properties: map[string]interface{}{"x": i},
		}); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
