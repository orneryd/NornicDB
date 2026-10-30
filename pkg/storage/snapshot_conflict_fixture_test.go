package storage

import (
	"testing"
)

// newSnapshotConflictEngine builds the on-disk engine plus the two edge
// endpoints every error-classification case in this file starts from.
func newSnapshotConflictEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine := newIsolatedBadgerEngine(t)
	createEdgeEndpoints(t, engine)
	return engine
}

// snapshotEdgeWriteCase declares one peer history. Every case keeps its own
// error-classification assertions; only the peer action, the pinned transaction
// and the write attempt are shared.
type snapshotEdgeWriteCase struct {
	// peer performs the writes that land after the transaction's begin
	// snapshot has been pinned.
	peer func(t *testing.T, engine *BadgerEngine)
	// edgeID is the edge the transaction then tries to update.
	edgeID string
}

// run applies the declared peer history and returns the UpdateEdge error from a
// transaction pinned before that history.
func (c snapshotEdgeWriteCase) run(t *testing.T) error {
	t.Helper()
	engine := newSnapshotConflictEngine(t)
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })

	c.peer(t, engine)

	return tx.UpdateEdge(EdgeUpdateRequest{After: &Edge{
		ID: EdgeID(c.edgeID), StartNode: "test:a", EndNode: "test:b", Type: "REL",
		Properties: map[string]interface{}{"x": "tx1"},
	}})
}
