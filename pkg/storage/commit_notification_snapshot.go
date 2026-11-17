package storage

import "slices"

// snapshotForNotification takes ownership of a bound embedding outcome. Its
// writer waits for delivery before returning, so the accepted private snapshot
// remains immutable for the queue, listeners and worker callback. Other public
// writes may return while delivery is queued and must detach their payloads.
func (op *Operation) snapshotForNotification() {
	op.DeletedEdgeIDs = slices.Clone(op.DeletedEdgeIDs)
	// Listener delivery consumes new images and deleted identities only.
	// Rollback images and derived facts remain owned by the commit frame.
	op.DeletedEdgeFacts = nil
	op.OldNode = nil
	op.OldEdge = nil
	if op.Node != nil && !(op.embeddingOutcome && op.Node.EmbeddingAttempt() != nil) {
		op.Node = copyNode(op.Node)
	}
	if op.Edge != nil {
		op.Edge = copyEdge(op.Edge)
	}
}
