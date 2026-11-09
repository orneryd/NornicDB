package storage

import "slices"

// snapshotForNotification takes ownership of a bound embedding outcome. Its
// writer waits for delivery before returning, so the accepted private snapshot
// remains immutable for the queue, listeners and worker callback. Other public
// writes may return while delivery is queued and must detach their payloads.
func (op *Operation) snapshotForNotification() {
	op.DeletedEdgeIDs = slices.Clone(op.DeletedEdgeIDs)
	op.DeletedEdgeFacts = slices.Clone(op.DeletedEdgeFacts)
	op.OldNode = copyNode(op.OldNode)
	op.OldEdge = copyEdge(op.OldEdge)
	if op.Node != nil && !(op.embeddingOutcome && op.Node.EmbeddingAttempt() != nil) {
		op.Node = copyNode(op.Node)
	}
	if op.Edge != nil {
		op.Edge = copyEdge(op.Edge)
	}
}
