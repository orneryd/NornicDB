package storage

// snapshotForNotification takes ownership of a bound embedding outcome. Its
// writer waits for delivery before returning, so the accepted private snapshot
// remains immutable for the queue, listeners and worker callback. Other public
// writes may return while delivery is queued and must detach their payloads.
func (op *Operation) snapshotForNotification() {
	if op.Node != nil && !(op.embeddingOutcome && op.Node.EmbeddingAttempt() != nil) {
		op.Node = copyNode(op.Node)
	}
	if op.Edge != nil {
		op.Edge = copyEdge(op.Edge)
	}
}

// bindVectorWorkOrigin is the sole writer of an operation admission identity.
// The operation value must be owned by its staging/notification frame. Its
// payload pointers may already be shared with readers or a retained commit:
// binding an envelope never mutates those bodies. Body provenance belongs to
// the buffered handover or the decoded read snapshot, before either is exposed.
func (op *Operation) bindVectorWorkOrigin(origin uint64) {
	if !op.vectorOriginBound {
		op.vectorWorkOrigin, op.vectorOriginBound = origin, true
	}
}
