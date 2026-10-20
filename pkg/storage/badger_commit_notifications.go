package storage

// A commit reserves its notification position while publishing, then becomes
// ready after storage locks and the database's quota/accounting lease release.
// A reentrant write enqueues behind its caller instead of waiting for itself.
type commitNotification struct {
	ops []Operation
	// reset, when set, reports a DeleteByPrefix of this prefix instead of ops.
	reset *string
	// consumed, when set, is closed once the notification was dispatched.
	consumed chan struct{}
	ready    bool
	next     *commitNotification
}

func (b *BadgerEngine) hasCommitListeners() bool {
	b.callbackMu.RLock()
	defer b.callbackMu.RUnlock()
	return len(b.onNodeCreated)+len(b.onNodeUpdated)+len(b.onNodeDeleted)+len(b.onEdgeCreated)+len(b.onEdgeUpdated)+len(b.onEdgeDeleted)+len(b.onNamespaceReset) > 0
}

// publishNamespaceReset reports a DeleteByPrefix after it removed everything,
// behind the notifications already reserved, so a consumer drops its state for
// the namespace after the events that preceded the deletion. DeleteByPrefix
// awaits the returned notification (awaitCommitNotification) once its locks
// are released, so a database recreated after it returns never meets the
// dropped one's derived state.
func (b *BadgerEngine) publishNamespaceReset(prefix string) *commitNotification {
	n := b.reserveCommitOperations(nil)
	if n == nil {
		return nil
	}
	n.reset = &prefix
	n.consumed = make(chan struct{})
	b.completeCommitNotification(n)
	return n
}

// awaitCommitNotification returns once n was dispatched. A consumer that
// deleted a prefix from its own callback would wait on itself; none does.
func (b *BadgerEngine) awaitCommitNotification(n *commitNotification) {
	if n != nil && n.consumed != nil {
		<-n.consumed
	}
}

func (b *BadgerEngine) reserveCommitOperations(ops []Operation) *commitNotification {
	if !b.hasCommitListeners() && !hasEmbeddingOutcome(ops) {
		return nil
	}
	n := &commitNotification{ops: ops}
	b.notificationMu.Lock()
	b.notifyWG.Add(1)
	if b.notificationTail == nil {
		b.notificationHead = n
	} else {
		b.notificationTail.next = n
	}
	b.notificationTail = n
	b.notificationMu.Unlock()
	return n
}

func (b *BadgerEngine) enqueueCommitOperation(op Operation) {
	if !b.hasCommitListeners() && !hasEmbeddingOutcome([]Operation{op}) {
		return
	}
	op.snapshotForNotification()
	n := b.reserveCommitOperations([]Operation{op})
	b.completeCommitNotification(n)
}

func hasEmbeddingOutcome(ops []Operation) bool {
	for _, op := range ops {
		if op.embeddingOutcome {
			return true
		}
	}
	return false
}

func (b *BadgerEngine) prepareCommitOperations(ops []Operation, snapshot bool) {
	for i := range ops {
		op := &ops[i]
		if snapshot {
			op.snapshotForNotification()
		}
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			b.graphMutationVersions.changed(namespaceForNodeID(op.Node.ID))
		case OpDeleteNode:
			b.graphMutationVersions.changed(namespaceForNodeID(op.NodeID))
			b.forgetManualSuppression(manualSuppressionNode, string(op.NodeID))
			b.deleteCleanup.recordDelete()
			for _, id := range op.DeletedEdgeIDs {
				b.graphMutationVersions.changed(namespaceForEdgeID(id))
				b.forgetManualSuppression(manualSuppressionEdge, string(id))
				b.deleteCleanup.recordDelete()
			}
		case OpCreateEdge, OpUpdateEdge:
			b.graphMutationVersions.changed(namespaceForEdgeID(op.Edge.ID))
		case OpDeleteEdge:
			b.graphMutationVersions.changed(namespaceForEdgeID(op.EdgeID))
			b.forgetManualSuppression(manualSuppressionEdge, string(op.EdgeID))
			b.deleteCleanup.recordDelete()
		}
	}
}

func (b *BadgerEngine) completeCommitNotification(n *commitNotification) {
	if n == nil {
		return
	}
	b.notificationMu.Lock()
	n.ready = true
	if b.notificationDraining {
		b.notificationMu.Unlock()
		return
	}
	b.notificationDraining = true
	var firstPanic any
	for {
		n = b.notificationHead
		if n == nil || !n.ready {
			b.notificationDraining = false
			b.notificationMu.Unlock()
			if firstPanic != nil {
				panic(firstPanic)
			}
			return
		}
		b.notificationHead = n.next
		if b.notificationHead == nil {
			b.notificationTail = nil
		}
		b.notificationMu.Unlock()
		func() {
			defer b.notifyWG.Done()
			if n.consumed != nil {
				defer close(n.consumed)
			}
			defer func() {
				if p := recover(); p != nil && firstPanic == nil {
					firstPanic = p
				}
			}()
			if n.reset != nil {
				b.dispatchNamespaceReset(*n.reset)
			} else {
				b.dispatchCommitOperations(n.ops)
			}
		}()
		b.notificationMu.Lock()
	}
}

func (b *BadgerEngine) dispatchCommitOperations(ops []Operation) {
	for _, op := range ops {
		switch op.Type {
		case OpCreateNode:
			b.dispatchNodeCreated(op.Node)
		case OpUpdateNode:
			b.dispatchNodeUpdatedOutcome(op.Node, op.embeddingOutcome)
		case OpDeleteNode:
			for _, id := range op.DeletedEdgeIDs {
				b.dispatchEdgeDeleted(id)
			}
			b.dispatchNodeDeleted(op.NodeID)
		case OpCreateEdge:
			b.dispatchEdgeCreated(op.Edge)
		case OpUpdateEdge:
			b.dispatchEdgeUpdated(op.Edge)
		case OpDeleteEdge:
			b.dispatchEdgeDeleted(op.EdgeID)
		}
	}
}
