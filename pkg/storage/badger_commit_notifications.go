package storage

// A commit reserves its notification position while publishing, then becomes
// ready after storage locks and the database's quota/accounting lease release.
// A reentrant write enqueues behind its caller instead of waiting for itself.
type commitNotification struct {
	ops   []Operation
	ready bool
	next  *commitNotification
}

func (b *BadgerEngine) hasCommitListeners() bool {
	b.callbackMu.RLock()
	defer b.callbackMu.RUnlock()
	return len(b.onNodeCreated)+len(b.onNodeUpdated)+len(b.onNodeDeleted)+len(b.onEdgeCreated)+len(b.onEdgeUpdated)+len(b.onEdgeDeleted) > 0
}

func (b *BadgerEngine) reserveCommitOperations(ops []Operation) *commitNotification {
	if !b.hasCommitListeners() && !b.hasNamespaceCatalogUpdates(ops) {
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
	if !b.hasCommitListeners() {
		if op.Type == OpCreateNode || op.Type == OpUpdateNode {
			b.publishNamespaceCatalog(op.Node)
		}
		return
	}
	if op.Node != nil {
		op.Node = copyNode(op.Node)
	}
	if op.Edge != nil {
		op.Edge = copyEdge(op.Edge)
	}
	n := b.reserveCommitOperations([]Operation{op})
	b.completeCommitNotification(n)
}

func (b *BadgerEngine) prepareCommitOperations(ops []Operation, snapshot bool) {
	for i := range ops {
		op := &ops[i]
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			b.graphMutationVersions.changed(namespaceForNodeID(op.Node.ID))
			if snapshot {
				op.Node = copyNode(op.Node)
			}
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
			if snapshot {
				op.Edge = copyEdge(op.Edge)
			}
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
	b.publishNamespaceCatalogOperations(n.ops)
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
			defer func() {
				if p := recover(); p != nil && firstPanic == nil {
					firstPanic = p
				}
			}()
			b.dispatchCommitOperations(n.ops)
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
			b.dispatchNodeUpdated(op.Node)
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
