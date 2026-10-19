package storage

// commitNotification is reserved in publication order, before the writer can
// be overtaken. It becomes ready only after its caches and publication locks
// are released. A buffered ACK reserves its place until replay retires the
// read overlay. Failed commits make their reserved place ready without ops.
type commitNotification struct {
	ops             []Operation
	ready, reserved bool
	// Buffered commits advance the logical revision at ACK, not again on flush.
	revisionPublished bool
	next              *commitNotification
}

func (b *BadgerEngine) reserveCommitNotification(n *commitNotification) {
	if n == nil {
		return
	}
	b.notificationMu.Lock()
	defer b.notificationMu.Unlock()
	if n.reserved {
		return
	}
	n.reserved = true
	b.notifyWG.Add(1)
	if b.notificationTail == nil {
		b.notificationHead = n
	} else {
		b.notificationTail.next = n
	}
	b.notificationTail = n
}

// beginNotifyingWrite gives nodes and edges the same lifetime transfer. The
// returned finish must be deferred before taking any other publication locks.
func (b *BadgerEngine) beginNotifyingWrite() (*commitNotification, func(), error) {
	release, err := b.beginWrite()
	if err != nil {
		return nil, nil, err
	}
	n := &commitNotification{}
	return n, func() { release(); b.completeCommitNotification(n) }, nil
}

func (b *BadgerEngine) completeCommitNotification(n *commitNotification) {
	if b.readyCommitNotification(n) {
		b.drainCommitNotifications()
	}
}

func (b *BadgerEngine) readyCommitNotification(n *commitNotification) bool {
	if n == nil {
		return false
	}
	b.publishNamespaceCatalogOperations(n.ops)
	// Mutation bookkeeping belongs to publication, even if a busy listener
	// delays delivery. Keep event payloads independent of subsequent caller edits.
	for i := range n.ops {
		op := &n.ops[i]
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if !n.revisionPublished {
				b.graphMutationVersions.changed(namespaceForNodeID(op.Node.ID))
			}
			op.Node = copyNode(op.Node)
		case OpDeleteNode:
			for _, id := range op.DeletedEdgeIDs {
				b.recordEdgeDeletion(id, !n.revisionPublished)
			}
			if !n.revisionPublished {
				b.graphMutationVersions.changed(namespaceForNodeID(op.NodeID))
			}
			b.deleteCleanup.recordDelete()
		case OpCreateEdge, OpUpdateEdge:
			if !n.revisionPublished {
				b.graphMutationVersions.changed(namespaceForEdgeID(op.Edge.ID))
			}
			op.Edge = copyEdge(op.Edge)
		case OpDeleteEdge:
			b.recordEdgeDeletion(op.EdgeID, !n.revisionPublished)
		}
	}
	b.notificationMu.Lock()
	reserved := n.reserved
	if reserved {
		n.ready = true
	}
	b.notificationMu.Unlock()
	return reserved
}

func (b *BadgerEngine) recordEdgeDeletion(id EdgeID, publishRevision bool) {
	if publishRevision {
		b.graphMutationVersions.changed(namespaceForEdgeID(id))
	}
	b.deleteCleanup.recordDelete()
}

// One drainer delivers every subscriber of a commit before the next commit.
// Reentrant writes/flushes only enqueue: waiting for their own notification
// here would deadlock the callback that the drainer is currently executing.
// The next ready writer resumes a drain stopped at an unfinished publication.
func (b *BadgerEngine) drainCommitNotifications() {
	b.notificationMu.Lock()
	if b.notificationDraining {
		b.notificationMu.Unlock()
		return
	}
	b.notificationDraining = true
	var firstPanic any
	for {
		n := b.notificationHead
		if n == nil || !n.ready {
			b.notificationDraining = false
			b.notificationMu.Unlock()
			if firstPanic != nil {
				panic(firstPanic)
			}
			return
		}
		b.notificationHead = n.next
		n.next = nil
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
			b.notifyCommitOperations(n.ops)
		}()
		b.notificationMu.Lock()
	}
}

// notifyCommitOperations is called only by the ordered drainer, outside locks.
func (b *BadgerEngine) notifyCommitOperations(ops []Operation) {
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
