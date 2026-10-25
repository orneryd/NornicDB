package storage

// The dispatcher is the sole delivery owner. Publication reserves an immutable
// event image; publication completion and buffered retirement independently
// make it ready. Neither transition runs callbacks or consumes other events.
type commitNotification struct {
	ops       []Operation
	reset     *string
	consumed  chan struct{}
	published bool
	replay    bool
	ready     bool
	next      *commitNotification
}

func (b *BadgerEngine) hasCommitListeners() bool {
	b.callbackMu.RLock()
	defer b.callbackMu.RUnlock()
	return len(b.onNodeCreated)+len(b.onNodeUpdated)+len(b.onNodeDeleted)+len(b.onEdgeCreated)+len(b.onEdgeUpdated)+len(b.onEdgeDeleted)+len(b.onNamespaceReset) > 0
}

func (b *BadgerEngine) reserveCommitOperations(ops []Operation) *commitNotification {
	return b.reserveNotification(ops, false)
}

func (b *BadgerEngine) reserveNotification(ops []Operation, replay bool) *commitNotification {
	if b.beforeCommitReservation != nil {
		b.beforeCommitReservation(ops)
	}
	if len(ops) == 0 {
		return nil
	}
	if !b.hasCommitListeners() && !hasBoundEmbeddingOutcome(ops) {
		return nil
	}
	image := make([]Operation, len(ops))
	copy(image, ops)
	for i := range image {
		image[i].snapshotForNotification()
	}
	return b.queueCommitNotification(&commitNotification{ops: image, replay: replay, consumed: make(chan struct{})})
}

func (b *BadgerEngine) queueCommitNotification(n *commitNotification) *commitNotification {
	b.notificationMu.Lock()
	b.notifyWG.Add(1)
	if b.notificationTail == nil {
		b.notificationHead = n
	} else {
		b.notificationTail.next = n
	}
	b.notificationTail = n
	b.notificationLast = n
	b.notificationMu.Unlock()
	return n
}

func (b *BadgerEngine) enqueueCommitOperation(op Operation) {
	b.completeCommitNotification(b.reserveCommitOperations([]Operation{op}))
}

// Reset ordering is consumed only at an explicit external lifecycle barrier.
func (b *BadgerEngine) publishNamespaceReset(prefix string) *commitNotification {
	if !b.hasCommitListeners() {
		return nil
	}
	n := b.queueCommitNotification(&commitNotification{reset: &prefix, consumed: make(chan struct{})})
	b.completeCommitNotification(n)
	return n
}

func (b *BadgerEngine) completeCommitNotification(n *commitNotification) {
	b.readyCommitNotification(n)
}

func (b *BadgerEngine) readyCommitNotification(n *commitNotification) bool {
	if n == nil {
		return false
	}
	b.notificationMu.Lock()
	n.published = true
	wake := !n.ready && !n.replay
	if wake {
		n.ready = true
	}
	b.notificationMu.Unlock()
	if wake {
		b.signalCommitNotifications()
	}
	return wake
}

func (b *BadgerEngine) retireCommitNotification(n *commitNotification) {
	if n == nil {
		return
	}
	b.notificationMu.Lock()
	n.replay = false
	wake := !n.ready && n.published
	if wake {
		n.ready = true
	}
	b.notificationMu.Unlock()
	if wake {
		b.signalCommitNotifications()
	}
}

func (b *BadgerEngine) signalCommitNotifications() {
	select {
	case b.notificationWake <- struct{}{}:
	default:
	}
}

func (b *BadgerEngine) startCommitDispatcher() {
	b.notificationWake = make(chan struct{}, 1)
	b.notificationStop = make(chan struct{})
	b.notificationDone = make(chan struct{})
	go b.runCommitDispatcher()
}

func (b *BadgerEngine) runCommitDispatcher() {
	defer close(b.notificationDone)
	b.notificationOwner.Store(currentGoroutineID())
	defer b.notificationOwner.Store(0)
	for {
		select {
		case <-b.notificationStop:
			return
		case <-b.notificationWake:
			for {
				b.notificationMu.Lock()
				n := b.notificationHead
				if n == nil || !n.ready {
					b.notificationMu.Unlock()
					break
				}
				b.notificationHead = n.next
				if b.notificationHead == nil {
					b.notificationTail = nil
				}
				n.next = nil
				b.notificationMu.Unlock()
				if n.reset != nil {
					b.dispatchNamespaceReset(*n.reset)
				} else {
					b.dispatchCommitOperations(n.ops)
				}
				// No producer mutates an entry after readiness. Release its
				// detached image before publishing consumed completion.
				b.consumeCommitNotification(n)
				if b.writeBehind != nil {
					b.writeBehind.pruneConsumedFrontier()
				}
				b.notifyWG.Done()
			}
		}
	}
}

// Lifecycle actions attach to real events rather than allocating queue holds.
// Registration and consumption share notificationMu, including a popped event.
func (b *BadgerEngine) afterCommitNotification(n *commitNotification, after func()) {
	b.notificationMu.Lock()
	if n == nil || notificationConsumed(n) {
		b.notificationMu.Unlock()
		after()
		return
	}
	if b.notificationCompletions == nil {
		b.notificationCompletions = make(map[*commitNotification][]func())
	}
	b.notificationCompletions[n] = append(b.notificationCompletions[n], after)
	b.notificationMu.Unlock()
}

func (b *BadgerEngine) consumeCommitNotification(n *commitNotification) {
	for {
		b.notificationMu.Lock()
		actions := b.notificationCompletions[n]
		delete(b.notificationCompletions, n)
		if len(actions) == 0 {
			n.ops = nil
			close(n.consumed)
			b.notificationMu.Unlock()
			return
		}
		b.notificationMu.Unlock()
		for _, action := range actions {
			b.callCommitListener(action)
		}
	}
}

func (b *BadgerEngine) listenerFrontier() (*commitNotification, bool) {
	reentrant := b.onCommitDispatcher()
	b.notificationMu.Lock()
	last := b.notificationLast
	b.notificationMu.Unlock()
	return last, reentrant
}

// Only explicit barriers inspect dispatcher identity. Ordinary writes never do.
func (b *BadgerEngine) onCommitDispatcher() bool {
	owner := b.notificationOwner.Load()
	return owner != 0 && owner == currentGoroutineID()
}

// WaitForListeners waits through the finite frontier reserved on entry,
// including an event already popped by the dispatcher. A dispatcher callback
// cannot consume itself; it may land data but leaves consumption to its return.
func (b *BadgerEngine) WaitForListeners() {
	if b.onCommitDispatcher() {
		return
	}
	b.notificationMu.Lock()
	last := b.notificationLast
	b.notificationMu.Unlock()
	if last != nil {
		<-last.consumed
	}
}

func (b *BadgerEngine) awaitCommitNotification(n *commitNotification) {
	if n != nil && !b.onCommitDispatcher() {
		<-n.consumed
	}
}

func (b *BadgerEngine) callCommitListener(call func()) {
	defer func() {
		if p := recover(); p != nil && b.log != nil {
			b.log.Error("storage listener panicked", "subsystem", "storage", "panic", p)
		}
	}()
	call()
}

func hasBoundEmbeddingOutcome(ops []Operation) bool {
	for _, op := range ops {
		if op.embeddingOutcome && op.Node != nil && op.Node.embeddingAttempt != nil {
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

func notificationConsumed(n *commitNotification) bool {
	select {
	case <-n.consumed:
		return true
	default:
		return false
	}
}
