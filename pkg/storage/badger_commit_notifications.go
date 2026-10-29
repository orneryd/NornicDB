package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"context"
	"sync"
)

// A commit reserves its notification position while publishing, then becomes
// ready after storage locks and the database's quota/accounting lease release.
// A reentrant write enqueues behind its caller instead of waiting for itself.
type commitNotification struct {
	ops []Operation
	// reset, when set, reports a DeleteByPrefix of this prefix instead of ops.
	reset *string
	// consumed, when set, is closed once the notification was dispatched.
	consumed chan struct{}
	// pooled marks a publication hold, returned to publicationHolds once it
	// was dispatched; its holder no longer refers to it after completing it.
	pooled   bool
	ready    bool
	reserved bool
	next     *commitNotification
}

// A direct event keeps its one-operation backing store with the queue entry.
type singleCommitNotification struct {
	commitNotification
	operation [1]Operation
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
	b.resetManagedEntityUsage(prefix)
	n := b.queueCommitOperations(nil, nil, notificationAwaited)
	if n == nil {
		return nil
	}
	n.reset = &prefix
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

// notificationAdmission selects when the canonical notification owner reserves
// its queue position. Physical writes reserve under timestamp assignment.
type notificationAdmission uint8

const (
	notificationImmediate notificationAdmission = iota
	notificationAtCommit
	// notificationAwaited reserves an immediate fence with one completion owner.
	notificationAwaited
)

// queueCommitOperations owns payload creation, queue admission and direct
// completion. At-commit payloads are reserved by the commit writer and become
// ready only after their caller releases storage and accounting locks.
func (b *BadgerEngine) queueCommitOperations(ops []Operation, direct *Operation, admission notificationAdmission) *commitNotification {
	// Immediate notifications describe an already visible mutation. Keep its
	// revision and deletion cleanup in the same canonical operation owner.
	// At-commit writes perform these effects inside ordered publication.
	if admission == notificationImmediate || admission == notificationAwaited {
		if direct != nil {
			b.prepareCommitOperations([]Operation{*direct}, false)
		} else {
			b.prepareCommitOperations(ops, false)
		}
	}
	var n *commitNotification
	if direct != nil {
		if !b.hasCommitListeners() && !hasEmbeddingOutcome([]Operation{*direct}) {
			return nil
		}
		operation := *direct
		operation.snapshotForNotification()
		single := &singleCommitNotification{operation: [1]Operation{operation}}
		n = &single.commitNotification
		ops = single.operation[:]
	}
	if !b.hasCommitListeners() && !hasEmbeddingOutcome(ops) {
		return nil
	}
	if n == nil {
		n = &commitNotification{}
	}
	n.ops = ops
	if admission == notificationAwaited {
		n.consumed = make(chan struct{})
	}
	if admission == notificationImmediate || admission == notificationAwaited {
		b.reserveCommitNotification(n)
		if direct != nil {
			b.completeCommitNotification(n)
		}
	}
	return n
}

// holdPublication reserves a place in the notification order that delivers
// nothing; the notifications queued behind it are delivered once it is
// completed. A write ordered after the write-behind buffer holds one from its
// write barrier until it released every lock it took, and a replay holds one
// while it publishes the commits it applied, so no listener runs on a
// goroutine that holds a write-order stripe or the commit gate, or that such
// a goroutine waits for. Without write-behind there is nothing to hold.
func (b *BadgerEngine) holdPublication() *commitNotification {
	if b.writeBehind == nil {
		return nil
	}
	n := publicationHolds.Get().(*commitNotification)
	n.pooled = true
	return b.queueCommitNotification(n)
}

var publicationHolds = sync.Pool{New: func() any { return new(commitNotification) }}

func (b *BadgerEngine) queueCommitNotification(n *commitNotification) *commitNotification {
	b.reserveCommitNotification(n)
	return n
}

func (b *BadgerEngine) reserveCommitNotification(n *commitNotification) {
	if n == nil {
		return
	}
	b.notificationMu.Lock()
	if n.reserved {
		b.notificationMu.Unlock()
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
	b.notificationMu.Unlock()
}

func hasEmbeddingOutcome(ops []Operation) bool {
	for _, op := range ops {
		if op.embeddingOutcome {
			return true
		}
	}
	return false
}

func (b *BadgerEngine) prepareCommitOperations(ops []Operation, snapshot bool, revisions ...bool) {
	publishRevision := len(revisions) == 0 || revisions[0]
	for i := range ops {
		op := &ops[i]
		if snapshot {
			op.snapshotForNotification()
		}
		switch op.Type {
		case OpCreateNode, OpUpdateNode:
			if publishRevision {
				b.graphMutationVersions.changed(namespaceForNodeID(op.Node.ID))
			}
		case OpDeleteNode:
			if publishRevision {
				b.graphMutationVersions.changed(namespaceForNodeID(op.NodeID))
			}
			b.forgetManualSuppression(manualSuppressionNode, string(op.NodeID))
			b.deleteCleanup.recordDelete()
			for _, id := range op.DeletedEdgeIDs {
				if publishRevision {
					b.graphMutationVersions.changed(namespaceForEdgeID(id))
				}
				b.forgetManualSuppression(manualSuppressionEdge, string(id))
				b.deleteCleanup.recordDelete()
			}
		case OpCreateEdge, OpUpdateEdge:
			if publishRevision {
				b.graphMutationVersions.changed(namespaceForEdgeID(op.Edge.ID))
			}
		case OpDeleteEdge:
			if publishRevision {
				b.graphMutationVersions.changed(namespaceForEdgeID(op.EdgeID))
			}
			b.forgetManualSuppression(manualSuppressionEdge, string(op.EdgeID))
			b.deleteCleanup.recordDelete()
		}
	}
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
	b.notificationMu.Lock()
	if !n.reserved {
		b.notificationMu.Unlock()
		return false
	}
	n.ready = true
	b.notificationMu.Unlock()
	return true
}

// drainCommitNotifications delivers the ready notifications at the head of
// the order, unless another goroutine is delivering them already.
// commitNotificationContext is the ordered drainer's context, its one owner.
// A drainer publishes the queued notifications of any commit, not only its
// caller's, and publication is not cancellable by contract, so no request
// context applies. Drain-path work takes this value, never a fresh context.
func commitNotificationContext() context.Context { return context.Background() }

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
			deliveringToListeners()
			if n.reset != nil {
				b.dispatchNamespaceReset(*n.reset)
			} else {
				for _, op := range n.ops {
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
		}()
		if n.pooled {
			*n = commitNotification{}
			publicationHolds.Put(n)
		}
		b.notificationMu.Lock()
	}
}
