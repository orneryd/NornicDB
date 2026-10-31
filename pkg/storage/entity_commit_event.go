package storage

// EntityCommitEvent is the origin-bearing view of the existing notification
// envelope. It is not a second event stream or a physical-completion receipt.
type EntityCommitEvent struct {
	Operation        Operation
	VectorWorkOrigin uint64
}

// EntityCommitCallback subscribes through the existing typed dispatch owners.
type EntityCommitCallback func(EntityCommitEvent)

type commitEventCallback[Payload any] func(Payload, uint64)

// subscribeEntityCommit adapts an envelope consumer at registration, into the
// SAME six typed callback lists used by OnNode/Edge*. Each operation is still
// delivered once by #1406's ordered drainer and typed dispatcher. Legacy
// registrations ignore the new metadata; no dispatcher or queue is duplicated.
func (b *BadgerEngine) subscribeEntityCommit(callback EntityCommitCallback) {
	b.callbackMu.Lock()
	defer b.callbackMu.Unlock()
	b.onNodeCreated = append(b.onNodeCreated, func(n *Node, origin uint64) {
		callback(EntityCommitEvent{Operation: Operation{Type: OpCreateNode, Node: n}, VectorWorkOrigin: origin})
	})
	b.onNodeUpdated = append(b.onNodeUpdated, func(n *Node, origin uint64) {
		callback(EntityCommitEvent{Operation: Operation{Type: OpUpdateNode, Node: n}, VectorWorkOrigin: origin})
	})
	b.onNodeDeleted = append(b.onNodeDeleted, func(id NodeID, origin uint64) {
		callback(EntityCommitEvent{Operation: Operation{Type: OpDeleteNode, NodeID: id}, VectorWorkOrigin: origin})
	})
	b.onEdgeCreated = append(b.onEdgeCreated, func(e *Edge, origin uint64) {
		callback(EntityCommitEvent{Operation: Operation{Type: OpCreateEdge, Edge: e}, VectorWorkOrigin: origin})
	})
	b.onEdgeUpdated = append(b.onEdgeUpdated, func(e *Edge, origin uint64) {
		callback(EntityCommitEvent{Operation: Operation{Type: OpUpdateEdge, Edge: e}, VectorWorkOrigin: origin})
	})
	b.onEdgeDeleted = append(b.onEdgeDeleted, func(id EdgeID, origin uint64) {
		callback(EntityCommitEvent{Operation: Operation{Type: OpDeleteEdge, EdgeID: id}, VectorWorkOrigin: origin})
	})
}

func (op Operation) entityEventID() string {
	switch op.Type {
	case OpCreateNode, OpUpdateNode:
		if op.Node != nil {
			return string(op.Node.ID)
		}
	case OpDeleteNode:
		return string(op.NodeID)
	case OpCreateEdge, OpUpdateEdge:
		if op.Edge != nil {
			return string(op.Edge.ID)
		}
	case OpDeleteEdge:
		return string(op.EdgeID)
	}
	return ""
}
