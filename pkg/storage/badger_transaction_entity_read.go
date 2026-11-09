package storage

import "context"

// transactionEntityState is selected only while tx.mu is held. Passing maps
// before taking that lock would race with transaction completion.
type transactionEntityState[ID ~string, T Node | Edge] struct {
	connected map[ID]*T
	pending   map[ID]*T
	deleted   map[ID]struct{}
}

func (tx *BadgerTransaction) nodeReadState() transactionEntityState[NodeID, Node] {
	return transactionEntityState[NodeID, Node]{connected: tx.connectedDeletes, pending: tx.pendingNodes, deleted: tx.deletedNodes}
}
func (tx *BadgerTransaction) edgeReadState() transactionEntityState[EdgeID, Edge] {
	return transactionEntityState[EdgeID, Edge]{pending: tx.pendingEdges, deleted: tx.deletedEdges}
}

// transactionPointPolicy is the one visibility contract a native point read
// carries into readTransactionEntity. A node read's policy exposes its existing
// visibility field; a bare readVisibility is its own policy. Both are small
// value types, so no boxed policy object is built per read and the completion
// step stays a typed method call rather than a runtime type switch.
type transactionPointPolicy interface {
	pointVisibility() readVisibility
}

// pointVisibility exposes a bare visibility policy as itself.
func (v readVisibility) pointVisibility() readVisibility { return v }

// pointVisibility exposes the node read's existing visibility.
func (p transactionNodeRead) pointVisibility() readVisibility { return p.visibility }

// readTransactionEntity owns the native point-read lifecycle, the overlay
// order and the one visible/stored completion decision. Typed decoders retain
// node hydration/projection and relationship semantics; neither state maps nor
// post-read filtering escape the operation lock.
func readTransactionEntity[ID ~string, T Node | Edge, P transactionPointPolicy](ctx context.Context, tx *BadgerTransaction, id ID, state func(*BadgerTransaction) transactionEntityState[ID, T], clone func(*T, P) *T, load func(context.Context, *BadgerTransaction, ID, P) (*T, error), policy P) (*T, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, err
	}
	if err := tx.pinNamespaceFromIDLocked(string(id)); err != nil {
		return nil, err
	}
	owned := state(tx)
	var entity *T
	var err error
	if connected, ok := owned.connected[id]; ok {
		entity = clone(connected, policy)
	} else if _, deleted := owned.deleted[id]; deleted {
		return nil, ErrNotFound
	} else if pending, ok := owned.pending[id]; ok {
		entity = clone(pending, policy)
	} else {
		entity, err = load(ctx, tx, id, policy)
	}
	if err != nil {
		return entity, err
	}
	// Live visibility completion, decided once for every source: overlay images
	// (pending writes, connected deletes) and cached snapshots never reach a
	// loader, so this read exit applies the same filter the committed loaders
	// apply. A stored policy keeps its intentional bypass inside that filter.
	// Stored entity reads deliberately bypass visibility. With no active
	// policy, avoid reading the scoring clock for every point lookup.
	if policy.pointVisibility() == visibleRead && tx.engine.visibilityFiltered() &&
		tx.engine.filterEntityByDecay(visibleRead, entity, DecayScoringTime()) {
		return nil, ErrNotFound
	}
	return entity, nil
}

type transactionNodeRead struct {
	mvccEntityRead
	properties []string
	projected  bool
}

func cloneTransactionNode(node *Node, policy transactionNodeRead) *Node {
	if policy.projected {
		return projectCachedNodeForRead(node, policy.properties, true)
	}
	return copyNode(node)
}
func loadTransactionNode(ctx context.Context, tx *BadgerTransaction, id NodeID, policy transactionNodeRead) (*Node, error) {
	if policy.projected {
		return tx.getCommittedNodeProjectedLocked(ctx, id, policy.properties)
	}
	return tx.getCommittedNodeReadLocked(ctx, id, policy.vectors, policy.visibility)
}
func cloneTransactionEdge(edge *Edge, _ readVisibility) *Edge { return copyEdge(edge) }

// loadTransactionEdge binds the committed edge read as a transaction entity
// loader, so the caller's context reaches the underlying read.
func loadTransactionEdge(ctx context.Context, tx *BadgerTransaction, id EdgeID, v readVisibility) (*Edge, error) {
	return tx.getCommittedEdgeReadLocked(ctx, id, v)
}
