package storage

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

// readTransactionEntity owns the native point-read lifecycle and overlay order.
// Typed decoders retain node hydration/projection and relationship semantics;
// neither state maps nor post-read filtering escape the operation lock.
func readTransactionEntity[ID ~string, T Node | Edge, P any](tx *BadgerTransaction, id ID, state func(*BadgerTransaction) transactionEntityState[ID, T], clone func(*T, P) *T, load func(*BadgerTransaction, ID, P) (*T, error), finish func(*BadgerTransaction, *T, P, error) (*T, error), policy P) (*T, error) {
	tx.mu.Lock()
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
		entity, err = load(tx, id, policy)
	}
	if finish != nil {
		return finish(tx, entity, policy, err)
	}
	return entity, err
}

type transactionNodeRead struct {
	mvccEntityRead
	properties []string
	projected  bool
}

func cloneTransactionNode(node *Node, policy transactionNodeRead) *Node {
	if policy.projected {
		return copyShaped(node, policy.properties, true)
	}
	return copyNode(node)
}
func loadTransactionNode(tx *BadgerTransaction, id NodeID, policy transactionNodeRead) (*Node, error) {
	if policy.projected {
		return tx.getCommittedNodeProjectedLocked(id, policy.properties)
	}
	return tx.getCommittedNodeReadLocked(id, policy.vectors, policy.visibility)
}
func cloneTransactionEdge(edge *Edge, _ readVisibility) *Edge { return copyEdge(edge) }
