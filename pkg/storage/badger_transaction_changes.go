package storage

// TransactionChanges describes the final staged entities, using physical,
// namespace-prefixed IDs. A nil value means deletion. Operations counts the
// buffered storage operations (the same count as OperationCount).
type TransactionChanges struct {
	Namespace  string
	Nodes      map[NodeID]*Node
	Edges      map[EdgeID]*Edge
	Operations int
	// HoldsMergeKey means admission must not wait behind a writer that could
	// need this transaction's unique MERGE key lock.
	HoldsMergeKey bool
}

// PendingChanges returns a private copy for commit admission and accounting.
// With includeEntities=false it returns only the allocation-free admission
// summary; request entities only when enforcing capacity or tracking bytes.
// For example, a database quota wrapper can compare these final entities with
// committed storage before calling CommitContext. Callers must exclusively own
// the transaction until that commit completes.
func (tx *BadgerTransaction) PendingChanges(includeEntities bool) TransactionChanges {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.pendingChangesLocked(includeEntities)
}

func (tx *BadgerTransaction) pendingChangesLocked(includeEntities bool) TransactionChanges {
	changes := TransactionChanges{
		Namespace:     tx.namespace,
		Operations:    len(tx.operations) + len(tx.connectedDeletes),
		HoldsMergeKey: tx.mergeKeyRelease != nil,
	}
	if !includeEntities {
		return changes
	}
	changes.Nodes = make(map[NodeID]*Node, len(tx.pendingNodes)+len(tx.deletedNodes)+len(tx.connectedDeletes))
	changes.Edges = make(map[EdgeID]*Edge, len(tx.pendingEdges)+len(tx.deletedEdges))
	for id, node := range tx.pendingNodes {
		changes.Nodes[id] = copyNode(node)
	}
	for id := range tx.deletedNodes {
		changes.Nodes[id] = nil
	}
	for id := range tx.connectedDeletes {
		changes.Nodes[id] = nil
	}
	for id, edge := range tx.pendingEdges {
		changes.Edges[id] = copyEdge(edge)
	}
	for id := range tx.deletedEdges {
		changes.Edges[id] = nil
	}
	return changes
}
