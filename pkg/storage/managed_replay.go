package storage

import "errors"

// Replay uses one transaction for the entire command. The operations are the
// storage owner's final physical operations, including cascade relationship
// deletes and the replacement flag; they are not independently committed CRUD.
func (m *ManagedWriteTransaction) stageReplay(op Operation) error {
	tx := m.BadgerTransaction
	ns := NewNamespacedEngine(tx.engine, tx.namespace)
	op = namespacedManagedOperation(ns, op)
	if op.Node != nil {
		if err := ns.GetSchema().RegisterTokens(op.Node.Labels, nil); err != nil {
			return err
		}
	}
	if op.Edge != nil {
		if err := ns.GetSchema().RegisterTokens(nil, []string{op.Edge.Type}); err != nil {
			return err
		}
	}
	switch op.Type {
	case OpCreateNode:
		_, err := tx.CreateNode(op.Node)
		return err
	case OpUpdateNode:
		return upsertNodeInTransaction(tx, op.Node)
	case OpDeleteNode:
		return tx.DeleteNode(op.NodeID)
	case OpCreateEdge:
		if op.ReplacesExisting {
			// The proposal already classified the stored record as a physical
			// replacement. A peer's local visibility policy is not admission.
			err := tx.UpdateEdge(op.Edge)
			if errors.Is(err, ErrNotFound) {
				return tx.CreateEdge(op.Edge)
			}
			if err == nil {
				tx.mu.Lock()
				operation := &tx.operations[len(tx.operations)-1]
				operation.Type, operation.ReplacesExisting = OpCreateEdge, true
				tx.mu.Unlock()
			}
			return err
		}
		return tx.CreateEdge(op.Edge)
	case OpUpdateEdge:
		return tx.UpdateEdge(op.Edge)
	case OpDeleteEdge:
		return tx.DeleteEdge(op.EdgeID)
	default:
		return ErrNotImplemented
	}
}
