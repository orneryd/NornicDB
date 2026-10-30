package storage

import "github.com/dgraph-io/badger/v4"

// keyConstraintNodes captures the committed equality-index candidates and the
// pending key candidates together, then resolves bodies in the captured view.
// A replay cannot move a holder between those candidate sets during the capture.
func (b *BadgerEngine) keyConstraintNodes(idx *CompositeIndex, label, property string, values []interface{}) ([]*Node, error) {
	var ids []NodeID
	var pending []*Node
	var states map[NodeID]bufferedEntityState
	if w := b.writeBehind; w != nil {
		w.ensurePendingValueTracked(label, property)
	}
	var nodes []*Node
	err := b.withReadView(func(w *WriteBehindBuffer) {
		ids = idx.LookupFull(values...)
		if w == nil {
			return
		}
		if valueKey, ok := indexValueKey(values[0]); ok {
			pending = b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedKey, label: label, property: property, valueKey: valueKey}).nodes
		} else {
			pendingNodes := b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})
			pending, _ = pendingNodes.nodes, pendingNodes.nodesTouched
		}
		states = make(map[NodeID]bufferedEntityState, len(ids))
		for _, id := range ids {
			states[id] = w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
		}
	}, func(txn *badger.Txn) error {
		seen := make(map[NodeID]bool, len(ids)+len(pending))
		for _, id := range ids {
			state := states[id]
			if state.deleted {
				continue
			}
			// A buffered image is retained: copy it once. A committed node is
			// decoded for this read and handed over.
			var node *Node
			if state.found {
				node = copyNode(state.node)
			} else {
				var err error
				node, err = b.loadNodeForTemporalTxn(txn, id, false)
				if err != nil {
					return err
				}
			}
			if node != nil {
				nodes = append(nodes, node)
				seen[id] = true
			}
		}
		for _, node := range pending {
			if !seen[node.ID] {
				nodes = append(nodes, node)
			}
		}
		return nil
	})
	return nodes, err
}
