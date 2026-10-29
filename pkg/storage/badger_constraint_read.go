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
		var captured bufferedEntityResult
		if valueKey, ok := indexValueKey(values[0]); ok {
			captured = b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedKey, label: label, property: property, valueKey: valueKey})
		} else {
			captured = b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})
		}
		pending = captured.nodes
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
			node := state.node
			if !state.found {
				var err error
				node, err = b.loadNodeForTemporalTxn(txn, id, false)
				if err != nil {
					return err
				}
			}
			if node != nil {
				nodes = append(nodes, copyNode(node))
				seen[id] = true
			}
		}
		for _, node := range pending {
			if !seen[node.ID] {
				nodes = append(nodes, copyNode(node))
			}
		}
		return nil
	})
	return nodes, err
}
