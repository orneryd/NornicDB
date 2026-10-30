package storage

import "github.com/dgraph-io/badger/v4"

// keyConstraintNodes captures the committed equality-index candidates and the
// pending key candidates together, then resolves bodies in the captured view.
// A replay cannot move a holder between those candidate sets during the capture.
// keep is the caller's selection: only the nodes it keeps are returned, and a
// buffered node it rejects is never copied (NOR-160).
func (b *BadgerEngine) keyConstraintNodes(idx *CompositeIndex, label, property string, values []interface{}, keep func(*Node) bool) ([]*Node, error) {
	var ids []NodeID
	var pending bufferedEntityResult
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
			pending = b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedKey, label: label, property: property, valueKey: valueKey})
		} else {
			pending = b.readBufferedEntities(w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})
		}
		states = make(map[NodeID]bufferedEntityState, len(ids))
		for _, id := range ids {
			states[id] = w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
		}
	}, func(txn *badger.Txn) error {
		seen := make(map[NodeID]bool, len(ids)+len(pending.nodes))
		for _, id := range ids {
			state := states[id]
			if state.deleted {
				continue
			}
			if state.found {
				// A buffered node is retained: copied once, if kept.
				seen[id] = true
				if keep(state.node) {
					nodes = append(nodes, copyNode(state.node))
				}
				continue
			}
			// A committed node is decoded for this read and handed over.
			node, err := b.loadNodeForTemporalTxn(txn, id, false)
			if err != nil {
				return err
			}
			if node == nil {
				continue
			}
			seen[id] = true
			if keep(node) {
				nodes = append(nodes, node)
			}
		}
		nodes = append(nodes, pending.ownedNodes(func(node *Node) bool { return !seen[node.ID] && keep(node) }, nil, false)...)
		return nil
	})
	return nodes, err
}
