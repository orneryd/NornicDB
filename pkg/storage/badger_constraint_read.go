package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// keyConstraintNodes captures the committed equality-index candidates and the
// pending key candidates together, then resolves bodies in the captured view.
// A replay cannot move a holder between those candidate sets during the capture.
// ctx is the validating request's context.
func (b *BadgerEngine) keyConstraintNodes(ctx context.Context, idx *CompositeIndex, label, property string, values []interface{}) ([]*Node, error) {
	var ids []NodeID
	var pending []*Node
	var states map[NodeID]bufferedEntityState
	if w := b.writeBehind; w != nil {
		w.ensurePendingValueTracked(label, property)
	}
	var nodes []*Node
	err := b.withReadView(ctx, func(w *WriteBehindBuffer) error {
		ids = idx.LookupFull(values...)
		if w == nil {
			return nil
		}
		if valueKey, ok := indexValueKey(values[0]); ok {
			pendingKey, err := b.readBufferedEntities(ctx, w, storedRead, bufferedEntityRead{kind: bufferedKey, label: label, property: property, valueKey: valueKey})
			if err != nil {
				return err
			}
			pending = pendingKey.nodes
		} else {
			pendingNodes, err := b.readBufferedEntities(ctx, w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})
			if err != nil {
				return err
			}
			pending, _ = pendingNodes.nodes, pendingNodes.nodesTouched
		}
		states = make(map[NodeID]bufferedEntityState, len(ids))
		for _, id := range ids {
			states[id] = w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
		}
		return nil
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
