package storage

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// keyConstraintNodes captures the committed equality-index candidates and the
// pending key candidates together, then resolves bodies in the captured view.
// A replay cannot move a holder between those candidate sets during the capture.
func (b *BadgerEngine) keyConstraintNodes(ctx context.Context, idx *CompositeIndex, label, property string, values []interface{}) ([]*Node, error) {
	var ids []NodeID
	var pending []*Node
	var states map[NodeID]bufferedEntityState
	if w := b.writeBehind; w != nil {
		if err := w.ensurePendingValueTracked(ctx, label, property); err != nil {
			return nil, err
		}
	}
	var nodes []*Node
	var captureErr error
	err := b.withReadView(ctx, func(w *WriteBehindBuffer) {
		ids, captureErr = idx.LookupFull(ctx, values...)
		if captureErr != nil {
			return
		}
		if w == nil {
			return
		}
		// The overlay capture is part of the write's own integrity read, so it
		// runs on that read's lifetime: a staging write's own context, the
		// commit's caller context during commit validation.
		valueKey, keyed, err := indexValueKey(ctx, values[0])
		if err != nil {
			captureErr = err
			return
		}
		if keyed {
			pendingNodes, err := b.readBufferedEntities(ctx, w, storedRead, bufferedEntityRead{kind: bufferedKey, label: label, property: property, valueKey: valueKey})
			if err != nil {
				captureErr = err
				return
			}
			pending = pendingNodes.nodes
		} else {
			pendingNodes, err := b.readBufferedEntities(ctx, w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})
			if err != nil {
				captureErr = err
				return
			}
			pending, _ = pendingNodes.nodes, pendingNodes.nodesTouched
		}
		states = make(map[NodeID]bufferedEntityState, len(ids))
		for _, id := range ids {
			states[id] = w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
		}
	}, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr
		}
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
