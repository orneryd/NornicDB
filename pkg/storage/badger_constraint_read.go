package storage

import "context"

import "github.com/dgraph-io/badger/v4"

// keyConstraintNodes captures the committed equality-index candidates and the
// pending key candidates together, then resolves bodies in the captured view.
// A replay cannot move a holder between those candidate sets during the capture.
func (b *BadgerEngine) keyConstraintNodes(idx *CompositeIndex, label, property string, values []interface{}) ([]*Node, error) {
	return b.keyConstraintNodesWithContext(context.Background(), idx, label, property, values)
}

func (b *BadgerEngine) keyConstraintNodesWithContext(ctx context.Context, idx *CompositeIndex, label, property string, values []interface{}) ([]*Node, error) {
	var ids []NodeID
	var pending []*Node
	var states map[NodeID]bufferedEntityState
	var captureErr error
	if w := b.writeBehind; w != nil {
		if err := w.ensurePendingValueTrackedWithContext(ctx, label, property); err != nil {
			return nil, err
		}
	}
	var nodes []*Node
	err := b.withReadView(func(w *WriteBehindBuffer) {
		if captureErr = ctx.Err(); captureErr != nil {
			return
		}
		ids, captureErr = idx.LookupFullWithContext(ctx, values...)
		if captureErr != nil {
			return
		}
		if w == nil {
			return
		}
		var captured bufferedEntityResult
		if valueKey, ok := indexValueKey(values[0]); ok {
			captured, captureErr = b.readBufferedEntitiesWithContext(ctx, w, storedRead, bufferedEntityRead{kind: bufferedKey, label: label, property: property, valueKey: valueKey})
		} else {
			captured, captureErr = b.readBufferedEntitiesWithContext(ctx, w, storedRead, bufferedEntityRead{kind: bufferedLabel, label: label})
		}
		pending = captured.nodes
		if captureErr != nil {
			return
		}
		states = make(map[NodeID]bufferedEntityState, len(ids))
		for _, id := range ids {
			if captureErr = ctx.Err(); captureErr != nil {
				return
			}
			states[id] = w.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: b, visibility: storedRead})
		}
	}, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		seen := make(map[NodeID]bool, len(ids)+len(pending))
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
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
			if err := ctx.Err(); err != nil {
				return err
			}
			if !seen[node.ID] {
				nodes = append(nodes, copyNode(node))
			}
		}
		return nil
	})
	return nodes, err
}
