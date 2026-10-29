package storage

import (
	"context"
	"errors"
)

// BatchGetNodes reads the transaction's pending and pinned point view. Missing
// IDs are omitted; an ended transaction remains an error even for an empty batch.
func (tx *BadgerTransaction) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	return tx.BatchGetNodesWithContext(context.Background(), ids)
}

// BatchGetNodesWithContext retains one transaction lock and snapshot while
// checking cancellation between the existing point-read operations. It opens
// no new read capture and never substitutes the current committed engine.
func (tx *BadgerTransaction) BatchGetNodesWithContext(ctx context.Context, ids []NodeID) (map[NodeID]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}
	result := make(map[NodeID]*Node, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		node, err := tx.getNodeReadLocked(id, visibleRead)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result[id] = node
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
