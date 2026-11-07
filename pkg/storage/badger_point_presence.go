package storage

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// readStoredPresence selects acknowledged physical existence through the same
// coherent overlay/native cut as entity reads, without hydrating a body or its
// embedding sidecars. Hidden entities remain present for integrity validation.
func (b *BadgerEngine) readStoredPresence(ctx context.Context, kind bufferedEntityKind, id string) (bool, error) {
	var present bool
	view, err := b.captureReadView(ctx, nil, readViewOptions{
		scope: namespaceForNodeID(NodeID(id)),
		cached: func(buffer *WriteBehindBuffer) (bool, error) {
			state := buffer.entityStateLocked(kind, id, bufferedReadVisibility{engine: b, visibility: storedRead})
			if !state.found {
				return false, nil
			}
			present = !state.deleted
			return true, nil
		},
	})
	if err != nil {
		return false, err
	}
	if view == nil {
		return present, nil
	}
	var key []byte
	if kind == bufferedEdgeEntity {
		key = edgeKey(EdgeID(id))
	} else {
		key = nodeKey(NodeID(id))
	}
	err = view.readAndClose(ctx, func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		present = err == nil
		return ctx.Err()
	})
	if err != nil {
		return false, err
	}
	return present, nil
}
