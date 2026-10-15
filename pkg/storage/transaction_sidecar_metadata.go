package storage

import (
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"time"
)

// retainedSidecarMetadataLocked reads through the writer to enroll the metadata
// key in conflict detection. A concurrent embedding worker must not be replaced
// by an older transaction's freshness update. Buffered metadata wins after the
// first update, since the pinned snapshot still has the original body stamp.
func (tx *BadgerTransaction) retainedSidecarMetadataLocked(node *Node) (map[string]any, bool, error) {
	key := embeddingMetaKey(node.ID)
	_, err := tx.badgerTx.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, false, fmt.Errorf("tracking embedding metadata: %w", err)
	}
	if tx.pendingDeletes[string(key)] {
		return nil, false, nil
	}
	var meta map[string]any
	if staged, ok := tx.pendingWrites[string(key)]; ok {
		err = decodeValue(staged, &meta)
	} else {
		// Keep the existing pinned-reader failure semantics as well as the
		// writer's conflict enrollment. Both readers share the same version.
		err = tx.withSnapshotViewLocked(func(view *badger.Txn) error {
			var fresh bool
			var readErr error
			meta, fresh, readErr = tx.engine.loadEmbeddingSidecar(view, node, node.ID)
			if !fresh {
				meta = nil
			}
			return readErr
		})
	}
	if err != nil {
		return nil, false, fmt.Errorf("decoding retained embedding metadata: %w", err)
	}
	if meta == nil {
		return nil, false, nil
	}
	if stamp, ok := meta[embeddingContentUpdatedAtKey].(string); ok && (node.UpdatedAt.IsZero() || stamp != node.UpdatedAt.UTC().Format(time.RFC3339Nano)) {
		return nil, false, nil
	}
	return meta, true, nil
}
