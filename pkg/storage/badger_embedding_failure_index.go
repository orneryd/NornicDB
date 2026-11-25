package storage

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// ensureEmbeddingFailureIndex rebuilds old stores before they are exposed.
// The marker is written only after every metadata record has been examined.
// An interrupted rebuild leaves the marker absent, so reads use the complete
// metadata scan and the next open starts the derived index again.
func (b *BadgerEngine) ensureEmbeddingFailureIndex(ctx context.Context) error {
	var ready bool
	if err := b.db.View(func(txn *badger.Txn) error {
		var err error
		ready, err = embeddingFailureIndexReady(txn)
		return err
	}); err != nil {
		return err
	}
	if ready {
		return nil
	}
	if err := b.db.DropPrefix(ctx, prefixDropOptions{engine: b, metadata: true}, []byte{prefixEmbeddingFailure}); err != nil {
		return fmt.Errorf("clear parked failure index: %w", err)
	}
	scan := storedRecordScan{
		prefix: prefixEmbedding,
		// Count inspected metadata toward the chunk budget even when it is
		// healthy. Otherwise a store with no failures stays in one transaction.
		batchSize: 500,
		logEvery:  100000,
		log:       b.log.With("subsystem", "index_rebuild", "index", "embedding_failure"),
		message:   "embedding failure index backfill progress",
		unit:      "metadata records",
	}
	if _, err := b.forEachStoredRecordInChunks(ctx, scan, func(txn *badger.Txn, key, value []byte) (int, bool, error) {
		id, ok := embeddingMetaFromKey(key)
		if !ok {
			return 0, false, nil
		}
		var meta map[string]any
		if err := decodeValue(value, &meta); err != nil {
			return 1, false, nil // The old scan also skipped unreadable metadata.
		}
		failed, _ := meta["embedding_failed"].(bool)
		if !failed {
			return 1, true, nil
		}
		if err := txn.Set(embeddingFailureKey(id), []byte{}); err != nil {
			return 0, false, err
		}
		return 1, true, nil
	}); err != nil {
		return fmt.Errorf("backfill parked failure index: %w", err)
	}
	return b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		return txn.Set(embeddingFailureIndexReadyKey, []byte{1})
	})
}
