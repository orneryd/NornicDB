package storage

import (
	"errors"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Test seam at the shared iterator, so regressions can assert that ordinary
// writes do not enter embedding cleanup at all (including an empty range).
var embeddingKeyScanHook atomic.Pointer[func(NodeID)]

// commitEmbeddingSidecar coordinates the only embedding writer that does
// not change a node body. Body writes already hold writeBarrier shared, so
// their current-range cleanup needs no new lock or sentinel point read.
// The exclusive barrier is released before the caller fires listeners.
func (b *BadgerEngine) commitEmbeddingSidecar(id NodeID, hasKeys bool, notification *commitNotification, write func(*commitWriter) error) error {
	// Every commit acknowledged before it is applied first, so its source
	// check sees the newest body and no replay of an older one lands over it;
	// the caller delivers the applied commits' notifications.
	release, err := b.beginDrainedWrite()
	if err != nil {
		return err
	}
	err = b.commitEngineWrite(write, notification)
	if err == nil {
		if hasKeys {
			b.lastEmbeddingSidecarWrite = b.db.oracle.published.Load()
			b.embeddingSidecarWrites.Store(id, b.lastEmbeddingSidecarWrite)
		} else {
			b.embeddingSidecarWrites.Delete(id)
		}
	}
	release()
	return err
}

// replaceEmbeddingKeys replaces the entire embedding key range: vectors,
// shard markers/parts and metadata. The iterator must close before a write
// rotates the commit writer's Badger batch. Only live keys are copied.
func (cw *commitWriter) replaceEmbeddingKeys(id NodeID, embeddings [][]float32, references ...embeddingReferenceView) error {
	var cleanup embeddingKeyCleanupOptions
	if len(references) > 0 {
		cleanup.references = &references[0]
	}
	if _, err := cw.removeEmbeddingKeys(id, 0, cleanup); err != nil {
		return err
	}
	return writeEmbeddingChunks(id, embeddings, func(key, value []byte) error {
		return cw.write(func(txn *badger.Txn) error { return txn.Set(key, value) })
	})
}

var errEmbeddingKeyLimit = errors.New("embedding key batch full")

// removeEmbeddingKeys is also the repair writer: a positive limit bounds the
// copied keys and deletions per repair transaction; zero replaces a full range.
func (cw *commitWriter) removeEmbeddingKeys(id NodeID, limit int, options ...embeddingKeyCleanupOptions) (int, error) {
	var keys [][]byte
	var cleanup embeddingKeyCleanupOptions
	if len(options) > 0 {
		cleanup = options[0]
	}
	scan := embeddingKeyScanOptions{references: cleanup.references}
	if cleanup.cursor != nil {
		scan.after = *cleanup.cursor
	}
	err := visitEmbeddingKeys(cw.txn, id, func(key []byte) error {
		keys = append(keys, key)
		if limit > 0 && len(keys) >= limit {
			return errEmbeddingKeyLimit
		}
		return nil
	}, scan)
	if err != nil && !errors.Is(err, errEmbeddingKeyLimit) {
		return 0, err
	}
	for _, key := range keys {
		if err := cw.write(func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
			return 0, err
		}
	}
	if cleanup.cursor != nil && len(keys) > 0 {
		*cleanup.cursor = append(append([]byte(nil), keys[len(keys)-1]...), 0)
	}
	return len(keys), nil
}

// writeEmbeddingChunks is shared by the atomic writer and transaction staging.
func writeEmbeddingChunks(id NodeID, embeddings [][]float32, set func([]byte, []byte) error) error {
	for index, embedding := range embeddings {
		kvs, err := buildEmbeddingChunkWriteKVs(id, index, embedding)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			if err := set(kv.key, kv.val); err != nil {
				return localizedError(localization.StorageClientNodeEmbeddingChunkStoreFailed(index, err), err)
			}
		}
	}
	return nil
}

// stageEmbeddingReplacementLocked retains the final desired range until
// commit. Repeated updates and a delete of a newly created node replace that
// intent, so no earlier staged chunks can survive. Inline bodies without
// separate keys need no intent or allocation.
func (tx *BadgerTransaction) stageEmbeddingReplacementLocked(id NodeID, embeddings [][]float32) {
	if tx.embeddingChanges == nil {
		tx.embeddingChanges = make(map[NodeID][][]float32)
	}
	tx.embeddingChanges[id] = embeddings
}

// collectLateEmbeddingChangesLocked accounts for sidecars that the node's
// snapshot read could not see. Commit holds writeBarrier shared, excluding
// further sidecar publication until the body commit or buffered ACK.
func (tx *BadgerTransaction) collectLateEmbeddingChangesLocked() {
	// No per-node probes, intents, views or iterators for a plain body write.
	// A sidecar published after the physical snapshot is the exceptional
	// case: discover only its touched nodes while publication is excluded.
	if tx.engine.lastEmbeddingSidecarWrite <= tx.badgerReadTs {
		return
	}
	stage := func(id NodeID) {
		if _, replaced := tx.embeddingChanges[id]; replaced {
			return
		}
		if version, exists := tx.engine.embeddingSidecarWrites.Load(id); exists && version.(uint64) > tx.badgerReadTs {
			tx.stageEmbeddingReplacementLocked(id, nil)
		}
	}
	// Never walk the engine's sidecar population: an unrelated embedding
	// publication must not make a plain update scale with the database.
	for id := range tx.deletedNodes {
		stage(id)
	}
	for id, pending := range tx.pendingNodes {
		if pending != nil && !pending.embeddingKeysPresent {
			stage(id)
		}
	}
}

// materializeEmbeddingChangesLocked encodes the desired vectors through the
// shared chunk writer. Synchronous publication resolves the current range now;
// buffered publication retains the range intent for ordered replay instead.
func (tx *BadgerTransaction) materializeEmbeddingChangesLocked(current bool) error {
	if len(tx.embeddingChanges) == 0 {
		return nil
	}
	materialize := func(view *badger.Txn) error {
		var references embeddingReferenceView
		if current {
			replacements := make([]NodeID, 0, len(tx.embeddingChanges))
			for id := range tx.embeddingChanges {
				replacements = append(replacements, id)
			}
			references = newEmbeddingReferenceView(view, tx.pendingWrites, tx.pendingDeletes, replacements)
		}
		for id, embeddings := range tx.embeddingChanges {
			if current {
				if err := visitEmbeddingKeys(view, id, func(key []byte) error {
					tx.bufferDelete(key)
					return nil
				}, embeddingKeyScanOptions{references: &references}); err != nil {
					return err
				}
			}
			if err := writeEmbeddingChunks(id, embeddings, func(key, value []byte) error {
				tx.bufferSet(key, value)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}
	if current {
		return tx.engine.withView(materialize)
	}
	// Encode through the same chunk writer at ACK, preserving the existing
	// binary WAL payload and synchronous encoding errors. Resolve only the
	// old range later, in replay order, before applying these point writes.
	return materialize(nil)
}

// forgetReplacedSidecars retires only ranges a successful body commit owned.
// The caller still holds writeBarrier, excluding a new sidecar publication.
func (b *BadgerEngine) forgetReplacedSidecars(changes map[NodeID][][]float32) {
	for id := range changes {
		b.embeddingSidecarWrites.Delete(id)
	}
}
