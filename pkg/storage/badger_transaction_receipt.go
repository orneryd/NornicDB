package storage

import (
	"strconv"

	"github.com/dgraph-io/badger/v4"
)

// PrepareLogicalCommitReceipt uses the existing durable flush-marker family
// for an entity replication receipt. Its existence read participates in the
// native transaction conflict set; its marker is staged with the entity data.
// Namespace must already be pinned. True means this logical commit is durable
// and the caller must not stage or commit its entities again.
func (tx *BadgerTransaction) PrepareLogicalCommitReceipt(logicalID string) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return false, err
	}
	if tx.namespace == "" || logicalID == "" {
		return false, ErrInvalidData
	}
	key := bufferedCommitMarkerKey(tx.namespace, "", logicalID)
	_, err := tx.badgerTx.Get(key)
	if err == nil {
		return true, nil
	}
	if err != badger.ErrKeyNotFound {
		return false, err
	}
	// The initial native read remains in the conflict set. The buffer owner
	// keeps retirement excluded until its durable fallback has completed.
	var present bool
	if w := tx.engine.writeBehind; w != nil {
		present, err = w.logicalCommitReceiptPresent(tx.engine, tx.namespace, logicalID)
	} else {
		present, err = tx.engine.bufferedFlushMarkerExists(tx.namespace, "", logicalID)
	}
	if present || err != nil {
		return present, err
	}
	tx.bufferSet(key, []byte{1})
	return false, nil
}

// logicalCommitReceiptPresent owns acknowledged-generation observation and
// retirement exclusion through the physical marker read. It reads metadata,
// never entity bodies, and retains the native receipt conflict enrollment.
func (w *WriteBehindBuffer) logicalCommitReceiptPresent(b *BadgerEngine, namespace, logicalID string) (bool, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	contains := func(buf *CommitBuffer) bool {
		if buf != nil {
			for _, commit := range buf.commits {
				if commit.namespace == namespace && commit.logicalTxID == logicalID {
					return true
				}
			}
		}
		return false
	}
	if contains(w.active) {
		return true, nil
	}
	for _, generation := range w.draining {
		if contains(generation) {
			return true, nil
		}
	}
	return b.bufferedFlushMarkerExists(namespace, "", logicalID)
}

// bufferedCommitMarkerKey is the single writer/reader key owner for both
// physical generation receipts and namespace-scoped logical entity receipts.
func bufferedCommitMarkerKey(namespace, txID, logicalID string) []byte {
	if logicalID != "" {
		txID = "replication/" + strconv.Itoa(len(namespace)) + ":" + namespace + "/" + logicalID
	}
	key := make([]byte, 0, 1+len(txID))
	key = append(key, prefixBufferedFlush)
	return append(key, txID...)
}
