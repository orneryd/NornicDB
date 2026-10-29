package storage

import (
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// bufferedReceiptRead identifies one buffered commit receipt a caller wants
// observed: the physical generation marker of an accepted write-behind record
// (txID, optionally carrying its logical lineage) or the namespace-scoped
// logical entity receipt (logicalID). native is the caller's own read-write
// Badger transaction; observing through it keeps the receipt read in that
// transaction's conflict set.
type bufferedReceiptRead struct {
	namespace, txID, logicalID string
	native                     *badger.Txn
}

// bufferedReceiptObservation is the single answer to a bufferedReceiptRead.
// key is the receipt key the observation read, so a native caller stages the
// marker under the key the observation already constructed.
type bufferedReceiptObservation struct {
	key     []byte
	present bool
}

// observeBufferedCommitReceipt owns native conflict enrollment, acknowledged
// generation visibility and the physical marker read. A physical-only request
// never observes pending generations. For native logical reads, retirement is
// excluded from the generation lookup through its physical fallback. The caller
// holds tx.mu before the buffer read lock, as with the previous receipt owner.
func (b *BadgerEngine) observeBufferedCommitReceipt(request bufferedReceiptRead) (bufferedReceiptObservation, error) {
	observation := bufferedReceiptObservation{}
	if request.txID == "" && request.logicalID == "" {
		return observation, nil
	}
	observation.key = bufferedCommitMarkerKey(request.namespace, request.txID, request.logicalID)
	lookup := func(txn *badger.Txn) error {
		_, err := txn.Get(observation.key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		observation.present = err == nil
		return err
	}
	if request.native != nil {
		if err := lookup(request.native); err != nil || observation.present {
			return observation, err
		}
	}
	if w := b.writeBehind; request.native != nil && w != nil && request.logicalID != "" {
		w.mu.RLock()
		defer w.mu.RUnlock()
		if w.logicalReceiptPresentLocked(request.namespace, request.logicalID) {
			observation.present = true
			return observation, nil
		}
	}
	err := b.withView(lookup)
	return observation, err
}

// logicalReceiptPresentLocked observes receipt metadata in acknowledged
// generations. The caller holds w.mu.RLock through the physical receipt read,
// preventing retirement between the generation observation and its fallback.
// Entity bodies remain owned by the canonical overlay visibility composition.
func (w *WriteBehindBuffer) logicalReceiptPresentLocked(namespace, logicalID string) bool {
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
		return true
	}
	for _, generation := range w.draining {
		if contains(generation) {
			return true
		}
	}
	return false
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

// isLogicalCommitReceipt identifies the nonempty receipt written during WAL
// replay. The old deindex queue shares its first key byte, so migration must
// check the complete length-prefixed key and marker value before retaining it.
func isLogicalCommitReceipt(key, value []byte) bool {
	if len(key) < 2 || key[0] != prefixBufferedFlush || len(value) != 1 || value[0] != 1 {
		return false
	}
	rest, ok := strings.CutPrefix(string(key[1:]), "replication/")
	if !ok {
		return false
	}
	length, rest, ok := strings.Cut(rest, ":")
	if !ok {
		return false
	}
	n, err := strconv.Atoi(length)
	if err != nil || n <= 0 || strconv.Itoa(n) != length || n >= len(rest) || rest[n] != '/' {
		return false
	}
	return n+1 < len(rest)
}
