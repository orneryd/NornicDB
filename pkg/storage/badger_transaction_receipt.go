package storage

import (
	"context"
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
func (b *BadgerEngine) observeBufferedCommitReceipt(ctx context.Context, request bufferedReceiptRead) (bufferedReceiptObservation, error) {
	observation := bufferedReceiptObservation{}
	if err := ctx.Err(); err != nil {
		return observation, err
	}
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
		if err := w.mu.RLockContext(ctx); err != nil {
			return bufferedReceiptObservation{}, err
		}
		defer w.mu.RUnlock()
		present, err := w.logicalReceiptPresentLocked(ctx, request.namespace, request.logicalID)
		if err != nil {
			return bufferedReceiptObservation{}, err
		}
		if present {
			observation.present = true
			return observation, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return bufferedReceiptObservation{}, err
	}
	err := b.withView(lookup)
	return observation, err
}

// logicalReceiptPresentLocked observes receipt metadata in acknowledged
// generations. The caller holds w.mu.RLock through the physical receipt read,
// preventing retirement between the generation observation and its fallback.
// Entity bodies remain owned by the canonical overlay visibility composition.
func (w *WriteBehindBuffer) logicalReceiptPresentLocked(ctx context.Context, namespace, logicalID string) (bool, error) {
	contains := func(ctx context.Context, buf *CommitBuffer) (bool, error) {
		if buf != nil {
			for _, commit := range buf.commits {
				if err := ctx.Err(); err != nil {
					return false, err
				}
				if commit.namespace == namespace && commit.logicalTxID == logicalID {
					return true, nil
				}
			}
		}
		return false, nil
	}
	present, err := contains(ctx, w.active)
	if err != nil || present {
		return present, err
	}
	for _, generation := range w.draining {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		present, err := contains(ctx, generation)
		if err != nil || present {
			return present, err
		}
	}
	return false, nil
}

// bufferedCommitMarkerKey is the single writer/reader key owner for both
// physical generation receipts and namespace-scoped logical entity receipts.
func bufferedCommitMarkerKey(namespace, txID, logicalID string) []byte {
	if logicalID != "" {
		var decimal [20]byte
		length := strconv.AppendInt(decimal[:0], int64(len(namespace)), 10)
		key := make([]byte, 0, 1+len("replication/")+len(length)+1+len(namespace)+1+len(logicalID))
		key = append(key, prefixBufferedFlush)
		key = append(key, "replication/"...)
		key = append(key, length...)
		key = append(key, ':')
		key = append(key, namespace...)
		key = append(key, '/')
		return append(key, logicalID...)
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
