package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Commit admission (NOR-412). Where the journal writes records larger than
// one frame, a commit's record can reach the journal although a later stage
// of the same commit would refuse it for its size; under the standard policy
// the frame refusal comes first. Two such limits are decided here, before
// anything of the commit is journaled:
//
//   - the write buffer's 64 MiB limit chooses the route
//     (bufferedForecastFitsLocked, in PublishCommit);
//   - the store's value limit is decided where the frame refusal was: the
//     journal asks before it writes a record that needs more than one frame
//     (WAL.appendDecidingLargeRecord; storeValueLimitErrorLocked for a
//     prepared commit, replayedBodyLimit for the engine's own writes).
//
// A record that fits one frame is appended without the journal's question.
// Refusals this file does not decide can still follow the journal.

// ErrStoreValueTooLarge reports a commit refused before anything of it was
// journaled because the store cannot hold one of its values.
var ErrStoreValueTooLarge = errors.New("storage: value exceeds the store's value limit")

// storeValueTooLargeError is ErrStoreValueTooLarge with the value and the
// limit. Its BoltErrorCode is the statement argument error
// (Neo.ClientError.Statement.ArgumentError): the refusal is about the size
// of one value, which a retry of the same commit does not change.
type storeValueTooLargeError struct{ detail string }

func (e *storeValueTooLargeError) Error() string {
	return ErrStoreValueTooLarge.Error() + ": " + e.detail
}

func (e *storeValueTooLargeError) Unwrap() error { return ErrStoreValueTooLarge }

func (e *storeValueTooLargeError) BoltErrorCode() string {
	return "Neo.ClientError.Statement.ArgumentError"
}

// storeValueFits returns nil when the store takes a value of size bytes, and
// otherwise the refusal naming what the value is and the store's limit.
func storeValueFits(limit int64, size int, what string, args ...any) error {
	if int64(size) <= limit {
		return nil
	}
	return &storeValueTooLargeError{detail: fmt.Sprintf("%s is %d bytes, more than the %d bytes this store holds in one value", fmt.Sprintf(what, args...), size, limit)}
}

// journalsLargeRecords reports whether b's commits are journaled by a WAL that
// writes records larger than one frame (WALRecordsLarge), the policy
// nornicdb.Open selects for a standalone server. Admission applies to those
// engines only. On any other engine a record that does not fit one frame is
// refused by the frame check (encodeWALEntryPayload), or there is no journal.
func (b *BadgerEngine) journalsLargeRecords() bool {
	w := b.bufferedCommitWAL
	return w != nil && w.recordPolicy == WALRecordsLarge
}

// storeValueLimit is the largest value the store db was opened with takes:
// Badger's own rule (Txn.modify). A value longer than its value-log file size
// is refused; an in-memory store also refuses one longer than its value
// threshold.
func storeValueLimit(db *managedBadgerDB) int64 {
	options := db.Opts()
	limit := options.ValueLogFileSize
	if options.InMemory && options.ValueThreshold < limit {
		limit = options.ValueThreshold
	}
	return limit
}

// storeValueMeasure is a kvWriter that stores nothing: Set refuses a value
// over the store's limit (storeValueFits) and otherwise does nothing. Commit
// admission hands it to the code that builds the history records and the
// change events, with the inputs admission has at that moment.
type storeValueMeasure struct {
	txn   *badger.Txn
	limit int64
	what  string
}

func (m *storeValueMeasure) Set(_, value []byte) error {
	return storeValueFits(m.limit, len(value), "%s", m.what)
}

func (m *storeValueMeasure) Delete([]byte) error { return nil }

func (m *storeValueMeasure) NewIterator(opt badger.IteratorOptions) *badger.Iterator {
	return m.txn.NewIterator(opt)
}

// stagedKeyName names a staged key in a refusal: the node or relationship
// whose body it holds, or the key's leading bytes.
func stagedKeyName(key string) string {
	if len(key) > 1 && key[0] == prefixNode {
		return fmt.Sprintf("node %q as stored", key[1:])
	}
	if len(key) > 1 && key[0] == prefixEdge {
		return fmt.Sprintf("relationship %q as stored", key[1:])
	}
	return fmt.Sprintf("the value of key %x", key[:min(len(key), 48)])
}

// storeValueLimitErrorLocked decides, for a commit whose journal record needs
// more than one frame, whether four kinds of value the commit gives the store
// are within the store's value limit, before the record is appended:
//
//   - the staged values, by their length (the bodies, the vectors stored
//     apart);
//   - each written node and relationship as replay of the record encodes it
//     (replayedBodyLimitError);
//   - the history records, by the commit writer's own selection and encoders,
//     with a zero version for the commit's (measureSupersededHistory);
//   - the commit's change events, in a database that captures changes, built
//     by changeEventsLocked in the capture mode read here; the commit reads
//     the mode again when it is applied (beginChangeCaptureLocked).
//
// Other values of the commit are not measured. The caller holds tx.mu; the
// transaction is parked at its prepared hand-off.
func (tx *BadgerTransaction) storeValueLimitErrorLocked(ctx context.Context) error {
	b := tx.engine
	limit := storeValueLimit(tx.badgerDB)
	for key, value := range tx.pendingWrites {
		if int64(len(value)) > limit && !tx.pendingDeletes[key] {
			return storeValueFits(limit, len(value), "%s", stagedKeyName(key))
		}
	}
	operations := tx.physicalOperationsLocked()
	states, err := newCommitStates(ctx, operations)
	if err != nil {
		return err
	}
	if err := b.replayedBodyLimitError(ctx, tx.badgerTx, tx.staged(), tx.namespace, states, limit); err != nil {
		return err
	}
	measure := &storeValueMeasure{txn: tx.badgerTx, limit: limit, what: "a history record of this commit"}
	if err := b.measureSupersededHistory(ctx, tx.badgerTx, measure, operations, states); err != nil {
		return err
	}
	if mode := b.ChangeCapture(tx.namespace); tx.namespace != "" && mode != ChangeCaptureOff {
		// The commit's place in the change order and its commit time are not
		// known yet: the largest sequence number and the current time stand in.
		measure.what = "a change event of this commit"
		return tx.changeEventsLocked(ctx, operations, math.MaxUint64, mode, time.Now(),
			func(read func(*badger.Txn) error) error { return read(tx.badgerTx) }, measure.Set)
	}
	return nil
}

// replayedBodyLimitError measures, for each node and relationship the commit
// leaves, the body encoded again from the commit's final image of it (states).
// It stands for the body a replay of the journal record writes. That body
// can be larger than the one the commit itself stages: a list of int32
// values is encoded again as int64 (commit_admission_test.go), and once a
// re-encoded body was one byte larger with no such list (cause not traced).
// txn and staged are the committing transaction's.
func (b *BadgerEngine) replayedBodyLimitError(ctx context.Context, txn *badger.Txn, staged kvWriter, namespace string, states commitStates, limit int64) error {
	for id, state := range states.nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if state.after == nil {
			continue
		}
		data, _, err := b.encodeNodeInTxn(txn, namespace, state.after)
		if err != nil {
			return err
		}
		if err := storeValueFits(limit, len(data), "node %q as the journal's record of it is applied", id); err != nil {
			return err
		}
	}
	for id, state := range states.edges {
		if err := ctx.Err(); err != nil {
			return err
		}
		if state.after == nil {
			continue
		}
		data, err := b.encodeEdgeInTxn(staged, namespace, state.after)
		if err != nil {
			return err
		}
		if err := storeValueFits(limit, len(data), "relationship %q as the journal's record of it is applied", id); err != nil {
			return err
		}
	}
	return nil
}

// replayedBodyLimit is the decision the journal asks of the engine's own
// write (the native route) before it writes a record of more than one frame.
// By then the engine has staged the write in its Badger transaction, where
// Badger refuses a value over its limit; this measures the re-encoded bodies.
func (cw *commitWriter) replayedBodyLimit() error {
	states, err := newCommitStates(cw.ctx, cw.entityOperations)
	if err != nil {
		return err
	}
	return cw.engine.replayedBodyLimitError(cw.ctx, cw.txn, cw.txn, firstOperationNamespace(cw.entityOperations), states, storeValueLimit(cw.db))
}

// bufferedForecastFitsLocked decides, before anything of the commit is
// journaled, whether the commit goes to the write buffer: whether a forecast
// of the buffered commit's size (estimateBufferedPayloadBytes) is within the
// buffer's limit. A commit forecast over it takes the direct route, which
// writes the commit to Badger before acknowledging it.
//
// The forecast is made over the staged state without the dictionary entries
// and derived-count deltas, which only the buffered commit's owners take; it
// can therefore be lower than the buffered commit's own, and
// commitBufferedLocked handles a commit that this admits and its own forecast
// then puts over the limit.
func (tx *BadgerTransaction) bufferedForecastFitsLocked() bool {
	forecast := estimateBufferedPayloadBytes(tx.namespace, tx.ID, tx.physicalOperationsLocked(), tx.pendingWrites, tx.pendingDeletes, nil, propKeyTxnDrain{}, 0, 0, 0)
	return forecast <= maxBufferedWALPayloadBytes
}
