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
// the frame refusal comes first. Three such limits are decided here, before
// anything of the commit is journaled:
//
//   - the write buffer's 64 MiB limit chooses the route
//     (bufferedForecastFitsLocked, in PublishCommit);
//   - the store's value limit is decided where the frame refusal was: the
//     journal asks before it writes a record that needs more than one frame
//     (WAL.appendDecidingLargeRecord; storeValueLimitErrorLocked for a
//     prepared commit, replayedBodyLimit for the engine's own writes);
//   - the store's key limit is decided where the route is chosen: a commit the
//     write buffer's WAL or the prepared hand-off accepts before writing the
//     commit's keys is refused here when one of those keys is longer than the
//     store takes (storeKeyLimitErrorLocked, in PublishCommit). That refusal
//     is definite whatever size the commit's record has, so the journal is
//     not asked for it.
//
// A record that fits one frame is appended without the journal's question.
// Refusals this file does not decide can still follow the journal.

// ErrStoreValueTooLarge reports a commit refused before anything of it was
// journaled because the store cannot hold one of its values.
var ErrStoreValueTooLarge = errors.New("storage: value exceeds the store's value limit")

// storeLimitError is the common definite storage-capacity rejection. Its cause
// distinguishes key and value limits; both are non-retryable argument errors.
type storeLimitError struct {
	cause  error
	detail string
}

func (e *storeLimitError) Error() string {
	return e.cause.Error() + ": " + e.detail
}

func (e *storeLimitError) Unwrap() error { return e.cause }

func (e *storeLimitError) BoltErrorCode() string {
	return "Neo.ClientError.Statement.ArgumentError"
}

// storeValueFits returns nil when the store takes a value of size bytes, and
// otherwise the refusal naming what the value is and the store's limit.
func storeValueFits(limit int64, size int, what string, args ...any) error {
	if int64(size) <= limit {
		return nil
	}
	return &storeLimitError{cause: ErrStoreValueTooLarge, detail: fmt.Sprintf("%s is %d bytes, more than the %d bytes this store holds in one value", fmt.Sprintf(what, args...), size, limit)}
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
//     by changeEventsLocked in the capture mode the commit decided
//     (changeCaptureLocked), the mode in which its application writes them.
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
	mode, err := tx.changeCaptureLocked(ctx)
	if err != nil {
		return err
	}
	if tx.namespace != "" && mode != ChangeCaptureOff {
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

// The store's key limit (B03/report6). Badger refuses a key longer than 65000
// bytes (Txn.modify, maxKeySize; Badger does not export the rule). A commit's
// keys reach Badger at different points of its route: the write buffer's WAL
// and the prepared hand-off both accept the commit before any of its keys is
// written, and the write-behind flusher, the commit's own batches or the
// dictionary's token persistence write them afterwards. A key over the limit is
// then refused only after the commit was accepted, which leaves an accepted
// mutation the store can never apply; for the write buffer, a generation that
// fails on every cycle. storeKeyLimitErrorLocked decides the limit where the
// route is chosen, while nothing of the commit is journaled.
//
// The staged keys a commit writes are its own; its other keys are derived from
// the names it writes (the derived label, edge-type and positional counts, and
// the property-key dictionary), so they are measured with the builders that
// write them.

// ErrStoreKeyTooLong reports a commit refused before anything of it was
// journaled because the store cannot hold one of its keys.
var ErrStoreKeyTooLong = errors.New("storage: key exceeds the store's key limit")

// storeKeyLimit is the longest key the store takes: Badger's own rule
// (Txn.modify), 65000 bytes, the length a table's header can address.
const storeKeyLimit = 65000

// storeKeyFits returns nil when the store takes a key of size bytes, and
// otherwise the refusal naming what the key is and the store's limit.
func storeKeyFits(size int, what string, args ...any) error {
	if size <= storeKeyLimit {
		return nil
	}
	return &storeLimitError{cause: ErrStoreKeyTooLong, detail: fmt.Sprintf("%s is %d bytes, more than the %d bytes this store holds in one key", fmt.Sprintf(what, args...), size, storeKeyLimit)}
}

// refusalNameLimit is how much of a name a refusal repeats. A refused name can
// be as long as the limit it exceeded.
const refusalNameLimit = 64

// refusalName shortens a name for a refusal.
func refusalName(name string) string {
	if len(name) <= refusalNameLimit {
		return name
	}
	return name[:refusalNameLimit] + "..."
}

// pendingKeyName names a staged key in a refusal: the node, relationship,
// label or relationship type whose key it is, and otherwise the key's leading
// bytes. stagedKeyName names the same facts for a value refusal; a key refusal
// is about the key, not about the value the commit stores under it.
func pendingKeyName(key string) string {
	raw := []byte(key)
	switch {
	case len(raw) > 1 && raw[0] == prefixNode:
		return fmt.Sprintf("the key of node %q", refusalName(string(raw[1:])))
	case len(raw) > 1 && raw[0] == prefixEdge:
		return fmt.Sprintf("the key of relationship %q", refusalName(string(raw[1:])))
	case len(raw) > 1 && raw[0] == prefixIDDictNodeForward:
		return fmt.Sprintf("the ID dictionary's forward key of node %q", refusalName(string(raw[1:])))
	case len(raw) > 1 && raw[0] == prefixIDDictEdgeForward:
		return fmt.Sprintf("the ID dictionary's forward key of relationship %q", refusalName(string(raw[1:])))
	case len(raw) > 10 && raw[0] == prefixLabelIndex && raw[len(raw)-9] == 0:
		return fmt.Sprintf("the label index key of label %q", refusalName(string(raw[1:len(raw)-9])))
	case len(raw) > 10 && raw[0] == prefixEdgeTypeIndex && raw[len(raw)-9] == 0:
		return fmt.Sprintf("the edge-type index key of relationship type %q", refusalName(string(raw[1:len(raw)-9])))
	case len(raw) > 26 && raw[0] == prefixEdgeBetweenIndex && raw[len(raw)-9] == 0:
		return fmt.Sprintf("the relationship lookup key of relationship type %q", refusalName(string(raw[17:len(raw)-9])))
	case len(raw) > 17 && raw[0] == prefixEdgeBetweenHead:
		return fmt.Sprintf("the relationship lookup key of relationship type %q", refusalName(string(raw[17:])))
	}
	return fmt.Sprintf("the key %x", raw[:min(len(raw), 48)])
}

// storeKeyLimitErrorLocked decides, before a route that accepts the commit
// ahead of its keys accepts it, whether every key the commit gives the store is
// within the store's key limit:
//
//   - the staged writes (pendingWrites), by their length: the node and
//     relationship bodies, the label and edge-type index entries, the
//     relationship lookup entries, the embedding chunks and the temporal
//     history entries. The ID dictionary's forward key of an entity is the
//     length of that entity's body key, so it is within this too;
//   - the derived count keys, by the canonical builder of each delta the
//     commit appends: countDeltaKey for a label and for an edge type, and
//     edgeTypeLabelCountKey for a positional (label, type) count. A delta
//     key's suffix is one width for every delta, so a zero suffix stands in
//     for the commit's;
//   - the property-key dictionary's forward key (propKeyForwardKey) of every
//     property name the commit's writes put in a namespace. A name the
//     dictionary already persists has a key of the same length, which the
//     store already holds, so measuring it refuses nothing that could be
//     written.
//
// The commit's other keys are short: the ID dictionary's reverse keys, the MVCC
// keys and the buffered commit marker are built from numeric IDs. The caller
// holds tx.mu, before the transaction is parked at its hand-off, so this
// measures the state its commit will write.
func (tx *BadgerTransaction) storeKeyLimitErrorLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for key := range tx.pendingWrites {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(key) > storeKeyLimit {
			return storeKeyFits(len(key), "%s", pendingKeyName(key))
		}
	}
	for key := range tx.pendingLabelCountDeltas {
		if err := ctx.Err(); err != nil {
			return err
		}
		if size := countDeltaKeySize(key.namespace, key.label); size > storeKeyLimit {
			return storeKeyFits(size, "the label count key of label %q", refusalName(key.label))
		}
	}
	for key := range tx.pendingEdgeTypeCountDeltas {
		if err := ctx.Err(); err != nil {
			return err
		}
		if size := countDeltaKeySize(key.namespace, key.edgeType); size > storeKeyLimit {
			return storeKeyFits(size, "the edge-type count key of relationship type %q", refusalName(key.edgeType))
		}
	}
	for key := range tx.pendingEdgeTypeLabelCountDeltas {
		if err := ctx.Err(); err != nil {
			return err
		}
		if size := edgeTypeLabelCountKeySize(key.namespace, key.label, key.edgeType); size > storeKeyLimit {
			return storeKeyFits(size, "the (label, type) count key of label %q and relationship type %q", refusalName(key.label), refusalName(key.edgeType))
		}
	}
	return tx.propertyKeyLimitErrorLocked(ctx)
}

// propertyKeyLimitErrorLocked decides the property-key dictionary's forward
// keys of the property names the commit's writes encode. The encoder allocates
// a token for every name in a body's properties, whatever the name's value
// (encodeTokenizedProperties), in the namespace of the entity's own ID.
func (tx *BadgerTransaction) propertyKeyLimitErrorLocked(ctx context.Context) error {
	measure := func(namespace string, properties map[string]any) error {
		for name := range properties {
			if err := ctx.Err(); err != nil {
				return err
			}
			if size := propKeyForwardKeySize(namespace, name); size > storeKeyLimit {
				return storeKeyFits(size, "the property-key dictionary's forward key of property %q in namespace %q", refusalName(name), namespace)
			}
		}
		return nil
	}
	for _, operation := range tx.physicalOperationsLocked() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if operation.Node != nil {
			if err := measure(namespaceForNodeID(operation.Node.ID), operation.Node.Properties); err != nil {
				return err
			}
		}
		if operation.Edge != nil {
			if err := measure(namespaceForEdgeID(operation.Edge.ID), operation.Edge.Properties); err != nil {
				return err
			}
		}
	}
	return nil
}
