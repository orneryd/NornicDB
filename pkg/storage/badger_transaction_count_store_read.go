package storage

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
)

// Transaction-scoped count-store reads (B10). An explicit transaction reads its
// rows at its own begin boundary, so the current committed counters describe
// them only until a peer commits into the transaction's namespace: after that,
// they hold rows the transaction does not read (a peer creation) or no longer
// hold rows it does (a peer deletion). Answering from the rows themselves is
// correct but materializes the counted population, which the earlier correction
// had to do for every count after such a commit.
//
// The counters are persisted per snapshot, so the same count is answerable
// where the transaction reads: a scoped capability carries a transaction's
// snapshot through the caller's reader chain to the one counter kernel the
// engine already uses for its current view (readCountStoreAt), which answers the
// counter at that snapshot and adds the transaction's own staged contribution
// under the same transaction lock. Nothing new is stored and no population is
// read.
//
// A capability is only proof when the native kernel consumes it. A custom
// reader or decorator that answers the request itself, or changes it, leaves
// the capability unconsumed, and the caller must not certify its answer as the
// transaction's (CountStoreReadScope.DescribesSnapshot).

// CountStoreReadScope is one explicit transaction's capability to read a count
// family at its own snapshot.
//
// A caller obtains it for one read from BadgerTransaction.ScopeCountStoreRead,
// installs it in that read's request, and hands the request to the reader it
// reads everything else through. The reader chain forwards the request as it
// is, and the native engine answers the counter from the transaction's snapshot
// plus the transaction's own staged contribution. The capability is opaque: it
// cannot be built or inspected by its caller, and it exposes no view of the
// transaction's storage.
//
// Each use takes the originating transaction's lock for the whole read and asks
// the transaction's lifecycle owner whether it may still read, so a use cannot
// outlive the snapshot it reads: a transaction that has closed, rolled back,
// handed its native client handle to an accepted commit, or been expired by
// resource pressure declines, exactly as its rows reads refuse, instead of
// reaching a disposed view; one that continued onto another snapshot declines
// instead of certifying the newer one. The lock is taken before the
// counter family locks, in the order a commit takes them, and it is never held
// while a caller's own reader runs.
type CountStoreReadScope struct {
	// engine is the engine the transaction belongs to; a capability is valid
	// only for a read answered by it.
	engine *BadgerEngine
	// tx is the originating transaction, read only under its lock.
	tx *BadgerTransaction
	// read is the one count-store read this capability was issued for.
	read countStoreReadIdentity
	// cut is the snapshot this capability was issued at, which consumption
	// revalidates.
	cut countStoreReadCut
	// answered records that the native counter kernel answered from tx's
	// snapshot. It is the capability's proof of consumption.
	answered atomic.Bool
}

// DescribesSnapshot reports whether the native counter kernel consumed this
// capability and answered from the transaction's snapshot.
//
// It is false for a capability a reader did not consume — one whose request it
// answered itself, refused, or rewrote — and for a nil capability, so an answer
// from such a reader is never certified as the transaction's own read.
func (scope *CountStoreReadScope) DescribesSnapshot() bool {
	return scope != nil && scope.answered.Load()
}

// countStoreReadCut identifies the snapshot a capability was issued at: the
// physical view the transaction pinned and the logical boundary it reads.
//
// A transaction that continues onto another snapshot (after a prepared-merge
// continuation, a batch snapshot advance, or ResumeAfterIndependentCommit)
// replaces both under its lock, so a capability retained from the earlier cut
// names a snapshot the transaction no longer reads. Consumption compares this
// identity rather than the transaction's engine or status alone: those stay the
// same across a continuation, and a counter read at the newer physical snapshot
// would then certify rows the capability was not issued for.
type countStoreReadCut struct {
	snapshot *badger.Txn
	readTS   MVCCVersion
}

// describes reports whether tx still reads the cut this identity was taken
// from. The caller holds tx.mu, so the comparison is stable.
func (cut countStoreReadCut) describes(tx *BadgerTransaction) bool {
	return cut.snapshot != nil && tx.snapshotTx == cut.snapshot && cut.readTS.Compare(tx.readTS) == 0
}

// countStoreReadIdentity is the one read a capability was issued for: the
// counter family and the scope of the counter keys it reads. A reader that
// changes any of them is answering a different read, which the transaction's
// snapshot was not proven to hold.
type countStoreReadIdentity struct {
	kind              CountStoreReadKind
	namespace         string
	name              string
	types             []string
	hiddenLabelPrefix string
}

// countStoreReadIdentityOf copies the read request names, so a caller that
// reuses its request afterwards cannot change what its capability was issued
// for.
func countStoreReadIdentityOf(request CountStoreReadRequest) countStoreReadIdentity {
	return countStoreReadIdentity{
		kind:              request.Kind,
		namespace:         request.Namespace,
		name:              request.Name,
		types:             append([]string(nil), request.Types...),
		hiddenLabelPrefix: request.HiddenLabelPrefix,
	}
}

// describes reports whether request is the read this capability was issued for.
func (identity countStoreReadIdentity) describes(request CountStoreReadRequest) bool {
	return identity.kind == request.Kind &&
		identity.namespace == request.Namespace &&
		identity.name == request.Name &&
		identity.hiddenLabelPrefix == request.HiddenLabelPrefix &&
		slices.Equal(identity.types, request.Types)
}

// readCountStore answers request from the snapshot this capability was issued
// for, when it was issued for exactly this read and engine, the transaction's
// lifecycle owner still admits the read, and the transaction still reads that
// cut. Otherwise the read is left unanswered (a zero CountStoreRead) and the
// caller's own certification decides, exactly as for a request with no
// capability.
//
// The whole read runs under the transaction's lock: that lock is the snapshot's
// lease (a close or a continuation discards the pinned view under it), it orders
// this read against the transaction's own staging, and it makes the snapshot
// read's own state marking race-free. The answer is the counter at the pinned
// snapshot plus the contribution the transaction's staged writes hold at that
// same instant, so a reader that stages a write and then forwards this request
// is composed into the answer it forwards.
func (scope *CountStoreReadScope) readCountStore(ctx context.Context, engine *BadgerEngine, request CountStoreReadRequest) (CountStoreRead, error) {
	if scope == nil || scope.tx == nil || scope.engine != engine || !scope.read.describes(request) {
		return CountStoreRead{}, nil
	}
	tx := scope.tx
	if err := tx.mu.LockContext(ctx); err != nil {
		return CountStoreRead{}, err
	}
	defer tx.mu.Unlock()
	// The transaction's lifecycle owner decides whether it may still read at
	// this cut: a transaction that ended, handed its native client handle to an
	// accepted commit, or was expired by resource pressure no longer reads the
	// snapshot this capability was issued for, so its read stays unanswered - a
	// disposed view is never read. A transaction that continued onto another
	// snapshot is not the one the capability was issued for either, so a newer
	// physical snapshot never certifies the capability's read.
	if tx.engine != engine || tx.ensureOperationAllowedLocked(transactionRead) != nil || !scope.cut.describes(tx) {
		return CountStoreRead{}, nil
	}
	if err := ctx.Err(); err != nil {
		return CountStoreRead{}, err
	}
	// A count reads no entity image. Keep its exact pinned cut without marking
	// an entity observation: MERGE may still advance an otherwise unobserved
	// transaction, as it could after the preexisting count-store route.
	answer, err := engine.readCountStoreAt(ctx, request, func(read func(*badger.Txn) error) error {
		if tx.namespace != "" {
			if err := engine.pendingNamespaceConflict(tx.namespace); err != nil {
				return err
			}
		}
		return read(scope.cut.snapshot)
	})
	if err != nil {
		if errors.Is(err, errMalformedDerivedCount) {
			// A malformed persisted count has no repair at a pinned snapshot:
			// the family's rebuild reads the current state, which counts rows
			// this snapshot does not hold. The read declines, and the caller
			// counts the rows it reads instead of trusting a newer view.
			return CountStoreRead{}, nil
		}
		return CountStoreRead{}, err
	}
	staged, tracked, err := tx.pendingCountStoreContributionLocked(ctx, request)
	if err != nil {
		return CountStoreRead{}, err
	}
	if !tracked || !staged.DescribesRows {
		// The transaction's staged writes hold a change this count cannot
		// express (a staged internal label, or a change that can move a
		// positional count): the rows it reads are the answer.
		return CountStoreRead{}, nil
	}
	answer.Count += staged.Count
	scope.answered.Store(true)
	return answer, nil
}

// ScopeCountStoreRead issues one count-store read at the transaction's own
// snapshot, for callers that read through a reader chain rather than through
// the transaction directly.
//
// described is false when the transaction cannot prove that a counter read at
// its snapshot describes the rows it reads: a request for another namespace, a
// write-behind transaction (whose reads are read-committed), a transaction
// without a pinned snapshot of its own read version, or a read whose answer
// needs the positional (label, type) counters while the transaction's cut holds
// an unpublished version. The caller then answers from the rows it reads.
//
// The read is declined for the same reason a closed transaction's rows read is
// refused: a transaction that ended, handed its native client handle to an
// accepted commit, or whose snapshot reader expired reads no counter at its
// snapshot. Issuance takes the transaction's exclusive lock, because that
// lifecycle owner closes an expired transaction where it decides.
//
// The returned request carries the capability; the caller's request is not
// modified. What the transaction's own staged writes contribute is composed by
// the native kernel at consumption, under the transaction's lock, so a write the
// caller's reader stages after planning is part of the answer.
func (tx *BadgerTransaction) ScopeCountStoreRead(ctx context.Context, request CountStoreReadRequest) (CountStoreReadRequest, bool, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return CountStoreReadRequest{}, false, err
	}
	defer tx.mu.Unlock()
	// The transaction's lifecycle owner decides whether it may still read at
	// all: a closed, detached or expired transaction issues no capability. The
	// lock above is exclusive because that owner closes a transaction whose
	// snapshot reader expired.
	if tx.ensureOperationAllowedLocked(transactionRead) != nil {
		return CountStoreReadRequest{}, false, nil
	}
	if tx.engine == nil || tx.snapshotTx == nil {
		return CountStoreReadRequest{}, false, nil
	}
	// A request that already carries a capability belongs to another
	// transaction's read; planning it again would certify two snapshots.
	if request.Scope != nil || request.Namespace == "" || tx.namespace != request.Namespace {
		return CountStoreReadRequest{}, false, nil
	}
	// Write-behind reads are read-committed: the buffer holds commits the
	// pinned snapshot does not, so only the live counters describe them.
	if tx.engine.writeBehind != nil {
		return CountStoreReadRequest{}, false, nil
	}
	// The pinned snapshot is the authoritative owner of this transaction's read
	// version only while readTS is still the namespace's begin boundary
	// (readsPinnedSnapshotBodyLocked). A version rebound behind it - a late pin
	// onto a namespace that did not exist at begin, or a boundary rewound or
	// replaced by a continuation - reads rows the snapshot does not describe.
	if !tx.readsPinnedSnapshotBodyLocked() {
		return CountStoreReadRequest{}, false, nil
	}
	// The positional (label, type) counters are applied after their commit's
	// rows are published, so a snapshot taken while a commit was between the
	// two holds rows whose positional counters it does not hold. The cut's copy
	// of the reservation ledger names every version unpublished at the
	// snapshot, so an empty copy proves no such commit was in flight: every
	// positional counter the snapshot holds describes its rows.
	if countStoreReadUsesPositionalCounts(request) && tx.inflightAtBegin != nil {
		return CountStoreReadRequest{}, false, nil
	}
	scope := &CountStoreReadScope{
		engine: tx.engine,
		tx:     tx,
		read:   countStoreReadIdentityOf(request),
		cut:    countStoreReadCut{snapshot: tx.snapshotTx, readTS: tx.readTS},
	}
	request.Scope = scope
	return request, true, nil
}

// countStoreReadUsesPositionalCounts reports whether one count-store read's
// answer depends on the positional (label, type) counters: a positional read
// counts them, and the visibility half of a relationship count asks them
// whether hidden endpoints hold relationships of the requested types.
func countStoreReadUsesPositionalCounts(request CountStoreReadRequest) bool {
	switch request.Kind {
	case CountStoreNodesByLabel:
		return false
	case CountStoreRelationshipsByType:
		// The per-type counter is scalar, so only the visibility half can need
		// the positional tier. An empty hidden prefix skips that half entirely
		// (the system database hides nothing).
		return request.HiddenLabelPrefix != ""
	}
	return true
}
