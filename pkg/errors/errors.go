package errors

import (
	stderrors "errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	// TransientDeadlockDetected is the retryable wire error code for lock deadlocks.
	TransientDeadlockDetected = "Neo.TransientError.Transaction.DeadlockDetected"
	// TransientOutdated is the retryable wire error code for stale MVCC snapshots.
	TransientOutdated = "Neo.TransientError.Transaction.Outdated"
)

var (
	// ErrTransactionConflict aliases the storage conflict sentinel used when an
	// optimistic transaction observes data changed after its snapshot.
	ErrTransactionConflict = storage.ErrConflict
	// ErrMVCCResourcePressure aliases the storage admission sentinel used when a
	// snapshot cannot be kept alive under current MVCC pressure.
	ErrMVCCResourcePressure = storage.ErrMVCCResourcePressure
	// ErrMVCCSnapshotGracefulCancel aliases the storage sentinel for snapshots
	// cancelled during high MVCC pressure.
	ErrMVCCSnapshotGracefulCancel = storage.ErrMVCCSnapshotGracefulCancel
	// ErrMVCCSnapshotHardExpired aliases the storage sentinel for snapshots
	// forcibly expired during critical MVCC pressure.
	ErrMVCCSnapshotHardExpired = storage.ErrMVCCSnapshotHardExpired
	// ErrTransactionDeadlock marks lock-ordering deadlocks that drivers should
	// retry as Neo4j-compatible transient transaction failures.
	ErrTransactionDeadlock = stderrors.New("transaction deadlock")
)

// MapTransientTransactionError maps known transaction failure sentinels to
// Neo4j-compatible transient transaction codes. It intentionally classifies by
// error reference rather than message text so localized or templated messages do
// not change retry semantics.
func MapTransientTransactionError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if stderrors.Is(err, ErrTransactionDeadlock) {
		return TransientDeadlockDetected, true
	}
	if stderrors.Is(err, ErrTransactionConflict) ||
		stderrors.Is(err, ErrMVCCResourcePressure) ||
		stderrors.Is(err, ErrMVCCSnapshotGracefulCancel) ||
		stderrors.Is(err, ErrMVCCSnapshotHardExpired) {
		return TransientOutdated, true
	}
	return "", false
}

// IsMergeCommitTimeUniqueConflict reports whether an error looks like a
// commit-time UNIQUE constraint violation from a concurrent-MERGE race, as
// opposed to a user-supplied duplicate-key violation in a CREATE. The
// distinction matters for protocol-level retry classification: a MERGE race
// is resolvable by a fresh attempt (the loser observes the peer's now-
// committed node and matches), while a user duplicate-key in CREATE is not.
//
// The classifier matches both pre- and post-v1.0.45 NornicDB wraps:
//   - older releases: "failed to commit implicit transaction: constraint
//     violation: Constraint violation (UNIQUE on Label.[prop]): Node with
//     prop=value already exists (nodeID: ...)"
//   - newer (explicit-tx commit): "commit failed: constraint violation:
//     Constraint violation (UNIQUE on Label.[prop]): Node with prop=value
//     already exists (nodeID: ...)"
//
// We classify by message shape because the underlying storage error is a
// plain fmt.Errorf without a sentinel wrap, and threading a MERGE-shape
// signal from the cypher executor through the storage layer is a larger
// design change than this surgical fix. The shape is precise enough that
// only MERGE/UNIQUE races match it — duplicate-key CREATEs share the
// same body but originate from a different call site that does not carry
// the "commit failed"/"failed to commit implicit transaction" prefix.
func IsMergeCommitTimeUniqueConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.Contains(msg, "constraint violation") {
		return false
	}
	if !strings.Contains(msg, "UNIQUE on") {
		return false
	}
	if !strings.Contains(msg, "already exists") {
		return false
	}
	return strings.Contains(msg, "failed to commit implicit transaction") ||
		strings.Contains(msg, "commit failed") ||
		strings.Contains(msg, "TransactionCommitFailed")
}
