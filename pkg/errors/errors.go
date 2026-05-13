package errors

import (
	stderrors "errors"
	"fmt"

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
	// ErrMergeCommitTimeUniqueConflict marks a commit-time UNIQUE violation from
	// a concurrent MERGE race. Retry-aware clients can safely replay because the
	// winner's committed node will be observed on the next attempt.
	ErrMergeCommitTimeUniqueConflict = stderrors.New("merge commit-time unique conflict")
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

// MarkMergeCommitTimeUniqueConflict wraps a UNIQUE constraint violation with a
// dedicated sentinel when the caller knows it came from a retry-safe MERGE
// commit race. Non-UNIQUE failures are returned unchanged.
func MarkMergeCommitTimeUniqueConflict(err error) error {
	if err == nil || stderrors.Is(err, ErrMergeCommitTimeUniqueConflict) {
		return err
	}
	var violation *storage.ConstraintViolationError
	if !stderrors.As(err, &violation) || violation == nil || violation.Type != storage.ConstraintUnique {
		return err
	}
	return fmt.Errorf("%w: %w", ErrMergeCommitTimeUniqueConflict, err)
}

// IsMergeCommitTimeUniqueConflict reports whether err was explicitly tagged as
// a retry-safe commit-time UNIQUE violation from a MERGE race.
func IsMergeCommitTimeUniqueConflict(err error) bool {
	return stderrors.Is(err, ErrMergeCommitTimeUniqueConflict)
}
