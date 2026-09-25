package errors

import (
	stderrors "errors"

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
	// Procedure catalog lifecycle errors.
	ErrProcedureCatalogReadFailed         = stderrors.New("cypher: procedure catalog read failed")
	ErrProcedureCatalogRecordDecodeFailed = stderrors.New("cypher: procedure catalog record decode failed")
	ErrProcedureCatalogRecordInvalid      = stderrors.New("cypher: procedure catalog record invalid")
	ErrProcedureRegistryReloadFailed      = stderrors.New("cypher: procedure registry reload failed")

	// DDL parse/validation errors.
	ErrInvalidFulltextRelationshipTypes = stderrors.New("cypher: invalid fulltext relationship types")
	ErrInvalidMergeChainQuery           = stderrors.New("cypher: invalid merge chain query")
	// ErrExpressionEvaluationFailed marks expression-evaluation failures that must
	// surface to callers instead of being treated as literal fallback values.
	ErrExpressionEvaluationFailed = stderrors.New("cypher: expression evaluation failed")
)

type mergeCommitTimeUniqueConflictError struct {
	err error
}

func (e *mergeCommitTimeUniqueConflictError) Error() string {
	return e.err.Error()
}

func (e *mergeCommitTimeUniqueConflictError) Unwrap() error {
	return e.err
}

func (e *mergeCommitTimeUniqueConflictError) Is(target error) bool {
	return target == ErrMergeCommitTimeUniqueConflict
}

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
		stderrors.Is(err, ErrMergeCommitTimeUniqueConflict) ||
		stderrors.Is(err, ErrMVCCResourcePressure) ||
		stderrors.Is(err, ErrMVCCSnapshotGracefulCancel) ||
		stderrors.Is(err, ErrMVCCSnapshotHardExpired) {
		return TransientOutdated, true
	}
	return "", false
}

// MarkMergeCommitTimeUniqueConflict wraps a UNIQUE constraint violation with a
// dedicated sentinel when the caller knows it came from a retry-safe MERGE
// commit race. Only a violation against a value another transaction committed
// after this one began (ConstraintViolationError.Concurrent) is a race: a
// violation against a value that was already stored fails the same way on
// every retry, so it stays a constraint violation (Neo4j's
// Schema.ConstraintValidationFailed). Other failures are returned unchanged.
func MarkMergeCommitTimeUniqueConflict(err error) error {
	if err == nil || stderrors.Is(err, ErrMergeCommitTimeUniqueConflict) {
		return err
	}
	var violation *storage.ConstraintViolationError
	if !stderrors.As(err, &violation) || violation == nil || violation.Type != storage.ConstraintUnique || !violation.Concurrent {
		return err
	}
	return &mergeCommitTimeUniqueConflictError{err: err}
}

// IsMergeCommitTimeUniqueConflict reports whether err was explicitly tagged as
// a retry-safe commit-time UNIQUE violation from a MERGE race.
func IsMergeCommitTimeUniqueConflict(err error) bool {
	return stderrors.Is(err, ErrMergeCommitTimeUniqueConflict)
}
