package storage

// CommitFailureError preserves an infrastructure failure encountered while
// validating a commit. It is distinct from a retryable write conflict.
// Protocol status belongs to the shared mapper in pkg/errors; Cause retains
// the original storage error through implicit and explicit transaction wrappers.
type CommitFailureError struct {
	Cause error
}

func (err *CommitFailureError) Error() string { return err.Cause.Error() }
func (err *CommitFailureError) Unwrap() error { return err.Cause }
