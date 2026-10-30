package storage

// CommitFailureError identifies an infrastructure read or decoding failure
// encountered while validating a commit. It preserves the original cause and
// is distinct from ordinary retryable conflicts and constraint violations.
// Protocol consumers classify it through the shared errors package.
type CommitFailureError struct {
	Cause error
}

func (failure *CommitFailureError) Error() string { return failure.Cause.Error() }
func (failure *CommitFailureError) Unwrap() error { return failure.Cause }
