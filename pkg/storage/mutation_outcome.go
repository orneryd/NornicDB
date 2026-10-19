package storage

import (
	"errors"
	"fmt"
)

// AppliedMutationError distinguishes a completed local mutation from a failed
// preparation. An acknowledgement timeout cannot roll that mutation back.
type AppliedMutationError struct{ Cause error }

func (e *AppliedMutationError) Error() string {
	return fmt.Sprintf("mutation applied; acknowledgement: %v", e.Cause)
}
func (e *AppliedMutationError) Unwrap() error         { return e.Cause }
func (e *AppliedMutationError) MutationApplied() bool { return true }

func MutationApplied(err error) bool {
	var outcome interface{ MutationApplied() bool }
	return errors.As(err, &outcome) && outcome.MutationApplied()
}
