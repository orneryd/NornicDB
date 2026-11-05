package replication

import (
	"context"
	"testing"
)

// applyQueued applies one command through the nonwaiting publication contract
// that native-result tests exercise and returns its native result.
//
// WALQueued is the mode for callers that do not need their own record's
// publication receipt; the exact own-record position is asserted by the
// publication tests instead. Keeping the single call here avoids restating the
// mode at every native-result assertion.
//
// Callers include helper goroutines, so the contract violation is reported with
// t.Errorf rather than a fail-fast assertion.
func applyQueued(t *testing.T, s Storage, cmd *Command) error {
	publication, err := s.ApplyCommand(context.Background(), cmd, WALQueued)
	if publication.Position != 0 {
		t.Errorf("WALQueued must not report a publication position, got %d", publication.Position)
	}
	return err
}
