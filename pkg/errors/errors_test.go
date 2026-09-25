package errors

import (
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestMapTransientTransactionError verifies the protocol-code boundary for
// retryable transaction failures and non-retryable ordinary errors.
func TestMapTransientTransactionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
		ok   bool
	}{
		{
			name: "deadlock",
			err:  fmt.Errorf("%w: waiting for transaction lock", ErrTransactionDeadlock),
			want: TransientDeadlockDetected,
			ok:   true,
		},
		{
			name: "transaction conflict",
			err:  fmt.Errorf("commit failed: %w: node changed after transaction start", ErrTransactionConflict),
			want: TransientOutdated,
			ok:   true,
		},
		{
			name: "resource pressure",
			err:  fmt.Errorf("begin read: %w", ErrMVCCSnapshotHardExpired),
			want: TransientOutdated,
			ok:   true,
		},
		{
			name: "merge commit-time unique conflict",
			err:  MarkMergeCommitTimeUniqueConflict(fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "TerraformResource", Properties: []string{"uid"}, Message: "Node with uid=X already exists (nodeID: nornic:abc)", Concurrent: true})),
			want: TransientOutdated,
			ok:   true,
		},
		{
			// #657: a duplicate of a value stored before the transaction began
			// fails on every retry, so it isn't a race.
			name: "merge unique violation against a stored value is not transient",
			err:  MarkMergeCommitTimeUniqueConflict(fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "TerraformResource", Properties: []string{"uid"}, Message: "Node with uid=X already exists (nodeID: nornic:abc)"})),
			ok:   false,
		},
		{
			name: "ordinary error",
			err:  stderrors.New("syntax error"),
			ok:   false,
		},
		{
			name: "commit-time unique text without query context is not transient",
			err:  fmt.Errorf("commit failed: constraint violation: Constraint violation (UNIQUE on TerraformResource.[uid]): Node with uid=X already exists (nodeID: nornic:abc)"),
			ok:   false,
		},
		{
			name: "empty",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := MapTransientTransactionError(tt.err)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("code = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMarkMergeCommitTimeUniqueConflict(t *testing.T) {
	uniqueErr := fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{
		Type:       storage.ConstraintUnique,
		Label:      "TerraformResource",
		Properties: []string{"uid"},
		Message:    "Node with uid=X already exists (nodeID: nornic:abc)",
		Concurrent: true,
	})
	marked := MarkMergeCommitTimeUniqueConflict(uniqueErr)
	if !IsMergeCommitTimeUniqueConflict(marked) {
		t.Fatal("expected unique constraint violation to be marked as merge commit-time conflict")
	}
	if marked.Error() != uniqueErr.Error() {
		t.Fatalf("marked error text = %q, want %q", marked.Error(), uniqueErr.Error())
	}

	nonUniqueErr := fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{
		Type:       storage.ConstraintExists,
		Label:      "TerraformResource",
		Properties: []string{"uid"},
		Message:    "missing required property",
	})
	if got := MarkMergeCommitTimeUniqueConflict(nonUniqueErr); got != nonUniqueErr {
		t.Fatal("non-unique constraint violation should not be wrapped")
	}

	storedValueErr := fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{
		Type:       storage.ConstraintUnique,
		Label:      "TerraformResource",
		Properties: []string{"uid"},
		Message:    "Node with uid=X already exists (nodeID: nornic:abc)",
	})
	if got := MarkMergeCommitTimeUniqueConflict(storedValueErr); got != storedValueErr {
		t.Fatal("a unique violation against a value stored before the transaction began should not be wrapped")
	}
}
