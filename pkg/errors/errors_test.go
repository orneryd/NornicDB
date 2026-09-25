package errors

import (
	stderrors "errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
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
			err:  MarkMergeCommitTimeUniqueConflict(fmt.Errorf("commit failed: constraint violation: %w", &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "TerraformResource", Properties: []string{"uid"}, Message: "Node with uid=X already exists (nodeID: nornic:abc)"})),
			want: TransientOutdated,
			ok:   true,
		},
		{
			// #703: retrying a transaction over the size limit fails the
			// same way, so it is not transient (see TestTransactionTooBigStatus).
			name: "transaction too big is not transient",
			err:  fmt.Errorf("commit failed: %w", badger.ErrTxnTooBig),
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
}

// TestTransactionTooBigStatus verifies that a transaction over the storage size
// limit reports Neo4j's non-transient General.TransactionOutOfMemoryError, with
// the cause and how to split the work, whether the error kept Badger's sentinel
// or only its text (#703).
func TestTransactionTooBigStatus(t *testing.T) {
	for name, err := range map[string]error{
		"sentinel":  fmt.Errorf("commit failed: %w", badger.ErrTxnTooBig),
		"text only": stderrors.New("failed to delete node: " + badger.ErrTxnTooBig.Error()),
	} {
		t.Run(name, func(t *testing.T) {
			code, message := Neo4jStatus(err)
			if code != ClientTransactionOutOfMemory {
				t.Fatalf("code = %q, want %q", code, ClientTransactionOutOfMemory)
			}
			if !HasNeo4jStatus(err) {
				t.Fatal("HasNeo4jStatus = false, want true")
			}
			if commitCode, _ := Neo4jCommitStatus(err); commitCode != ClientTransactionOutOfMemory {
				t.Fatalf("commit code = %q, want %q", commitCode, ClientTransactionOutOfMemory)
			}
			for _, want := range []string{badger.ErrTxnTooBig.Error(), "CALL { ... } IN TRANSACTIONS"} {
				if !strings.Contains(message, want) {
					t.Fatalf("message %q does not contain %q", message, want)
				}
			}
		})
	}
}

// TestMarkCommitRolledBackIsIdempotent verifies nil stays nil and a failure
// already tagged as rolled back is returned as it is.
func TestMarkCommitRolledBackIsIdempotent(t *testing.T) {
	if MarkCommitRolledBack(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	marked := MarkCommitRolledBack(stderrors.New("commit failed"))
	if MarkCommitRolledBack(marked) != marked {
		t.Fatal("an error already marked must be returned unchanged")
	}
	if !IsCommitRolledBack(marked) {
		t.Fatal("marked error must report rolled back")
	}
}
