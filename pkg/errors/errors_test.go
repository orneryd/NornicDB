package errors

import (
	stderrors "errors"
	"fmt"
	"testing"
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
