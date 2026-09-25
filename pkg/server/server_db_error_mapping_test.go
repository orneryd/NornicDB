package server

import (
	stderrors "errors"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// TestMapTransientTransactionError verifies that HTTP transaction endpoints use
// error identity, not localized message text, to decide retryable failures.
func TestMapTransientTransactionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "conflict changed after start",
			err:  fmt.Errorf("commit failed: %w: node x changed after transaction start", nornicerrors.ErrTransactionConflict),
			want: "Neo.TransientError.Transaction.Outdated",
		},
		{
			name: "deadlock",
			err:  fmt.Errorf("%w: waiting for lock", nornicerrors.ErrTransactionDeadlock),
			want: "Neo.TransientError.Transaction.DeadlockDetected",
		},
		{
			name: "graceful snapshot expiration",
			err:  fmt.Errorf("failed to create node: %w", nornicerrors.ErrMVCCSnapshotGracefulCancel),
			want: "Neo.TransientError.Transaction.Outdated",
		},
		{
			name: "hard snapshot expiration",
			err:  fmt.Errorf("begin read: %w", nornicerrors.ErrMVCCSnapshotHardExpired),
			want: "Neo.TransientError.Transaction.Outdated",
		},
		{
			name: "transaction too big is a non-transient client error",
			err:  stderrors.New("commit failed: Txn is too big to fit into one request"),
			want: nornicerrors.ClientTransactionOutOfMemory,
		},
		{
			name: "syntax error passthrough",
			err:  stderrors.New("invalid input 'RETURNN'"),
			want: "Neo.ClientError.Statement.SyntaxError",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _ := mapSessionExecError(tc.err)
			if got != tc.want {
				t.Fatalf("code mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestMapSessionExecErrorBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		wantCode    string
		wantMessage string
	}{
		{
			name:        "nil error",
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			wantMessage: "",
		},
		{
			name:        "transient error",
			err:         fmt.Errorf("commit failed: %w", nornicerrors.ErrTransactionDeadlock),
			wantCode:    "Neo.TransientError.Transaction.DeadlockDetected",
			wantMessage: "commit failed: transaction deadlock",
		},
		{
			name:        "neo prefix with message",
			err:         stderrors.New("Neo.ClientError.Transaction.ForbiddenDueToTransactionType: writes are not allowed"),
			wantCode:    "Neo.ClientError.Transaction.ForbiddenDueToTransactionType",
			wantMessage: "writes are not allowed",
		},
		{
			name:        "neo prefix without delimiter",
			err:         stderrors.New("Neo.ClientError.Transaction.Terminated"),
			wantCode:    "Neo.ClientError.Transaction.Terminated",
			wantMessage: "Neo.ClientError.Transaction.Terminated",
		},
		{
			name:        "wrapped neo code",
			err:         stderrors.New("apply input failed: Neo.ClientError.Schema.ConstraintValidationFailed: duplicate value"),
			wantCode:    "Neo.ClientError.Schema.ConstraintValidationFailed",
			wantMessage: "duplicate value",
		},
		{
			name:        "plain fallback",
			err:         stderrors.New("invalid input"),
			wantCode:    "Neo.ClientError.Statement.SyntaxError",
			wantMessage: "invalid input",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotCode, gotMessage := mapSessionExecError(tc.err)
			if gotCode != tc.wantCode {
				t.Fatalf("code mismatch: got %q want %q", gotCode, tc.wantCode)
			}
			if gotMessage != tc.wantMessage {
				t.Fatalf("message mismatch: got %q want %q", gotMessage, tc.wantMessage)
			}
		})
	}
}
