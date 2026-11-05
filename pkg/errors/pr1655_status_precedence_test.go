package errors

import (
	stderrors "errors"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPR1655CommitFailureIdentityPrecedesCause(t *testing.T) {
	unique := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "Doc", Properties: []string{"id"}, Message: "claimed by concurrent commit", Cause: storage.ErrConflict}
	for _, tc := range []struct {
		name    string
		failure error
		code    string
	}{
		{"true unique claim", unique, ConstraintValidationFailed},
		{"infrastructure", &storage.CommitFailureError{Cause: storage.ErrConflict}, DatabaseTransactionCommitFailed},
		{"ordinary conflict", storage.ErrConflict, TransientOutdated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("commit: %w", tc.failure)
			require.True(t, stderrors.Is(wrapped, storage.ErrConflict), "causal identity remains available")
			for _, mapStatus := range []func(error) (string, string){Neo4jStatus, func(err error) (string, string) { return Neo4jStatus(TransactionControlFailure(TerminalCommit, err)) }} {
				code, message := mapStatus(wrapped)
				require.Equal(t, tc.code, code)
				require.NotEmpty(t, message)
			}
		})
	}
}
