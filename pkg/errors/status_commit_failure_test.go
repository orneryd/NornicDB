package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCommitFailureStatusContract(t *testing.T) {
	const want = "Neo.DatabaseError.Transaction.TransactionCommitFailed"
	require.Equal(t, want, TransactionCommitFailed)
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"unclassified commit", errors.New("disk failure"), want},
		{"write conflict", storage.ErrConflict, TransientOutdated},
		{"constraint", &storage.ConstraintViolationError{Type: storage.ConstraintUnique}, ConstraintValidationFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, message := Neo4jCommitStatus(tc.err)
			require.Equal(t, tc.want, code)
			require.NotEmpty(t, message)
		})
	}
}

func TestStorageCommitFailureStatusSurvivesWrappers(t *testing.T) {
	for _, err := range []error{
		&storage.CommitFailureError{Cause: badger.ErrDiscardedTxn},
		fmt.Errorf("implicit commit: %w", &storage.CommitFailureError{Cause: badger.ErrDiscardedTxn}),
		fmt.Errorf("outer: %w", fmt.Errorf("explicit commit: %w", &storage.CommitFailureError{Cause: badger.ErrDiscardedTxn})),
	} {
		for _, mapper := range []func(error) (string, string){Neo4jStatus, Neo4jCommitStatus} {
			code, message := mapper(err)
			require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
			require.Contains(t, message, badger.ErrDiscardedTxn.Error())
		}
		require.True(t, HasNeo4jStatus(err))
		require.False(t, IsCompileTimeError(err))
		require.ErrorIs(t, err, badger.ErrDiscardedTxn)
		_, transient := MapTransientTransactionError(err)
		require.False(t, transient)
	}
}
