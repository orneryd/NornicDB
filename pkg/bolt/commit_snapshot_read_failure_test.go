package bolt

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type commitFailureExecutor struct {
	mockTransactionalExecutor
	commits int
}

func (e *commitFailureExecutor) CommitTransaction(ctx context.Context) error {
	e.commits++
	return e.mockTransactionalExecutor.CommitTransaction(ctx)
}

// An unclassified COMMIT failure uses the same shared database-error default.
// Real snapshot failures are exercised end to end in storage's external tests.
func TestCommitFailureFallbackIsNotRetryable(t *testing.T) {
	for _, query := range []string{"CREATE (:Doc {id: 'one'})", "MERGE (n:Doc {id: 'one'})"} {
		t.Run(query, func(t *testing.T) {
			cause := errors.New("storage commit failed")
			executor := &commitFailureExecutor{
				mockTransactionalExecutor: mockTransactionalExecutor{commitError: cause},
			}
			conn := &mockConn{}
			session := newTestSession(conn, executor)
			session.inTransaction = true
			primeTestTransactionLifecycle(t, session)
			session.recordExplicitTransactionWrite(query, nil, true)
			require.False(t, session.canRetryMergeCommitConflict(cause))
			err := session.handleCommit(nil)
			require.ErrorIs(t, err, cause)
			metadata := failureMetadataFromResponse(t, conn.writeData)
			require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", metadata["code"])
			require.False(t, isRetryableBoltStatus(metadata["code"].(string)))
			require.Contains(t, metadata["message"], cause.Error())
			require.Equal(t, 1, executor.commits)
		})
	}
}
