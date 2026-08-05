package bolt

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type invalidatingTransactionalExecutor struct {
	mockTransactionalExecutor
	invalidationCalls int
	pendingWrites     bool
}

func (e *invalidatingTransactionalExecutor) InvalidateCommittedWriteCaches() {
	e.invalidationCalls++
}

func (e *invalidatingTransactionalExecutor) HasPendingTransactionWrites() bool {
	return e.pendingWrites
}

func TestBoltCommitInvalidatesSharedCachesOnlyAfterSuccessfulWrites(t *testing.T) {
	t.Run("successful write", func(t *testing.T) {
		executor := &invalidatingTransactionalExecutor{pendingWrites: true}
		session := newTestSession(&mockConn{}, executor)
		session.baseExec = executor
		session.inTransaction = true
		session.txHasMerge = true

		require.NoError(t, session.handleCommit(nil))
		require.Equal(t, 1, executor.invalidationCalls)
	})

	t.Run("read only", func(t *testing.T) {
		executor := &invalidatingTransactionalExecutor{}
		session := newTestSession(&mockConn{}, executor)
		session.baseExec = executor
		session.inTransaction = true

		require.NoError(t, session.handleCommit(nil))
		require.Zero(t, executor.invalidationCalls)
	})

	t.Run("failed write", func(t *testing.T) {
		executor := &invalidatingTransactionalExecutor{pendingWrites: true}
		executor.commitError = fmt.Errorf("%w: stale snapshot", storage.ErrConflict)
		session := newTestSession(&mockConn{}, executor)
		session.baseExec = executor
		session.inTransaction = true
		session.txHasMerge = true

		require.NoError(t, session.handleCommit(nil))
		require.Zero(t, executor.invalidationCalls)
	})
}
