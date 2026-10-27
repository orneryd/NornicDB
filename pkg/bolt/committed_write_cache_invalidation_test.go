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

type commitCacheTestOutcome uint8

const (
	commitCacheTestWrite commitCacheTestOutcome = iota
	commitCacheTestReadOnly
	commitCacheTestConflict
)

func TestBoltCommitInvalidatesSharedCachesOnlyAfterSuccessfulWrites(t *testing.T) {
	for _, tc := range []struct {
		name              string
		outcome           commitCacheTestOutcome
		wantInvalidations int
	}{{"successful write", commitCacheTestWrite, 1}, {"read only", commitCacheTestReadOnly, 0}, {"failed write", commitCacheTestConflict, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			executor := &invalidatingTransactionalExecutor{pendingWrites: tc.outcome != commitCacheTestReadOnly}
			if tc.outcome == commitCacheTestConflict {
				executor.commitError = fmt.Errorf("%w: stale snapshot", storage.ErrConflict)
			}
			session := newTestSession(&mockConn{}, executor)
			session.baseExec = executor
			session.inTransaction = true
			if tc.outcome != commitCacheTestReadOnly {
				session.txHasMerge = true
			}
			primeTestTransactionLifecycle(t, session)
			require.NoError(t, session.handleCommit(nil))
			if tc.outcome == commitCacheTestWrite {
				require.Equal(t, tc.wantInvalidations, executor.invalidationCalls)
			} else {
				require.Zero(t, executor.invalidationCalls)
			}
			if tc.outcome == commitCacheTestConflict {
				require.True(t, session.failedUntilReset)
				require.False(t, session.transactionCleanupFailed)
			}
		})
	}
}
