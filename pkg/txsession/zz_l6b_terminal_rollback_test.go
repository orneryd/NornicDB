package txsession

// Lane L6b (closeout): P01 L-H7-008, a transaction whose session holds a
// terminal lifecycle error stays listed after the client's ROLLBACK.
// rollbackAndDelete (manager.go:775, branch :790-794) with the session's
// terminalErr set to the snapshot cancellation the manager classifies as
// terminal (isTerminalLifecycleError, :876-886), against a control with no
// terminal error. Decisive values: whether ROLLBACK reached the executor
// (HasActiveTransaction) and whether SHOW TRANSACTIONS still lists it.

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL6bTerminalSessionRollbackSkipped(t *testing.T) {
	for _, tc := range []struct {
		name     string
		terminal error
	}{{"control-no-terminal-error", nil}, {"terminal-snapshot-cancel", storage.ErrMVCCSnapshotGracefulCancel}} {
		t.Run(tc.name, func(t *testing.T) {
			engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			t.Cleanup(func() { _ = engine.Close() })
			store := storage.NewNamespacedEngine(engine, "nornic")
			executor := cypher.NewStorageExecutor(store)
			manager := NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) { return executor, nil }, t.Context())
			t.Cleanup(func() { _ = manager.Close() })
			session, err := manager.Open(context.Background(), "nornic")
			require.NoError(t, err)
			activeBefore := executor.HasActiveTransaction()
			observer := cypher.NewStorageExecutor(store)
			listed := func() int {
				res, err := observer.Execute(context.Background(), "SHOW TRANSACTIONS", nil)
				if err != nil || res == nil {
					return -1
				}
				return len(res.Rows)
			}
			before := listed()
			session.mu.Lock()
			session.terminalErr = tc.terminal
			session.mu.Unlock()
			rbErr := manager.RollbackAndDelete(context.Background(), session)
			activeAfter := executor.HasActiveTransaction()
			after := listed()
			t.Logf("L6B_P01 %s isTerminalLifecycleError=%v | before: active=%v listed=%d | RollbackAndDelete err=%v | after: active=%v listed=%d",
				tc.name, isTerminalLifecycleError(tc.terminal), activeBefore, before, rbErr, activeAfter, after)
			if tc.terminal != nil && activeAfter {
				t.Errorf("CAUSE P01: rollbackAndDelete forgets the session at manager.go:790-794 without sending ROLLBACK; the executor transaction stays active (listed=%d)", after)
			}
		})
	}
}
