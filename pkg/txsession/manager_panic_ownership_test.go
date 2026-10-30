package txsession

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type managerPanicCommitEngine struct {
	storage.Engine
	base    *storage.MemoryEngine
	started *storage.BadgerTransaction
}

func (e *managerPanicCommitEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	tx, err := e.base.BeginTransaction()
	e.started = tx
	return tx, err
}
func (*managerPanicCommitEngine) CommitTransaction(context.Context, *storage.BadgerTransaction) error {
	panic("session commit panic")
}

// The panic occurs synchronously; a one-hour idle TTL prevents a competing
// expiry callback. TryLock observes the lock without stranding another goroutine.
func TestManagerPanicReleasesOperationMutex(t *testing.T) {
	for _, operation := range []string{"statement", "commit"} {
		t.Run(operation, func(t *testing.T) {
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			engine := &managerPanicCommitEngine{Engine: base, base: base}
			mgr := NewManager(time.Hour, nil)
			executor := cypher.NewStorageExecutor(engine)
			session, err := mgr.OpenWithExecutorForOwner(context.Background(), "nornic", executor, "")
			require.NoError(t, err)
			deadline := session.ExpiresAt()
			t.Cleanup(func() {
				if executor.HasActiveTransaction() {
					_, rollbackErr := executor.Execute(context.Background(), "ROLLBACK", nil)
					require.NoError(t, rollbackErr)
				}
				if engine.started != nil && engine.started.IsActive() {
					require.NoError(t, engine.started.Rollback())
				}
				session.mu.Lock()
				mgr.stopExpiryTimerLocked(session)
				session.mu.Unlock()
			})
			if operation == "statement" {
				require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "sessionpanic.fail", Mode: cypher.ProcedureModeRead}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
					panic("session statement panic")
				}))
				t.Cleanup(cypher.ClearUserProcedures)
				require.PanicsWithValue(t, "session statement panic", func() { _, _ = mgr.ExecuteInSession(context.Background(), session, "CALL sessionpanic.fail()", nil) })
			} else {
				require.PanicsWithValue(t, "session commit panic", func() { _, _ = mgr.CommitAndDelete(context.Background(), session) })
			}
			unlocked := session.mu.TryLock()
			// On the broken implementation this test owns the sole completed operation;
			// repair its lock so even fail-first fixture cleanup remains bounded.
			armed := session.expiryTimer != nil
			session.mu.Unlock()
			released := session.requestMu.TryLock()
			if released {
				session.requestMu.Unlock()
			}
			if operation == "statement" {
				require.Equal(t, deadline, session.ExpiresAt(), "panic preserves the existing idle deadline")
				require.True(t, armed, "panic leaves the idle timer armed at that deadline")
				require.NoError(t, mgr.RollbackAndDelete(context.Background(), session))
			}
			require.True(t, unlocked, "panic must release the completed operation's mutex")
			require.True(t, released, "panic must release the request lease")
		})
	}
}
