package txsession

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSessionOwnsStorageUntilTerminalOperation(t *testing.T) {
	for _, terminal := range []string{"commit", "rollback", "begin failure"} {
		t.Run(terminal, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			executor := cypher.NewStatementExecutor(storage.NewNamespacedEngine(engine, "owned"))
			closed := 0
			executor.SetOwnedStorageCleanup(func() error { closed++; return nil })
			if terminal == "begin failure" {
				executor.SetSharedExecutor(true)
			}
			manager := NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) { return executor, nil })
			session, err := manager.Open(context.Background(), "owned")
			if terminal == "begin failure" {
				require.Error(t, err)
				require.Equal(t, 1, closed)
				return
			}
			require.NoError(t, err)
			require.Zero(t, closed, "BEGIN retains request-owned resources")
			if terminal == "commit" {
				_, err = manager.CommitAndDelete(context.Background(), session)
			} else {
				err = manager.RollbackAndDelete(context.Background(), session)
			}
			require.NoError(t, err)
			require.Equal(t, 1, closed)
			manager.Delete(session.ID)
			executor.ReleaseOwnedStorage()
			require.Equal(t, 1, closed, "one lifecycle owner closes exactly once")
		})
	}
}
