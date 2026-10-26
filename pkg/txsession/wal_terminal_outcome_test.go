// Author: Codex, NOR-180 returned terminal outcomes at COMMIT admission, 2026-10-27.
package txsession

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"github.com/stretchr/testify/require"
)

func TestWALTerminalCommitAdmissionKeepsCleanup(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			engine := walterminal.New(t)
			manager := NewManager(time.Minute, func(string) (*cypher.StorageExecutor, error) {
				return cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic")), nil
			})
			session, err := manager.Open(context.Background(), "nornic")
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.RollbackAndDelete(context.Background(), session) })
			_, err = manager.ExecuteInSession(context.Background(), session, "CREATE (:WalWire)", nil)
			require.NoError(t, err)
			if closed {
				require.NoError(t, engine.WAL.Close())
			}
			execution, cancel := txphase.WithExecutionDeadline(context.Background(), time.Minute)
			cancel()
			_, err = manager.CommitAndDelete(execution, session)
			require.ErrorIs(t, err, context.Canceled)
			if closed {
				require.ErrorIs(t, err, storage.ErrWALClosed)
				var diagnostic interface{ StatusDiagnostic() localization.Message }
				require.ErrorAs(t, err, &diagnostic)
				require.Contains(t, err.Error(), "WAL tx abort")
			} else {
				require.NotContains(t, err.Error(), "WAL tx abort")
			}
			_, present := manager.Get(session.ID)
			require.False(t, present, "existing session retirement still completes")
			require.Zero(t, walterminal.Count(t, engine))
		})
	}
}
