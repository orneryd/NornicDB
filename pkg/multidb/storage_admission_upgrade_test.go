package multidb

import (
	"context"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedCommitByteUpgradeAfterPreparationIsKnownConflict(t *testing.T) {
	for _, fits := range []bool{true, false} {
		name := "fits"
		if !fits {
			name = "exceeds"
		}
		t.Run(name, func(t *testing.T) {
			manager, db := setupTestManager(t)
			engine, err := manager.GetStorage(db)
			require.NoError(t, err)
			tracker := engine.(*sizeTrackingEngine)
			require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, &Limits{Storage: StorageLimits{MaxNodes: 10}}))
			begin := func() *storage.BadgerTransaction {
				tx, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace(db))
				_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(db + ":pending"), Properties: map[string]any{"value": "pending"}})
				require.NoError(t, err)
				return tx
			}
			tx := begin()
			defer tx.Rollback()
			tracker.writeMu.Lock()
			locked := true
			defer func() {
				if locked {
					tracker.writeMu.Unlock()
				}
			}()
			done := make(chan error, 1)
			go func() { done <- tracker.CommitTransaction(context.Background(), tx) }()
			// Detached is published at the native prepare entry, after count-only
			// installation and before manager admission, which remains held above.
			require.Eventually(t, func() bool { return !tx.IsActive() }, time.Second, time.Millisecond)
			maxBytes := int64(100000)
			if !fits {
				maxBytes = 1
			}
			require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, &Limits{Storage: StorageLimits{MaxNodes: 10, MaxBytes: maxBytes}}))
			tracker.writeMu.Unlock()
			locked = false
			select {
			case err = <-done:
			case <-time.After(time.Second):
				t.Fatal("admission attempted an exclusive byte upgrade under its own prepared read lease")
			}
			require.ErrorIs(t, err, storage.ErrConflict)
			require.True(t, nornicerrors.IsCommitRolledBack(err))
			require.False(t, tx.IsActive())
			nodes, err := engine.NodeCount()
			require.NoError(t, err)
			require.Zero(t, nodes)
			manager.mu.RLock()
			info := manager.databases[db]
			manager.mu.RUnlock()
			info.sizeMu.RLock()
			require.Empty(t, info.pending)
			require.Equal(t, reservedUsage{}, info.reserved)
			info.sizeMu.RUnlock()
			retry := begin()
			defer retry.Rollback()
			err = tracker.CommitTransaction(context.Background(), retry)
			if fits {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrStorageLimitExceeded)
				require.True(t, nornicerrors.IsCommitRolledBack(err))
			}
			nodes, err = engine.NodeCount()
			require.NoError(t, err)
			if fits {
				require.EqualValues(t, 1, nodes)
			} else {
				require.Zero(t, nodes)
			}
			info.sizeMu.RLock()
			require.True(t, info.sizeInitialized)
			require.Empty(t, info.pending)
			require.Equal(t, reservedUsage{}, info.reserved)
			info.sizeMu.RUnlock()
		})
	}
}
