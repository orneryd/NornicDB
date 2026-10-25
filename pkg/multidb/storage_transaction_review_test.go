package multidb

import (
	"context"
	"fmt"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func stagedQuotaNode(t *testing.T, manager *DatabaseManager, db, id string) *storage.BadgerTransaction {
	t.Helper()
	tx, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace(db))
	_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(db + ":" + id), Labels: []string{"Review"}})
	require.NoError(t, err)
	return tx
}

func TestTransactionQuotaListenerReentry(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(fmt.Sprintf("limited=%v", limited), func(t *testing.T) {
			manager, db := setupTestManager(t)
			if limited {
				require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: 4, MaxBytes: 1 << 20}}))
			}
			engine, err := manager.GetStorage(db)
			require.NoError(t, err)
			tracker := engine.(*sizeTrackingEngine)
			base := manager.inner.(*storage.MemoryEngine)
			completed := make(chan error, 1)
			callbackFailed := false
			base.OnNodeCreated(func(node *storage.Node) {
				if node.ID != storage.NodeID(db+":outer") {
					return
				}
				go func() {
					size, _, _ := manager.GetStorageSize(db)
					if size <= 0 {
						completed <- fmt.Errorf("listener saw stale byte accounting: %d", size)
						return
					}
					_, err := engine.CreateNode(&storage.Node{ID: "child", Labels: []string{"Review"}})
					completed <- err
				}()
				select {
				case err := <-completed:
					if err != nil {
						callbackFailed = true
						t.Error(err)
					}
				case <-time.After(time.Second):
					callbackFailed = true
					t.Error("listener re-entry blocked on admission/publication locks")
				}
			})
			require.NoError(t, tracker.CommitTransaction(context.Background(), stagedQuotaNode(t, manager, db, "outer")))
			storage.WaitForListeners(base)
			if callbackFailed {
				select {
				case <-completed:
				case <-time.After(time.Second):
					t.Fatal("re-entry did not finish after commit returned")
				}
			}
			count, err := engine.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 2, count)
		})
	}
}

func TestTransactionQuotaAdmissionCancellationWhileWaiting(t *testing.T) {
	manager, db := setupTestManagerWithLimits(t, &Limits{Storage: StorageLimits{MaxNodes: 5}})
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	tracker := engine.(*sizeTrackingEngine)
	tx := stagedQuotaNode(t, manager, db, "canceled")
	tracker.writeMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); done <- tracker.CommitTransaction(ctx, tx) }()
	<-started
	<-ctx.Done()
	var commitErr error
	select {
	case commitErr = <-done:
		tracker.writeMu.Unlock()
	case <-time.After(time.Second):
		tracker.writeMu.Unlock()
		commitErr = <-done
		t.Error("cancellation did not interrupt admission wait")
	}
	require.ErrorIs(t, commitErr, context.DeadlineExceeded)
	require.True(t, nornicerrors.IsCommitRolledBack(commitErr), "pre-publication cancellation has a known rollback outcome")
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestTransactionQuotaUnlimitedWritersDoNotWaitForListener(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	tracker := engine.(*sizeTrackingEngine)
	firstTx := stagedQuotaNode(t, manager, db, "first")
	secondTx := stagedQuotaNode(t, manager, db, "second")
	entered, release := make(chan struct{}), make(chan struct{})
	manager.inner.(*storage.MemoryEngine).OnNodeCreated(func(node *storage.Node) {
		if node.ID == storage.NodeID(db+":first") {
			close(entered)
			<-release
		}
	})
	firstDone := make(chan error, 1)
	go func() { firstDone <- tracker.CommitTransaction(context.Background(), firstTx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("first callback never started")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- tracker.CommitTransaction(context.Background(), secondTx) }()
	select {
	case err := <-secondDone:
		close(release)
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		require.NoError(t, <-secondDone)
		t.Error("unlimited disjoint writer serialized behind listener")
	}
	require.NoError(t, <-firstDone)
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestTransactionQuotaWireStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limits Limits
		code   string
	}{
		{"capacity", Limits{Storage: StorageLimits{MaxNodes: 1}}, "Neo.ClientError.General.ResourceLimit"},
		{"rate", Limits{Rate: RateLimits{MaxWritesPerSecond: 1}}, "Neo.TransientError.General.ResourceExhaustion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, db := setupTestManagerWithLimits(t, &tc.limits)
			engine, err := manager.GetStorage(db)
			require.NoError(t, err)
			tracker := engine.(*sizeTrackingEngine)
			require.NoError(t, tracker.CommitTransaction(context.Background(), stagedQuotaNode(t, manager, db, "first")))
			err = tracker.CommitTransaction(context.Background(), stagedQuotaNode(t, manager, db, "rejected"))
			require.Error(t, err)
			for _, mapper := range []func(error) (string, string){nornicerrors.Neo4jStatus, nornicerrors.Neo4jCommitStatus} {
				code, _ := mapper(err)
				require.Equal(t, tc.code, code)
			}
			require.True(t, nornicerrors.IsCommitRolledBack(err))
		})
	}
}
