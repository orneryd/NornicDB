package multidb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionQuotaRateRefillAndCancellation(t *testing.T) {
	manager, db := setupTestManagerWithLimits(t, &Limits{Rate: RateLimits{MaxWritesPerSecond: 1}})
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	tracker := engine.(*sizeTrackingEngine)
	clock := newManualClock(time.Unix(1, 0))
	tracker.checker.writeRateLimiter = newRateLimiterWithClock(1, clock.Now)
	commit := func(ctx context.Context, id string) error {
		t.Helper()
		tx, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
		require.NoError(t, err)
		_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(db + ":" + id)})
		require.NoError(t, err)
		return tracker.CommitTransaction(ctx, tx)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, commit(ctx, "canceled"), context.Canceled)
	require.NoError(t, commit(context.Background(), "a"), "canceled commit did not consume a token")
	require.ErrorIs(t, commit(context.Background(), "b"), ErrRateLimitExceeded)
	clock.Advance(time.Second)
	require.NoError(t, commit(context.Background(), "c"))
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestTransactionQuotaMergeAdmissionDoesNotWaitForWriter(t *testing.T) {
	manager, db := setupTestManager(t)
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)
	tracker := engine.(*sizeTrackingEngine)
	schema := engine.GetSchema()
	require.NoError(t, schema.AddConstraint(storage.Constraint{Name: "quota_key", Type: storage.ConstraintUnique, Label: "Quota", Properties: []string{"id"}}))
	require.NoError(t, storage.RefreshUniqueConstraintValuesForEngine(engine, schema))
	tx, err := manager.inner.(*storage.MemoryEngine).BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace(db))
	require.NoError(t, tx.PrepareMergeKey(context.Background(), "Quota", "id", int64(1)))
	require.True(t, tx.PendingChanges(false).HoldsMergeKey)
	_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID(db + ":merged"), Labels: []string{"Quota"}, Properties: map[string]interface{}{"id": int64(1)}})
	require.NoError(t, err)
	tracker.writeMu.Lock()
	done := make(chan error, 1)
	go func() { done <- tracker.CommitTransaction(context.Background(), tx) }()
	select {
	case err := <-done:
		tracker.writeMu.Unlock()
		require.ErrorIs(t, err, storage.ErrConflict)
	case <-time.After(time.Second):
		tracker.writeMu.Unlock()
		<-done
		t.Fatal("MERGE admission waited behind a writer that may need its key")
	}
}
