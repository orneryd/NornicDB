package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestDeindexCleanupJob_AdditionalBranches(t *testing.T) {
	t.Run("RunOnce scan error on closed engine", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		require.NoError(t, engine.Close())

		job := NewDeindexCleanupJob(engine, 0)
		_, err = job.RunOnce(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "scan pending work items")
	})

	t.Run("RunOnce returns context canceled mid-loop", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
			return enqueueWorkItemInTxn(txn, "test:ctx-cancel", "NODE")
		}))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		job := NewDeindexCleanupJob(engine, 0)
		processed, err := job.RunOnce(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, processed)
	})

	t.Run("processWorkItem deletes when catalog missing", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		job := NewDeindexCleanupJob(engine, 0)
		item := &DeindexWorkItem{
			WorkItemID: "deindex:test:missing-cat",
			TargetID:   "test:missing-cat",
			Status:     "pending",
		}
		require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
			data, err := msgpack.Marshal(item)
			if err != nil {
				return err
			}
			return txn.Set(deindexWorkItemKey(item.WorkItemID), data)
		}))

		require.NoError(t, job.processWorkItem(item))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(deindexWorkItemKey(item.WorkItemID))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("processWorkItem deletes when catalog already deindexed", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		job := NewDeindexCleanupJob(engine, 0)
		item := &DeindexWorkItem{
			WorkItemID: "deindex:test:already",
			TargetID:   "test:already",
			Status:     "pending",
		}
		require.NoError(t, engine.PutIndexEntryCatalog(item.TargetID, &IndexEntryCatalog{
			TargetID:    item.TargetID,
			TargetScope: "NODE",
			Deindexed:   true,
		}))
		require.NoError(t, engine.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(txn *badger.Txn) error {
			data, err := msgpack.Marshal(item)
			if err != nil {
				return err
			}
			return txn.Set(deindexWorkItemKey(item.WorkItemID), data)
		}))

		require.NoError(t, job.processWorkItem(item))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(deindexWorkItemKey(item.WorkItemID))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("retryWorkItem marks failed after many retries", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		job := NewDeindexCleanupJob(engine, 0)
		item := &DeindexWorkItem{
			WorkItemID: "deindex:test:retry",
			TargetID:   "test:retry",
			Status:     "pending",
			RetryCount: 10,
		}
		job.retryWorkItem(item)
		require.Equal(t, "failed", item.Status)
		require.Equal(t, 11, item.RetryCount)
	})
}
