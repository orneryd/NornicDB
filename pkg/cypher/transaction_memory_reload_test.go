package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionMemoryPoolReloadSnapshotsRunningStatement(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(64<<20, TransactionMemoryLimits{DatabaseTotalMax: 64 << 20})
	tx := &runningTransaction{database: "nornic"}
	executor := &StorageExecutor{}
	executor.beginTransactionMemory(tx, nil)
	tracker := tx.memory.Load()
	require.NotNil(t, tracker)
	defer tracker.end()
	require.NoError(t, tracker.root.grow(2<<20))
	SetDatabaseTransactionMemoryLimits("nornic", TransactionMemoryLimits{DatabaseTotalMax: 1 << 20})
	require.NoError(t, tracker.root.grow(2<<20), "a running statement must retain its original database pool ceiling")
	ConfigureTransactionMemory(1<<20, TransactionMemoryLimits{})
	require.NoError(t, tracker.root.grow(2<<20), "a running statement must retain its original process pool ceiling")
	next := &runningTransaction{database: "nornic"}
	executor.beginTransactionMemory(next, nil)
	nextTracker := next.memory.Load()
	require.NotNil(t, nextTracker)
	defer nextTracker.end()
	require.Error(t, nextTracker.root.grow(1), "new statements must enforce the new limit against all existing charges")
}
