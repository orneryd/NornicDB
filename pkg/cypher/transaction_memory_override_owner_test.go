package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseTransactionMemorySettingsInheritIndependently(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 100, DatabaseTotalMax: 200})
	value := int64(50)
	SetDatabaseTransactionMemoryLimits("selection", DatabaseTransactionMemoryLimits{TransactionMax: &value})
	database := transactionMemory.database("selection")
	require.Equal(t, int64(50), database.transactionMax.Load())
	require.Equal(t, int64(200), database.pool.limit.Load())
	value = 999
	require.Equal(t, int64(50), database.transactionMax.Load(), "configuration must copy pointer values")
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 150, DatabaseTotalMax: 250})
	require.Equal(t, int64(50), database.transactionMax.Load())
	require.Equal(t, int64(250), database.pool.limit.Load())
	SetDatabaseTransactionMemoryLimits("selection", DatabaseTransactionMemoryLimits{DatabaseTotalMax: new(int64(0))})
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 175, DatabaseTotalMax: 275})
	require.Equal(t, int64(175), database.transactionMax.Load())
	require.Zero(t, database.pool.limit.Load(), "explicit zero stays unlimited")
	SetDatabaseTransactionMemoryLimits("selection", DatabaseTransactionMemoryLimits{})
	require.Same(t, database, transactionMemory.database("selection"))
	require.Equal(t, int64(175), database.transactionMax.Load())
	require.Equal(t, int64(275), database.pool.limit.Load())
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 300, DatabaseTotalMax: 400})
	require.Equal(t, int64(300), database.transactionMax.Load())
	require.Equal(t, int64(400), database.pool.limit.Load())
	SetDatabaseTransactionMemoryLimits("selection", DatabaseTransactionMemoryLimits{TransactionMax: new(int64(-1))})
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 500, DatabaseTotalMax: 600})
	require.Zero(t, database.transactionMax.Load(), "negative Go inputs retain explicit unlimited normalization")
	require.Equal(t, int64(600), database.pool.limit.Load())
}

func TestDatabaseTransactionMemoryOverrideResetRetainsLiveCharges(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 64 << 20, DatabaseTotalMax: 64 << 20})
	executor := &StorageExecutor{}
	tx := &runningTransaction{database: "selection"}
	executor.beginTransactionMemory(tx, nil)
	tracker := tx.memory.Load()
	require.NotNil(t, tracker)
	defer tracker.end()
	require.NoError(t, tracker.root.grow(2<<20))
	database := tracker.database
	globalBefore, databaseBefore := TransactionMemoryPoolUsage("selection")
	SetDatabaseTransactionMemoryLimits("selection", DatabaseTransactionMemoryLimits{DatabaseTotalMax: new(int64(1 << 20))})
	SetDatabaseTransactionMemoryLimits("selection", DatabaseTransactionMemoryLimits{})
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 1 << 20, DatabaseTotalMax: 1 << 20})
	globalAfter, databaseAfter := TransactionMemoryPoolUsage("selection")
	require.Equal(t, globalBefore, globalAfter)
	require.Equal(t, databaseBefore, databaseAfter)
	require.Same(t, database, transactionMemory.database("selection"))
	require.NoError(t, tracker.root.grow(2<<20), "running statement retains admitted ceiling")
	next := &runningTransaction{database: "selection"}
	executor.beginTransactionMemory(next, nil)
	nextTracker := next.memory.Load()
	require.NotNil(t, nextTracker)
	defer nextTracker.end()
	require.Same(t, database, nextTracker.database)
	require.Error(t, nextTracker.root.grow(1), "new inherited ceiling sees all retained charges")
}
