package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHasPendingTransactionWritesUsesTransactionOperationState(t *testing.T) {
	var nilExecutor *StorageExecutor
	require.False(t, nilExecutor.HasPendingTransactionWrites())

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	require.False(t, exec.HasPendingTransactionWrites())
	_, err := exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	require.False(t, exec.HasPendingTransactionWrites())
	_, err = exec.Execute(context.Background(), "CREATE (:PendingWrite {id: 'write'})", nil)
	require.NoError(t, err)
	require.True(t, exec.HasPendingTransactionWrites())
	_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
	require.False(t, exec.HasPendingTransactionWrites())
}

func TestHasPendingTransactionWritesFailsSafeForUnknownTransaction(t *testing.T) {
	exec := &StorageExecutor{txContext: &TransactionContext{active: true, tx: struct{}{}}}
	require.True(t, exec.HasPendingTransactionWrites())
}
