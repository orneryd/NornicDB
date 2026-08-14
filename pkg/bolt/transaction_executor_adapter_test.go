package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionalAdapterTransactionBranches(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	adapter := &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{
			executor: cypher.NewStorageExecutor(base),
		},
	}

	require.NoError(t, adapter.BeginTransaction(context.Background(), nil))
	require.NoError(t, adapter.BeginTransaction(context.Background(), nil),
		"duplicate adapter begin must preserve the active storage transaction")
	require.False(t, adapter.HasPendingTransactionWrites())
	require.NoError(t, adapter.CommitTransaction(context.Background()))
	require.NoError(t, adapter.CommitTransaction(context.Background()),
		"commit without an active adapter transaction is idempotent")

	result, err := adapter.Execute(context.Background(), "RETURN 1", nil)
	require.NoError(t, err)
	require.Equal(t, [][]any{{int64(1)}}, result.Rows)
}

func TestTransactionalAdapterPropagatesBeginAndCommitErrors(t *testing.T) {
	closed := storage.NewMemoryEngine()
	require.NoError(t, closed.Close())
	beginAdapter := &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{
			executor: cypher.NewStorageExecutor(closed),
		},
	}
	require.Error(t, beginAdapter.BeginTransaction(context.Background(), nil))
	require.False(t, beginAdapter.inTx,
		"a returned StorageExecutor BEGIN error is self-cleaned and must remain recoverable")
	require.NoError(t, beginAdapter.RollbackTransaction(context.Background()))

	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	commitAdapter := &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{
			executor: cypher.NewStorageExecutor(base),
		},
		inTx: true,
	}
	require.ErrorContains(t, commitAdapter.CommitTransaction(context.Background()), "no active transaction")
}
