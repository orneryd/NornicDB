package bolt

import (
	"bytes"
	"context"
	"log/slog"
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

func TestTransactionalAdapterRollbackAfterRunError(t *testing.T) {
	for _, testCase := range []struct {
		query             string
		transactionActive bool
	}{
		// A failed statement leaves the transaction open and failed (#683):
		// the ROLLBACK below ends it.
		{"RETURN 1 + {a: 1} AS x", true},
		{"RETURN date('x') AS x", true},
		{"RETURN toInteger([1]) AS x", true},
		{"RETURN range(1, 10, 0) AS x", true},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			adapter := &transactionalBoltQueryExecutorAdapter{
				boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: cypher.NewStorageExecutor(base)},
			}
			require.NoError(t, adapter.BeginTransaction(context.Background(), nil))
			_, err := adapter.Execute(context.Background(), testCase.query, nil)
			require.Error(t, err)
			require.Equal(t, testCase.transactionActive, adapter.inTx)
			require.NoError(t, adapter.RollbackTransaction(context.Background()))
			require.NoError(t, adapter.BeginTransaction(context.Background(), nil))
			_, err = adapter.Execute(context.Background(), "RETURN 1 AS x", nil)
			require.NoError(t, err)
			require.NoError(t, adapter.CommitTransaction(context.Background()))
		})
	}
}

func TestBoltResetAfterExpressionErrorKeepsConnection(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{"nornic": storage.NewNamespacedEngine(base, "nornic")}, defaultDB: "nornic",
	}
	var logs bytes.Buffer
	server := NewWithDatabaseManager(&Config{
		Port: 0, MaxConnections: 8, ReadBufferSize: 8192, WriteBufferSize: 8192,
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	for _, query := range []string{"RETURN 1 + {a: 1} AS x", "RETURN date('x') AS x"} {
		beginExplicitTransaction(t, conn, nil)
		require.NoError(t, SendRun(t, conn, "CREATE (:ResetProbe {name: 'discarded'})", nil, nil))
		require.NoError(t, ReadSuccess(t, conn))
		require.NoError(t, SendPull(t, conn, nil))
		require.NoError(t, ReadSuccess(t, conn))
		require.NoError(t, SendRun(t, conn, query, nil, nil))
		code, _, err := AssertFailure(t, conn)
		require.NoError(t, err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		require.NoError(t, SendReset(t, conn))
		require.NoError(t, ReadSuccess(t, conn))
		require.NoError(t, SendRun(t, conn, "MATCH (n:ResetProbe) RETURN count(n) AS x", nil, nil))
		require.NoError(t, ReadSuccess(t, conn))
		require.NoError(t, SendPull(t, conn, nil))
		row, err := AssertRecord(t, conn)
		require.NoError(t, err)
		require.Equal(t, []any{int64(0)}, row)
		require.NoError(t, ReadSuccess(t, conn))
	}
	require.NotContains(t, logs.String(), "explicit transaction cleanup failed")
	require.NotContains(t, logs.String(), "message handling error")
}
