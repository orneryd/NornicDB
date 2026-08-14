package bolt

import (
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltExplicitTransactionTerminalPathsReleaseResources(t *testing.T) {
	tests := []struct {
		name      string
		terminate func(*testing.T, net.Conn)
	}{
		{
			name: "reset",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, SendReset(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			},
		},
		{
			name: "rollback",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, SendRollback(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			},
		},
		{
			name: "goodbye",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, SendGoodbye(t, conn))
			},
		},
		{
			name: "eof",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, conn.Close())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, port := startTransactionLifecycleServer(t)
			baselineReaders := base.ActiveReaders()
			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, nil)
			runExplicitStatement(t, conn,
				"CREATE (r:Repository {repo_id: 'terminal-abandoned'})", nil)
			require.Greater(t, base.ActiveReaders(), baselineReaders,
				"explicit transaction must own a reader before %s", tt.name)

			tt.terminate(t, conn)
			require.Eventually(t, func() bool {
				return base.ActiveReaders() == baselineReaders
			}, time.Second, 10*time.Millisecond,
				"%s must roll back and release the transaction reader", tt.name)

			fresh := openBoltTestConn(t, port)
			records := runBoltQueryAndCollectRecords(t, fresh,
				"MATCH (r:Repository {repo_id: 'terminal-abandoned'}) RETURN count(r)")
			require.Equal(t, [][]any{{int64(0)}}, records,
				"%s must not persist the abandoned write", tt.name)
			runBoltQueryAndCollectRecords(t, fresh,
				"CREATE (r:Repository {repo_id: 'terminal-fresh'})")
		})
	}
}

func TestBoltExplicitTransactionTerminalPathsRecordSinglePersistentWALAbort(t *testing.T) {
	tests := []struct {
		name      string
		metadata  map[string]any
		terminate func(*testing.T, net.Conn)
	}{
		{
			name: "timeout",
			metadata: map[string]any{
				"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
			},
		},
		{
			name: "reset",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, SendReset(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			},
		},
		{
			name: "rollback",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, SendRollback(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			},
		},
		{
			name: "goodbye",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, SendGoodbye(t, conn))
			},
		},
		{
			name: "eof",
			terminate: func(t *testing.T, conn net.Conn) {
				require.NoError(t, conn.Close())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			badger, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			walDir := t.TempDir()
			wal, err := storage.NewWAL(walDir, nil)
			require.NoError(t, err)
			walStore := storage.NewWALEngine(badger, wal)
			t.Cleanup(func() { require.NoError(t, walStore.Close()) })
			store := storage.NewNamespacedEngine(walStore, "nornic")
			port := startTransactionLifecycleServerWithStore(t, store)
			baselineReaders := badger.ActiveReaders()

			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, tt.metadata)
			runExplicitStatement(t, conn,
				"CREATE (r:Repository {repo_id: 'persistent-terminal-abandoned'})", nil)
			require.Greater(t, badger.ActiveReaders(), baselineReaders)
			if tt.terminate != nil {
				tt.terminate(t, conn)
			}
			require.Eventually(t, func() bool {
				return badger.ActiveReaders() == baselineReaders
			}, transactionLifecycleShortCleanupDeadline, 10*time.Millisecond,
				"%s must release the persistent Badger transaction reader", tt.name)

			fresh := openBoltTestConn(t, port)
			records := runBoltQueryAndCollectRecords(t, fresh,
				"MATCH (r:Repository {repo_id: 'persistent-terminal-abandoned'}) RETURN count(r)")
			require.Equal(t, [][]any{{int64(0)}}, records)

			require.NoError(t, wal.Sync())
			entries, err := storage.ReadWALEntriesFromDir(walDir)
			require.NoError(t, err)
			operations := make(map[storage.OperationType]int)
			for _, entry := range entries {
				operations[entry.Operation]++
			}
			require.Equal(t, 1, operations[storage.OpTxBegin], "%s tx_begin count", tt.name)
			require.Equal(t, 1, operations[storage.OpTxAbort], "%s tx_abort count", tt.name)
			require.Zero(t, operations[storage.OpTxCommit], "%s tx_commit count", tt.name)
		})
	}
}
