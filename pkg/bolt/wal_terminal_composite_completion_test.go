// Author: Codex, NOR-180 explicit composite wire completion, 2026-11-01.
package bolt

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoltWALTerminalCompositeExplicitCompletion(t *testing.T) {
	for _, mode := range []string{"healthy", "wal", "action", "both", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			f := walterminal.NewCompositeTerminal(t)
			executor := &transactionalBoltQueryExecutorAdapter{boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: f.Executor}}
			conn := openBoltTestConn(t, startControlledTransactionServer(t, executor))
			beginExplicitTransaction(t, conn, nil)
			require.NoError(t, SendRun(t, conn, "USE cmp.local CREATE (:WalWire)", nil, nil))
			require.NoError(t, ReadSuccess(t, conn))
			require.NoError(t, SendPull(t, conn, nil))
			require.NoError(t, ReadSuccess(t, conn))
			if mode == "action" || mode == "both" {
				f.ActionError = storage.ErrStorageRestoring
				require.NoError(t, SendRun(t, conn, "USE cmp.local MATCH (n:WalWire) CALL kp.suppress(n)", nil, nil))
				require.NoError(t, ReadSuccess(t, conn))
				require.NoError(t, SendPull(t, conn, nil))
				require.NoError(t, ReadSuccess(t, conn))
			}
			f.CloseOnCommit = mode == "wal" || mode == "both"
			expected := int64(1)
			if mode == "rollback" {
				expected = 0
				require.NoError(t, f.WAL.Close())
				require.NoError(t, SendRollback(t, conn))
			} else {
				require.NoError(t, SendCommit(t, conn))
			}
			if mode == "healthy" {
				require.NoError(t, ReadSuccess(t, conn))
			} else {
				code, message, err := AssertFailure(t, conn)
				require.NoError(t, err, "terminal completion must not become SUCCESS")
				require.False(t, (&db.Neo4jError{Code: code}).IsRetriable())
				if mode != "rollback" {
					require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
				}
				if mode == "wal" || mode == "both" || mode == "rollback" {
					require.Contains(t, message, storage.ErrWALClosed.Error())
				}
				if mode == "action" || mode == "both" {
					require.Contains(t, message, storage.ErrStorageRestoring.Error())
				}
			}
			require.Equal(t, expected, f.Count(t))
		})
	}
}
