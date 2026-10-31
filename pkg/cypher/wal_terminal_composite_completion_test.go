// Author: Codex, NOR-180 current explicit composite completion, 2026-11-01.
package cypher_test

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWALTerminalCompositeExplicitCompletion(t *testing.T) {
	for _, mode := range []string{"healthy", "wal", "action", "both", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			f := walterminal.NewCompositeTerminal(t)
			ctx := context.Background()
			_, err := f.Executor.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = f.Executor.Execute(ctx, "USE cmp.local CREATE (:WalWire)", nil)
			require.NoError(t, err)
			if mode == "action" || mode == "both" {
				f.ActionError = storage.ErrStorageRestoring
				_, err = f.Executor.Execute(ctx, "USE cmp.local MATCH (n:WalWire) CALL kp.suppress(n)", nil)
				require.NoError(t, err)
			}
			f.CloseOnCommit = mode == "wal" || mode == "both"
			end := "COMMIT"
			expected := int64(1)
			if mode == "rollback" {
				end = "ROLLBACK"
				expected = 0
				require.NoError(t, f.WAL.Close())
			}
			result, err := f.Executor.Execute(ctx, end, nil)
			require.Equal(t, expected, f.Count(t), "native outcome must remain truthful")
			require.False(t, f.Executor.HasActiveTransaction())
			if mode == "healthy" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err, "completion failure must reach current invocation: %+v", result)
			if mode == "wal" || mode == "both" || mode == "rollback" {
				require.ErrorIs(t, err, storage.ErrWALClosed)
			}
			if mode == "action" || mode == "both" {
				require.ErrorIs(t, err, storage.ErrStorageRestoring)
			}
			code, _ := nornicerrors.Neo4jStatus(err)
			if end == "COMMIT" {
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
			}
			require.False(t, (&db.Neo4jError{Code: code}).IsRetriable())
		})
	}
}

func TestWALTerminalCompositeImplicitCompletion(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "wal"}[closed], func(t *testing.T) {
			f := walterminal.NewCompositeTerminal(t)
			f.CloseOnCommit = closed
			result, err := f.Executor.Execute(context.Background(), "USE cmp.local CREATE (:WalWire)", nil)
			require.Equal(t, int64(1), f.Count(t))
			if !closed {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, storage.ErrWALClosed, "implicit completion: %+v", result)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.False(t, (&db.Neo4jError{Code: code}).IsRetriable())
		})
	}
}
