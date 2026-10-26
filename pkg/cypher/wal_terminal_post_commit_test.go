// Author: Claude, NOR-180 post-commit completion contract, 2026-10-30.
package cypher

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A transaction that committed natively and whose post-commit action then
// fails still writes its logical COMMIT record, explicit or implicit alike.
// When that record also fails, the reply keeps the action's failure, the WAL
// failure and the committed truth; when it succeeds, the action's failure is
// the reply.
func TestWALTerminalPostCommitActionFailure(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			name := map[bool]string{false: "implicit", true: "explicit"}[explicit] + map[bool]string{false: "/open", true: "/closed"}[closed]
			t.Run(name, func(t *testing.T) {
				wal, err := storage.NewWAL(t.TempDir(), nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = wal.Close() })
				engine := &walOutcomeEngine{MemoryEngine: newTestMemoryEngine(t), wal: wal}
				store := storage.NewNamespacedEngine(engine, "wal_outcome")
				_, err = store.CreateNode(&storage.Node{ID: "post-commit", Labels: []string{"WalOutcome"}})
				require.NoError(t, err)
				cause := errors.New("post-commit suppression rejected")
				exec := NewStorageExecutor(&rejectingSuppressionEngine{Engine: store, cause: cause})
				// Close the logical WAL right after the native commit, before
				// any post-commit action or the COMMIT record.
				engine.commit = func(ctx context.Context, tx *storage.BadgerTransaction) error {
					err := tx.CommitContext(ctx)
					if err == nil && closed {
						err = wal.Close()
					}
					return err
				}
				ctx := context.Background()
				if explicit {
					_, err = exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "MATCH (n:WalOutcome) SET n.touched = true", nil)
					require.NoError(t, err)
				}
				_, err = exec.Execute(ctx, "MATCH (n:WalOutcome) CALL kp.suppress(n)", nil)
				if explicit {
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "COMMIT", nil)
				}
				require.ErrorIs(t, err, cause, "the post-commit action's failure is the primary")
				if closed {
					require.ErrorIs(t, err, storage.ErrWALClosed, "the COMMIT record is still attempted after the action")
				} else {
					require.NotErrorIs(t, err, storage.ErrWALClosed)
				}
				// The action's failure is this commit's failure on every route,
				// whether or not the COMMIT record also failed.
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionCommitFailed", code)
				require.False(t, exec.HasActiveTransaction())
				if explicit {
					// Native truth: the explicit transaction's write committed.
					rows, err := NewStorageExecutor(store).Execute(ctx, "MATCH (n:WalOutcome {touched: true}) RETURN count(n)", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(1)}}, rows.Rows)
				}
			})
		}
	}
}
