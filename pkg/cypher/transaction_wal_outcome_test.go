// Author: Codex, NOR-180 implementation lane, 2026-10-26.
package cypher

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The WAL is independent of native storage, as allowed by GetWAL. Its failure
// therefore tests terminal bookkeeping without manufacturing a graph write loss.
type walOutcomeEngine struct {
	*storage.MemoryEngine
	wal    *storage.WAL
	commit func(context.Context, *storage.BadgerTransaction) error
}

func (e *walOutcomeEngine) GetWAL() *storage.WAL { return e.wal }

func (e *walOutcomeEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	if e.commit != nil {
		return e.commit(ctx, tx)
	}
	return tx.CommitContext(ctx)
}

func newWALOutcomeExecutor(t testing.TB) (*StorageExecutor, *walOutcomeEngine, storage.Engine) {
	t.Helper()
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wal.Close()) })
	engine := &walOutcomeEngine{MemoryEngine: newTestMemoryEngine(t), wal: wal}
	store := storage.NewNamespacedEngine(engine, "wal_outcome")
	return NewStorageExecutor(store), engine, storage.NewNamespacedEngine(engine.MemoryEngine, "wal_outcome")
}

func requireWALOutcomeCount(t *testing.T, store storage.Engine, want int64) {
	t.Helper()
	result, err := NewStorageExecutor(store).Execute(context.Background(), "MATCH (n:WalOutcome) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{want}}, result.Rows)
}

func TestWALTerminalExplicitOutcomes(t *testing.T) {
	for _, command := range []string{"COMMIT", "ROLLBACK"} {
		for _, closed := range []bool{false, true} {
			name := command + "/open"
			if closed {
				name = command + "/closed"
			}
			t.Run(name, func(t *testing.T) {
				exec, engine, store := newWALOutcomeExecutor(t)
				ctx := context.Background()
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "CREATE (:WalOutcome)", nil)
				require.NoError(t, err)
				if closed {
					require.NoError(t, engine.wal.Close())
				}
				result, err := exec.Execute(ctx, command, nil)
				if closed {
					require.ErrorIs(t, err, storage.ErrWALClosed)
					require.Nil(t, result, "a failed terminal record cannot return a success response or receipt")
					if command == "COMMIT" {
						require.False(t, nornicerrors.IsCommitRolledBack(err), "native commit succeeded")
						require.Contains(t, err.Error(), "committed")
					}
				} else {
					require.NoError(t, err)
					if command == "COMMIT" {
						receipt, ok := result.Metadata["receipt"].(*storage.Receipt)
						require.True(t, ok)
						require.NotEmpty(t, receipt.TxID)
						require.Greater(t, receipt.WALSeqEnd, receipt.WALSeqStart)
					}
				}
				require.Nil(t, exec.txContext, "native termination must still clear the transaction")
				var want int64
				if command == "COMMIT" {
					want = 1
				}
				requireWALOutcomeCount(t, store, want)
			})
		}
	}
}

func TestWALTerminalNativeFailureRetainsBothCauses(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "open", true: "closed"}[closed], func(t *testing.T) {
			exec, engine, store := newWALOutcomeExecutor(t)
			primary := &storage.ConstraintViolationError{Type: storage.ConstraintUnique}
			engine.commit = func(_ context.Context, tx *storage.BadgerTransaction) error {
				require.NoError(t, tx.Rollback())
				return primary
			}
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE (:WalOutcome)", nil)
			require.NoError(t, err)
			if closed {
				require.NoError(t, engine.wal.Close())
			}
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.ErrorIs(t, err, primary)
			require.True(t, nornicerrors.IsCommitRolledBack(err))
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, nornicerrors.ConstraintValidationFailed, code)
			if closed {
				require.ErrorIs(t, err, storage.ErrWALClosed)
			}
			require.Nil(t, exec.txContext)
			requireWALOutcomeCount(t, store, 0)
		})
	}
}

func TestWALTerminalRollbackRetainsNativeFailure(t *testing.T) {
	exec, engine, _ := newWALOutcomeExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	tx := exec.txContext.tx.(*storage.BadgerTransaction)
	require.NoError(t, tx.Rollback())
	require.NoError(t, engine.wal.Close())
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.ErrorIs(t, err, storage.ErrTransactionClosed)
	require.ErrorIs(t, err, storage.ErrWALClosed)
	// An explicit ROLLBACK's own unclassified failure is a rollback failure
	// (Neo4j 5.26 Status.Transaction.TransactionRollbackFailed), not the
	// statement SyntaxError default.
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.DatabaseError.Transaction.TransactionRollbackFailed", code)
	require.Nil(t, exec.txContext)
}

type walOutcomeProcedureKey struct{}

type walOutcomeProcedure struct {
	wal       *storage.WAL
	failure   error
	write     bool
	terminate bool
}

func registerWALOutcomeProcedure(t *testing.T) {
	t.Helper()
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "nor180.terminal", Signature: "nor180.terminal() :: (value :: INTEGER)",
		Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	}, func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		control := ctx.Value(walOutcomeProcedureKey{}).(*walOutcomeProcedure)
		if control.write {
			if _, err := exec.Execute(ctx, "CREATE (:WalOutcome)", nil); err != nil {
				return nil, err
			}
		}
		if err := control.wal.Close(); err != nil {
			return nil, err
		}
		if control.terminate {
			ctx.Value(ctxKeyRunningTransaction{}).(*runningTransaction).terminated.Store(true)
		}
		return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{int64(1)}}}, control.failure
	}))
}

func TestWALTerminalImplicitOutcomes(t *testing.T) {
	registerWALOutcomeProcedure(t)
	for _, mode := range []string{"commit", "empty", "execution failure", "commit failure", "terminated"} {
		t.Run(mode, func(t *testing.T) {
			exec, engine, store := newWALOutcomeExecutor(t)
			primary := errors.New("NOR-180 primary statement failure")
			control := &walOutcomeProcedure{wal: engine.wal, write: mode != "empty"}
			control.terminate = mode == "terminated"
			if mode == "execution failure" {
				control.failure = primary
			}
			if mode == "commit failure" {
				engine.commit = func(_ context.Context, tx *storage.BadgerTransaction) error {
					require.NoError(t, tx.Rollback())
					return primary
				}
			}
			ctx := context.WithValue(context.Background(), walOutcomeProcedureKey{}, control)
			_, err := exec.Execute(ctx, "CALL nor180.terminal() YIELD value RETURN value", nil)
			require.ErrorIs(t, err, storage.ErrWALClosed)
			if mode == "execution failure" || mode == "commit failure" {
				require.ErrorIs(t, err, primary)
			}
			if mode == "terminated" {
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Transaction.Terminated", code)
			}
			var want int64
			if mode == "commit" {
				want = 1
				require.Contains(t, err.Error(), "committed")
				require.False(t, nornicerrors.IsCommitRolledBack(err))
			}
			requireWALOutcomeCount(t, store, want)
		})
	}
}

func TestWALTerminalScriptAbortRetainsBookkeepingFailure(t *testing.T) {
	registerWALOutcomeProcedure(t)
	exec, engine, store := newWALOutcomeExecutor(t)
	primary := errors.New("NOR-180 script failure")
	ctx := context.WithValue(context.Background(), walOutcomeProcedureKey{}, &walOutcomeProcedure{
		wal: engine.wal, failure: primary, write: true,
	})
	_, err := exec.Execute(ctx, "BEGIN CALL nor180.terminal() YIELD value RETURN value COMMIT", nil)
	require.ErrorIs(t, err, primary)
	require.ErrorIs(t, err, storage.ErrWALClosed)
	requireWALOutcomeCount(t, store, 0)
}

func TestWALTerminalFailedAndTerminatedCommitRetainsBookkeepingFailure(t *testing.T) {
	registerWALOutcomeProcedure(t)
	for _, terminated := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed", true: "terminated"}[terminated], func(t *testing.T) {
			exec, engine, store := newWALOutcomeExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			if terminated {
				_, err = exec.Execute(ctx, "CREATE (:WalOutcome)", nil)
				require.NoError(t, err)
				require.NoError(t, engine.wal.Close())
				exec.txContext.running.terminated.Store(true)
			} else {
				ctx = context.WithValue(ctx, walOutcomeProcedureKey{}, &walOutcomeProcedure{
					wal: engine.wal, failure: errors.New("NOR-180 failed transaction"), write: true,
				})
				_, err = exec.Execute(ctx, "CALL nor180.terminal() YIELD value RETURN value", nil)
				require.Error(t, err)
			}
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.ErrorIs(t, err, storage.ErrWALClosed)
			code, _ := nornicerrors.Neo4jStatus(err)
			wantCode := "Neo.ClientError.Transaction.TransactionMarkedAsFailed"
			if terminated {
				wantCode = "Neo.ClientError.Transaction.Terminated"
			}
			require.Equal(t, wantCode, code)
			require.Nil(t, exec.txContext)
			requireWALOutcomeCount(t, store, 0)
		})
	}
}

func TestWALTerminalInlineEmbeddingFailure(t *testing.T) {
	exec, engine, store := newWALOutcomeExecutor(t)
	primary := errors.New("NOR-180 embedding rejected")
	exec.QueryEmbedderBinding().Store(&queryEmbedderStub{
		embedFn: func(context.Context, string) ([]float32, error) {
			require.NoError(t, engine.wal.Close())
			return nil, primary
		},
		chunkFn: func(text string, _, _ int) ([]string, error) { return []string{text}, nil },
	})
	_, err := exec.Execute(context.Background(), "CREATE (:WalOutcome {content:'terminal outcome'}) WITH EMBEDDING", nil)
	require.ErrorIs(t, err, primary)
	require.ErrorIs(t, err, storage.ErrWALClosed)
	requireWALOutcomeCount(t, store, 0)
}

func TestWALTerminalImplicitSuccessReceipt(t *testing.T) {
	for _, withWAL := range []bool{false, true} {
		t.Run(map[bool]string{false: "without WAL", true: "with WAL"}[withWAL], func(t *testing.T) {
			exec, engine, store := newWALOutcomeExecutor(t)
			if !withWAL {
				engine.wal = nil
			}
			result, err := exec.Execute(context.Background(), "CREATE (:WalOutcome) RETURN 1 AS value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
			if withWAL {
				receipt, ok := result.Metadata["receipt"].(*storage.Receipt)
				require.True(t, ok)
				require.NotEmpty(t, receipt.TxID)
				require.NotEmpty(t, receipt.Hash)
				require.Equal(t, "wal_outcome", receipt.Database)
				require.Greater(t, receipt.WALSeqEnd, receipt.WALSeqStart)
			} else {
				require.Nil(t, result.Metadata["receipt"])
			}
			requireWALOutcomeCount(t, store, 1)
		})
	}
}

func TestWALTerminalTerminatedStatementCleanup(t *testing.T) {
	exec, engine, store := newWALOutcomeExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:WalOutcome)", nil)
	require.NoError(t, err)
	require.NoError(t, engine.wal.Close())
	exec.txContext.running.terminated.Store(true)
	_, err = exec.Execute(ctx, "RETURN 1", nil)
	require.ErrorIs(t, err, storage.ErrWALClosed)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Transaction.Terminated", code)
	require.Nil(t, exec.txContext)
	requireWALOutcomeCount(t, store, 0)
}

func BenchmarkWALTerminalTransactions(b *testing.B) {
	for _, withWAL := range []bool{false, true} {
		for _, mode := range []string{"implicit", "COMMIT", "ROLLBACK"} {
			prefix := "without_wal/"
			if withWAL {
				prefix = "with_wal/"
			}
			b.Run(prefix+mode, func(b *testing.B) {
				exec, engine, _ := newWALOutcomeExecutor(b)
				if !withWAL {
					engine.wal = nil
				}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if mode != "implicit" {
						if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := exec.Execute(ctx, "CREATE (n:WalOutcome) DELETE n", nil); err != nil {
						b.Fatal(err)
					}
					if mode != "implicit" {
						if _, err := exec.Execute(ctx, mode, nil); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
