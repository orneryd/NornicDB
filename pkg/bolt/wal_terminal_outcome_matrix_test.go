// Author: Claude, NOR-180 terminal outcome matrix (Bolt), 2026-10-30.
// Self-contained so it runs unchanged on release as a red-first overlay.
package bolt

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// boltMatrixEngine has a logical WAL independent of native storage, so the
// matrix can fail the WAL alone, and hooks on native begin and commit.
type boltMatrixEngine struct {
	*storage.MemoryEngine
	wal    *storage.WAL
	begin  error
	commit func(context.Context, *storage.BadgerTransaction) error
}

func (e *boltMatrixEngine) GetWAL() *storage.WAL { return e.wal }
func (e *boltMatrixEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	if e.begin != nil {
		return nil, e.begin
	}
	return e.MemoryEngine.BeginTransaction()
}
func (e *boltMatrixEngine) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	if e.commit != nil {
		return e.commit(ctx, tx)
	}
	return tx.CommitContext(ctx)
}

// boltMatrixSuppression rejects the post-commit manual-suppression action.
type boltMatrixSuppression struct {
	storage.Engine
	cause error
}

func (e *boltMatrixSuppression) GetInnerEngine() storage.Engine { return e.Engine }
func (e *boltMatrixSuppression) SetManualSuppression(storage.StoredEntityKind, string, bool) error {
	return e.cause
}

// boltMatrixPrimary is a statement failure with its own Neo4j status.
type boltMatrixPrimary struct{ msg string }

func (e *boltMatrixPrimary) Error() string         { return e.msg }
func (e *boltMatrixPrimary) BoltErrorCode() string { return boltMatrixPrimaryCode }

const boltMatrixPrimaryCode = "Neo.TransientError.General.MemoryPoolOutOfMemoryError"

// boltMatrixBeginRejector starts the real transaction, stages a write, then
// rejects BEGIN, so the server must compensate.
type boltMatrixBeginRejector struct {
	*transactionalBoltQueryExecutorAdapter
	before func()
	cause  error
}

func (a *boltMatrixBeginRejector) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	if err := a.transactionalBoltQueryExecutorAdapter.BeginTransaction(ctx, metadata); err != nil {
		return err
	}
	if _, err := a.transactionalBoltQueryExecutorAdapter.Execute(ctx, "CREATE (:WalMatrix {begin: true})", nil); err != nil {
		return err
	}
	a.before()
	return a.cause
}

var boltMatrixProcedureID atomic.Uint64

// The required Neo4j status per cell (Neo4j 5.26 Status.Transaction for the
// generic control failures; a failure with its own status keeps it).
type boltMatrixCell struct {
	stage    string // statement, commit, post-commit, rollback, reset, begin, implicit-begin
	explicit bool
	closed   bool
	code     string // "" = the operation succeeds
	native   int64  // committed writes after the cell
}

func boltMatrixCells() []boltMatrixCell {
	var cells []boltMatrixCell
	for _, explicit := range []bool{false, true} {
		for _, closed := range []bool{false, true} {
			cells = append(cells,
				boltMatrixCell{"statement", explicit, closed, boltMatrixPrimaryCode, 0},
				boltMatrixCell{"commit", explicit, closed, "Neo.DatabaseError.Transaction.TransactionCommitFailed", 0},
				boltMatrixCell{"post-commit", explicit, closed, "Neo.DatabaseError.Transaction.TransactionCommitFailed", 1},
			)
			if !explicit {
				cells = append(cells, boltMatrixCell{"implicit-begin", explicit, closed, "Neo.DatabaseError.Transaction.TransactionStartFailed", 0})
			}
			if explicit {
				rollback := ""
				if closed {
					rollback = "Neo.DatabaseError.Transaction.TransactionRollbackFailed"
				}
				cells = append(cells,
					boltMatrixCell{"rollback", explicit, closed, rollback, 0},
					boltMatrixCell{"reset", explicit, closed, rollback, 0},
					boltMatrixCell{"begin", explicit, closed, "Neo.DatabaseError.Transaction.TransactionStartFailed", 0},
				)
			}
		}
	}
	return cells
}

func TestBoltWALTerminalOutcomeMatrix(t *testing.T) {
	for _, cell := range boltMatrixCells() {
		name := fmt.Sprintf("%s/explicit=%v/closed=%v", cell.stage, cell.explicit, cell.closed)
		t.Run(name, func(t *testing.T) {
			wal, err := storage.NewWAL(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wal.Close() })
			engine := &boltMatrixEngine{MemoryEngine: fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), wal: wal}
			store := storage.NewNamespacedEngine(engine, "nornic")
			_, err = store.CreateNode(&storage.Node{ID: "matrix-seed", Labels: []string{"WalMatrixSeed"}})
			require.NoError(t, err)
			closeWAL := func() {
				if cell.closed {
					require.NoError(t, wal.Close())
				}
			}
			var backing storage.Engine = store
			causeText := ""
			query := "CREATE (:WalMatrix {m: true})"
			switch cell.stage {
			case "statement":
				name := fmt.Sprintf("nor180.matrixbolt%d", boltMatrixProcedureID.Add(1))
				causeText = "matrix statement failure"
				require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
					func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
						if _, err := exec.Execute(ctx, "CREATE (:WalMatrix {m: true})", nil); err != nil {
							return nil, err
						}
						closeWAL()
						return nil, &boltMatrixPrimary{causeText}
					}))
				query = "CALL " + name + "() YIELD value RETURN value"
			case "commit":
				causeText = "matrix native commit failure"
				engine.commit = func(_ context.Context, tx *storage.BadgerTransaction) error {
					closeWAL()
					_ = tx.Rollback()
					return errors.New(causeText)
				}
			case "post-commit":
				causeText = "matrix post-commit action rejected"
				backing = &boltMatrixSuppression{Engine: store, cause: errors.New(causeText)}
				engine.commit = func(ctx context.Context, tx *storage.BadgerTransaction) error {
					err := tx.CommitContext(ctx)
					if err == nil {
						closeWAL()
					}
					return err
				}
				query = "MATCH (n:WalMatrixSeed) SET n.m = true WITH n CALL kp.suppress(n)"
			case "begin":
				causeText = "matrix begin rejected"
			case "implicit-begin":
				// Open: the native snapshot cannot start. Closed: the snapshot
				// starts and the WAL begin record cannot be written.
				causeText = "matrix implicit begin rejected"
				if cell.closed {
					causeText = storage.ErrWALClosed.Error()
					closeWAL()
				} else {
					engine.begin = errors.New(causeText)
				}
			}
			exec := cypher.NewStorageExecutor(backing)
			adapter := &transactionalBoltQueryExecutorAdapter{boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: exec}}
			var server QueryExecutor = adapter
			if cell.stage == "begin" {
				server = &boltMatrixBeginRejector{transactionalBoltQueryExecutorAdapter: adapter, before: closeWAL, cause: errors.New(causeText)}
			}
			conn := openBoltTestConn(t, startControlledTransactionServer(t, server))

			var code, message string
			fail := func() {
				var err error
				code, message, err = AssertFailure(t, conn)
				require.NoError(t, err)
			}
			pull := func() {
				require.NoError(t, SendPull(t, conn, nil))
				for {
					kind, _, err := ReadMessage(conn)
					require.NoError(t, err)
					if kind == MsgSuccess {
						return
					}
					require.Equal(t, byte(MsgRecord), kind)
				}
			}
			switch {
			case cell.stage == "begin":
				require.NoError(t, SendBegin(t, conn, nil))
				fail()
			case !cell.explicit:
				require.NoError(t, SendRun(t, conn, query, nil, nil))
				fail()
			default:
				beginExplicitTransaction(t, conn, nil)
				require.NoError(t, SendRun(t, conn, query, nil, nil))
				if cell.stage == "statement" {
					fail()
					break
				}
				require.NoError(t, ReadSuccess(t, conn))
				pull()
				switch cell.stage {
				case "commit", "post-commit":
					require.NoError(t, SendCommit(t, conn))
					fail()
				case "rollback", "reset":
					closeWAL()
					if cell.stage == "rollback" {
						require.NoError(t, SendRollback(t, conn))
					} else {
						require.NoError(t, SendReset(t, conn))
					}
					if cell.code == "" {
						require.NoError(t, ReadSuccess(t, conn))
					} else {
						fail()
					}
				}
			}
			// Native truth first, then the cause, then the required status.
			truth := "MATCH (n:WalMatrix {m: true}) RETURN count(n)"
			if cell.stage == "post-commit" {
				truth = "MATCH (n:WalMatrixSeed {m: true}) RETURN count(n)"
			}
			rows, err := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine.MemoryEngine, "nornic")).Execute(context.Background(), truth, nil)
			require.NoError(t, err)
			require.Equal(t, cell.native, rows.Rows[0][0], "native commit truth")
			if cell.code == "" {
				return
			}
			if causeText != "" {
				require.Contains(t, message, causeText, "the primary cause is retained")
			}
			// An explicit statement failure only marks its transaction failed;
			// the abort record is attempted by the later ROLLBACK, RESET or COMMIT.
			if cell.closed && cell.stage != "begin" && !(cell.stage == "statement" && cell.explicit) {
				require.True(t, strings.Contains(message, "WAL tx"), "the WAL record failure is reported: %s", message)
			}
			require.Equal(t, cell.code, code, "required Neo4j status")
		})
	}
}
