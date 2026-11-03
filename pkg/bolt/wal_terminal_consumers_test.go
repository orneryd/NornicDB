// Author: Codex, NOR-180 consumer error-flow regression tests, 2026-10-27.
package bolt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var walConsumerProcedureID atomic.Uint64

type walConsumerExecutor struct {
	*transactionalBoltQueryExecutorAdapter
	run   func(context.Context, string, map[string]any) (*QueryResult, error)
	begin func(context.Context, map[string]any) error
}

func newWALConsumerExecutor(engine *walterminal.Engine) *walConsumerExecutor {
	return &walConsumerExecutor{transactionalBoltQueryExecutorAdapter: &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))},
	}}
}
func (e *walConsumerExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	if e.run != nil {
		return e.run(ctx, query, params)
	}
	return e.transactionalBoltQueryExecutorAdapter.Execute(ctx, query, params)
}
func (e *walConsumerExecutor) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	if e.begin != nil {
		return e.begin(ctx, metadata)
	}
	return e.transactionalBoltQueryExecutorAdapter.BeginTransaction(ctx, metadata)
}

// This assertion uses the public structural diagnostic contract, so the same
// test file builds on d663 without depending on a new implementation symbol.
func requireWALConsumerCauses(t *testing.T, err, primary error, closed bool) {
	t.Helper()
	require.ErrorIs(t, err, primary)
	if closed {
		require.ErrorIs(t, err, storage.ErrWALClosed)
		var diagnostic interface{ StatusDiagnostic() localization.Message }
		require.ErrorAs(t, err, &diagnostic)
	}
}

func TestBoltWALTerminalCanceledRunReset(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			engine := walterminal.New(t)
			started := make(chan struct{})
			name := fmt.Sprintf("nor180.cancel%d", walConsumerProcedureID.Add(1))
			require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: name, Signature: name + "() :: (value :: INTEGER)", Mode: cypher.ProcedureModeWrite, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "INTEGER"}}}, func(ctx context.Context, exec *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
				if _, err := exec.Execute(ctx, "CREATE (:WalWire)", nil); err != nil {
					return nil, err
				}
				if closed {
					if err := engine.WAL.Close(); err != nil {
						return nil, err
					}
				}
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}))
			executor := newWALConsumerExecutor(engine)
			observed := make(chan error, 1)
			executor.run = func(ctx context.Context, q string, p map[string]any) (*QueryResult, error) {
				r, err := executor.transactionalBoltQueryExecutorAdapter.Execute(ctx, q, p)
				observed <- err
				return r, err
			}
			conn := openBoltTestConn(t, startControlledTransactionServer(t, executor))
			require.NoError(t, SendRun(t, conn, "CALL "+name+"() YIELD value RETURN value", nil, nil))
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("RUN did not stage its write")
			}
			require.NoError(t, SendReset(t, conn))
			if closed {
				code, message, err := AssertFailure(t, conn)
				require.NoError(t, err)
				require.Equal(t, "Neo.ClientError.Procedure.ProcedureCallFailed", code)
				require.Contains(t, message, context.Canceled.Error())
				require.Contains(t, message, storage.ErrWALClosed.Error())
			}
			require.NoError(t, ReadSuccess(t, conn), "RESET retains its healthy cancellation response after RUN is answered")
			requireWALConsumerCauses(t, <-observed, context.Canceled, closed)
			require.Zero(t, walterminal.Count(t, engine))
		})
	}
}

func TestBoltWALTerminalRunTimeoutKeepsPriorCleanup(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%v", closed), func(t *testing.T) {
			engine := walterminal.New(t)
			executor := newWALConsumerExecutor(engine)
			observed := make(chan error, 1)
			executor.run = func(ctx context.Context, q string, p map[string]any) (*QueryResult, error) {
				if q != "RETURN 1 /* owner cleans timed-out transaction */" {
					return executor.transactionalBoltQueryExecutorAdapter.Execute(ctx, q, p)
				}
				<-ctx.Done()
				// A provider may finish its own storage transaction before returning
				// the RUN error. Lifecycle's later cleanup then has nothing to do.
				cleanup := executor.transactionalBoltQueryExecutorAdapter.RollbackTransaction(context.WithoutCancel(ctx))
				primary := nornicerrors.MarkStatementTimeout(context.DeadlineExceeded, nornicerrors.ErrClientTransactionTimeout)
				err := errors.Join(primary, cleanup)
				observed <- err
				return nil, err
			}
			conn := openBoltTestConn(t, startControlledTransactionServer(t, executor))
			beginExplicitTransaction(t, conn, map[string]any{"tx_timeout": int64(250)})
			require.NoError(t, SendRun(t, conn, "CREATE (:WalWire)", nil, nil))
			require.NoError(t, ReadSuccess(t, conn))
			require.NoError(t, SendPull(t, conn, nil))
			require.NoError(t, ReadSuccess(t, conn))
			if closed {
				require.NoError(t, engine.WAL.Close())
			}
			require.NoError(t, SendRun(t, conn, "RETURN 1 /* owner cleans timed-out transaction */", nil, nil))
			code, message, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Equal(t, transactionTimedOutCode, code)
			require.Contains(t, message, transactionTimedOutMsg)
			if closed {
				require.Contains(t, message, storage.ErrWALClosed.Error())
			} else {
				require.NotContains(t, message, "WAL tx abort")
			}
			requireWALConsumerCauses(t, <-observed, context.DeadlineExceeded, closed)
			require.Zero(t, walterminal.Count(t, engine))
		})
	}
}

func TestBoltWALTerminalBeginCompensationIdentity(t *testing.T) {
	// An unclassified BEGIN failure takes the BEGIN status; a primary with its
	// own status keeps it, also when the compensating rollback fails.
	for _, kind := range []string{"unclassified", "memory"} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%v", kind, closed), func(t *testing.T) {
				engine := walterminal.New(t)
				executor := newWALConsumerExecutor(engine)
				primary := errors.New("provider rejected admitted BEGIN")
				wantCode := "Neo.DatabaseError.Transaction.TransactionStartFailed"
				if kind == "memory" {
					primary = walterminal.Primary("memory")
					wantCode, _ = nornicerrors.Neo4jStatus(primary)
				}
				executor.begin = func(ctx context.Context, m map[string]any) error {
					if err := executor.transactionalBoltQueryExecutorAdapter.BeginTransaction(ctx, m); err != nil {
						return err
					}
					if _, err := executor.transactionalBoltQueryExecutorAdapter.Execute(ctx, "CREATE (:WalWire)", nil); err != nil {
						return err
					}
					if closed {
						if err := engine.WAL.Close(); err != nil {
							return err
						}
					}
					return primary
				}
				serverConn, client := net.Pipe()
				t.Cleanup(func() { _ = serverConn.Close(); _ = client.Close() })
				session := newTestSession(serverConn, executor)
				session.authenticated = true
				done := make(chan error, 1)
				go func() { done <- session.handleMessage() }()
				require.NoError(t, SendMessage(client, append([]byte{0xb1, MsgBegin}, encodePackStreamMap(map[string]any{})...)))
				code, message, err := AssertFailure(t, client)
				require.NoError(t, err)
				require.Equal(t, wantCode, code)
				require.Contains(t, message, primary.Error())
				returned := <-done
				if closed {
					// The actual BEGIN request returned this fatal error after writing
					// its response, so check identity across the response boundary too.
					require.Contains(t, message, storage.ErrWALClosed.Error())
					requireWALConsumerCauses(t, returned, primary, true)
					require.True(t, session.transactionCleanupFailed)
				} else {
					require.NoError(t, returned)
				}
				require.Zero(t, walterminal.Count(t, engine))
			})
		}
	}
}

// A session executor can own cleanup independently of the read it streams.
// This fixture streams a real Cypher result and returns a REAL failed WAL abort
// from its provider-owned transaction after cancellation. It exercises the
// QueryExecutor error contract without falsely declaring a WRITE procedure READ.
func TestBoltWALTerminalLazyConsumers(t *testing.T) {
	for _, mode := range []string{"reset_abandons", "pull_then_reset", "discard"} {
		for _, closed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/closed=%v", mode, closed), func(t *testing.T) {
				engine := walterminal.New(t)
				cleanupExec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
				_, err := cleanupExec.Execute(context.Background(), "BEGIN", nil)
				require.NoError(t, err)
				_, err = cleanupExec.Execute(context.Background(), "CREATE (:WalWire)", nil)
				require.NoError(t, err)
				executor := newWALConsumerExecutor(engine)
				observed := make(chan error, 1)
				produced := make(chan struct{})
				executor.run = func(ctx context.Context, q string, p map[string]any) (*QueryResult, error) {
					r, runErr := executor.transactionalBoltQueryExecutorAdapter.Execute(ctx, q, p)
					close(produced)
					<-ctx.Done()
					if closed {
						if err := engine.WAL.Close(); err != nil {
							return nil, err
						}
					}
					_, cleanupErr := cleanupExec.Execute(cypher.WithTransactionControl(context.WithoutCancel(ctx)), "ROLLBACK", nil)
					err := errors.Join(ctx.Err(), runErr, cleanupErr)
					observed <- err
					return r, err
				}
				conn := openBoltTestConn(t, startControlledTransactionServer(t, executor))
				require.NoError(t, SendRun(t, conn, "UNWIND range(1,1200) AS i RETURN i", nil, nil))
				require.NoError(t, ReadSuccess(t, conn), "real Cypher started streaming")
				if mode == "pull_then_reset" {
					require.NoError(t, SendPull(t, conn, map[string]any{"n": int64(-1)}))
					select {
					case <-produced:
					case <-time.After(5 * time.Second):
						t.Fatal("stream did not produce its rows")
					}
				}
				if mode == "discard" {
					sendDiscard(t, conn, -1)
				} else {
					require.NoError(t, SendReset(t, conn))
				}
				var kind byte
				var metadata map[string]any
				for {
					var data []byte
					kind, data, err = ReadMessage(conn)
					require.NoError(t, err)
					if kind == MsgRecord {
						continue
					}
					if kind != MsgIgnored {
						metadata, _, err = decodePackStreamMap(data, 0)
						require.NoError(t, err)
					}
					break
				}
				if closed {
					require.Equal(t, byte(MsgFailure), kind)
					code, _ := metadata["code"].(string)
					message, _ := metadata["message"].(string)
					// Author: Codex,R5. Cancellation cannot replace the completed
					// provider rollback's owned status in any delivery mode.
					require.Equal(t, "Neo.DatabaseError.Transaction.TransactionRollbackFailed", code)
					require.Contains(t, message, storage.ErrWALClosed.Error())
					require.Contains(t, message, context.Canceled.Error())
				} else if mode == "pull_then_reset" {
					require.Equal(t, byte(MsgIgnored), kind)
				} else {
					require.Equal(t, byte(MsgSuccess), kind)
				}
				if mode == "pull_then_reset" {
					require.NoError(t, ReadSuccess(t, conn))
				}
				requireWALConsumerCauses(t, <-observed, context.Canceled, closed)
				require.False(t, cleanupExec.HasActiveTransaction(), "provider terminal cleanup actually ran")
				require.Zero(t, walterminal.Count(t, engine))
			})
		}
	}
}
