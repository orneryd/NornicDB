// Author: Codex, NOR-180 implementation lane, 2026-10-27.
package bolt

import (
	"context"
	"github.com/orneryd/nornicdb/internal/testutil/walterminal"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoltWALTerminalAdmissionCleanup(t *testing.T) {
	for _, phase := range []string{"wire", "result"} {
		for _, closed := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "/open", true: "/closed"}[closed], func(t *testing.T) {
				engine := walterminal.New(t)
				var used atomic.Int64
				used.Store(1)
				policy := memory.NewPolicy(8<<20, 0, time.Minute, func() (memory.Sample, error) { return memory.Sample{Used: used.Load()}, nil }, time.Now)
				require.NoError(t, policy.Refresh())
				mgr := &mockDBManager{stores: map[string]storage.Engine{"nornic": storage.NewNamespacedEngine(engine, "nornic")}, defaultDB: "nornic"}
				server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 8, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
				server.SetMemoryPolicy(policy)
				conn := openBoltTestConn(t, startBoltTestServer(t, server))
				beginExplicitTransaction(t, conn, nil)
				require.NoError(t, SendRun(t, conn, "CREATE (:WalWire)", nil, nil))
				require.NoError(t, ReadSuccess(t, conn))
				require.NoError(t, SendPull(t, conn, nil))
				require.NoError(t, ReadSuccess(t, conn))
				reject := func() error {
					if closed {
						if err := engine.WAL.Close(); err != nil {
							return err
						}
					}
					used.Store(8 << 20)
					return policy.Refresh()
				}
				query := "RETURN 1"
				if phase == "wire" {
					require.NoError(t, reject())
				} else {
					require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "nor180.resultpressure", Signature: "nor180.resultpressure() :: (value :: STRING)", Mode: cypher.ProcedureModeRead, Returns: []cypher.ProcedureColumn{{Name: "value", Type: "STRING"}}}, func(context.Context, *cypher.StorageExecutor, string, []interface{}) (*cypher.ExecuteResult, error) {
						if err := reject(); err != nil {
							return nil, err
						}
						return &cypher.ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{"retained result"}}}, nil
					}))
					query = "CALL nor180.resultpressure() YIELD value RETURN value"
				}
				require.NoError(t, SendRun(t, conn, query, nil, nil))
				code, message, err := AssertFailure(t, conn)
				require.NoError(t, err)
				require.Equal(t, "Neo.TransientError.General.MemoryPoolOutOfMemoryError", code)
				require.Contains(t, message, localization.ApplicationMemoryPressure().Fallback)
				if closed {
					require.Contains(t, message, storage.ErrWALClosed.Error())
					require.Contains(t, message, "WAL tx abort")
				}
				require.Zero(t, walterminal.Count(t, engine))
			})
		}
	}
}

func TestBoltWALTerminalPrimaryStatusMessage(t *testing.T) {
	for _, kind := range []string{"timeout", "memory"} {
		for _, closed := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/open", true: "/closed"}[closed], func(t *testing.T) {
				engine := walterminal.New(t)
				primary := walterminal.Primary(kind)
				walterminal.RejectCommit(engine, primary, closed)
				port := startTransactionLifecycleServerWithStore(t, storage.NewNamespacedEngine(engine, "nornic"))
				conn := openBoltTestConn(t, port)
				require.NoError(t, SendRun(t, conn, "CREATE (:WalWire)", nil, nil))
				code, message, err := AssertFailure(t, conn)
				require.NoError(t, err)
				primaryCode, primaryMessage := nornicerrors.Neo4jStatus(primary)
				require.Equal(t, primaryCode, code)
				require.Contains(t, message, primaryMessage)
				if closed {
					require.Contains(t, message, storage.ErrWALClosed.Error())
					require.Regexp(t, `failed to write WAL tx abort for [^\s:]+:`, message)
				} else {
					require.NotContains(t, message, "WAL tx abort")
				}
				require.Zero(t, walterminal.Count(t, engine))
			})
		}
	}
}
func TestBoltWALTerminalRollbackResponse(t *testing.T) {
	testBoltWALTerminalControlResponse(t, false)
}

func TestBoltWALTerminalResetResponse(t *testing.T) {
	testBoltWALTerminalControlResponse(t, true)
}

func testBoltWALTerminalControlResponse(t *testing.T, reset bool) {
	engine := walterminal.New(t)
	port := startTransactionLifecycleServerWithStore(t, storage.NewNamespacedEngine(engine, "nornic"))
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, nil)
	require.NoError(t, SendRun(t, conn, "CREATE (:WalWire)", nil, nil))
	require.NoError(t, ReadSuccess(t, conn))
	require.NoError(t, SendPull(t, conn, nil))
	require.NoError(t, ReadSuccess(t, conn))
	require.NoError(t, engine.WAL.Close())
	if reset {
		require.NoError(t, SendReset(t, conn))
	} else {
		require.NoError(t, SendRollback(t, conn))
	}
	code, message, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.DatabaseError.Transaction.TransactionRollbackFailed", code)
	require.Contains(t, message, storage.ErrWALClosed.Error())
	require.Zero(t, walterminal.Count(t, engine))
}
