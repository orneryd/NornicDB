package bolt

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

func TestBoltRunRejectsMemoryPressureBeforeDecodeAndRecoversAfterReset(t *testing.T) {
	var used atomic.Int64
	used.Store(100 << 20)
	policy := memory.NewPolicy(100<<20, 0, 2*time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: used.Load(), Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	var executions atomic.Int64
	exec := &mockExecutor{executeFunc: func(context.Context, string, map[string]any) (*QueryResult, error) {
		executions.Add(1)
		return &QueryResult{Columns: []string{"x"}, Rows: [][]any{{int64(1)}}}, nil
	}}
	conn := &mockConn{}
	session := newTestSession(conn, exec)
	session.server.SetMemoryPolicy(policy)

	// Malformed PackStream must not be decoded when pressure already bars RUN.
	require.NoError(t, session.dispatchMessage(MsgRun, []byte{0xff}))
	require.Zero(t, executions.Load())
	require.True(t, session.failedUntilReset)
	require.Contains(t, string(conn.writeData), "Neo.TransientError.General.MemoryPoolOutOfMemoryError")
	require.Zero(t, policy.Reserved())

	used.Store(1 << 20)
	require.NoError(t, policy.Refresh())
	require.NoError(t, session.dispatchMessage(MsgReset, nil))
	conn.writeData = nil
	require.NoError(t, session.dispatchMessage(MsgRun, gh769RunBody(t, "RETURN 1 AS x")))
	require.EqualValues(t, 1, executions.Load())
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}))
	require.Positive(t, policy.Reserved())
	require.NoError(t, session.handlePull(nil))
	require.Zero(t, policy.Reserved())
}

func TestBoltResultReservationLivesUntilPullOrReset(t *testing.T) {
	policy := memory.NewPolicy(100<<20, 0, 2*time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: 1 << 20, Ceiling: 1 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	exec := &mockExecutor{executeFunc: func(context.Context, string, map[string]any) (*QueryResult, error) {
		return &QueryResult{Columns: []string{"x"}, Rows: [][]any{{"owned-row"}}}, nil
	}}
	session := newTestSession(&mockConn{}, exec)
	session.server.SetMemoryPolicy(policy)
	require.NoError(t, session.handleRun(gh769RunBody(t, "RETURN 'owned-row' AS x")))
	require.Positive(t, policy.Reserved(), "RUN result must retain its reservation until PULL")
	require.NoError(t, session.handlePull(nil))
	require.Zero(t, policy.Reserved())

	require.NoError(t, session.handleRun(gh769RunBody(t, "RETURN 'owned-row' AS x")))
	require.Positive(t, policy.Reserved())
	require.NoError(t, session.handleReset(nil))
	require.Zero(t, policy.Reserved(), "RESET must release an abandoned result")
}
