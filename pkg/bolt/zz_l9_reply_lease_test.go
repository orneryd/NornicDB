package bolt

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/stretchr/testify/require"
)

type l9ObservingBoltConn struct {
	mockConn
	reserved func() int64
	observed []int64
}

func (conn *l9ObservingBoltConn) Write(buf []byte) (int, error) {
	conn.observed = append(conn.observed, conn.reserved())
	return conn.mockConn.Write(buf)
}

func TestL9BoltSuccessPrecedesRequestLeaseRelease(t *testing.T) {
	policy := memory.NewPolicy(100<<20, 0, time.Second, func() (memory.Sample, error) {
		return memory.Sample{Used: 1 << 20}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	conn := &l9ObservingBoltConn{reserved: policy.Reserved}
	executor := &mockExecutor{executeFunc: func(context.Context, string, map[string]any) (*QueryResult, error) {
		return &QueryResult{Columns: []string{"ok"}, Rows: [][]any{{int64(1)}}}, nil
	}}
	session := newTestSession(conn, executor)
	session.server.SetMemoryPolicy(policy)
	require.NoError(t, session.handleRun(gh769RunBody(t, "RETURN 1 AS ok")))
	resultBytes := policy.Reserved()
	require.Positive(t, resultBytes)
	conn.observed = nil

	const wireForecast = 64 << 10
	body := append([]byte{0xB1, MsgPull}, encodePackStreamMap(map[string]any{"n": int64(-1)})...)
	conn.readData = append([]byte{0, byte(len(body))}, body...)
	conn.readData = append(conn.readData, 0, 0)
	msg, _, err := session.readWireMessage(nil)
	require.NoError(t, err)
	require.Equal(t, int64(wireForecast)+resultBytes, policy.Reserved())
	require.NoError(t, session.processMessage(msg))
	t.Logf("result=%d reserved at reply write=%v after dispatch=%d", resultBytes, conn.observed, policy.Reserved())
	require.NotEmpty(t, conn.observed)
	require.Contains(t, conn.observed, int64(wireForecast)+resultBytes)
	require.Zero(t, policy.Reserved())
}
