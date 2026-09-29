package bolt

// gh769_autocommit_failure_ignores_pipeline_test.go — regression pins for
// #769: a FAILURE on an auto-commit RUN poisons the connection until RESET
// per the Bolt spec, so a pipelined PULL (or any queued message) gets
// IGNORED instead of an empty SUCCESS. The empty-SUCCESS divergence
// desynchronized drivers under concurrent auto-commit MERGE conflicts:
// clients hung forever or failed with "Expected structure, found marker 00".

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func gh769RunBody(t *testing.T, query string) []byte {
	t.Helper()
	body := encodePackStreamStringInto(nil, query)
	body = encodePackStreamMapInto(body, map[string]any{})
	return encodePackStreamMapInto(body, map[string]any{})
}

func TestGh769_AutocommitRunFailurePoisonsUntilReset(t *testing.T) {
	exec := &mockExecutor{executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
		if query == "FAIL_ME" {
			return nil, errors.New("commit failed: conflict detected: node n changed after transaction start")
		}
		return &QueryResult{Columns: []string{"x"}, Rows: [][]any{{int64(1)}}}, nil
	}}
	conn := &mockConn{}
	session := newTestSession(conn, exec)

	// Auto-commit RUN fails: exactly one FAILURE reply.
	require.NoError(t, session.dispatchMessage(MsgRun, gh769RunBody(t, "FAIL_ME")))
	require.True(t, session.failedUntilReset, "a failed auto-commit RUN must require RESET")
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgFailure}), "FAILURE reply expected, got %x", conn.writeData)

	// The pipelined PULL must be answered with IGNORED, not an empty SUCCESS.
	before := len(conn.writeData)
	require.NoError(t, session.dispatchMessage(MsgPull, nil))
	ignored := conn.writeData[before:]
	require.Equal(t, []byte{0x00, 0x02, 0xB0, MsgIgnored, 0x00, 0x00}, ignored,
		"queued PULL after a failed RUN must get IGNORED, got %x", ignored)

	// Any further message before RESET is ignored too.
	before = len(conn.writeData)
	require.NoError(t, session.dispatchMessage(MsgRun, gh769RunBody(t, "RETURN 1 AS x")))
	require.Equal(t, []byte{0x00, 0x02, 0xB0, MsgIgnored, 0x00, 0x00}, conn.writeData[before:],
		"a RUN before RESET must get IGNORED, got %x", conn.writeData[before:])

	// RESET restores the connection.
	conn.writeData = nil
	require.NoError(t, session.dispatchMessage(MsgReset, nil))
	require.False(t, session.failedUntilReset)
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}), "RESET success expected, got %x", conn.writeData)

	// The same connection then serves statements normally.
	conn.writeData = nil
	require.NoError(t, session.dispatchMessage(MsgRun, gh769RunBody(t, "RETURN 1 AS x")))
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}), "RUN after RESET must succeed, got %x", conn.writeData)
	require.NotContains(t, string(conn.writeData), string(byte(0xB0))+string(byte(MsgIgnored)))
}

func TestGh769_SuccessfulAutocommitStillFlowsWithoutIgnored(t *testing.T) {
	exec := &mockExecutor{executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
		return &QueryResult{Columns: []string{"x"}, Rows: [][]any{{int64(1)}}}, nil
	}}
	conn := &mockConn{}
	session := newTestSession(conn, exec)

	require.NoError(t, session.handleRun(gh769RunBody(t, "RETURN 1 AS x")))
	require.False(t, session.failedUntilReset)
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}))

	// PULL after a successful RUN streams normally: RECORD then SUCCESS.
	conn.writeData = nil
	require.NoError(t, session.handlePull(nil))
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgRecord}), "expected a RECORD, got %x", conn.writeData)
	require.True(t, bytes.Contains(conn.writeData, []byte{0xB1, MsgSuccess}), "expected PULL SUCCESS, got %x", conn.writeData)
}
