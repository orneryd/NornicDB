package bolt

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingTimeoutRollbackExecutor struct {
	mockExecutor
	rollbackEntered chan struct{}
	rollbackRelease <-chan struct{}
	rollbackErr     error
	rollbackPanic   any
	rollbackOnce    sync.Once
}

func (e *blockingTimeoutRollbackExecutor) BeginTransaction(context.Context, map[string]any) error {
	return nil
}

func (e *blockingTimeoutRollbackExecutor) CommitTransaction(context.Context) error { return nil }

func (e *blockingTimeoutRollbackExecutor) RollbackTransaction(context.Context) error {
	e.rollbackOnce.Do(func() { close(e.rollbackEntered) })
	<-e.rollbackRelease
	if e.rollbackPanic != nil {
		panic(e.rollbackPanic)
	}
	return e.rollbackErr
}

func TestBoltTimeoutResponseJoinsCleanupAndFailsClosedOnCleanupFailure(t *testing.T) {
	for _, tt := range []struct {
		name          string
		rollbackErr   error
		rollbackPanic any
		wantFailure   bool
	}{
		{name: "success", wantFailure: true},
		{name: "error", rollbackErr: errors.New("forced timeout rollback failure")},
		{name: "panic", rollbackPanic: "forced timeout rollback panic"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseRollback := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseRollback)
			executor := &blockingTimeoutRollbackExecutor{
				rollbackEntered: make(chan struct{}),
				rollbackRelease: release,
				rollbackErr:     tt.rollbackErr,
				rollbackPanic:   tt.rollbackPanic,
			}
			port := startControlledTransactionServer(t, executor)
			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, map[string]any{"tx_timeout": int64(100)})
			select {
			case <-executor.rollbackEntered:
			case <-time.After(time.Second):
				t.Fatal("timeout cleanup did not enter rollback")
			}
			require.NoError(t, SendCommit(t, conn))
			assertNoBoltResponseBeforeRelease(t, conn)
			releaseRollback()
			if tt.wantFailure {
				code, _, err := AssertFailure(t, conn)
				require.NoError(t, err)
				require.Equal(t, transactionTimedOutCode, code)
				return
			}
			requireBoltConnectionClosed(t, conn)
		})
	}
}

func assertNoBoltResponseBeforeRelease(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, _, err := ReadMessage(conn)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "unexpected response before cleanup release: %v", err)
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
}
