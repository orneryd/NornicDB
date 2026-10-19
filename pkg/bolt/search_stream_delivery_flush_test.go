package bolt

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type pageDeliveryFlushExecutor struct {
	QueryExecutor
	flushErr error
}

func (e *pageDeliveryFlushExecutor) Flush() error { return e.flushErr }

func TestSearchStreamDeliveryBoltDeferredFlushFailure(t *testing.T) {
	session, _, svc := newPageDeliverySession(t, 1)
	session.executor = &pageDeliveryFlushExecutor{QueryExecutor: session.executor, flushErr: errors.New("synthetic deferred flush failure")}
	require.NoError(t, session.handleBegin(encodePackStreamMap(map[string]any{})))
	require.NoError(t, session.handleRun(buildRunMessageData("CREATE (:DeferredFlushFixture)", nil, nil)))
	require.True(t, session.pendingFlush)
	require.NoError(t, session.handleRun(buildRunMessageData("CALL db.retrieve({mode:'id',n:1})", nil, nil)))
	require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(-1)})))
	require.True(t, session.failedUntilReset)
	_, err := startReplacementPage(svc)
	require.NoError(t, err, "the final PULL must retain its delivery batch until deferred flush failure cleanup")
}
