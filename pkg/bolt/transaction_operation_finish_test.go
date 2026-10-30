package bolt

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransactionOperationFinishJoinsTimeoutCleanupBeforeDisposition(t *testing.T) {
	release := make(chan struct{})
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: release,
	}
	requested := make(chan struct{}, 1)
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(context.Background(), 0, "nornic", executor, nil,
		func(reason transactionTerminalReason, _ string, _ time.Duration, _ error) {
			if reason == transactionTerminalTimeoutCleanupRequested {
				requested <- struct{}{}
			}
		}))
	_, err := lifecycle.claimOperation()
	require.NoError(t, err)
	go lifecycle.expire()
	select {
	case <-requested:
	case <-time.After(time.Second):
		t.Fatal("timeout did not claim pending operation cleanup")
	}

	finished := make(chan error, 1)
	go func() { finished <- lifecycle.finishOperation() }()
	select {
	case <-executor.rollbackEntered:
	case <-time.After(time.Second):
		t.Fatal("operation owner did not enter timeout rollback")
	}
	select {
	case err := <-finished:
		t.Fatalf("operation disposition returned before timeout cleanup: %v", err)
	default:
	}
	close(release)
	require.ErrorIs(t, <-finished, errTransactionTimedOut)
}

func TestTransactionOperationFinishCommitsResponseBeforeLaterIdleExpiry(t *testing.T) {
	release := make(chan struct{})
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: release,
	}
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(
		context.Background(), 0, "nornic", executor, nil, nil))
	_, err := lifecycle.claimOperation()
	require.NoError(t, err)
	require.NoError(t, lifecycle.finishOperation(),
		"operation completion must atomically commit its response disposition")

	go lifecycle.expire()
	select {
	case <-executor.rollbackEntered:
	case <-time.After(time.Second):
		t.Fatal("later idle expiry did not enter rollback")
	}
	close(release)
}
