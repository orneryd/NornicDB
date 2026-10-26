package storage

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
)

func TestAcceptedReceiptTimeoutRetainsTerminalOwnership(t *testing.T) {
	var settled atomic.Int32
	ackErr := errors.New("applied but standby ack failed")
	r, resolve := NewCommitReceipt(func(outcome CommitOutcome) {
		require.Equal(t, CommitApplied, outcome.State)
		require.ErrorIs(t, outcome.Err, ackErr)
		settled.Add(1)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, r.Wait(ctx), context.Canceled)
	require.Zero(t, settled.Load())
	require.NoError(t, resolve.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: errors.New("recovery needed")}))
	require.Zero(t, settled.Load())
	select {
	case <-r.Done():
		t.Fatal("recovery pending is not terminal")
	default:
	}
	require.NoError(t, resolve.Resolve(CommitOutcome{State: CommitApplied, Err: ackErr}))
	require.ErrorIs(t, r.Wait(context.Background()), ackErr)
	require.Equal(t, int32(1), settled.Load())
	require.Error(t, resolve.Resolve(CommitOutcome{State: CommitDiscarded}))
	require.Equal(t, int32(1), settled.Load())
}
func TestAcceptedReceiptSettlesExactlyOnce(t *testing.T) {
	var count atomic.Int32
	var winners atomic.Int32
	r, resolve := NewCommitReceipt(func(CommitOutcome) { count.Add(1) })
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if resolve.Resolve(CommitOutcome{State: CommitDiscarded}) == nil {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, r.Wait(context.Background()))
	require.Equal(t, int32(1), count.Load())
	require.Equal(t, int32(1), winners.Load())
}

func TestAcceptedReceiptPendingPhysicalProgressesToApplied(t *testing.T) {
	var count atomic.Int32
	receipt, owner := NewCommitReceipt(func(CommitOutcome) { count.Add(1) })
	timeout := errors.New("transport commit timeout")
	require.NoError(t, owner.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: timeout}))
	require.Equal(t, CommitRecoveryPending, receipt.WaitPhysicalOutcome(context.Background()).State)
	owner.PublishPhysical(CommitOutcome{State: CommitApplied})
	require.Equal(t, CommitApplied, receipt.WaitPhysicalOutcome(context.Background()).State)
	select {
	case <-receipt.Done():
		t.Fatal("physical application is not full listener/ACK completion")
	default:
	}
	require.NoError(t, owner.Resolve(CommitOutcome{State: CommitApplied}))
	require.NoError(t, receipt.Wait(context.Background()))
	require.Equal(t, int32(1), count.Load())
}
