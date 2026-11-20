package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nor288RejectObserverBound keeps a settlement observer that a regression runs
// in the deciding frame from hanging the test binary; the assertions fail first.
const nor288RejectObserverBound = 10 * time.Second

// NOR-288: the definite-rejection owner has two separate frames. The command's
// native cleanup belongs to the deciding frame, so a transport whose Shutdown
// joins that frame also joins the cleanup. Only the caller's settlement observer
// runs outside it, because that observer may re-enter the same Shutdown. These
// tests establish both halves plus the receipt's decision/delivery order with
// deterministic steps, not by observing that a goroutine eventually unblocks.

// TestCommitReceiptDecisionPrecedesSettlementDelivery pins the receipt contract
// the transport waiters rely on: the terminal outcome is recorded before its
// observer runs, and Done (the delivered settlement) closes only afterwards.
func TestCommitReceiptDecisionPrecedesSettlementDelivery(t *testing.T) {
	var receipt *CommitReceipt
	var delivered int
	var decidedAtDelivery, doneAtDelivery bool
	receipt, owner := NewCommitReceipt(func(CommitOutcome) {
		delivered++
		outcome, _ := receipt.Decision()
		decidedAtDelivery = outcome.State == CommitDiscarded
		select {
		case <-receipt.Done():
			doneAtDelivery = true
		default:
		}
	})
	require.NoError(t, owner.Resolve(CommitOutcome{State: CommitDiscarded, Err: ErrStorageClosed}))
	require.True(t, decidedAtDelivery, "the accepted decision is recorded before its observer runs")
	require.False(t, doneAtDelivery, "Done closes only after the observer returns")
	require.Equal(t, 1, delivered, "one terminal outcome is delivered once")
	select {
	case <-receipt.Done():
	default:
		t.Fatal("Done must close once the observer has returned")
	}
	outcome := receipt.Outcome()
	require.Equal(t, CommitDiscarded, outcome.State)
	require.ErrorIs(t, outcome.Err, ErrStorageClosed)
	require.Error(t, owner.Resolve(CommitOutcome{State: CommitApplied}), "a terminal receipt keeps its one outcome")
}

// TestCommitReceiptDecisionSignalRearmsWhilePending pins the observation
// contract the transport waiter uses instead of Done, with no waiting:
//
//   - a recovery-pending recording is not a decision: it closes the channel a
//     prior snapshot returned and rearms the receipt for the next recording;
//   - the terminal recording closes the rearms channel and is observable while
//     its foreign observer is still blocked, so a joined transport waiter leaves
//     on the decision;
//   - Done stays open until that observer returns.
func TestCommitReceiptDecisionSignalRearmsWhilePending(t *testing.T) {
	entered := make(chan struct{})
	permit := make(chan struct{})
	receipt, owner := NewCommitReceipt(func(CommitOutcome) {
		close(entered)
		select {
		case <-permit:
		case <-time.After(nor288RejectObserverBound):
		}
	})
	t.Cleanup(func() {
		select {
		case <-permit:
		default:
			close(permit)
		}
	})
	outcome, pending := receipt.Decision()
	require.Equal(t, CommitRecoveryPending, outcome.State)

	require.NoError(t, owner.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: ErrStorageClosed}))
	select {
	case <-pending:
	default:
		t.Fatal("a recovery-pending recording must wake the observer of the state it replaced")
	}
	outcome, recorded := receipt.Decision()
	require.Equal(t, CommitRecoveryPending, outcome.State, "a pending observation is not a decision")
	select {
	case <-recorded:
		t.Fatal("a pending recording must rearm the observer instead of ending it")
	default:
	}

	discarded := CommitOutcome{State: CommitDiscarded, Err: ErrStorageClosed}
	settle, decided, err := owner.claim(discarded)
	require.NoError(t, err)
	require.True(t, decided)
	// The definite-rejection composition: recorded here, delivered from a frame
	// of the receipt's own.
	go owner.deliverTerminal(settle, discarded)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the detached settlement observer did not start")
	}
	select {
	case <-recorded:
	default:
		t.Fatal("the terminal decision must be observable before its observer returns")
	}
	outcome, terminal := receipt.Decision()
	require.Equal(t, CommitDiscarded, outcome.State)
	require.ErrorIs(t, outcome.Err, ErrStorageClosed)
	select {
	case <-terminal:
	default:
		t.Fatal("the recorded terminal decision must keep its signal closed")
	}
	select {
	case <-receipt.Done():
		t.Fatal("a detached delivery closed Done without waiting for its observer")
	default:
	}
	close(permit)
	select {
	case <-receipt.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done did not close after the observer returned")
	}
	require.Equal(t, CommitDiscarded, receipt.Outcome().State)
	require.Error(t, owner.Resolve(CommitOutcome{State: CommitApplied}), "the detached delivery is the only terminal outcome")
}

// acceptedRejectFixture is one accepted command whose native frame is still
// staged: the state a transport's definite rejection acts on.
//
// Acceptance retains an extra authority hold that only settlement releases. The
// cleanup below is registered unconditionally, after acceptance, so it runs
// before the engine close in LIFO order even when an assertion ends the test
// before the rejection does; otherwise that close waits for a hold nobody
// releases and the whole package times out instead of reporting the assertion.
func acceptedRejectFixture(t *testing.T, nodeID NodeID) (*BadgerEngine, *AcceptedEntityCommitHandle, *PreparedEntityCommit) {
	t.Helper()
	b := newTestEngine(t)
	tx, err := b.BeginTransaction(t.Context(), OrdinaryBegin)
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(t.Context(), &Node{ID: nodeID})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	h, _, err := AcceptEntityCommit(context.Background(), b, EntityTransactionRequest{Prepared: p})
	require.NoError(t, err)
	t.Cleanup(h.Settled)
	return b, h, p
}

// TestAcceptedRejectRetiresFrameBeforeItsNotification establishes the cleanup
// and the outcome a transport's Shutdown must be able to rely on: the native
// frame and its accepted authority are retired in the deciding frame, the
// settlement observer sees that retirement, and the terminal signal still waits
// for the observer.
func TestAcceptedRejectRetiresFrameBeforeItsNotification(t *testing.T) {
	_, h, _ := acceptedRejectFixture(t, "test:reject-cleanup")
	require.True(t, h.NativeFrameRetained())
	entered := make(chan struct{})
	permit := make(chan struct{})
	retainedAtDelivery := make(chan bool, 1)
	receipt, owner := NewCommitReceipt(func(CommitOutcome) {
		retainedAtDelivery <- h.NativeFrameRetained()
		close(entered)
		select {
		case <-permit:
		case <-time.After(nor288RejectObserverBound):
		}
	})
	t.Cleanup(func() {
		select {
		case <-permit:
		default:
			close(permit)
		}
	})
	h.Reject(owner, ErrStorageClosed)
	require.False(t, h.NativeFrameRetained(), "the definite rejection retires native staging and authority in the deciding frame")
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the rejection notification did not start")
	}
	require.False(t, <-retainedAtDelivery, "the settlement observer sees an already retired frame")
	select {
	case <-receipt.Done():
		t.Fatal("the deciding frame delivered the terminal signal itself")
	default:
	}
	close(permit)
	select {
	case <-receipt.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done did not close after the observer returned")
	}
	outcome := receipt.Outcome()
	require.Equal(t, CommitDiscarded, outcome.State)
	require.ErrorIs(t, outcome.Err, ErrStorageClosed)
	// Later settlement of the same command is idempotent and cannot fabricate a
	// second outcome.
	h.Settled()
	require.Error(t, owner.Resolve(CommitOutcome{State: CommitApplied}))
	require.False(t, h.NativeFrameRetained())
}

// TestAcceptedRejectStaysDiscardedWithFrameEngineAdmissionSealed keeps the
// accepted owner from substituting recovery-pending for a definite rejection
// when the frame engine is already stopping. A rejection the outer log never
// recorded is decided, not deferred to a join the closing engine would refuse.
func TestAcceptedRejectStaysDiscardedWithFrameEngineAdmissionSealed(t *testing.T) {
	b, h, _ := acceptedRejectFixture(t, "test:reject-sealed")
	b.work.CloseAdmission()
	var delivered int
	receipt, owner := NewCommitReceipt(func(CommitOutcome) { delivered++ })
	h.Reject(owner, ErrStorageClosed)
	select {
	case <-receipt.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("a definite rejection stayed unsettled after the frame engine sealed admission")
	}
	outcome := receipt.Outcome()
	require.Equal(t, CommitDiscarded, outcome.State, "a definite rejection is never reported as recovery-pending")
	require.ErrorIs(t, outcome.Err, ErrStorageClosed)
	require.Equal(t, 1, delivered)
	require.False(t, h.NativeFrameRetained())
}
