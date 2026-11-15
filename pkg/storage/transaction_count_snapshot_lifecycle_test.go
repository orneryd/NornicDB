package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Transaction-scoped count-store reads, lifecycle half (B10). An explicit
// transaction reads its rows at its own begin boundary, so the counters a
// capability answers follow that transaction's lifecycle rather than its status
// snapshot alone: a frame that has handed its native client handle to an
// accepted commit, and a frame whose snapshot reader resource pressure expired,
// still report an active status but no longer read the snapshot the capability
// was issued at. The capability therefore goes through the same lifecycle owner
// every other read of the transaction goes through - rows reads, adjacency
// scans, staging and commit preparation - and such a read declines instead of
// certifying a counter at a snapshot the transaction no longer reads.

// b10HandOffFrameToAcceptedCommit begins a transaction pinned to the test
// namespace, stages one node of the counted label, and hands the frame's native
// client handle to an accepted commit. The returned prepared commit is parked
// at its decision, so the frame stays active and detached until the caller's
// cleanup aborts it.
func b10HandOffFrameToAcceptedCommit(t *testing.T, engine *BadgerEngine) *BadgerTransaction {
	t.Helper()
	ctx := t.Context()
	tx := beginPinnedTransaction(t, engine)
	_, err := tx.CreateNode(ctx, &Node{ID: "test:handoff", Labels: []string{"L"}})
	require.NoError(t, err)
	// Accepted-commit preparation uses its own lifetime for the command it
	// hands over, so it runs on a context the parking frame does not inherit.
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// TestTransactionCountScopeDeclinesAfterAcceptedCommitHandoff is the detach
// case on the consuming side: the frame stages its work and hands its native
// client handle to an accepted commit, which leaves its status active while the
// commit runs. A capability retained from before that handoff must not certify
// the frame's counter afterwards, exactly as the frame's own rows reads refuse
// to run.
func TestTransactionCountScopeDeclinesAfterAcceptedCommitHandoff(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	tx := beginPinnedTransaction(t, engine)
	t.Cleanup(func() { _ = tx.Rollback() })

	request := transactionCountScopedLabelRequest()
	scoped, described, err := tx.ScopeCountStoreRead(ctx, request)
	require.NoError(t, err)
	require.True(t, described, "an active frame plans its own snapshot read")

	// The frame stages a row of the counted label and detaches for its accepted
	// commit. Its status stays active: only ownership is gone.
	_, err = tx.CreateNode(ctx, &Node{ID: "test:handoff", Labels: []string{"L"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)

	// The rows read of a detached frame is closed...
	_, err = tx.GetNodesByLabel(ctx, "L")
	require.ErrorIs(t, err, ErrTransactionClosed, "a detached frame's rows reads refuse to run")

	// ...so its retained count capability must not certify the frame's counter.
	answer, err := engine.ReadCountStore(ctx, scoped)
	require.NoError(t, err, "a retained capability declines; it reads no frame handed to an accepted commit")
	require.False(t, answer.DescribesRows)
	require.False(t, scoped.Scope.DescribesSnapshot())
}

// TestTransactionCountScopeIssuedAfterAcceptedCommitHandoffDeclines is the same
// lifecycle owner on the issuing side: a detached frame plans no capability, so
// nothing downstream can certify a counter at its snapshot.
func TestTransactionCountScopeIssuedAfterAcceptedCommitHandoffDeclines(t *testing.T) {
	engine := newIsolatedBadgerEngine(t)
	ctx := t.Context()
	createEdgeEndpoints(t, engine)
	tx := b10HandOffFrameToAcceptedCommit(t, engine)

	scoped, described, err := tx.ScopeCountStoreRead(ctx, transactionCountScopedLabelRequest())
	require.NoError(t, err, "issuance declines; it does not report the frame's closed reads as a read failure")
	require.False(t, described, "a detached frame issues no snapshot capability")
	require.Nil(t, scoped.Scope)
}

// TestTransactionCountScopeDeclinesAnExpiredSnapshotReader is the expiry case:
// the lifecycle owner closes a transaction whose snapshot reader crossed its
// resource-pressure threshold, and a capability issued before that must decline
// rather than read a snapshot the lifecycle is entitled to reclaim. The
// transaction fixture is the engine's existing lifecycle controller stub, so the
// hard and graceful thresholds are reached deterministically.
func TestTransactionCountScopeDeclinesAnExpiredSnapshotReader(t *testing.T) {
	for _, expiry := range []struct {
		name string
		set  func(*txLifecycleControllerStub)
		want error
	}{
		{
			name: "graceful cancel",
			set:  func(controller *txLifecycleControllerStub) { controller.gracefulExpire = true },
			want: ErrMVCCSnapshotGracefulCancel,
		},
		{
			name: "hard expiration",
			set:  func(controller *txLifecycleControllerStub) { controller.hardExpire = true },
			want: ErrMVCCSnapshotHardExpired,
		},
	} {
		t.Run(expiry.name, func(t *testing.T) {
			engine := newIsolatedBadgerEngine(t)
			controller := &txLifecycleControllerStub{enabled: true}
			engine.SetLifecycleController(controller)
			ctx := t.Context()
			createEdgeEndpoints(t, engine)
			tx := beginPinnedTransaction(t, engine)
			t.Cleanup(func() { _ = tx.Rollback() })

			scoped, described, err := tx.ScopeCountStoreRead(ctx, transactionCountScopedLabelRequest())
			require.NoError(t, err)
			require.True(t, described)

			controller.mu.Lock()
			expiry.set(controller)
			controller.mu.Unlock()

			answer, err := engine.ReadCountStore(ctx, scoped)
			require.NoError(t, err, "an expired capability declines; it does not read a reclaimable snapshot")
			require.False(t, answer.DescribesRows)
			require.False(t, scoped.Scope.DescribesSnapshot())

			// The use observed the lifecycle owner's own decision: the
			// transaction is closed with the expiry sentinel, so the rows the
			// count falls back to report the same expiration.
			require.Equal(t, TxStatusRolledBack, tx.Status)
			_, err = tx.GetNodesByLabel(ctx, "L")
			require.ErrorIs(t, err, expiry.want)
		})
	}
}
