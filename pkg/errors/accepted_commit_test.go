// Author: NOR-288 implementation lane (Codex), 2026-10-26.
package errors

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAcceptedCommitFailureIsNotRetryableRejection(t *testing.T) {
	err := fmt.Errorf("commit: %w", &storage.AcceptedCommitError{State: storage.CommitRecoveryPending, LogicalID: "accepted", Err: storage.ErrConflict})
	code, message := Neo4jStatus(err)
	require.Equal(t, TransactionOutcomeUnknown, code)
	require.Contains(t, message, "committed-pending or unknown")
	require.NotErrorIs(t, err, storage.ErrConflict)
	require.NotErrorIs(t, err, ErrTransactionConflict)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.ErrorIs(t, accepted.Cause(), storage.ErrConflict)
	transient := IsTransientTransactionError(err)
	require.False(t, transient)
}

func TestAcceptedOutcomeContainsEveryJoinedCause(t *testing.T) {
	for _, state := range []storage.CommitOutcomeState{storage.CommitRecoveryPending, storage.CommitApplied} {
		r, owner := storage.NewCommitReceipt(nil)
		inner := &storage.AcceptedCommitError{State: storage.CommitRecoveryPending, LogicalID: "joined-accepted", Err: context.Canceled}
		joined := fmt.Errorf("transport: %w", stderrors.Join(inner, storage.ErrConflict))
		require.NoError(t, owner.Resolve(storage.CommitOutcome{State: state, Err: joined}))
		outcome := r.WaitOutcome(t.Context())
		require.NotErrorIs(t, outcome.Err, storage.ErrConflict)
		var accepted *storage.AcceptedCommitError
		require.ErrorAs(t, outcome.Err, &accepted)
		require.Equal(t, state, accepted.State)
		require.Equal(t, "joined-accepted", accepted.LogicalID)
		require.ErrorIs(t, accepted.Cause(), storage.ErrConflict)
		var nested *storage.AcceptedCommitError
		require.ErrorAs(t, accepted.Cause(), &nested)
		require.ErrorIs(t, nested.Cause(), context.Canceled)
	}
}

func TestAcceptedWaitCancellationPreservesUnknownOutcome(t *testing.T) {
	r, owner := storage.NewCommitReceipt(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, err := range []error{r.Wait(ctx), r.WaitOutcome(ctx).Err, r.WaitPhysicalOutcome(ctx).Err} {
		var accepted *storage.AcceptedCommitError
		require.ErrorAs(t, err, &accepted)
		require.ErrorIs(t, accepted.Cause(), context.Canceled)
		code, message := Neo4jStatus(err)
		require.Equal(t, TransactionOutcomeUnknown, code)
		require.Contains(t, message, "committed-pending or unknown")
	}
	require.NoError(t, owner.Resolve(storage.CommitOutcome{State: storage.CommitApplied, Err: context.DeadlineExceeded}))
	err := r.Wait(context.Background())
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.ErrorIs(t, accepted.Cause(), context.DeadlineExceeded)
	code, message := Neo4jStatus(err)
	require.Equal(t, TransactionOutcomeUnknown, code)
	require.Contains(t, message, "committed; acknowledgement failed")
}

func TestAcceptedReceiptCannotBecomeRejectedOrSkippedByCause(t *testing.T) {
	var legacy storage.ReplayResult
	legacy.Record(storage.WALEntry{Operation: storage.OpCreateNode, Sequence: 1}, storage.ErrAlreadyExists)
	require.Equal(t, 1, legacy.Skipped, "ordinary duplicate replay remains an expected skip")
	for _, cause := range []error{storage.ErrConflict, storage.ErrAlreadyExists, storage.ErrNotFound,
		storage.ErrDeadlock, storage.ErrMVCCResourcePressure, storage.ErrMVCCSnapshotGracefulCancel,
		storage.ErrMVCCSnapshotHardExpired, ErrMergeCommitTimeUniqueConflict, context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			r, owner := storage.NewCommitReceipt(nil)
			require.NoError(t, owner.Resolve(storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: cause}))
			err := r.WaitOutcome(t.Context()).Err
			var replay storage.ReplayResult
			replay.Record(storage.WALEntry{Operation: storage.OpEntityCommit, Sequence: 1}, err)
			require.Zero(t, replay.Skipped, "an accepted pending command is not an idempotent skipped create")
			require.Equal(t, 1, replay.Failed)
			require.Len(t, replay.Errors, 1)
			require.Same(t, err, replay.Errors[0].Error)
			require.NotErrorIs(t, err, cause, "accepted outcome must not inherit a rejection/skip identity")
			var accepted *storage.AcceptedCommitError
			require.ErrorAs(t, err, &accepted)
			require.ErrorIs(t, accepted.Cause(), cause)
			code, _ := Neo4jStatus(err)
			require.Equal(t, TransactionOutcomeUnknown, code)
		})
	}
}
