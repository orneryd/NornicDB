package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Close-phase checks of the storage
// consumers of accepted work, completing the phases the other tests in this
// package hold (during: TestAcceptedCloseBeforeNativeSubmissionPreservesRecovery,
// TestAcceptedCloseStopsRetryWorkAndReplays,
// TestAcceptedRecoveryCannotRestartWhileEngineCloses,
// TestAcceptedWALCloseRetiresPendingNativeFrameWithoutDiscard). Before: an
// accepted record replayed into a closed engine is reported pending with its
// identity, never applied or discarded, without waiting. After: an applied
// accepted command survives Close and reopen, directly and through the WAL.

func acceptedClosePhaseFrame(t *testing.T, engine *BadgerEngine, id string) (*PreparedEntityCommit, []byte) {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: NodeID(id), Properties: map[string]any{"v": "committed"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: p})
	require.NoError(t, err)
	return p, payload
}

func requireWithin[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
	}
	var zero T
	return zero
}

func TestAcceptedDirectReplayIntoClosedEnginePhaseBefore(t *testing.T) {
	dir := t.TempDir()
	base, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	p, payload := acceptedClosePhaseFrame(t, base, "test:before")
	logicalID := p.LogicalID()
	p.Abort()
	require.NoError(t, base.Close())
	envelope, operations, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	type result struct {
		outcome CommitOutcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, err := SubmitAcceptedEntityTransaction(context.Background(), base, envelope, operations, nil)
		if err != nil {
			done <- result{err: err}
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- result{outcome: receipt.WaitOutcome(ctx)}
	}()
	r := requireWithin(t, "replay into a closed engine", done)
	if r.err != nil {
		require.ErrorIs(t, r.err, ErrStorageClosed)
		return
	}
	require.Equal(t, CommitRecoveryPending, r.outcome.State, "a closed engine neither applies nor discards accepted work")
	var accepted *AcceptedCommitError
	require.ErrorAs(t, r.outcome.Err, &accepted)
	require.Equal(t, logicalID, accepted.LogicalID)
	require.ErrorIs(t, accepted.Cause(), ErrStorageClosed)
	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	_, err = reopened.GetStoredNode("test:before")
	require.True(t, errors.Is(err, ErrNotFound), "nothing was applied across the closed cut")
}

func TestAcceptedDirectApplicationSurvivesClosePhaseAfter(t *testing.T) {
	dir := t.TempDir()
	base, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	p, payload := acceptedClosePhaseFrame(t, base, "test:after")
	p.Abort()
	envelope, operations, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	receipt, err := SubmitAcceptedEntityTransaction(context.Background(), base, envelope, operations, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, receipt.Wait(ctx))
	closed := make(chan error, 1)
	go func() { closed <- base.Close() }()
	require.NoError(t, requireWithin(t, "Close after an applied accepted command", closed))
	reopened, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	node, err := reopened.GetStoredNode("test:after")
	require.NoError(t, err)
	require.Equal(t, "committed", node.Properties["v"])
}

func TestAcceptedWALApplicationSurvivesClosePhaseAfter(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	owner := NewWALEngine(base, wal)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:accepted", Properties: map[string]any{"v": "committed"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	receipt, err := owner.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, receipt.Wait(ctx))
	closed := make(chan error, 1)
	go func() { closed <- owner.Close() }()
	require.NoError(t, requireWithin(t, "WAL Close after an applied accepted command", closed))
	requireAcceptedCloseReplay(t, dir)
}
