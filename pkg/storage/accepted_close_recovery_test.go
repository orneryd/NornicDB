// Author: NOR-288 implementation lane (Codex), 2026-10-27.
package storage

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func acceptedCloseFixture(t *testing.T) (*BadgerEngine, *WAL, *WALEngine, string) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	base, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	dir := t.TempDir()
	wal, err := NewWAL(dir, &WALConfig{Dir: dir, SyncMode: "immediate"})
	require.NoError(t, err)
	owner := NewWALEngine(base, wal)
	t.Cleanup(func() { _ = owner.Close() })
	return base, wal, owner, dir
}

func waitAcceptedEngineClosing(t *testing.T, base *BadgerEngine) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		base.mu.RLock()
		closing := base.closing
		base.mu.RUnlock()
		if closing {
			return
		}
		select {
		case <-deadline:
			t.Fatal("Close did not seal native admission")
		default:
			runtime.Gosched()
		}
	}
}

func requireAcceptedCloseReplay(t *testing.T, dir string) {
	t.Helper()
	recovered, result, err := RecoverFromWALWithResult(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = recovered.Close() })
	require.Empty(t, result.Errors)
	node, err := recovered.GetStoredNode("test:accepted")
	require.NoError(t, err)
	require.Equal(t, "committed", node.Properties["v"])
	require.NoError(t, recovered.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:accepted", Properties: map[string]any{"v": "writable"}}}))
}

func TestAcceptedCloseBeforeNativeSubmissionPreservesRecovery(t *testing.T) {
	base, wal, _, dir := acceptedCloseFixture(t)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:accepted", Properties: map[string]any{"v": "committed"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	_, err = p.RetainAcceptedAuthority(context.Background())
	require.NoError(t, err)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: p})
	require.NoError(t, err)
	require.NoError(t, wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
	closed := make(chan error, 1)
	go func() { closed <- base.Close() }()
	waitAcceptedEngineClosing(t, base)
	r, err := base.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outcome := r.WaitOutcome(ctx)
	require.Equal(t, CommitRecoveryPending, outcome.State)
	var accepted *AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &accepted)
	require.ErrorIs(t, accepted.Cause(), ErrStorageClosed)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("closed native admission retained its staging frame")
	}
	require.NoError(t, wal.Close())
	requireAcceptedCloseReplay(t, dir)
}

func TestAcceptedCloseStopsRetryWorkAndReplays(t *testing.T) {
	base, wal, owner, dir := acceptedCloseFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	var attempts atomic.Int32
	require.NoError(t, base.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) {
			if attempts.Add(1) == 3 {
				close(entered)
				<-release
			}
			return nil, ErrConflict
		}, nil
	}, nil))
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:accepted", Properties: map[string]any{"v": "committed"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	r, err := owner.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("native retries did not reach the closing schedule")
	}
	closed := make(chan error, 1)
	go func() { closed <- owner.Close() }()
	waitAcceptedEngineClosing(t, base)
	unblock.Do(func() { close(release) })
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Close did not join accepted retry work")
	}
	outcome := r.WaitOutcome(ctx)
	require.Equal(t, CommitRecoveryPending, outcome.State)
	var accepted *AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &accepted)
	require.EqualValues(t, 3, attempts.Load(), "no retry work survives Close")
	require.EqualValues(t, 1, wal.Sequence(), "native retries never append another command")
	requireAcceptedCloseReplay(t, dir)
}

func TestAcceptedRecoveryCannotRestartWhileEngineCloses(t *testing.T) {
	base, wal, owner, dir := acceptedCloseFixture(t)
	require.NoError(t, base.InitializeStoredEntityObserver("test", func(StoredEntitySnapshot) (StoredEntityObserver, error) {
		return func(StoredEntityPublication) (func(), error) { return nil, ErrInvalidData }, nil
	}, nil))
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:accepted", Properties: map[string]any{"v": "committed"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(context.Background(), EntityCommitRequest{LogicalID: tx.ID})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: p})
	require.NoError(t, err)
	r, err := owner.SubmitEntityCommit(context.Background(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var firstAccepted *AcceptedCommitError
	require.ErrorAs(t, r.WaitOutcome(ctx).Err, &firstAccepted)
	require.ErrorIs(t, firstAccepted.Cause(), ErrInvalidData)
	// This unrelated admitted frame holds the lifecycle barrier so Close can
	// seal admission before the recovery call reaches the existing owner.
	guard, err := base.BeginTransaction()
	require.NoError(t, err)
	_, err = guard.CreateNode(&Node{ID: "guard:uncommitted"})
	require.NoError(t, err)
	held, err := guard.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: guard.ID})
	require.NoError(t, err)
	t.Cleanup(held.Abort)
	closed := make(chan error, 1)
	go func() { closed <- base.Close() }()
	waitAcceptedEngineClosing(t, base)
	replayEnvelope, replayOperations, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	recovery, err := SubmitAcceptedEntityTransaction(ctx, base, replayEnvelope, replayOperations, nil)
	require.NoError(t, err)
	outcome := recovery.WaitOutcome(ctx)
	var accepted *AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &accepted)
	require.Equal(t, p.LogicalID(), accepted.LogicalID, "recovery keeps the accepted command identity")
	require.ErrorIs(t, accepted.Cause(), ErrStorageClosed)
	require.EqualValues(t, 1, wal.Sequence(), "closing recovery cannot append a new command")
	held.Abort()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("recovery resumed work across the closed admission cut")
	}
	require.NoError(t, wal.Close())
	requireAcceptedCloseReplay(t, dir)
}
