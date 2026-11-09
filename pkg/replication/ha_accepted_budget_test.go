package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func awaitReplicationValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("replication test worker did not reach its boundary")
	}
	var zero T
	return zero
}

type haBudgetPeer struct {
	PeerConnection
	once sync.Once
	sent chan context.Context
}

func (p *haBudgetPeer) SendWALBatch(ctx context.Context, entries []*WALEntry) (*WALBatchResponse, error) {
	p.once.Do(func() { p.sent <- ctx })
	return p.PeerConnection.SendWALBatch(ctx, entries)
}

func observeHABudgetACK(primary *HAStandbyReplicator) <-chan context.Context {
	primary.mu.Lock()
	defer primary.mu.Unlock()
	peer := &haBudgetPeer{PeerConnection: primary.standbyConn, sent: make(chan context.Context, 1)}
	primary.standbyConn = peer
	return peer.sent
}

type haObservedApply struct {
	ctx  context.Context
	mode WALPublicationMode
}

// Keep real preparation, native application, FIFO publication and receipts.
// The gate parks only accepted execution, not the caller's admission.
type haBudgetStorage struct {
	*StorageAdapter
	prepared chan context.Context
	applied  chan haObservedApply
	release  <-chan struct{}
}

func (s *haBudgetStorage) PrepareEntityCommand(ctx context.Context, cmd *Command) (*Command, func(storage.CommitOutcome), bool, error) {
	s.prepared <- ctx
	return s.StorageAdapter.PrepareEntityCommand(ctx, cmd)
}

func (s *haBudgetStorage) ApplyCommand(ctx context.Context, cmd *Command, mode WALPublicationMode) (CommandPublication, error) {
	s.applied <- haObservedApply{ctx, mode}
	if s.release != nil {
		<-s.release
	}
	return s.StorageAdapter.ApplyCommand(ctx, cmd, mode)
}

func TestHAAcceptedEntityWritesPreservePublicationDeadline(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		name := "direct"
		if bulk {
			name = "bulk"
		}
		t.Run(name, func(t *testing.T) {
			fixture := setupTestAdapter(t)
			owner := &haBudgetStorage{StorageAdapter: fixture.adapter, prepared: make(chan context.Context, 1), applied: make(chan haObservedApply, 1)}
			primary := startOwnPositionPrimary(t, owner, SyncQuorum)
			sent := observeHABudgetACK(primary)
			engine := NewReplicatedEngine(owner.engine, primary, time.Second)
			node := &storage.Node{ID: "test:accepted-budget", Labels: []string{"Budget"}}
			var err error
			if bulk {
				err = engine.BulkCreateNodes(t.Context(), []*storage.Node{node})
			} else {
				_, err = engine.CreateNode(t.Context(), node)
			}
			require.NoError(t, err)
			preparationDeadline, ok := awaitReplicationValue(t, owner.prepared).Deadline()
			require.True(t, ok)
			apply := awaitReplicationValue(t, owner.applied)
			publicationDeadline, ok := apply.ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, preparationDeadline, publicationDeadline,
				"accepted execution must not replace the engine budget with HA's default or restart it")
			require.Equal(t, WALPublished, apply.mode)
			ackDeadline, bounded := awaitReplicationValue(t, sent).Deadline()
			require.True(t, bounded)
			require.Equal(t, publicationDeadline, ackDeadline, "ACK cannot start a second publication budget")
			assertHABudgetRecord(t, owner.StorageAdapter, primary, node.ID)
		})
	}
}

func assertHABudgetRecord(t *testing.T, adapter *StorageAdapter, primary *HAStandbyReplicator, id storage.NodeID) {
	t.Helper()
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(1), entries[0].Position)
	require.Equal(t, CmdCommitTransaction, entries[0].Command.Type)
	require.NotEmpty(t, entries[0].Command.RequestID)
	position, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Equal(t, entries[0].Position, position)
	require.Equal(t, position, primary.walStreamer.LastAcked())
	node, err := storage.GetStoredNode(t.Context(), adapter.engine, id)
	require.NoError(t, err)
	require.Equal(t, []string{"Budget"}, node.Labels)
}

func TestHAAcceptedExplicitReceiptKeepsBudgetAndNativeOwnership(t *testing.T) {
	for _, mode := range []SyncMode{SyncQuorum, SyncAsync} {
		t.Run(string(mode), func(t *testing.T) {
			fixture := setupTestAdapter(t)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			owner := &haBudgetStorage{StorageAdapter: fixture.adapter, applied: make(chan haObservedApply, 1), release: release}
			primary := startOwnPositionPrimary(t, owner, mode)
			sent := observeHABudgetACK(primary)
			t.Cleanup(unblock)
			engine := NewReplicatedEngine(owner.engine, primary, time.Second)
			tx, err := storage.BeginEntityTransaction(t.Context(), owner.engine, storage.CommitModeExplicit)
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = tx.CreateNode(t.Context(), &storage.Node{ID: "test:explicit-budget", Labels: []string{"Budget"}})
			require.NoError(t, err)
			prepared, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "explicit-budget"})
			require.NoError(t, err)
			defer prepared.Abort()
			defer unblock()
			deadline := time.Now().Add(time.Second)
			ctx, cancel := context.WithDeadline(t.Context(), deadline)
			defer cancel()
			receipt, err := engine.SubmitEntityCommit(ctx, prepared, nil)
			require.NoError(t, err)
			apply := awaitReplicationValue(t, owner.applied)
			// SubmitEntityCommit has already canceled its queue-only context.
			// Neither that nor the caller cancel may cancel accepted execution.
			cancel()
			require.NoError(t, apply.ctx.Err())
			got, bounded := apply.ctx.Deadline()
			if mode == SyncQuorum {
				require.True(t, bounded)
				require.Equal(t, deadline, got)
				require.Equal(t, WALPublished, apply.mode)
			} else {
				require.False(t, bounded, "async must not acquire a publication deadline")
				require.Equal(t, WALQueued, apply.mode)
			}
			unblock()
			waitCtx, stopWait := context.WithTimeout(t.Context(), 5*time.Second)
			defer stopWait()
			outcome := receipt.WaitOutcome(waitCtx)
			require.NoError(t, outcome.Err)
			require.Equal(t, storage.CommitApplied, outcome.State)
			require.NoError(t, fixture.adapter.FlushWAL())
			if mode == SyncAsync {
				primary.streamPendingWAL(t.Context())
			} else {
				ackDeadline, bounded := awaitReplicationValue(t, sent).Deadline()
				require.True(t, bounded)
				require.Equal(t, deadline, ackDeadline)
			}
			assertHABudgetRecord(t, fixture.adapter, primary, "test:explicit-budget")
		})
	}
}

func TestHAAcceptedExpiredPublicationBudgetStillCompletesNative(t *testing.T) {
	fixture := setupTestAdapter(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	owner := &haBudgetStorage{StorageAdapter: fixture.adapter, applied: make(chan haObservedApply, 1), release: release}
	primary := startOwnPositionPrimary(t, owner, SyncQuorum)
	t.Cleanup(unblock)
	engine := NewReplicatedEngine(owner.engine, primary, time.Second)
	tx, err := storage.BeginEntityTransaction(t.Context(), owner.engine, storage.CommitModeExplicit)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: "test:expired-budget", Labels: []string{"Budget"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "expired-budget"})
	require.NoError(t, err)
	defer prepared.Abort()
	defer unblock()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	receipt, err := engine.SubmitEntityCommit(ctx, prepared, nil)
	require.NoError(t, err)
	apply := awaitReplicationValue(t, owner.applied)
	deadline, _ := ctx.Deadline()
	actualDeadline, bounded := apply.ctx.Deadline()
	require.True(t, bounded)
	require.Equal(t, deadline, actualDeadline)
	awaitReplicationValue(t, ctx.Done())
	awaitReplicationValue(t, apply.ctx.Done())
	require.ErrorIs(t, apply.ctx.Err(), context.DeadlineExceeded)
	unblock()
	waitCtx, stopWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopWait()
	outcome := receipt.WaitOutcome(waitCtx)
	require.Equal(t, storage.CommitApplied, outcome.State, "deadline expiry never discards accepted native work")
	// Accepted commands are not retryable: errors.Is stops at the accepted
	// boundary, so the diagnostic cause is traversed explicitly.
	requireAcceptedDiagnosticCause(t, outcome.Err, context.DeadlineExceeded)
	require.NoError(t, fixture.adapter.FlushWAL())
	primary.streamPendingWAL(t.Context())
	assertHABudgetRecord(t, fixture.adapter, primary, "test:expired-budget")
}

func TestHAAcceptedPublicationFailureKeepsNativeResult(t *testing.T) {
	for _, mode := range []SyncMode{SyncQuorum, SyncAsync} {
		t.Run(string(mode), func(t *testing.T) {
			fixture := setupTestAdapter(t)
			primary := startOwnPositionPrimary(t, fixture.adapter, mode)
			engine := NewReplicatedEngine(fixture.adapter.engine, primary, time.Second)
			require.NoError(t, fixture.adapter.wal.Close())
			_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "test:publication-failure", Labels: []string{"Budget"}})
			if mode == SyncQuorum {
				var accepted *storage.AcceptedCommitError
				require.ErrorAs(t, err, &accepted)
				require.Equal(t, storage.CommitApplied, accepted.State)
				require.NotEmpty(t, accepted.LogicalID)
				// Accepted publication failure is not a retryable WAL rejection.
				requireAcceptedDiagnosticCause(t, err, storage.ErrWALClosed)
			} else {
				require.NoError(t, err, "async reports native success, not asynchronous publication failure")
			}
			node, err := storage.GetStoredNode(t.Context(), fixture.adapter.engine, "test:publication-failure")
			require.NoError(t, err)
			require.Equal(t, []string{"Budget"}, node.Labels)
			position, err := fixture.adapter.GetWALPosition()
			require.NoError(t, err)
			require.Zero(t, position, "a failed append cannot produce a publication receipt")
			require.Zero(t, primary.walStreamer.LastAcked(), "no ACK can cover an unpublished record")
		})
	}
}
