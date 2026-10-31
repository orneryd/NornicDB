// Author: NOR-288 implementation lane (Codex), 2026-10-27.
package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcceptedPreNativeWALFailureRetiresStaging(t *testing.T) {
	base := storage.NewMemoryEngine()
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
	fixture.adapter.stopWALWriterForTest()
	fixture.adapter.walQueue = nil // Supported synchronous fallback, without a competing writer.
	require.NoError(t, fixture.adapter.wal.Close())
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&storage.Node{ID: "test:pre-native", Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "pre-native-wal-failure"})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	r, err := fixture.engine.SubmitEntityCommit(t.Context(), p, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	outcome := r.WaitOutcome(ctx)
	require.Equal(t, storage.CommitRecoveryPending, outcome.State)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &accepted)
	require.Equal(t, p.LogicalID(), accepted.LogicalID)
	closed := make(chan error, 1)
	go func() { closed <- base.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		// Release the old detached frame after observing the public Close red.
		// This is cleanup, not the mechanism that makes the head pass.
		assert.Fail(t, "accepted pre-native failure leaked its staging frame into Close")
		p.Abort()
		require.NoError(t, <-closed)
	}
}

func TestAcceptedPreNativeDurableWALAcknowledgementRecovery(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
	a := fixture.adapter
	a.stopWALWriterForTest()
	a.walQueue = nil
	failure := errors.New("replication WAL durable acknowledgement unavailable")
	a.walAppendAckHook = func() error {
		if err := a.wal.Sync(); err != nil {
			return err
		}
		return failure
	}
	tx, err := storage.BeginEntityTransaction(base, storage.CommitModeExplicit)
	require.NoError(t, err)
	_, err = tx.CreateNode(&storage.Node{ID: "test:durable-pre-native", Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "durable-pre-native"})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	var settled atomic.Int32
	r, err := fixture.engine.SubmitEntityCommit(t.Context(), p, func(storage.CommitOutcome) { settled.Add(1) })
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	outcome := r.WaitOutcome(ctx)
	require.Equal(t, storage.CommitRecoveryPending, outcome.State)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &accepted)
	require.Equal(t, p.LogicalID(), accepted.LogicalID)
	require.ErrorIs(t, accepted.Cause(), failure)
	require.Zero(t, settled.Load())
	_, err = base.GetStoredNode("test:durable-pre-native")
	require.ErrorIs(t, err, storage.ErrNotFound, "uncertain I/O must park, not apply")
	entries, err := a.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the fallback really recorded the accepted command")
	require.Equal(t, p.LogicalID(), entries[0].Command.RequestID)
	require.Nil(t, entries[0].Command.accepted, "persistent replay cannot retain the local native handle")
	peer := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = peer.Close() })
	peerAdapter := setupTestAdapter(t, adapterFixtureOptions{backing: peer}).adapter
	require.NoError(t, peerAdapter.ApplyCommand(context.Background(), entries[0].Command), "a replica applies the durable command independently of its failed acknowledgement")
	peerNode, err := peer.GetStoredNode("test:durable-pre-native")
	require.NoError(t, err)
	require.Equal(t, "accepted", peerNode.Properties["v"])
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, entries[0].Command.Data))
	require.NoError(t, r.Wait(ctx), "the original receipt joins the parked native owner")
	require.EqualValues(t, 1, settled.Load())
	count, err := base.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	localNode, err := base.GetStoredNode("test:durable-pre-native")
	require.NoError(t, err)
	require.Equal(t, peerNode.Properties, localNode.Properties)
	require.True(t, peerNode.CreatedAt.Equal(localNode.CreatedAt))
	require.True(t, peerNode.UpdatedAt.Equal(localNode.UpdatedAt))
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, entries[0].Command.Data))
	require.EqualValues(t, 1, settled.Load(), "duplicate native replay cannot settle twice")
	require.NoError(t, base.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "test:durable-pre-native", Properties: map[string]any{"v": "writable"}}}))
	require.NoError(t, base.Close(), "recovery releases the original staging/authority")
}
