package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-29. Raft commits the accepted command,
// then the adapter's durable append fails its acknowledgement before native
// submission. The committed entry must park the original frame and join its
// receipt to serialized recovery, exactly as the Standalone transport does.
func TestAcceptedRaftCommittedPreNativeFailureRecovery(t *testing.T) {
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
	raft, _ := newCoverageRaftReplicator(t)
	raft.storage = a
	raft.started.Store(true)
	raft.mu.Lock()
	raft.state, raft.currentTerm = StateLeader, 1
	raft.mu.Unlock()
	t.Cleanup(func() { _ = raft.Shutdown() })
	engine := NewReplicatedEngine(base, raft, time.Second)

	tx, err := storage.BeginEntityTransaction(base, storage.CommitModeExplicit)
	require.NoError(t, err)
	_, err = tx.CreateNode(&storage.Node{ID: "test:raft-pre-native", Properties: map[string]any{"v": "accepted"}})
	require.NoError(t, err)
	p, err := tx.PrepareEntityCommit(t.Context(), storage.EntityCommitRequest{LogicalID: "raft-pre-native"})
	require.NoError(t, err)
	t.Cleanup(p.Abort)
	var settled atomic.Int32
	receipt, err := engine.SubmitEntityCommit(t.Context(), p, func(storage.CommitOutcome) { settled.Add(1) })
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case future := <-raft.applyCh:
		raft.processApply(ctx, future)
	case <-ctx.Done():
		t.Fatal("accepted command did not reach consensus queue")
	}
	require.EqualValues(t, 1, raft.commitIndex, "the entry is committed before the adapter fails")
	require.Zero(t, raft.lastApplied, "a failed application keeps the committed entry for ordered retry")

	outcome := receipt.WaitOutcome(ctx)
	require.Equal(t, storage.CommitRecoveryPending, outcome.State)
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, outcome.Err, &accepted)
	require.Equal(t, p.LogicalID(), accepted.LogicalID)
	requireAcceptedDiagnosticCause(t, outcome.Err, failure)
	require.Zero(t, settled.Load())
	_, err = base.GetStoredNode("test:raft-pre-native")
	require.ErrorIs(t, err, storage.ErrNotFound, "uncertain I/O must park, not apply")
	entries, err := a.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the synchronous fallback really recorded the accepted command")
	require.Equal(t, p.LogicalID(), entries[0].Command.RequestID)

	// Serialized recovery on the same engine must borrow the parked frame's
	// authority instead of queueing behind an unparked original frame.
	recovered := make(chan error, 1)
	go func() { recovered <- storage.ApplyAcceptedEntityTransaction(base, entries[0].Command.Data) }()
	select {
	case err := <-recovered:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("serialized recovery waits behind the unparked original native frame")
	}
	require.NoError(t, receipt.Wait(ctx), "the original receipt joins the recovered native owner")
	require.EqualValues(t, 1, settled.Load())
	node, err := base.GetStoredNode("test:raft-pre-native")
	require.NoError(t, err)
	require.Equal(t, "accepted", node.Properties["v"])
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, entries[0].Command.Data))
	require.EqualValues(t, 1, settled.Load(), "duplicate native replay cannot settle twice")
	require.NoError(t, base.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "test:raft-pre-native", Properties: map[string]any{"v": "writable"}}}))
	require.NoError(t, raft.Shutdown())
	require.NoError(t, base.Close(), "recovery releases the original staging and authority")
}
