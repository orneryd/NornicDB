// Author: NOR-288 implementation lane (Codex), 2026-10-27.
package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// preNativeWALFaultStorage injects a transport-storage acknowledgement failure
// after a real append, before native submission. The adapter's old synchronous
// overflow fallback no longer exists; this exercises the accepted transport
// contract without reinstating that retired production path.
type preNativeWALFaultStorage struct {
	Storage
	adapter *StorageAdapter
	failure error
}

func (s *preNativeWALFaultStorage) ApplyCommand(ctx context.Context, cmd *Command, mode WALPublicationMode) (CommandPublication, error) {
	if err := ctx.Err(); err != nil {
		return CommandPublication{}, err
	}
	a := s.adapter
	a.walMu.Lock()
	defer a.walMu.Unlock()
	record := replicationWALRecord{Timestamp: cmd.Timestamp.UnixNano(), Command: cmd}
	if err := a.wal.Append(storage.OperationType("replication_command"), record); err != nil {
		return CommandPublication{}, err
	}
	if err := a.wal.Sync(); err != nil {
		return CommandPublication{}, err
	}
	position := a.wal.Sequence()
	a.walPosition.Store(position)
	a.appendToMemWAL(position, record, cmd)
	return CommandPublication{Position: position}, s.failure
}

func TestAcceptedPreNativeWALFailureRetiresStaging(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
	fixture.engine.replicator.(*StandaloneReplicator).storage = &preNativeWALFaultStorage{Storage: fixture.adapter, adapter: fixture.adapter}
	require.NoError(t, fixture.adapter.wal.Close())
	tx, err := base.BeginTransaction(t.Context())
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: "test:pre-native", Properties: map[string]any{"v": "accepted"}})
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
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	fixture := setupTestAdapter(t, adapterFixtureOptions{backing: base, standalone: true, timeout: time.Second})
	a := fixture.adapter
	failure := errors.New("replication WAL durable acknowledgement unavailable")
	fixture.engine.replicator.(*StandaloneReplicator).storage = &preNativeWALFaultStorage{Storage: a, adapter: a, failure: failure}
	tx, err := storage.BeginEntityTransaction(t.Context(), base, storage.CommitModeExplicit)
	require.NoError(t, err)
	_, err = tx.CreateNode(t.Context(), &storage.Node{ID: "test:durable-pre-native", Properties: map[string]any{"v": "accepted"}})
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
	require.ErrorIs(t, accepted.Err, failure)
	require.Zero(t, settled.Load())
	_, err = base.GetStoredNode(t.Context(), "test:durable-pre-native")
	require.ErrorIs(t, err, storage.ErrNotFound, "uncertain I/O must park, not apply")
	entries, err := a.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the transport storage really recorded the accepted command")
	require.Equal(t, p.LogicalID(), entries[0].Command.RequestID)
	require.Nil(t, entries[0].Command.accepted, "persistent replay cannot retain the local native handle")
	peer := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = peer.Close() })
	peerAdapter := setupTestAdapter(t, adapterFixtureOptions{backing: peer}).adapter
	require.NoError(t, applyQueued(t, peerAdapter, entries[0].Command), "a replica applies the durable command independently of its failed acknowledgement")
	peerNode, err := peer.GetStoredNode(t.Context(), "test:durable-pre-native")
	require.NoError(t, err)
	require.Equal(t, "accepted", peerNode.Properties["v"])
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, entries[0].Command.Data))
	require.NoError(t, r.Wait(ctx), "the original receipt joins the parked native owner")
	require.EqualValues(t, 1, settled.Load())
	count, err := base.NodeCount(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	localNode, err := base.GetStoredNode(t.Context(), "test:durable-pre-native")
	require.NoError(t, err)
	require.Equal(t, peerNode.Properties, localNode.Properties)
	require.True(t, peerNode.CreatedAt.Equal(localNode.CreatedAt))
	require.True(t, peerNode.UpdatedAt.Equal(localNode.UpdatedAt))
	require.NoError(t, storage.ApplyAcceptedEntityTransaction(base, entries[0].Command.Data))
	require.EqualValues(t, 1, settled.Load(), "duplicate native replay cannot settle twice")
	require.NoError(t, base.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: "test:durable-pre-native", Properties: map[string]any{"v": "writable"}}}))
	require.NoError(t, base.Close(), "recovery releases the original staging/authority")
}
