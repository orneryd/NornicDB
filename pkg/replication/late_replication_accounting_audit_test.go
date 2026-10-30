package replication

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type managedRouteReplicator struct {
	Replicator
	peer           *StorageAdapter
	reject         bool
	calls          int
	delay          bool
	pending        *Command
	pendingOwner   storage.CommitReceiptOwner
	pendingReceipt *storage.CommitReceipt
}

func (r *managedRouteReplicator) Apply(cmd *Command, timeout time.Duration) error {
	r.calls++
	if r.reject {
		return ErrNotLeader
	}
	if r.delay {
		r.pending = cmd
		return context.DeadlineExceeded
	}
	if err := r.Replicator.Apply(cmd, timeout); err != nil {
		return err
	}
	// Only wire fields cross to the peer, as with replication transport/WAL.
	return r.peer.ApplyCommand(&Command{Type: cmd.Type, Data: cmd.Data, Timestamp: cmd.Timestamp, RequestID: cmd.RequestID})
}

func TestManagedReplicationLateApplyUsesAccountingOwner(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	peer := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer peer.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
	require.NoError(t, err)
	defer remote.Close()
	local := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, local.Start(context.Background()))
	r := &managedRouteReplicator{Replicator: local, peer: remote}
	manager, err := multidb.NewDatabaseManager(t.Context(), NewReplicatedEngine(base, r, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "late_apply"}))
	view, err := manager.GetStorage("late_apply")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}})
	require.NoError(t, err)
	before, _, _ := manager.GetStorageSize("late_apply")
	r.delay = true
	err = view.UpdateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: map[string]any{"payload": "late committed body"}})
	// Author: NOR-288 lane, 2026-10-29. This is an accepted wait outcome,
	// rather than a new-command rejection with a retryable deadline identity.
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	requireAcceptedDiagnosticCause(t, err, context.DeadlineExceeded)
	require.False(t, nornicerrors.IsCommitRolledBack(err), "replication timeout cannot certify logical rollback")
	require.NotNil(t, r.pending)
	unchanged, _, _ := manager.GetStorageSize("late_apply")
	require.Equal(t, before, unchanged)
	// Consensus can apply after the originating caller has returned.
	require.NoError(t, r.applyPending(adapter, r.pending))
	node, err := view.GetNode("n")
	require.NoError(t, err)
	require.Equal(t, "late committed body", node.Properties["payload"])
	tracked, _, _ := manager.GetStorageSize("late_apply")
	recount, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	exact, _, _ := recount.GetStorageSize("late_apply")
	require.Equal(t, exact, tracked)
	require.Greater(t, tracked, before)
}

// The fake implements the new accepted-owner contract explicitly rather than
// inheriting Standalone's submission and silently bypassing its delay/reject
// behavior. This is the same wire command and actual adapter ApplyCommand.
func (r *managedRouteReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if r.reject {
		return nil, ErrNotLeader
	}
	if r.delay {
		r.calls++
		r.pending = cmd
		receipt, owner := storage.NewCommitReceipt(settle)
		r.pendingOwner, r.pendingReceipt = owner, receipt
		if err := owner.Resolve(storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: context.DeadlineExceeded}); err != nil {
			return nil, err
		}
		return receipt, nil
	}
	return submitAcceptedByApply(ctx, cmd, settle, func() error {
		if r.reject {
			return ErrNotLeader
		}
		return nil
	}, r.Apply, 0)
}
func (r *managedRouteReplicator) applyPending(adapter *StorageAdapter, cmd *Command) error {
	err := adapter.ApplyCommand(cmd)
	if err == nil {
		err = r.peer.ApplyCommand(&Command{Type: cmd.Type, Data: cmd.Data, Timestamp: cmd.Timestamp, RequestID: cmd.RequestID})
	}
	// The accepted owner joins the late application; accounting is published
	// by the terminal settlement the caller observes next.
	cmd.accepted.Settle(r.pendingOwner, err)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return r.pendingReceipt.Wait(ctx)
	}
	return err
}
