package replication

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type acceptedMirrorStorage struct {
	Storage
	peer     *StorageAdapter
	calls    atomic.Int32
	ackError error
}

func (s *acceptedMirrorStorage) PrepareEntityCommand(ctx context.Context, cmd *Command) (*Command, func(storage.CommitOutcome), bool, error) {
	return s.Storage.(*StorageAdapter).PrepareEntityCommand(ctx, cmd)
}
func (s *acceptedMirrorStorage) ApplyCommand(_ context.Context, cmd *Command) error {
	s.calls.Add(1)
	if err := s.Storage.ApplyCommand(context.Background(), cmd); err != nil {
		return err
	}
	if s.peer != nil {
		// Exercise actual serialized ownership isolation, not shared command pointers.
		bytes, err := storage.EncodeCommandPayload(cmd)
		if err != nil {
			return err
		}
		var peer Command
		if err = storage.DecodeCommandPayload(bytes, &peer); err != nil {
			return err
		}
		if err = s.peer.ApplyCommand(context.Background(), &peer); err != nil {
			return err
		}
	}
	return s.ackError
}

// newAcceptedReplicationFixture owns the WAL adapters and running replicator.
// A nil peer retains the standalone route without allocating a second adapter.
func newAcceptedReplicationFixture(t *testing.T, base, peer storage.Engine, config *Config) (*StandaloneReplicator, *acceptedMirrorStorage) {
	t.Helper()
	local, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })
	mirror := &acceptedMirrorStorage{Storage: local}
	if peer != nil {
		remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, remote.Close()) })
		mirror.peer = remote
	}
	r := NewStandaloneReplicator(config, mirror)
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, r.Shutdown()) })
	return r, mirror
}

func TestAcceptedStandaloneExplicitTransactionReplicatesOriginalSnapshot(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	peer, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	r, mirror := newAcceptedReplicationFixture(t, base, peer, &Config{})
	engine := NewReplicatedEngine(base, r, time.Second)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	node := &storage.Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"value": "prepared"}, EmbedMeta: map[string]any{"embedding_model": "source"}}
	node.SetRuntimeMetadata(storage.NodeRuntimeMetadata{VisibilitySuppressed: true})
	_, err = tx.CreateNode(node)
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: "replicated-logical"})
	require.NoError(t, err)
	defer prepared.Abort()
	route, found := storage.EntityCommitSubmitterFor(engine)
	require.True(t, found)
	require.True(t, route.RequiresSubmission)
	require.NoError(t, route.CheckAdmission())
	receipt, err := route.Submitter.SubmitEntityCommit(context.Background(), prepared, nil)
	require.NoError(t, err)
	require.NoError(t, receipt.Wait(context.Background()))
	require.Equal(t, storage.CommitApplied, receipt.Outcome().State)
	a, err := base.GetStoredNode("test:a")
	require.NoError(t, err)
	b, err := peer.GetStoredNode("test:a")
	require.NoError(t, err)
	assertStoredNodeParity(t, a, b)
	require.Equal(t, int32(1), mirror.calls.Load())
	// The historical rejected-origin counterexample is rejected before any record.
	stale, err := base.BeginTransaction()
	require.NoError(t, err)
	defer stale.Rollback()
	require.NoError(t, stale.UpdateGraphNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "test:a", Properties: map[string]any{"value": "stale"}}, Policy: storage.ClientNodeEdits}))
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: "test:a", Properties: map[string]any{"value": "newer"}}}))
	before := mirror.calls.Load()
	rejected, err := stale.PrepareEntityCommit(context.Background(), storage.EntityCommitRequest{LogicalID: "must-not-record"})
	require.ErrorIs(t, err, storage.ErrConflict)
	require.Nil(t, rejected)
	require.Equal(t, before, mirror.calls.Load())
	a, err = base.GetStoredNode("test:a")
	require.NoError(t, err)
	b, err = peer.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "newer", a.Properties["value"])
	require.Equal(t, "newer", b.Properties["value"])
}
func TestAcceptedFollowerRejectsBeforePreparationOrRecord(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	r := &HAStandbyReplicator{}
	r.started.Store(true)
	engine := NewReplicatedEngine(base, r, time.Second)
	route, found := storage.EntityCommitSubmitterFor(engine)
	require.True(t, found)
	require.ErrorIs(t, route.CheckAdmission(), ErrNotLeader)
}

func TestDatabaseDropPreservesAcceptedCatalogAcknowledgementError(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	r, mirror := newAcceptedReplicationFixture(t, base, nil, &Config{})
	engine := NewReplicatedEngine(base, r, time.Second)
	manager, err := multidb.NewDatabaseManager(t.Context(), engine, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "victim"}))
	view, err := manager.GetStorage("victim")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "keep", Properties: map[string]interface{}{"value": "before"}})
	require.NoError(t, err)
	sentinel := errors.New("catalog applied but acknowledgement failed")
	mirror.ackError = sentinel
	err = manager.DropDatabase(t.Context(), "victim")
	var accepted *storage.AcceptedCommitError
	require.ErrorAs(t, err, &accepted)
	// Author: NOR-288 lane (Claude), 2026-10-30. The DROP intent was accepted,
	// then a later acknowledgement failed: the drop is pending completion by its
	// owner, and the outcome is not retryable. The cause stays diagnostic.
	require.Equal(t, storage.CommitRecoveryPending, accepted.State)
	requireAcceptedDiagnosticCause(t, err, sentinel)
	retryable := nornicerrors.IsTransientTransactionError(err)
	require.False(t, retryable)
	require.True(t, manager.Exists("victim"))
	_, err = manager.GetStorage("victim")
	require.ErrorIs(t, err, multidb.ErrDatabaseOffline)
	mirror.ackError = nil
	require.NoError(t, manager.DropDatabase(t.Context(), "victim"))
	require.False(t, manager.Exists("victim"))
	_, err = base.GetStoredNode("victim:keep")
	require.ErrorIs(t, err, storage.ErrNotFound)
}
