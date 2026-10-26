package replication

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

type acceptedMirrorStorage struct {
	Storage
	peer  *StorageAdapter
	calls atomic.Int32
}

func (s *acceptedMirrorStorage) PrepareEntityCommand(ctx context.Context, cmd *Command) (*Command, func(storage.CommitOutcome), bool, error) {
	return s.Storage.(*StorageAdapter).PrepareEntityCommand(ctx, cmd)
}
func (s *acceptedMirrorStorage) ApplyCommand(cmd *Command) error {
	s.calls.Add(1)
	if err := s.Storage.ApplyCommand(cmd); err != nil {
		return err
	}
	// Exercise actual serialized ownership isolation, not shared command pointers.
	bytes, err := storage.EncodeCommandPayload(cmd)
	if err != nil {
		return err
	}
	var peer Command
	if err = storage.DecodeCommandPayload(bytes, &peer); err != nil {
		return err
	}
	return s.peer.ApplyCommand(&peer)
}
func TestAcceptedStandaloneExplicitTransactionReplicatesOriginalSnapshot(t *testing.T) {
	base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	peer, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	localAdapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = localAdapter.Close() })
	peerAdapter, err := NewStorageAdapterWithWAL(peer, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = peerAdapter.Close() })
	mirror := &acceptedMirrorStorage{Storage: localAdapter, peer: peerAdapter}
	r := NewStandaloneReplicator(&Config{}, mirror)
	require.NoError(t, r.Start(context.Background()))
	t.Cleanup(func() { _ = r.Shutdown() })
	engine := NewReplicatedEngine(base, r, time.Second)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.CreateNode(&storage.Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"value": "prepared"}, EmbedMeta: map[string]any{"embedding_model": "source"}, VisibilitySuppressed: true})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), "replicated-logical")
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
	require.NoError(t, stale.UpdateGraphNode(&storage.Node{ID: "test:a", Properties: map[string]any{"value": "stale"}}, storage.ClientNodeEdits))
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: "test:a", Properties: map[string]any{"value": "newer"}}))
	before := mirror.calls.Load()
	rejected, err := stale.PrepareEntityCommit(context.Background(), "must-not-record")
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
