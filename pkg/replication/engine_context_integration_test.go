package replication

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type completionContextEngine struct {
	storage.Engine
	nativeContext context.Context
}

func (e *completionContextEngine) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	e.nativeContext = ctx
	return e.Engine.CreateNode(ctx, node)
}

func TestStorageAdapterWaitCancellationDoesNotCancelNativeApply(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	engine := &completionContextEngine{Engine: base}
	adapter, err := NewStorageAdapterWithWAL(engine, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	waiter, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = adapter.ApplyCommand(waiter, publicationNodeCommand(t, "test:wait-independent"), WALQueued)
	require.NoError(t, err)
	require.NotNil(t, engine.nativeContext)
	require.Nil(t, engine.nativeContext.Done(), "accepted native completion is independent of the publication waiter")
	require.NoError(t, engine.nativeContext.Err())
	node, err := base.GetNode(t.Context(), "test:wait-independent")
	require.NoError(t, err)
	require.Equal(t, storage.NodeID("test:wait-independent"), node.ID)
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "test:wait-independent", publishedNodeID(t, entries[0]))
}

func TestReplicatedEngineCanceledPreparationDoesNotPublishAndRetries(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	replicator := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, replicator.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, replicator.Shutdown()) })
	engine := NewReplicatedEngine(base, replicator, time.Second)
	request, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = engine.CreateNode(request, &storage.Node{ID: "test:retry", Labels: []string{"Context"}})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, adapter.FlushWAL())
	position, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Zero(t, position)
	count, err := base.NodeCount(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: "test:retry", Labels: []string{"Context"}})
	require.NoError(t, err)
	count, err = base.NodeCount(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(0, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "healthy retry publishes once")
}

type leaderPreparationContextKey struct{}
type leaderPreparationContextStorage struct {
	*StorageAdapter
	cancel context.CancelFunc
	seen   context.Context
}

func (s *leaderPreparationContextStorage) PrepareEntityCommand(ctx context.Context, cmd *Command) (*Command, func(storage.CommitOutcome), bool, error) {
	s.seen = ctx
	if cancel := s.cancel; cancel != nil {
		s.cancel = nil
		cancel()
	}
	return s.StorageAdapter.PrepareEntityCommand(ctx, cmd)
}

type forwardContextReplicator struct {
	Replicator
	seen context.Context
}

func (r *forwardContextReplicator) Apply(ctx context.Context, cmd *Command, timeout time.Duration) error {
	r.seen = ctx
	return r.Replicator.Apply(ctx, cmd, timeout)
}

func TestReplicatedEngineLeaderPreparationReceivesRequestContext(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	owner := &leaderPreparationContextStorage{StorageAdapter: adapter}
	replicator := NewStandaloneReplicator(DefaultConfig(), owner)
	require.NoError(t, replicator.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, replicator.Shutdown()) })
	capture := &forwardContextReplicator{Replicator: replicator}
	engine := NewReplicatedEngine(base, capture, time.Second)
	request, cancel := context.WithCancel(context.WithValue(t.Context(), leaderPreparationContextKey{}, "request"))
	defer cancel()
	owner.cancel = cancel
	err = engine.ForwardEntityChanges(request, []storage.Operation{{Type: storage.OpCreateNode, Node: &storage.Node{ID: "test:leader-request"}}}, storage.CommitModeDirect)
	require.Same(t, request, capture.seen, "forwarding preserves a nonnil caller context")
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, owner.seen)
	require.Equal(t, "request", owner.seen.Value(leaderPreparationContextKey{}))
	require.NoError(t, adapter.FlushWAL())
	position, err := adapter.GetWALPosition()
	require.NoError(t, err)
	require.Zero(t, position)
	count, err := base.NodeCount(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	err = engine.ForwardEntityChanges(t.Context(), []storage.Operation{{Type: storage.OpCreateNode, Node: &storage.Node{ID: "test:leader-request"}}}, storage.CommitModeDirect)
	require.NoError(t, err)
	count, err = base.NodeCount(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}
