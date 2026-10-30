package linkpredict

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type graphBuilderReadEngine struct {
	storage.Engine
	read func(storage.NodeID) ([]*storage.Edge, error)
}

func (e *graphBuilderReadEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	return e.read(id)
}

func TestGraphBuilderPropagatesRelationshipReadFailure(t *testing.T) {
	store := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer store.Close()
	_, err := store.CreateNode(&storage.Node{ID: "test:a"})
	require.NoError(t, err)
	failed := errors.New("relationship read unavailable")
	engine := &graphBuilderReadEngine{Engine: store, read: func(storage.NodeID) ([]*storage.Edge, error) { return nil, failed }}
	cfg := DefaultBuildConfig()
	cfg.WorkerCount = 1
	cfg.CachePath = t.TempDir()
	cfg.GCAfterChunk = false
	builder := NewGraphBuilder(engine, cfg)
	graph, err := builder.Build(context.Background())
	require.ErrorIs(t, err, failed)
	require.Nil(t, graph)
	require.Zero(t, builder.Stats().BuildsCompleted)
	_, err = os.Stat(builder.cacheFilePath())
	require.True(t, os.IsNotExist(err), "a failed partial graph must not be cached")
}
func TestGraphBuilderReportsCancellationDuringLastChunk(t *testing.T) {
	store := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer store.Close()
	_, err := store.CreateNode(&storage.Node{ID: "test:a"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine := &graphBuilderReadEngine{Engine: store, read: func(storage.NodeID) ([]*storage.Edge, error) { cancel(); return nil, nil }}
	cfg := DefaultBuildConfig()
	cfg.WorkerCount = 1
	cfg.GCAfterChunk = false
	builder := NewGraphBuilder(engine, cfg)
	graph, err := builder.Build(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, graph)
	require.Zero(t, builder.Stats().BuildsCompleted)
}

func TestGraphBuilderCancellationWaitsForOwnedWorkers(t *testing.T) {
	store := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer store.Close()
	for _, id := range []storage.NodeID{"test:a", "test:b", "test:c"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int64
	engine := &graphBuilderReadEngine{Engine: store, read: func(storage.NodeID) ([]*storage.Edge, error) {
		if reads.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := DefaultBuildConfig()
	cfg.WorkerCount = 1
	cfg.GCAfterChunk = false
	type reply struct {
		graph Graph
		err   error
	}
	done := make(chan reply, 1)
	go func() { g, err := NewGraphBuilder(engine, cfg).Build(ctx); done <- reply{g, err} }()
	<-started
	cancel()
	select {
	case <-done:
		close(release)
		t.Fatal("builder returned while a worker still owned graph writes")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case result := <-done:
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.graph)
	case <-time.After(2 * time.Second):
		t.Fatal("canceled build failed to join its worker")
	}
	require.Equal(t, int64(1), reads.Load(), "cancellation must not start queued storage reads")
}
