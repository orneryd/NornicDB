package search

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// consumerNotFoundBarrier pauses the first armed read that observes a missing
// node, after the observation and before the reader acts on it. It needs no
// goroutine stacks: the observation itself is the barrier.
type consumerNotFoundBarrier struct {
	storage.Engine
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *consumerNotFoundBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := b.Engine.GetNode(id)
	if errors.Is(err, storage.ErrNotFound) && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return node, err
}

// An orphan observation made before a recreate must not remove the newer,
// valid vector once the recreate has been indexed.
func consumerOrphanObservationRace(t *testing.T, read func(context.Context, *Service) error) {
	t.Helper()
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	barrier := &consumerNotFoundBarrier{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
	svc := NewService(barrier, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	old := &storage.Node{ID: "source", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(old)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(old))
	require.NoError(t, store.DeleteNode("source"))
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- read(ctx, svc) }()
	select {
	case <-barrier.entered:
	case <-time.After(5 * time.Second):
		close(barrier.release)
		t.Fatal("reader did not observe the orphan")
	}
	newer := &storage.Node{ID: "source", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err = store.CreateNode(newer)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(newer))
	close(barrier.release)
	require.NoError(t, <-done)
	_, err = store.GetNode("source")
	require.NoError(t, err, "the recreated source is stored")
	require.Equal(t, 1, svc.EmbeddingCount(), "an older not-found observation removed a newer live vector")
}

func TestSearchMaterializationOrphanKeepsNewerIndex(t *testing.T) {
	consumerOrphanObservationRace(t, func(ctx context.Context, svc *Service) error {
		_, err := svc.Search(ctx, "", []float32{1, 0}, DefaultSearchOptions())
		return err
	})
}

func TestSearchTypeFilterOrphanKeepsNewerIndex(t *testing.T) {
	consumerOrphanObservationRace(t, func(ctx context.Context, svc *Service) error {
		opts := DefaultSearchOptions()
		opts.Types = []string{"Doc"}
		_, err := svc.Search(ctx, "", []float32{1, 0}, opts)
		return err
	})
}
