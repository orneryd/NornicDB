package search

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type sol1482OrphanReadBarrier struct {
	storage.Engine
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *sol1482OrphanReadBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := b.Engine.GetNode(id)
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return node, err
}

func TestSol1482R2OrphanObservationCannotRemoveNewerIndex(t *testing.T) {
	ctx := context.Background()
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	barrier := &sol1482OrphanReadBarrier{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
	svc := NewServiceWithDimensions(barrier, 2)
	t.Cleanup(func() { _ = svc.Close() })
	old := &storage.Node{ID: "source", ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(old)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(old))
	require.NoError(t, store.DeleteNode("source"))
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() { _, err := svc.Search(ctx, "", []float32{1, 0}, DefaultSearchOptions()); done <- err }()
	select {
	case <-barrier.entered:
	case <-time.After(3 * time.Second):
		close(barrier.release)
		t.Fatal("search did not read orphan")
	}
	newer := &storage.Node{ID: "source", Properties: map[string]any{"content": "currentword"}, ChunkEmbeddings: [][]float32{{0, 1}}}
	_, err = store.CreateNode(newer)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(newer))
	close(barrier.release)
	require.NoError(t, <-done)
	require.Equal(t, 1, svc.EmbeddingCount(), "an older not-found observation removed a newer live index")
}
