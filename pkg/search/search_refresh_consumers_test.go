package search

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// orphanReadBarrier keeps the original read trigger while sharing the gate's
// signal/release owner. Consumer reads pause on ErrNotFound; the SOL control
// pauses the first armed read regardless of its result.
type orphanReadBarrier struct {
	storage.Engine
	*fixtureowner.PauseGate
	missingOnly bool
}

func (b *orphanReadBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	node, err := b.Engine.GetNode(id)
	if !b.missingOnly || errors.Is(err, storage.ErrNotFound) {
		b.Pause(b.Armed, b.Entered)
	}
	return node, err
}

type orphanObservationCase struct {
	old, newer                        *storage.Node
	missingOnly, assertStored         bool
	timeout                           time.Duration
	timeoutMessage, completionMessage string
	read                              func(context.Context, *Service) error
}

func docOrphanObservation(read func(context.Context, *Service) error) orphanObservationCase {
	return orphanObservationCase{
		old:         &storage.Node{ID: "source", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}},
		newer:       &storage.Node{ID: "source", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}},
		missingOnly: true, assertStored: true, timeout: 5 * time.Second,
		timeoutMessage:    "reader did not observe the orphan",
		completionMessage: "an older not-found observation removed a newer live vector", read: read,
	}
}

// runOrphanObservation owns seed/index/delete, paused read, recreate/index and
// completion. Case inputs retain the original image, trigger, timeout and oracle.
func runOrphanObservation(t *testing.T, scenario orphanObservationCase) {
	t.Helper()
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	barrier := &orphanReadBarrier{Engine: store, PauseGate: fixtureowner.NewPauseGate(), missingOnly: scenario.missingOnly}
	svc := NewService(barrier, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(barrier.ReleaseWrites)
	_, err := store.CreateNode(scenario.old)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(scenario.old))
	require.NoError(t, store.DeleteNode("source"))
	barrier.Armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- scenario.read(ctx, svc) }()
	fixtureowner.WaitForBarrier(t, barrier.Entered, scenario.timeout, barrier.ReleaseWrites, scenario.timeoutMessage)
	_, err = store.CreateNode(scenario.newer)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(scenario.newer))
	barrier.ReleaseWrites()
	require.NoError(t, <-done)
	if scenario.assertStored {
		_, err = store.GetNode("source")
		require.NoError(t, err, "the recreated source is stored")
	}
	require.Equal(t, 1, svc.EmbeddingCount(), scenario.completionMessage)
}

func TestSearchMaterializationOrphanKeepsNewerIndex(t *testing.T) {
	runOrphanObservation(t, docOrphanObservation(func(ctx context.Context, svc *Service) error {
		_, err := svc.Search(ctx, "", []float32{1, 0}, DefaultSearchOptions())
		return err
	}))
}

func TestSearchTypeFilterOrphanKeepsNewerIndex(t *testing.T) {
	runOrphanObservation(t, docOrphanObservation(func(ctx context.Context, svc *Service) error {
		opts := DefaultSearchOptions()
		opts.Types = []string{"Doc"}
		_, err := svc.Search(ctx, "", []float32{1, 0}, opts)
		return err
	}))
}
