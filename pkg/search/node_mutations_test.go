package search

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNodeMutationReservationPreservesNewerLiveMutation(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprintf("remove=%t", remove), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = base.Close() })
			engine := storage.NewNamespacedEngine(base, "test")
			svc := NewService(engine, 2, nil)
			t.Cleanup(func() { _ = svc.Close() })
			old := &storage.Node{ID: "source", Properties: map[string]any{"content": "obsolete"}, ChunkEmbeddings: [][]float32{{1, 0}}}
			reservation := svc.ReserveNodeMutation(old.ID)
			newer := storage.CopyNode(old)
			newer.Properties["content"] = "current"
			newer.ChunkEmbeddings = [][]float32{{0, 1}}
			_, err := engine.CreateNode(newer)
			require.NoError(t, err)
			require.NoError(t, svc.RefreshNode(newer.ID))
			if remove {
				require.NoError(t, engine.DeleteNode(old.ID))
				require.NoError(t, svc.RefreshNode(old.ID))
			}
			// An old drain can arrive after the latest generation was applied
			// and its bookkeeping retired. It must still remain obsolete.
			require.NoError(t, svc.IndexNodeWithMutation(old, reservation))
			require.NoError(t, svc.RemoveNodeWithMutation(reservation))
			if remove {
				require.Zero(t, svc.EmbeddingCount())
			} else {
				require.Equal(t, 1, svc.EmbeddingCount())
				opts := DefaultSearchOptions()
				opts.MinRRFScore = 0
				response, err := svc.Search(context.Background(), "current", nil, opts)
				require.NoError(t, err)
				require.Len(t, response.Results, 1)
				response, err = svc.Search(context.Background(), "obsolete", nil, opts)
				require.NoError(t, err)
				require.Empty(t, response.Results)
			}
			svc.nodeMutationMu.Lock()
			require.Empty(t, svc.pendingNodeMutations)
			svc.nodeMutationMu.Unlock()
		})
	}
}

func TestNodeMutationConcurrentReservationsAndDirectWrites(t *testing.T) {
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	var writers sync.WaitGroup
	for writer := range 8 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := range 64 {
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i%8)), ChunkEmbeddings: [][]float32{{1, 0}}}
				reservation := svc.ReserveNodeMutation(node.ID)
				if (writer+i)%2 == 0 {
					_ = engine.DeleteNode(node.ID)
				} else if err := engine.UpdateNode(node); err != nil {
					_, _ = engine.CreateNode(node)
				}
				_ = svc.RefreshNode(node.ID)
				_ = svc.IndexNodeWithMutation(node, reservation)
			}
		}()
	}
	writers.Wait()
	for i := range 8 {
		id := storage.NodeID(fmt.Sprintf("n%d", i))
		stale := svc.ReserveNodeMutation(id)
		_ = engine.DeleteNode(id)
		require.NoError(t, svc.RefreshNode(id))
		require.NoError(t, svc.IndexNodeWithMutation(&storage.Node{ID: id, ChunkEmbeddings: [][]float32{{1, 0}}}, stale))
	}
	require.Zero(t, svc.EmbeddingCount())
}

// A refresh rejected at entry never happened: the reservation it superseded
// stays current, so the older queued or pending write still applies.
func TestDiscardedReservationRestoresTheOneItSuperseded(t *testing.T) {
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	older := svc.ReserveNodeMutation("source")
	rejected := svc.ReserveNodeMutation("source")
	svc.DiscardNodeMutation(rejected)
	require.NoError(t, svc.IndexNodeWithMutation(&storage.Node{ID: "source", ChunkEmbeddings: [][]float32{{1, 0}}}, older))
	require.Equal(t, 1, svc.EmbeddingCount(), "a rejected reservation superseded the older write")
}

// A refresh indexes what storage holds when it reads, and a refresh reserved
// before a newer one stays obsolete after the newer one retired.
func TestRefreshNodeFollowsStorage(t *testing.T) {
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	node := &storage.Node{ID: "source", Properties: map[string]any{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	stale := svc.ReserveNodeMutation("source")
	require.NoError(t, svc.RefreshNode("source"))
	require.Equal(t, 1, svc.EmbeddingCount())
	require.NoError(t, engine.DeleteNode("source"))
	require.NoError(t, svc.RefreshNode("source"))
	require.Zero(t, svc.EmbeddingCount())
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.RefreshNodeWithMutation(stale))
	require.Zero(t, svc.EmbeddingCount(), "a superseded refresh applied after the newer one retired")
	svc.nodeMutationMu.Lock()
	require.Empty(t, svc.pendingNodeMutations)
	svc.nodeMutationMu.Unlock()
}

// edgeReadBarrier pauses the first armed relationship read after it returns.
type edgeReadBarrier struct {
	storage.Engine
	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (b *edgeReadBarrier) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	edge, err := b.Engine.GetEdge(id)
	if b.armed {
		b.armed = false
		close(b.entered)
		<-b.release
	}
	return edge, err
}

// A relationship refresh that read the edge before a newer delete and its
// refresh applied must not put its vector back.
func TestRefreshEdgeReadBeforeNewerDeleteCannotResurrect(t *testing.T) {
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"vec": []float32{1, 0}}}))
	barrier := &edgeReadBarrier{Engine: engine, armed: true, entered: make(chan struct{}), release: make(chan struct{})}
	svc := NewService(barrier, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	older := svc.ReserveEdgeMutation("r")
	done := make(chan error, 1)
	go func() { done <- svc.RefreshEdgeWithMutation(older) }()
	<-barrier.entered
	require.NoError(t, engine.DeleteEdge("r"))
	require.NoError(t, svc.RefreshEdge("r"))
	close(barrier.release)
	require.NoError(t, <-done)
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "an older relationship refresh resurrected a deleted vector")
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"vec": []float32{1, 0}}}))
	require.NoError(t, svc.RefreshEdge("r"))
	require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "a refreshed relationship is indexed")
}
