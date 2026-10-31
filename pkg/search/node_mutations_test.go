package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestNodeMutationReservationsPreserveNewerWrites owns the reservation
// contract for a node's index entries: an older write that applies after a
// newer generation retired is obsolete, both when the newer write left the
// node indexed (a live re-creation, or a storage delete plus refresh) and when
// it removed it (a removal, or a storage delete), and under concurrent writers
// mixing reservations with direct storage writes.
func TestNodeMutationReservationsPreserveNewerWrites(t *testing.T) {
	t.Run("older live write after a newer one", func(t *testing.T) {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("remove=%t", remove), func(t *testing.T) {
				base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				t.Cleanup(func() { _ = base.Close() })
				engine := storage.NewNamespacedEngine(base, "test")
				svc := NewService(engine, 2, nil)
				t.Cleanup(func() { _ = svc.Close() })
				old := &storage.Node{ID: "source", Properties: map[string]any{"content": "obsolete"}, ChunkEmbeddings: [][]float32{{1, 0}}}
				reservation := svc.ReserveNodeMutation(old.ID, storage.CaptureVectorWorkOrigin())
				newer := storage.CopyNode(old)
				newer.Properties["content"] = "current"
				newer.ChunkEmbeddings = [][]float32{{0, 1}}
				_, err := engine.CreateNode(newer)
				require.NoError(t, err)
				require.NoError(t, svc.RefreshNode(newer.ID, storage.CaptureVectorWorkOrigin()))
				if remove {
					require.NoError(t, engine.DeleteNode(old.ID))
					require.NoError(t, svc.RefreshNode(old.ID, storage.CaptureVectorWorkOrigin()))
				}
				// A reserved write can arrive after the latest generation was applied
				// and its bookkeeping retired. It must still remain obsolete.
				require.NoError(t, svc.applyReservedNodeIndex(old, reservation))
				require.NoError(t, svc.applyReservedNodeRemoval(reservation))
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
				require.Empty(t, svc.nodeMutations.pending)
				svc.nodeMutationMu.Unlock()
			})
		}
	})

	t.Run("concurrent reservations and direct writes", func(t *testing.T) {
		base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
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
					reservation := svc.ReserveNodeMutation(node.ID, storage.CaptureVectorWorkOrigin())
					if (writer+i)%2 == 0 {
						_ = engine.DeleteNode(node.ID)
					} else if err := engine.UpdateNode(storage.NodeUpdateRequest{After: node}); err != nil {
						_, _ = engine.CreateNode(node)
					}
					_ = svc.RefreshNode(node.ID, storage.CaptureVectorWorkOrigin())
					if stored, err := engine.GetNode(node.ID); err == nil {
						_ = svc.applyReservedNodeIndex(stored, reservation)
					} else {
						_ = svc.applyReservedNodeRemoval(reservation)
					}
				}
			}()
		}
		writers.Wait()
		for i := range 8 {
			id := storage.NodeID(fmt.Sprintf("n%d", i))
			stale := svc.ReserveNodeMutation(id, storage.CaptureVectorWorkOrigin())
			_ = engine.DeleteNode(id)
			require.NoError(t, svc.RefreshNode(id, storage.CaptureVectorWorkOrigin()))
			// The older reservation is obsolete: handing it a payload must not
			// restore the deleted vector.
			require.NoError(t, svc.applyReservedNodeIndex(&storage.Node{ID: id, ChunkEmbeddings: [][]float32{{1, 0}}}, stale))
		}
		require.Zero(t, svc.EmbeddingCount())
	})
}

// A refresh rejected at entry never happened: the reservation it superseded
// stays current, so the older queued or pending write still applies.
func TestDiscardedReservationRestoresTheOneItSuperseded(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	older := svc.ReserveNodeMutation("source", storage.CaptureVectorWorkOrigin())
	rejected := svc.ReserveNodeMutation("source", storage.CaptureVectorWorkOrigin())
	svc.DiscardNodeMutation(rejected)
	require.NoError(t, svc.applyReservedNodeIndex(&storage.Node{ID: "source", ChunkEmbeddings: [][]float32{{1, 0}}}, older))
	require.Equal(t, 1, svc.EmbeddingCount(), "a rejected reservation superseded the older write")
}

// A refresh indexes what storage holds when it reads, and a refresh reserved
// before a newer one stays obsolete after the newer one retired.
func TestRefreshNodeFollowsStorage(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	node := &storage.Node{ID: "source", Properties: map[string]any{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	stale := svc.ReserveNodeMutation("source", storage.CaptureVectorWorkOrigin())
	require.NoError(t, svc.RefreshNode("source", storage.CaptureVectorWorkOrigin()))
	require.Equal(t, 1, svc.EmbeddingCount())
	require.NoError(t, engine.DeleteNode("source"))
	require.NoError(t, svc.RefreshNode("source", storage.CaptureVectorWorkOrigin()))
	require.Zero(t, svc.EmbeddingCount())
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.RefreshNodeWithMutation(stale))
	require.Zero(t, svc.EmbeddingCount(), "a superseded refresh applied after the newer one retired")
	svc.nodeMutationMu.Lock()
	require.Empty(t, svc.nodeMutations.pending)
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
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
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
	older := svc.ReserveEdgeMutation("r", storage.CaptureVectorWorkOrigin())
	done := make(chan error, 1)
	go func() { done <- svc.RefreshEdgeWithMutation(older) }()
	<-barrier.entered
	require.NoError(t, engine.DeleteEdge("r"))
	require.NoError(t, svc.RefreshEdge("r", storage.CaptureVectorWorkOrigin()))
	close(barrier.release)
	require.NoError(t, <-done)
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "an older relationship refresh resurrected a deleted vector")
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"vec": []float32{1, 0}}}))
	require.NoError(t, svc.RefreshEdge("r", storage.CaptureVectorWorkOrigin()))
	require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "a refreshed relationship is indexed")
}
