package search

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type solR3ReadBarrier struct {
	storage.Engine
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *solR3ReadBarrier) pause() {
	if b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
}
func (b *solR3ReadBarrier) open() { b.once.Do(func() { close(b.release) }) }
func (b *solR3ReadBarrier) GetNode(id storage.NodeID) (*storage.Node, error) {
	n, e := b.Engine.GetNode(id)
	if id == "source" {
		b.pause()
	}
	return n, e
}
func (b *solR3ReadBarrier) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	n, e := b.Engine.GetEdge(id)
	if id == "r" {
		b.pause()
	}
	return n, e
}

// Both kinds, cold/warm, captured-present/missing, newer delete/update,
// and a newer refresh still pending versus already applied and retired.
func TestSol1482R3RefreshReadMatrix(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, edge := range []bool{false, true} {
			for _, missing := range []bool{false, true} {
				for _, remove := range []bool{false, true} {
					if missing && remove {
						continue
					}
					for _, pending := range []bool{false, true} {
						t.Run(fmt.Sprintf("warm=%t/edge=%t/missing=%t/remove=%t/pending=%t", warm, edge, missing, remove, pending), func(t *testing.T) {
							ctx := context.Background()
							base := newTestMemoryEngine(t)
							t.Cleanup(func() { _ = base.Close() })
							store := storage.NewNamespacedEngine(base, "test")
							for _, id := range []storage.NodeID{"a", "b"} {
								_, err := store.CreateNode(&storage.Node{ID: id})
								require.NoError(t, err)
							}
							node := &storage.Node{ID: "source", Properties: map[string]any{"content": "oldword"}, ChunkEmbeddings: [][]float32{{1, 0}}}
							rel := &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "CITES", Properties: map[string]any{"vec": []float32{1, 0}}}
							if edge {
								require.NoError(t, store.CreateEdge(rel))
							} else {
								_, err := store.CreateNode(node)
								require.NoError(t, err)
							}
							b := &solR3ReadBarrier{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
							t.Cleanup(b.open)
							svc := NewService(b, 2, nil)
							t.Cleanup(func() { _ = svc.Close() })
							if warm {
								require.NoError(t, svc.BuildIndexes(ctx))
							}
							if missing {
								if edge {
									require.NoError(t, store.DeleteEdge("r"))
								} else {
									require.NoError(t, store.DeleteNode("source"))
								}
							}
							b.armed.Store(true)
							done := make(chan error, 1)
							go func() {
								if edge {
									done <- svc.RefreshEdge("r")
								} else {
									done <- svc.RefreshNode("source")
								}
							}()
							select {
							case <-b.entered:
							case <-time.After(5 * time.Second):
								b.open()
								t.Fatal("older refresh did not read")
							}
							if remove {
								if edge {
									require.NoError(t, store.DeleteEdge("r"))
								} else {
									require.NoError(t, store.DeleteNode("source"))
								}
							} else {
								if edge {
									rel.Properties["vec"] = []float32{0, 1}
									if missing {
										require.NoError(t, store.CreateEdge(rel))
									} else {
										require.NoError(t, store.UpdateEdge(rel))
									}
								} else {
									node.Properties["content"] = "newword"
									node.ChunkEmbeddings = [][]float32{{0, 1}}
									if missing {
										_, err := store.CreateNode(node)
										require.NoError(t, err)
									} else {
										require.NoError(t, store.UpdateNode(node))
									}
								}
							}
							var newer func() error
							if edge {
								m := svc.ReserveEdgeMutation("r")
								newer = func() error { return svc.RefreshEdgeWithMutation(m) }
							} else {
								m := svc.ReserveNodeMutation("source")
								newer = func() error { return svc.RefreshNodeWithMutation(m) }
							}
							if !pending {
								require.NoError(t, newer())
							}
							b.open()
							require.NoError(t, <-done)
							if pending {
								require.NoError(t, newer())
							}
							if edge {
								svc.mu.RLock()
								vec := append([]float32(nil), svc.edgePropVector["r"]["vec"]...)
								svc.mu.RUnlock()
								if remove {
									require.Empty(t, vec)
								} else {
									require.Equal(t, []float32{0, 1}, vec)
								}
							} else {
								if remove {
									require.Zero(t, svc.EmbeddingCount())
								} else {
									require.Equal(t, 1, svc.EmbeddingCount())
									hits, err := svc.VectorQueryNodes(ctx, []float32{0, 1}, VectorQuerySpec{Limit: 1})
									require.NoError(t, err)
									require.Len(t, hits, 1)
									require.InDelta(t, 1, hits[0].Score, 1e-6)
								}
							}
							svc.nodeMutationMu.Lock()
							require.Empty(t, svc.nodeMutations.pending)
							require.Empty(t, svc.edgeMutations.pending)
							svc.nodeMutationMu.Unlock()
						})
					}
				}
			}
		}
	}
}

type solR3RelationshipSnapshot struct {
	storage.Engine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *solR3RelationshipSnapshot) open() { b.once.Do(func() { close(b.release) }) }
func (b *solR3RelationshipSnapshot) AllEdges() ([]*storage.Edge, error) {
	edges, err := b.Engine.AllEdges()
	close(b.entered)
	<-b.release
	return edges, err
}

// The optional relationship index starts separately from BuildIndexes. Its
// snapshot is another publisher and must not overwrite a completed refresh.
func TestSol1482R3RelationshipStartupCannotOverwriteRefresh(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprintf("remove=%t", remove), func(t *testing.T) {
			base := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "test")
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := store.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			edge := &storage.Edge{ID: "r", Type: "CITES", StartNode: "a", EndNode: "b", Properties: map[string]any{"reason": "oldword"}}
			require.NoError(t, store.CreateEdge(edge))
			b := &solR3RelationshipSnapshot{Engine: store, entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(b.open)
			svc := NewService(b, 2, nil)
			t.Cleanup(func() { _ = svc.Close() })
			svc.EnableRelationshipSearch(nil)
			svc.StartRelationshipIndex()
			select {
			case <-b.entered:
			case <-time.After(5 * time.Second):
				b.open()
				t.Fatal("relationship startup did not take snapshot")
			}
			if remove {
				require.NoError(t, store.DeleteEdge("r"))
			} else {
				edge.Properties["reason"] = "newword"
				require.NoError(t, store.UpdateEdge(edge))
			}
			require.NoError(t, svc.RefreshEdge("r"))
			b.open()
			select {
			case <-svc.relationshipSearch().ready:
			case <-time.After(5 * time.Second):
				t.Fatal("relationship startup did not finish")
			}
			if remove {
				rs := svc.relationshipSearch()
				rs.mu.RLock()
				entry := rs.entries["r"]
				rs.mu.RUnlock()
				require.Nil(t, entry, "startup resurrected the deleted relationship entry")
			} else {
				response, err := svc.SearchRelationships(context.Background(), "newword", nil, &SearchOptions{Limit: 10, Relationships: true})
				require.NoError(t, err)
				require.Len(t, response.Results, 1, "startup erased the newer relationship text")
			}
		})
	}
}
