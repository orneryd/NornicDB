package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerNodeReadsRejectCorruption(t *testing.T) {
	reads := map[string]func(*BadgerEngine) error{
		"first": func(b *BadgerEngine) error {
			_, err := b.GetFirstNodeByLabelInScope(t.Context(), "test:", "L")
			return err
		},
		"label": func(b *BadgerEngine) error {
			_, err := b.GetNodesByLabelInScope(context.Background(), "test:", "L")
			return err
		},
		"stream": func(b *BadgerEngine) error {
			return b.StreamNodesByLabelProjectedInScope(context.Background(), "test:", "L", nil, func(*Node) error { return nil })
		},
		"projected": func(b *BadgerEngine) error {
			return b.StreamNodesByLabelProjectedInScope(context.Background(), "test:", "L", []string{"x"}, func(*Node) error { return nil })
		},
		"all": func(b *BadgerEngine) error { _, err := b.AllNodes(context.Background()); return err },
		"batch": func(b *BadgerEngine) error {
			_, err := b.BatchGetNodes(context.Background(), []NodeID{"test:a1", "test:a2"})
			return err
		},
		"batch without embeddings": func(b *BadgerEngine) error {
			_, err := b.BatchGetNodesWithoutEmbeddings(context.Background(), []NodeID{"test:a1", "test:a2"})
			return err
		},
	}
	for name, read := range reads {
		for _, corrupt := range []bool{false, true} {
			state := "missing"
			if corrupt {
				state = "corrupt"
			}
			t.Run(name+"/"+state, func(t *testing.T) {
				b := newTestEngine(t)
				for _, id := range []NodeID{"test:a1", "test:a2"} {
					_, err := b.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"x": int64(2)}})
					require.NoError(t, err)
				}
				require.NoError(t, b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
					if corrupt {
						return txn.Set(nodeKey("test:a1"), []byte{0xff})
					}
					return txn.Delete(nodeKey("test:a1"))
				}))
				b.nodeCacheMu.Lock()
				clear(b.nodeCache)
				b.nodeCacheMu.Unlock()
				err := read(b)
				if corrupt {
					require.ErrorContains(t, err, "0xff")
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestBadgerEdgeReadsRejectCorruption(t *testing.T) {
	reads := map[string]func(*BadgerEngine) error{
		"all":  func(b *BadgerEngine) error { _, err := b.AllEdges(context.Background()); return err },
		"type": func(b *BadgerEngine) error { _, err := b.GetEdgesByType(context.Background(), "R"); return err },
		"outgoing": func(b *BadgerEngine) error {
			_, err := b.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Node: "test:a"})
			return err
		},
		"incoming": func(b *BadgerEngine) error {
			_, err := b.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:z"})
			return err
		},
		"adjacent": func(b *BadgerEngine) error {
			_, err := b.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBoth, Node: "test:a"})
			return err
		},
		"between": func(b *BadgerEngine) error {
			_, err := b.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:z"})
			return err
		},
		"legacy between": func(b *BadgerEngine) error {
			_, err := b.edgesBetweenFromLegacyOutgoingIndex(context.Background(), "test:a", "test:z", "R")
			return err
		},
		"matching": func(b *BadgerEngine) error {
			_, err := b.MatchEdgesBetween(context.Background(), "test:a", "test:z", "R", []string{"x"}, func(*Edge) bool { return true })
			return err
		},
	}
	for name, read := range reads {
		for _, warm := range []bool{false, true} {
			for _, corrupt := range []bool{false, true} {
				state := "missing"
				if corrupt {
					state = "corrupt"
				}
				if warm {
					state += "/warm adjacency"
				}
				t.Run(name+"/"+state, func(t *testing.T) {
					b := newTestEngine(t)
					for _, id := range []NodeID{"test:a", "test:z"} {
						_, err := b.CreateNode(t.Context(), &Node{ID: id})
						require.NoError(t, err)
					}
					require.NoError(t, b.CreateEdge(t.Context(), &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:z", Type: "R", Properties: map[string]any{"x": int64(1)}}))
					if warm {
						_, err := b.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBoth, Node: "test:a"})
						require.NoError(t, err)
						_, err = b.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:z"})
						require.NoError(t, err)
					}
					require.NoError(t, b.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
						if corrupt {
							return txn.Set(edgeKey("test:e"), []byte{0xff})
						}
						return txn.Delete(edgeKey("test:e"))
					}))
					b.cacheInvalidateEdges()
					b.InvalidateEdgeTypeCache()
					err := read(b)
					if corrupt {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				})
			}
		}

	}
}

func TestBadgerAdjacencyReadsPropagateTransactionErrors(t *testing.T) {
	t.Run("discarded transaction", func(t *testing.T) {
		b := newTestEngine(t)
		require.NoError(t, b.withView(func(txn *badger.Txn) error {
			txn.Discard()
			edge, err := b.readIndexedEdgeInTxn(txn, "test:uncached", b.edgeCacheGen.current())
			require.ErrorIs(t, err, badger.ErrDiscardedTxn)
			require.Nil(t, edge)
			return nil
		}))
	})
	t.Run("closed engine", func(t *testing.T) {
		b := newTestEngine(t)
		require.NoError(t, b.Close())
		edges, err := b.materializeAdjEdges(context.Background(), []EdgeID{"test:uncached"}, visibleRead)
		require.ErrorIs(t, err, ErrStorageClosed)
		require.Nil(t, edges)
	})
}
