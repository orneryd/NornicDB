package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerNodeReadsRejectCorruption(t *testing.T) {
	reads := map[string]func(*BadgerEngine) error{
		"first": func(b *BadgerEngine) error { _, err := b.GetFirstNodeByLabelInScope("test:", "L"); return err },
		"label": func(b *BadgerEngine) error { _, err := b.GetNodesByLabelInScope("test:", "L"); return err },
		"stream": func(b *BadgerEngine) error {
			return b.StreamNodesByLabelProjectedInScope("test:", "L", nil, func(*Node) error { return nil })
		},
		"projected": func(b *BadgerEngine) error {
			return b.StreamNodesByLabelProjectedInScope("test:", "L", []string{"x"}, func(*Node) error { return nil })
		},
		"all": func(b *BadgerEngine) error { _, err := b.AllNodes(); return err },
		"batch": func(b *BadgerEngine) error {
			_, err := b.BatchGetNodes([]NodeID{"test:a1", "test:a2"})
			return err
		},
		"batch without embeddings": func(b *BadgerEngine) error {
			_, err := b.BatchGetNodesWithoutEmbeddings([]NodeID{"test:a1", "test:a2"})
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
					_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"x": int64(2)}})
					require.NoError(t, err)
				}
				require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
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
		"all":      func(b *BadgerEngine) error { _, err := b.AllEdges(); return err },
		"type":     func(b *BadgerEngine) error { _, err := b.GetEdgesByType("R"); return err },
		"outgoing": func(b *BadgerEngine) error { _, err := b.GetOutgoingEdges("test:a"); return err },
		"incoming": func(b *BadgerEngine) error { _, err := b.GetIncomingEdges("test:z"); return err },
		"adjacent": func(b *BadgerEngine) error { _, _, err := b.GetAdjacentEdges("test:a"); return err },
		"between":  func(b *BadgerEngine) error { _, err := b.GetEdgesBetween("test:a", "test:z"); return err },
		"legacy between": func(b *BadgerEngine) error {
			_, err := b.edgesBetweenFromLegacyOutgoingIndex("test:a", "test:z", "R")
			return err
		},
		"matching": func(b *BadgerEngine) error {
			_, err := b.MatchEdgesBetween("test:a", "test:z", "R", []string{"x"}, func(*Edge) bool { return true })
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
						_, err := b.CreateNode(&Node{ID: id})
						require.NoError(t, err)
					}
					require.NoError(t, b.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:z", Type: "R", Properties: map[string]any{"x": int64(1)}}))
					if warm {
						_, _, err := b.GetAdjacentEdges("test:a")
						require.NoError(t, err)
						_, err = b.GetIncomingEdges("test:z")
						require.NoError(t, err)
					}
					require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
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
			edge, err := b.readIndexedEdgeInTxn(txn, "test:uncached")
			require.ErrorIs(t, err, badger.ErrDiscardedTxn)
			require.Nil(t, edge)
			return nil
		}))
	})
	t.Run("closed engine", func(t *testing.T) {
		b := newTestEngine(t)
		require.NoError(t, b.Close())
		edges, err := b.materializeAdjEdges([]EdgeID{"test:uncached"})
		require.ErrorIs(t, err, ErrStorageClosed)
		require.Nil(t, edges)
	})
}
