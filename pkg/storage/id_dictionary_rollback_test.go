package storage

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// The pending-entry lifecycle: an allocated entry is pending until the
// transaction that wrote its keys commits; a discarded transaction leaves it
// pending; any transaction that resolves a pending entry writes its keys and
// clears the mark when it commits; deleting an entry drops the mark.
func TestIDDictionaryPendingEntries(t *testing.T) {
	engine := createTestBadgerEngine(t)
	d := engine.idDict
	newTxn := writeTxnFor(t, engine)
	pending := func(node NodeID, edge EdgeID) (bool, bool) {
		_, n := d.nodePendingWriter(node)
		_, e := d.edgePendingWriter(edge)
		return n, e
	}
	forwardKeys := func(txn *badger.Txn) (bool, bool) {
		_, nodeErr := txn.Get(nodeIDForwardKey("p:n"))
		_, edgeErr := txn.Get(edgeIDForwardKey("p:e"))
		return nodeErr == nil, edgeErr == nil
	}

	// Allocated, then discarded: still pending, keys gone.
	first := newTxn()
	nodeNum, err := resolveOrAllocateNumericIDInTxn(d, first, NodeID("p:n"))
	require.NoError(t, err)
	edgeNum, err := resolveOrAllocateNumericIDInTxn(d, first, EdgeID("p:e"))
	require.NoError(t, err)
	n, e := pending("p:n", "p:e")
	require.True(t, n)
	require.True(t, e)
	// The transaction that wrote a pending entry's keys resolves it again
	// without writing them again.
	again, err := resolveOrAllocateNumericIDInTxn(d, first, NodeID("p:n"))
	require.NoError(t, err)
	require.Equal(t, nodeNum, again)
	again, err = resolveOrAllocateNumericIDInTxn(d, first, EdgeID("p:e"))
	require.NoError(t, err)
	require.Equal(t, edgeNum, again)
	require.Equal(t, []NodeID{"p:n"}, d.txnCounters[first].nodes)
	require.Equal(t, []EdgeID{"p:e"}, d.txnCounters[first].edges)
	require.False(t, d.txnWrote(nil, d.txnCounters[first]))
	d.discardTxnCounters(first)
	first.Discard()
	n, e = pending("p:n", "p:e")
	require.True(t, n)
	require.True(t, e)

	// Resolved by another transaction: same numbers, keys written, and the
	// commit clears the marks.
	second := newTxn()
	gotNode, err := resolveOrAllocateNumericIDInTxn(d, second, NodeID("p:n"))
	require.NoError(t, err)
	gotEdge, err := resolveOrAllocateNumericIDInTxn(d, second, EdgeID("p:e"))
	require.NoError(t, err)
	require.Equal(t, nodeNum, gotNode)
	require.Equal(t, edgeNum, gotEdge)
	hasNode, hasEdge := forwardKeys(second)
	require.True(t, hasNode)
	require.True(t, hasEdge)
	d.flushTxnCounters(second)
	require.NoError(t, engine.db.commit(context.Background(), second, nil))
	d.markTxnCommitted(second)
	n, e = pending("p:n", "p:e")
	require.False(t, n)
	require.False(t, e)

	// A committed entry resolves without writing its keys again.
	third := newTxn()
	_, err = resolveOrAllocateNumericIDInTxn(d, third, EdgeID("p:e"))
	require.NoError(t, err)
	require.Empty(t, d.txnCounters[third])
	third.Discard()

	// Deleting an entry drops its mark; marking an unknown txn is a no-op.
	fourth := newTxn()
	_, err = resolveOrAllocateNumericIDInTxn(d, fourth, NodeID("p:gone"))
	require.NoError(t, err)
	_, err = resolveOrAllocateNumericIDInTxn(d, fourth, EdgeID("p:gone-e"))
	require.NoError(t, err)
	require.NoError(t, deleteIDEntryInTxn(fourth, &d.nodes, nodeForwardShard, nodeReverseShard, nodeIDForwardKey, prefixIDDictNodeReverse, "p:gone", 0, idPairByString))
	require.NoError(t, deleteIDEntryInTxn(fourth, &d.edges, edgeForwardShard, edgeReverseShard, edgeIDForwardKey, prefixIDDictEdgeReverse, "p:gone-e", 0, idPairByString))
	n, e = pending("p:gone", "p:gone-e")
	require.False(t, n)
	require.False(t, e)
	d.markTxnCommitted(fourth)
	d.discardTxnCounters(fourth)
	fourth.Discard()
}

// Two writers allocating the same ID: the one that loses the race adopts
// the winner's number and writes the winner's keys over its own, so whichever
// transaction commits persists the number the dictionary uses.
func TestIDDictionaryRaceLoserWritesWinnersKeys(t *testing.T) {
	t.Run("winner accepted", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		d := engine.idDict
		newTxn := writeTxnFor(t, engine)
		winner := newTxn()
		nodeNum, err := resolveOrAllocateNumericIDInTxn(d, winner, NodeID("r:n"))
		require.NoError(t, err)
		edgeNum, err := resolveOrAllocateNumericIDInTxn(d, winner, EdgeID("r:e"))
		require.NoError(t, err)

		// The loser misses the entries on its first look, allocates, and finds
		// the winner's on its re-check.
		setForward := func(present bool) {
			nodes, edges := &d.nodes[nodeForwardShard("r:n")], &d.edges[edgeForwardShard("r:e")]
			nodes.mu.Lock()
			edges.mu.Lock()
			if present {
				nodes.forward["r:n"], edges.forward["r:e"] = nodeNum, edgeNum
			} else {
				delete(nodes.forward, "r:n")
				delete(edges.forward, "r:e")
			}
			edges.mu.Unlock()
			nodes.mu.Unlock()
		}
		setForward(false)
		restore := func() { setForward(true) }
		idAllocationRecheckHook.Store(&restore)
		t.Cleanup(func() { idAllocationRecheckHook.Store(nil) })
		loser := newTxn()
		gotNode, err := resolveOrAllocateNumericIDInTxn(d, loser, NodeID("r:n"))
		require.NoError(t, err)
		edges := &d.edges[edgeForwardShard("r:e")]
		edges.mu.Lock()
		delete(edges.forward, "r:e") // restored by the node allocation's hook
		edges.mu.Unlock()
		gotEdge, err := resolveOrAllocateNumericIDInTxn(d, loser, EdgeID("r:e"))
		require.NoError(t, err)
		require.Equal(t, nodeNum, gotNode)
		require.Equal(t, edgeNum, gotEdge)
		require.Equal(t, nodeNum, forwardNum(t, loser, nodeIDForwardKey("r:n")))
		require.Equal(t, edgeNum, forwardNum(t, loser, edgeIDForwardKey("r:e")))

	})
	t.Run("winner rejected", func(t *testing.T) {
		for _, kind := range []byte{freelistKindNode, freelistKindEdge} {
			t.Run(string(kind), func(t *testing.T) {
				b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
				require.NoError(t, err)
				t.Cleanup(func() { _ = b.Close() })
				newTxn := writeTxnFor(t, b)
				winner := newTxn()
				var winningID uint64
				if kind == freelistKindNode {
					winningID, err = resolveOrAllocateNumericIDInTxn(b.idDict, winner, NodeID("test:race"))
				} else {
					winningID, err = resolveOrAllocateNumericIDInTxn(b.idDict, winner, EdgeID("test:race"))
				}
				require.NoError(t, err)
				b.idDict.flushTxnCounters(winner)
				state := b.idDict.takeTxnCommitting(winner)
				require.NotNil(t, state)

				// Simulate both writers missing the name initially, then expose the
				// winner at the recheck, as in TestIDDictionaryRaceLoserWritesWinnersKeys.
				setForward := func(present bool) {
					if kind == freelistKindNode {
						sh := &b.idDict.nodes[nodeForwardShard("test:race")]
						sh.mu.Lock()
						if present {
							sh.forward["test:race"] = winningID
						} else {
							delete(sh.forward, "test:race")
						}
						sh.mu.Unlock()
					} else {
						sh := &b.idDict.edges[edgeForwardShard("test:race")]
						sh.mu.Lock()
						if present {
							sh.forward["test:race"] = winningID
						} else {
							delete(sh.forward, "test:race")
						}
						sh.mu.Unlock()
					}
				}
				setForward(false)
				restore := func() { setForward(true) }
				reject := func() { b.idDict.abandonPending(state) }
				idAllocationRecheckHook.Store(&restore)
				idAllocationAdoptionHook.Store(&reject)
				t.Cleanup(func() { idAllocationRecheckHook.Store(nil); idAllocationAdoptionHook.Store(nil) })
				loser := newTxn()
				var got uint64
				if kind == freelistKindNode {
					got, err = resolveOrAllocateNumericIDInTxn(b.idDict, loser, NodeID("test:race"))
				} else {
					got, err = resolveOrAllocateNumericIDInTxn(b.idDict, loser, EdgeID("test:race"))
				}
				require.NoError(t, err)
				require.NotEqual(t, winningID, got)
				if kind == freelistKindNode {
					require.Equal(t, got, forwardNum(t, loser, nodeIDForwardKey("test:race")))
					name, ok := b.idDict.lookupNodeIDByNum(got)
					require.True(t, ok)
					require.Equal(t, NodeID("test:race"), name)
				} else {
					require.Equal(t, got, forwardNum(t, loser, edgeIDForwardKey("test:race")))
					name, ok := b.idDict.lookupEdgeIDByNum(got)
					require.True(t, ok)
					require.Equal(t, EdgeID("test:race"), name)
				}
			})
		}

	})
}

func forwardNum(t *testing.T, txn *badger.Txn, key []byte) uint64 {
	t.Helper()
	item, err := txn.Get(key)
	require.NoError(t, err)
	var num uint64
	require.NoError(t, item.Value(func(val []byte) error {
		num = binary.BigEndian.Uint64(val)
		return nil
	}))
	return num
}

// writeTxnFor returns a function opening write transactions on engine's
// store, each discarded and its read released when the test ends.
func writeTxnFor(t *testing.T, engine *BadgerEngine) func() *badger.Txn {
	return func() *badger.Txn {
		txn, readTs, err := engine.db.beginTxn(true)
		require.NoError(t, err)
		t.Cleanup(func() {
			txn.Discard()
			engine.db.endRead(readTs)
		})
		return txn
	}
}
