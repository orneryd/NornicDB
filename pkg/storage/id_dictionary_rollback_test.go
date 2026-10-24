package storage

import (
	"encoding/binary"
	"testing"

	"github.com/dgraph-io/badger/v4"
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
	nodeNum, err := d.resolveOrAllocateNodeNumIDInTxn(first, "p:n")
	require.NoError(t, err)
	edgeNum, err := d.resolveOrAllocateEdgeNumIDInTxn(first, "p:e")
	require.NoError(t, err)
	n, e := pending("p:n", "p:e")
	require.True(t, n)
	require.True(t, e)
	// The transaction that wrote a pending entry's keys resolves it again
	// without writing them again.
	again, err := d.resolveOrAllocateNodeNumIDInTxn(first, "p:n")
	require.NoError(t, err)
	require.Equal(t, nodeNum, again)
	again, err = d.resolveOrAllocateEdgeNumIDInTxn(first, "p:e")
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
	gotNode, err := d.resolveOrAllocateNodeNumIDInTxn(second, "p:n")
	require.NoError(t, err)
	gotEdge, err := d.resolveOrAllocateEdgeNumIDInTxn(second, "p:e")
	require.NoError(t, err)
	require.Equal(t, nodeNum, gotNode)
	require.Equal(t, edgeNum, gotEdge)
	hasNode, hasEdge := forwardKeys(second)
	require.True(t, hasNode)
	require.True(t, hasEdge)
	d.flushTxnCounters(second)
	require.NoError(t, engine.db.commit(unknownNodeCacheWrites(nodeCacheUnknownManaged), second))
	d.markTxnCommitted(second)
	n, e = pending("p:n", "p:e")
	require.False(t, n)
	require.False(t, e)

	// A committed entry resolves without writing its keys again.
	third := newTxn()
	_, err = d.resolveOrAllocateEdgeNumIDInTxn(third, "p:e")
	require.NoError(t, err)
	require.Empty(t, d.txnCounters[third])
	third.Discard()

	// Deleting an entry drops its mark; marking an unknown txn is a no-op.
	fourth := newTxn()
	_, err = d.resolveOrAllocateNodeNumIDInTxn(fourth, "p:gone")
	require.NoError(t, err)
	_, err = d.resolveOrAllocateEdgeNumIDInTxn(fourth, "p:gone-e")
	require.NoError(t, err)
	require.NoError(t, d.deleteNodeEntryInTxn(fourth, "p:gone"))
	require.NoError(t, d.deleteEdgeEntryInTxn(fourth, "p:gone-e"))
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
	engine := createTestBadgerEngine(t)
	d := engine.idDict
	newTxn := writeTxnFor(t, engine)
	winner := newTxn()
	nodeNum, err := d.resolveOrAllocateNodeNumIDInTxn(winner, "r:n")
	require.NoError(t, err)
	edgeNum, err := d.resolveOrAllocateEdgeNumIDInTxn(winner, "r:e")
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
	gotNode, err := d.resolveOrAllocateNodeNumIDInTxn(loser, "r:n")
	require.NoError(t, err)
	edges := &d.edges[edgeForwardShard("r:e")]
	edges.mu.Lock()
	delete(edges.forward, "r:e") // restored by the node allocation's hook
	edges.mu.Unlock()
	gotEdge, err := d.resolveOrAllocateEdgeNumIDInTxn(loser, "r:e")
	require.NoError(t, err)
	require.Equal(t, nodeNum, gotNode)
	require.Equal(t, edgeNum, gotEdge)
	require.Equal(t, nodeNum, forwardNum(t, loser, nodeIDForwardKey("r:n")))
	require.Equal(t, edgeNum, forwardNum(t, loser, edgeIDForwardKey("r:e")))
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
