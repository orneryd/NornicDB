package storage

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// idDictTestEngine returns a BadgerEngine on a temp dir so the
// id-dictionary tests have a real KV store to write txn-scoped state
// against. The MemoryEngine wrapper is used because callers need both
// the engine surface (for round-trip Create/Delete) and direct access
// to the embedded *BadgerEngine.idDict.
func idDictTestEngine(t *testing.T) *MemoryEngine {
	t.Helper()
	e := newTestMemoryEngine(t)
	return e
}

func TestIDDictionary_CurrentFreelistTTL(t *testing.T) {
	d := newIDDictionary()

	// Default when not set: defaultIDFreelistTTL.
	require.Equal(t, defaultIDFreelistTTL, d.currentFreelistTTL())

	// Negative or zero falls back to default.
	d.setFreelistTTL(0)
	require.Equal(t, defaultIDFreelistTTL, d.currentFreelistTTL())
	d.setFreelistTTL(-time.Second)
	require.Equal(t, defaultIDFreelistTTL, d.currentFreelistTTL())

	// Positive override is honored.
	d.setFreelistTTL(2 * time.Hour)
	require.Equal(t, 2*time.Hour, d.currentFreelistTTL())
}

func TestIDDictionary_PushFreeEdge_BumpsPendingAndPersists(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return d.pushFreeEdgeInTxn(txn, 42)
	}))

	_, edges := d.FreelistPending()
	require.Equal(t, int64(1), edges, "pushFreeEdge must increment pending counter")

	// Same call counts staged in the on-disk freelist.
	count, err := d.freelistStagedCount(e.db, freelistKindEdge)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestIDDictionary_FreelistStagedCount_Both(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		if err := d.pushFreeNodeInTxn(txn, 1); err != nil {
			return err
		}
		if err := d.pushFreeNodeInTxn(txn, 2); err != nil {
			return err
		}
		return d.pushFreeEdgeInTxn(txn, 100)
	}))

	nodes, err := d.freelistStagedCount(e.db, freelistKindNode)
	require.NoError(t, err)
	require.Equal(t, 2, nodes)

	edges, err := d.freelistStagedCount(e.db, freelistKindEdge)
	require.NoError(t, err)
	require.Equal(t, 1, edges)
}

func TestIDDictionary_DeleteNodeEntryInTxn_RemovesForwardAndReverse(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id NodeID = "test:n-delete"

	// Allocate a numID for the node.
	var numAllocated uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		num, err := resolveOrAllocateNumericIDInTxn(d, txn, NodeID(id))
		numAllocated = num
		return err
	}))
	require.Greater(t, numAllocated, uint64(0))

	// Confirm both lookup directions hit before delete.
	got, ok := d.lookupNodeNumID(id)
	require.True(t, ok)
	require.Equal(t, numAllocated, got)
	gotID, ok := d.lookupNodeIDByNum(numAllocated)
	require.True(t, ok)
	require.Equal(t, id, gotID)

	// Delete drops both.
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return deleteIDEntryInTxn(txn, &d.nodes, nodeForwardShard, nodeReverseShard, nodeIDForwardKey, prefixIDDictNodeReverse, id, 0, idPairByString)
	}))
	_, ok = d.lookupNodeNumID(id)
	require.False(t, ok)
	_, ok = d.lookupNodeIDByNum(numAllocated)
	require.False(t, ok)

	// Deleting an unknown ID is a no-op.
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return deleteIDEntryInTxn(txn, &d.nodes, nodeForwardShard, nodeReverseShard, nodeIDForwardKey, prefixIDDictNodeReverse, "test:nonexistent", 0, idPairByString)
	}))
}

func TestIDDictionary_DeleteEdgeEntryInTxn_RemovesForwardAndReverse(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id EdgeID = "test:e-delete"
	var numAllocated uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		num, err := resolveOrAllocateNumericIDInTxn(d, txn, EdgeID(id))
		numAllocated = num
		return err
	}))
	require.Greater(t, numAllocated, uint64(0))

	gotID, ok := d.lookupEdgeIDByNum(numAllocated)
	require.True(t, ok)
	require.Equal(t, id, gotID)

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return deleteIDEntryInTxn(txn, &d.edges, edgeForwardShard, edgeReverseShard, edgeIDForwardKey, prefixIDDictEdgeReverse, id, 0, idPairByString)
	}))
	_, ok = d.lookupEdgeIDByNum(numAllocated)
	require.False(t, ok)
}

func TestIDDictionary_ForgetCache_DropsInMemoryOnly(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		t.Run(kind, func(t *testing.T) {
			e := idDictTestEngine(t)
			d := e.BadgerEngine.idDict
			id := "test:" + kind + "-forget"
			var num uint64
			var forward []byte
			var reversePrefix byte
			require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
				var err error
				if kind == "node" {
					num, err = resolveOrAllocateNumericIDInTxn(d, txn, NodeID(id))
					forward, reversePrefix = nodeIDForwardKey(NodeID(id)), prefixIDDictNodeReverse
				} else {
					num, err = resolveOrAllocateNumericIDInTxn(d, txn, EdgeID(id))
					forward, reversePrefix = edgeIDForwardKey(EdgeID(id)), prefixIDDictEdgeReverse
				}
				return err
			}))
			require.Greater(t, num, uint64(0))
			if kind == "node" {
				_, present := d.lookupNodeNumID(NodeID(id))
				require.True(t, present)
				_, present = d.lookupNodeIDByNum(num)
				require.True(t, present)
				d.forgetNodeCache(NodeID(id))
				_, present = d.lookupNodeNumID(NodeID(id))
				require.False(t, present)
				_, present = d.lookupNodeIDByNum(num)
				require.False(t, present)
				d.forgetNodeCache("test:nope")
			} else {
				_, present := d.lookupEdgeNumID(EdgeID(id))
				require.True(t, present)
				_, present = d.lookupEdgeIDByNum(num)
				require.True(t, present)
				d.forgetEdgeCache(EdgeID(id))
				_, present = d.lookupEdgeNumID(EdgeID(id))
				require.False(t, present)
				_, present = d.lookupEdgeIDByNum(num)
				require.False(t, present)
				d.forgetEdgeCache("test:nope")
			}
			require.NoError(t, e.db.View(func(txn *badger.Txn) error {
				if _, err := txn.Get(forward); err != nil {
					return err
				}
				var numBytes [8]byte
				binary.BigEndian.PutUint64(numBytes[:], num)
				_, err := txn.Get(append([]byte{reversePrefix}, numBytes[:]...))
				return err
			}))
		})
	}
}

func TestIDDictionary_DeleteEdgeEntryByNumInTxn(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id EdgeID = "test:e-bynum"
	var num uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		var err error
		num, err = resolveOrAllocateNumericIDInTxn(d, txn, EdgeID(id))
		return err
	}))

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return deleteIDEntryInTxn(txn, &d.edges, edgeForwardShard, edgeReverseShard, edgeIDForwardKey, prefixIDDictEdgeReverse, EdgeID(""), num, idPairByNumber)
	}))
	_, ok := d.lookupEdgeIDByNum(num)
	require.False(t, ok)

	// Deleting a numID with no string entry is still safe (deletes the
	// reverse key only).
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return deleteIDEntryInTxn(txn, &d.edges, edgeForwardShard, edgeReverseShard, edgeIDForwardKey, prefixIDDictEdgeReverse, EdgeID(""), 999_999, idPairByNumber)
	}))
}

func TestIDDictionary_DeleteNodeEntryByNumInTxn(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id NodeID = "test:n-bynum"
	var num uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		var err error
		num, err = resolveOrAllocateNumericIDInTxn(d, txn, NodeID(id))
		return err
	}))

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return deleteIDEntryInTxn(txn, &d.nodes, nodeForwardShard, nodeReverseShard, nodeIDForwardKey, prefixIDDictNodeReverse, NodeID(""), num, idPairByNumber)
	}))
	_, ok := d.lookupNodeIDByNum(num)
	require.False(t, ok)
}

func TestIDDictionary_DeleteByNumberAfterCacheEviction(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict
	const nodeID NodeID = "test:evicted-node"
	const edgeID EdgeID = "test:evicted-edge"
	var nodeNum, edgeNum uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		var err error
		nodeNum, err = resolveOrAllocateNumericIDInTxn(d, txn, NodeID(nodeID))
		if err != nil {
			return err
		}
		edgeNum, err = resolveOrAllocateNumericIDInTxn(d, txn, EdgeID(edgeID))
		return err
	}))
	d.forgetNodeCache(nodeID)
	d.forgetEdgeCache(edgeID)
	key := func(prefix byte, num uint64) []byte {
		var suffix [8]byte
		binary.BigEndian.PutUint64(suffix[:], num)
		return append([]byte{prefix}, suffix[:]...)
	}
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		if err := deleteIDEntryInTxn(txn, &d.nodes, nodeForwardShard, nodeReverseShard, nodeIDForwardKey, prefixIDDictNodeReverse, NodeID(""), nodeNum, idPairByNumber); err != nil {
			return err
		}
		return deleteIDEntryInTxn(txn, &d.edges, edgeForwardShard, edgeReverseShard, edgeIDForwardKey, prefixIDDictEdgeReverse, EdgeID(""), edgeNum, idPairByNumber)
	}))
	require.NoError(t, e.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key(prefixIDDictNodeReverse, nodeNum))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		_, err = txn.Get(key(prefixIDDictEdgeReverse, edgeNum))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		_, err = txn.Get(nodeIDForwardKey(nodeID))
		require.NoError(t, err)
		_, err = txn.Get(edgeIDForwardKey(edgeID))
		require.NoError(t, err)
		return nil
	}))
}

func TestIDDictionary_PopAgedFreelistEntry_RespectsTTL(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return d.pushFreeNodeInTxn(txn, 42)
	}))

	// Just-pushed entry is still within TTL → no rewrite.
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		num, ok, err := d.popAgedFreelistEntryInTxn(txn, freelistKindNode, time.Hour)
		require.NoError(t, err)
		require.False(t, ok, "fresh freelist entry must not be claimed")
		require.Equal(t, uint64(0), num)
		return nil
	}))

	// With TTL=0 the entry is considered aged enough to claim.
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		num, ok, err := d.popAgedFreelistEntryInTxn(txn, freelistKindNode, 0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(42), num)
		return nil
	}))

	// Pending counter decrements after the claim.
	nodes, _ := d.FreelistPending()
	require.Equal(t, int64(0), nodes)

	// Empty freelist → fast path returns (0, false, nil).
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		num, ok, err := d.popAgedFreelistEntryInTxn(txn, freelistKindNode, 0)
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, uint64(0), num)
		return nil
	}))
}

func TestBadgerEngine_IDDictCountersAndFreelistPending(t *testing.T) {
	e := idDictTestEngine(t)

	// Counters start at zero before any IDs allocated.
	nodes, edges := e.IDDictCounters()
	require.Equal(t, uint64(0), nodes)
	require.Equal(t, uint64(0), edges)
	pn, pe := e.IDDictFreelistPending()
	require.Equal(t, int64(0), pn)
	require.Equal(t, int64(0), pe)

	// Allocate some IDs by creating nodes and edges through the
	// public engine API. Counters must advance.
	for _, id := range []NodeID{"test:n1", "test:n2"} {
		_, err := e.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{}})
		require.NoError(t, err)
	}
	require.NoError(t, e.CreateEdge(&Edge{
		ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2",
		Type: "REL", Properties: map[string]any{},
	}))

	nodes, edges = e.IDDictCounters()
	require.GreaterOrEqual(t, nodes, uint64(2))
	require.GreaterOrEqual(t, edges, uint64(1))
}
