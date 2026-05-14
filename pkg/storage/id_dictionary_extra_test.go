package storage

import (
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
	e := NewMemoryEngine()
	t.Cleanup(func() { _ = e.Close() })
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
		num, err := d.resolveOrAllocateNodeNumIDInTxn(txn, id)
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
		return d.deleteNodeEntryInTxn(txn, id)
	}))
	_, ok = d.lookupNodeNumID(id)
	require.False(t, ok)
	_, ok = d.lookupNodeIDByNum(numAllocated)
	require.False(t, ok)

	// Deleting an unknown ID is a no-op.
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return d.deleteNodeEntryInTxn(txn, "test:nonexistent")
	}))
}

func TestIDDictionary_DeleteEdgeEntryInTxn_RemovesForwardAndReverse(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id EdgeID = "test:e-delete"
	var numAllocated uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		num, err := d.resolveOrAllocateEdgeNumIDInTxn(txn, id)
		numAllocated = num
		return err
	}))
	require.Greater(t, numAllocated, uint64(0))

	gotID, ok := d.lookupEdgeIDByNum(numAllocated)
	require.True(t, ok)
	require.Equal(t, id, gotID)

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return d.deleteEdgeEntryInTxn(txn, id)
	}))
	_, ok = d.lookupEdgeIDByNum(numAllocated)
	require.False(t, ok)
}

func TestIDDictionary_ForgetNodeCache_DropsInMemoryOnly(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id NodeID = "test:n-forget"
	var num uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		var err error
		num, err = d.resolveOrAllocateNodeNumIDInTxn(txn, id)
		return err
	}))

	// forgetNodeCache must drop both in-memory map entries.
	d.forgetNodeCache(id)
	_, ok := d.lookupNodeNumID(id)
	require.False(t, ok)
	_, ok = d.lookupNodeIDByNum(num)
	require.False(t, ok)

	// Calling on an unknown ID is harmless.
	d.forgetNodeCache("test:nope")
}

func TestIDDictionary_ForgetEdgeCache_DropsInMemoryOnly(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id EdgeID = "test:e-forget"
	var num uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		var err error
		num, err = d.resolveOrAllocateEdgeNumIDInTxn(txn, id)
		return err
	}))

	d.forgetEdgeCache(id)
	_, ok := d.lookupEdgeIDByNum(num)
	require.False(t, ok)

	d.forgetEdgeCache("test:nope")
}

func TestIDDictionary_DeleteEdgeEntryByNumInTxn(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id EdgeID = "test:e-bynum"
	var num uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		var err error
		num, err = d.resolveOrAllocateEdgeNumIDInTxn(txn, id)
		return err
	}))

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return d.deleteEdgeEntryByNumInTxn(txn, num)
	}))
	_, ok := d.lookupEdgeIDByNum(num)
	require.False(t, ok)

	// Deleting a numID with no string entry is still safe (deletes the
	// reverse key only).
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return d.deleteEdgeEntryByNumInTxn(txn, 999_999)
	}))
}

func TestIDDictionary_DeleteNodeEntryByNumInTxn(t *testing.T) {
	e := idDictTestEngine(t)
	d := e.BadgerEngine.idDict

	const id NodeID = "test:n-bynum"
	var num uint64
	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		var err error
		num, err = d.resolveOrAllocateNodeNumIDInTxn(txn, id)
		return err
	}))

	require.NoError(t, e.db.Update(func(txn *badger.Txn) error {
		return d.deleteNodeEntryByNumInTxn(txn, num)
	}))
	_, ok := d.lookupNodeIDByNum(num)
	require.False(t, ok)
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
