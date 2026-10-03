package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A transaction stages its statement-time writes (stagedKV): its existence
// checks see them, its Badger transaction does not.
func TestStagedKV_ExistenceSeesStagedWrites(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:committed", Labels: []string{"X"}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	w := tx.staged()

	has := func(key []byte) bool {
		t.Helper()
		ok, err := kvHas(w, key)
		require.NoError(t, err)
		return ok
	}
	committed, staged := nodeKey("test:committed"), []byte("staged-key")
	require.True(t, has(committed))
	require.False(t, has(staged))

	require.NoError(t, w.Set(staged, []byte("v")))
	require.True(t, has(staged))
	require.False(t, kvStagedDeleted(w, staged))

	require.NoError(t, w.Delete(staged))
	require.NoError(t, w.Delete(committed))
	require.False(t, has(staged))
	require.False(t, has(committed), "a staged delete hides the committed key")
	require.True(t, kvStagedDeleted(w, committed))

	// Iteration reads the Badger transaction, which staging leaves alone.
	it := w.NewIterator(badgerPrefixIteratorOptions(committed))
	it.Rewind()
	require.True(t, it.ValidForPrefix(committed))
	it.Close()

	txn, readTs := engine.db.beginTxn(false)
	defer engine.db.endRead(readTs)
	defer txn.Discard()
	require.False(t, kvStagedDeleted(txn, committed), "a Badger transaction has no staged deletes")
}

// Creating several entities in one transaction reuses each freed numeric ID
// once: a transaction stages its freelist pops, so each later pop skips the
// entries it already took.
func TestTransaction_ReusesEachFreedNumericIDOnce(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory:      true,
		EngineOptions: EngineOptions{IDFreelistTTL: 20 * time.Millisecond},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	freed := map[uint64]bool{}
	for i := 0; i < 3; i++ {
		id := NodeID(fmt.Sprintf("test:old-%d", i))
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"X"}})
		require.NoError(t, err)
		num, ok := engine.idDict.lookupNodeNumID(id)
		require.True(t, ok)
		freed[num] = true
		require.NoError(t, engine.DeleteNode(id))
	}
	_, err = engine.PruneMVCCVersions(context.Background(), MVCCPruneOptions{})
	require.NoError(t, err)
	time.Sleep(40 * time.Millisecond)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err := tx.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:new-%d", i)), Labels: []string{"X"}})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	reused := map[uint64]bool{}
	for i := 0; i < 3; i++ {
		num, ok := engine.idDict.lookupNodeNumID(NodeID(fmt.Sprintf("test:new-%d", i)))
		require.True(t, ok)
		require.False(t, reused[num], "numeric ID %d given to two nodes", num)
		reused[num] = true
	}
	require.Equal(t, freed, reused)
}

// The exact relationship lookup answers "none" for a type that does not
// connect a connected pair, without scanning.
func TestMatchEdgesBetween_OtherTypeBetweenThePair(t *testing.T) {
	engine := newTestEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"X"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "KNOWS"}))
	all := func(*Edge) bool { return true }

	edges, err := engine.MatchEdgesBetween("test:a", "test:b", "LIKES", nil, all)
	require.NoError(t, err)
	require.Empty(t, edges)
	edges, err = engine.MatchEdgesBetween("test:a", "test:b", "KNOWS", nil, all)
	require.NoError(t, err)
	require.Len(t, edges, 1)
}
