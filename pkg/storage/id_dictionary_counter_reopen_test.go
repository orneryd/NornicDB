package storage

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// persistedIDCounter reads the on-disk numID counter for one kind, or 0 when
// the key has never been written.
func persistedIDCounter(t *testing.T, engine *BadgerEngine, key []byte) uint64 {
	t.Helper()
	var value uint64
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = binary.BigEndian.Uint64(val)
			return nil
		})
	}))
	return value
}

// rewindPersistedIDCounter puts the on-disk counter key back to an earlier
// value (deleting it when the earlier value is 0). This is the exact on-disk
// state a crash leaves between a committed user transaction and the
// follow-up persistCounters write: the forward/reverse dictionary entries and
// the entity bodies are durable (they travel inside the user transaction) but
// the counter high-water mark is not.
func rewindPersistedIDCounter(t *testing.T, engine *BadgerEngine, key []byte, value uint64) {
	t.Helper()
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		if value == 0 {
			return txn.Delete(key)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], value)
		return txn.Set(key, buf[:])
	}))
}

// TestIDDictionary_ReopenNeverReallocatesACommittedNumID pins the recovery
// contract of the numeric ID dictionary: after a reopen, a freshly created
// entity must never receive a numID that a durable entity already holds.
//
// The persisted counter is written AFTER the user transaction commits
// (persistCounters runs in its own badger transaction so it stays out of the
// user transaction's conflict set). A process crash, a kill -9, or an engine
// Close racing the commit tail can therefore leave committed entities whose
// numIDs sit above the persisted counter. Before the fix, loadFromBadger
// trusted the counter key alone, so the next allocation reissued those
// numIDs: two string node IDs then mapped to one numID, and every compact
// index keyed by numID (outgoing/incoming adjacency, label, edge-between,
// MVCC heads) silently merged the two nodes. Observed in the wild as a graph
// where `MATCH (a)-[r]->(b)` returned each relationship twice, once under a
// start node from a different tenant, after a backend restart mid-commit.
func TestIDDictionary_ReopenNeverReallocatesACommittedNumID(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	nodeCounterBefore := persistedIDCounter(t, engine1, idCounterNodeKey)
	edgeCounterBefore := persistedIDCounter(t, engine1, idCounterEdgeKey)

	for i := 1; i <= 3; i++ {
		_, err := engine1.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("tenant-a:%d", i)),
			Labels:     []string{"Resource"},
			Properties: map[string]any{"tenant": "a", "ordinal": int64(i)},
		})
		require.NoError(t, err)
	}
	require.NoError(t, engine1.CreateEdge(&Edge{
		ID: "tenant-a:e1", StartNode: "tenant-a:1", EndNode: "tenant-a:2",
		Type: "CONTAINS", Properties: map[string]any{"tenant": "a"},
	}))
	numA1, ok := engine1.idDict.lookupNodeNumID("tenant-a:1")
	require.True(t, ok)
	numAE1, ok := engine1.idDict.lookupEdgeNumID("tenant-a:e1")
	require.True(t, ok)

	// The committed entities are durable; only the counter write is lost.
	rewindPersistedIDCounter(t, engine1, idCounterNodeKey, nodeCounterBefore)
	rewindPersistedIDCounter(t, engine1, idCounterEdgeKey, edgeCounterBefore)
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	for i := 1; i <= 3; i++ {
		_, err := engine2.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("tenant-b:%d", i)),
			Labels:     []string{"Resource"},
			Properties: map[string]any{"tenant": "b", "ordinal": int64(i)},
		})
		require.NoError(t, err)
	}
	require.NoError(t, engine2.CreateEdge(&Edge{
		ID: "tenant-b:e1", StartNode: "tenant-b:1", EndNode: "tenant-b:2",
		Type: "CONTAINS", Properties: map[string]any{"tenant": "b"},
	}))

	numB1, ok := engine2.idDict.lookupNodeNumID("tenant-b:1")
	require.True(t, ok)
	require.NotEqual(t, numA1, numB1,
		"tenant-b:1 was issued numID %d, which durable node tenant-a:1 already holds; the dictionary must reconcile its counter with the committed forward map on open", numA1)
	numBE1, ok := engine2.idDict.lookupEdgeNumID("tenant-b:e1")
	require.True(t, ok)
	require.NotEqual(t, numAE1, numBE1,
		"tenant-b:e1 was issued edge numID %d, which durable edge tenant-a:e1 already holds", numAE1)

	// The user-visible symptom: adjacency is keyed by numID, so an alias makes
	// one tenant's relationships appear under the other tenant's node.
	outA, err := engine2.GetOutgoingEdges("tenant-a:1")
	require.NoError(t, err)
	require.Len(t, outA, 1, "tenant-a:1 must own exactly its own relationship after reopen")
	require.Equal(t, EdgeID("tenant-a:e1"), outA[0].ID)
	require.Equal(t, NodeID("tenant-a:1"), outA[0].StartNode)

	outB, err := engine2.GetOutgoingEdges("tenant-b:1")
	require.NoError(t, err)
	require.Len(t, outB, 1, "tenant-b:1 must own exactly its own relationship")
	require.Equal(t, EdgeID("tenant-b:e1"), outB[0].ID)
	require.Equal(t, NodeID("tenant-b:1"), outB[0].StartNode)

	// The reverse map must still resolve every durable numID to its own node.
	gotA1, ok := engine2.idDict.lookupNodeIDByNum(numA1)
	require.True(t, ok)
	require.Equal(t, NodeID("tenant-a:1"), gotA1)
}

// TestIDDictionary_ReopenKeepsAHigherPersistedCounter is the control: when
// the persisted counter is already at or above the committed maximum (the
// normal clean path), reconciliation must not move it.
func TestIDDictionary_ReopenKeepsAHigherPersistedCounter(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "solo:1", Labels: []string{"L"}})
	require.NoError(t, err)
	// Persist a counter well above anything the forward map holds, as if
	// allocations had been rolled back after the counter write.
	rewindPersistedIDCounter(t, engine1, idCounterNodeKey, 1_000)
	rewindPersistedIDCounter(t, engine1, idCounterEdgeKey, 2_000)
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })
	require.Equal(t, uint64(1_000), engine2.idDict.nextNode.Load())
	require.Equal(t, uint64(2_000), engine2.idDict.nextEdge.Load())
}
