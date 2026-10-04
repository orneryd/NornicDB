package storage

// Range scans must stay inside their key range. Badger's reverse iterator
// ignores IteratorOptions.Prefix, and a forward iterator without one keeps
// prefetching past the range, so either kind skipped every deleted key next
// to the range on each scan. DROP DATABASE deletes the dropped database's
// MVCC adjacency keys; a relationship read of an edgeless node in another
// database then walked all of them, and DETACH DELETE of 100k such nodes went
// from 6 s to 36 min (#850).

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestAdjacencyReadsIgnoreDeletedNeighbourKeys(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	// The dropped database is created first, so its node numbers sort below
	// the surviving nodes' adjacency prefixes.
	const droppedEdges = 20_000
	for i := 0; i <= droppedEdges; i++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("gone:n%d", i)), Labels: []string{"V"}})
		require.NoError(t, err)
	}
	for i := 0; i < droppedEdges; i++ {
		require.NoError(t, engine.CreateEdge(&Edge{
			ID: EdgeID(fmt.Sprintf("gone:e%d", i)), Type: "NEXT",
			StartNode: NodeID(fmt.Sprintf("gone:n%d", i)), EndNode: NodeID(fmt.Sprintf("gone:n%d", i+1)),
		}))
	}
	const probes = 500
	for i := 0; i < probes; i++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("keep:n%d", i)), Labels: []string{"V"}})
		require.NoError(t, err)
	}

	readAll := func() time.Duration {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		start := time.Now()
		for i := 0; i < probes; i++ {
			id := NodeID(fmt.Sprintf("keep:n%d", i))
			out, err := tx.GetOutgoingEdges(id)
			require.NoError(t, err)
			in, err := tx.GetIncomingEdges(id)
			require.NoError(t, err)
			require.Empty(t, out)
			require.Empty(t, in)
		}
		return time.Since(start)
	}
	before := readAll()

	_, edgesDeleted, err := engine.DeleteByPrefix("gone:")
	require.NoError(t, err)
	require.EqualValues(t, droppedEdges, edgesDeleted)

	after := readAll()
	// Bounded scans cost the same with or without deleted neighbours; an
	// unbounded scan walks 2×20,000 deleted keys per read.
	require.Less(t, after, 10*before+200*time.Millisecond, "before drop %s, after drop %s", before, after)
}

// The latest-at-or-before lookup returns the greatest version key at or
// below the version and rejects a malformed key there, as the reverse seek did.
func TestLoadMVCCRecordAtOrBeforeBoundedScan(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	nodeID := NodeID("n1")
	v1 := MVCCVersion{CommitTimestamp: time.Unix(100, 0).UTC(), CommitSequence: 1}
	v2 := MVCCVersion{CommitTimestamp: time.Unix(200, 0).UTC(), CommitSequence: 2}
	v3 := MVCCVersion{CommitTimestamp: time.Unix(300, 0).UTC(), CommitSequence: 3}
	for i, v := range []MVCCVersion{v1, v3} {
		require.NoError(t, engine.AppendNodeVersion(&Node{ID: nodeID, Labels: []string{"V"}, Properties: map[string]any{"n": int64(i)}}, v))
	}
	load := func(version MVCCVersion) (int64, MVCCVersion, error) {
		var n int64
		var got MVCCVersion
		err := engine.withView(func(txn *badger.Txn) error {
			record, at, err := engine.loadNodeMVCCRecordAtOrBeforeInTxn(txn, nodeID, version)
			if err != nil {
				return err
			}
			n, got = record.Node.Properties["n"].(int64), at
			return nil
		})
		return n, got, err
	}
	n, at, err := load(v2)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
	require.Zero(t, at.Compare(v1))
	n, at, err = load(maxMVCCVersion())
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Zero(t, at.Compare(v3))
	_, _, err = load(MVCCVersion{CommitTimestamp: time.Unix(50, 0).UTC()})
	require.ErrorIs(t, err, ErrNotFound)

	// A short key under the prefix, below every version, is the greatest key
	// at or below an earlier version.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(append(engine.mvccNodeVersionPrefixString(nodeID), 0x01), []byte{0})
	}))
	_, _, err = load(MVCCVersion{CommitTimestamp: time.Unix(50, 0).UTC()})
	require.ErrorContains(t, err, "invalid mvcc key length")
}

// descendingPrefixKeys visits a prefix's keys at or below a bound, greatest
// first, and its cost does not depend on deleted keys beside the prefix.
func TestDescendingPrefixKeys(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	key := func(group byte, n int) []byte { return []byte{0xE0, group, byte(n >> 8), byte(n)} }
	set := func(keys ...[]byte) {
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			for _, k := range keys {
				if err := txn.Set(k, []byte{1}); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	// Groups 1..4 hold 1, 2, 3 and 5 keys; group 0 is a run of deleted keys
	// directly below them, group 9 a live neighbour above.
	for group, count := range map[byte]int{1: 1, 2: 2, 3: 3, 4: 5} {
		for n := 1; n <= count; n++ {
			set(key(group, n*10))
		}
	}
	set(key(9, 1))
	visit := func(group byte, upper int, limit int) []int {
		var got []int
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			return descendingPrefixKeys(txn, []byte{0xE0, group}, key(group, upper), func(item *badger.Item) (bool, error) {
				k := item.Key()
				got = append(got, int(k[2])<<8|int(k[3]))
				return len(got) < limit, nil
			})
		}))
		return got
	}
	require.Empty(t, visit(5, 100, 10), "empty prefix")
	require.Empty(t, visit(1, 5, 10), "every key above the bound")
	require.Equal(t, []int{10}, visit(1, 100, 10))
	require.Equal(t, []int{20, 10}, visit(2, 100, 10))
	require.Equal(t, []int{10}, visit(2, 15, 10))
	require.Equal(t, []int{30, 20, 10}, visit(3, 30, 10), "the bound is inclusive")
	require.Equal(t, []int{40, 30, 20, 10}, visit(4, 45, 10))
	require.Equal(t, []int{40, 30}, visit(4, 45, 2), "visit stops the scan")
	failure := errors.New("visit failed")
	require.ErrorIs(t, engine.withView(func(txn *badger.Txn) error {
		return descendingPrefixKeys(txn, []byte{0xE0, 4}, key(4, 100), func(*badger.Item) (bool, error) { return true, failure })
	}), failure)
	require.ErrorIs(t, engine.withView(func(txn *badger.Txn) error {
		return descendingPrefixKeys(txn, []byte{0xE0, 1}, key(1, 100), func(*badger.Item) (bool, error) { return true, failure })
	}), failure)

	lookups := func() time.Duration {
		start := time.Now()
		for i := 0; i < 2000; i++ {
			visit(1, 100, 10)
			visit(2, 15, 10)
		}
		return time.Since(start)
	}
	before := lookups()
	var deleted [][]byte
	for n := 0; n < 50_000; n++ {
		deleted = append(deleted, []byte{0xE0, 0, byte(n >> 16), byte(n >> 8), byte(n)})
	}
	for start := 0; start < len(deleted); start += 5000 {
		set(deleted[start : start+5000]...)
	}
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		for _, k := range deleted {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}))
	after := lookups()
	require.Less(t, after, 10*before+200*time.Millisecond, "before the deleted run %s, after %s", before, after)
}

// The temporal-history lookups walk the history greatest first through
// descendingPrefixKeys, skipping malformed keys and returning load errors.
func TestTemporalHistoryLookupsDescending(t *testing.T) {
	engine := newTestEngine(t)
	ns := "test"
	sm := engine.GetSchemaForNamespace(ns)
	require.NoError(t, sm.AddConstraint(Constraint{
		Name: "temporal_role", Type: ConstraintTemporal, EntityType: ConstraintEntityNode,
		Label: "Role", Properties: []string{"role", "valid_from", "valid_to"},
	}))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n1 := &Node{ID: "test:n1", Labels: []string{"Role"}, Properties: map[string]any{"role": "captain", "valid_from": base, "valid_to": base.Add(24 * time.Hour)}}
	_, err := engine.CreateNode(n1)
	require.NoError(t, err)
	c := sm.GetConstraintsForLabels([]string{"Role"})[0]

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		target, err := engine.writeTemporalIndexForNodeInTxn(txn, ns, n1, c)
		require.NoError(t, err)
		prefix := temporalHistoryPrefix(target.desc)
		require.NoError(t, txn.Set(append([]byte{}, prefix...), []byte{}), "malformed: no node ID")

		node, err := engine.temporalHistoryNodeAsOfInTxn(txn, target, base.Add(12*time.Hour), nil, false)
		require.NoError(t, err)
		require.Equal(t, NodeID("test:n1"), node.ID)
		node, err = engine.temporalHistoryNodeAsOfInTxn(txn, target, base.Add(36*time.Hour), nil, false)
		require.NoError(t, err)
		require.Nil(t, node, "n1 has ended; the malformed key is skipped")

		// A history entry whose node body does not decode fails the lookups.
		require.NoError(t, txn.Set(temporalHistoryKey(target.desc, base.Add(30*time.Hour), "test:bad"), []byte{}))
		require.NoError(t, txn.Set(nodeKey("test:bad"), []byte("not a node")))
		_, err = engine.temporalHistoryNodeAsOfInTxn(txn, target, base.Add(36*time.Hour), nil, false)
		require.Error(t, err)
		_, _, err = engine.temporalAdjacentNodesInTxn(txn, target, base.Add(36*time.Hour), "")
		require.Error(t, err)
		return nil
	}))
}
