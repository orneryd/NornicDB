package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// rawVersionRecord returns the stored bytes of a node or relationship version
// record.
func rawVersionRecord(t *testing.T, eng *BadgerEngine, key []byte) []byte {
	t.Helper()
	require.NotNil(t, key)
	var raw []byte
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		raw, err = item.ValueCopy(nil)
		return err
	}))
	return raw
}

func nodeHeadVersion(t *testing.T, eng *BadgerEngine, id NodeID) MVCCVersion {
	t.Helper()
	head, err := eng.GetNodeCurrentHead(id)
	require.NoError(t, err)
	return head.Version
}

func edgeHeadVersion(t *testing.T, eng *BadgerEngine, id EdgeID) MVCCVersion {
	t.Helper()
	head, err := eng.GetEdgeCurrentHead(id)
	require.NoError(t, err)
	return head.Version
}

// While retention keeps history, an update archives the superseded node as an
// undo record, and every version reads back exactly: in engine and
// transaction updates, after a delete (which archives the last version
// complete), and at or before any version.
func TestUndoHistoryRebuildsNodeVersions(t *testing.T) {
	eng := createMVCCBadgerEngine(t)
	id := NodeID("test:n")
	big := make(map[string]any)
	for i := 0; i < 40; i++ {
		big["p"+string(rune('a'+i%26))+string(rune('a'+i/26))] = "some longer property text that stays the same"
	}
	v1Props := map[string]any{"a": int64(1), "b": "x", "list": []any{int64(1), int64(2)}}
	for key, value := range big {
		v1Props[key] = value
	}
	_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: v1Props})
	require.NoError(t, err)
	v1 := nodeHeadVersion(t, eng, id)

	v2Props := map[string]any{"a": int64(2), "b": "x", "c": true, "list": []any{int64(1), int64(2)}}
	for key, value := range big {
		v2Props[key] = value
	}
	require.NoError(t, eng.UpdateNode(&Node{ID: id, Labels: []string{"L", "M"}, Properties: v2Props}))
	v2 := nodeHeadVersion(t, eng, id)

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode(id)
	require.NoError(t, err)
	node.Properties = map[string]any{"a": int64(3)}
	require.NoError(t, tx.UpdateNode(node))
	require.NoError(t, tx.Commit())
	v3 := nodeHeadVersion(t, eng, id)

	// Both superseded versions are undo records, smaller than a complete one.
	for _, version := range []MVCCVersion{v1, v2} {
		raw := rawVersionRecord(t, eng, eng.mvccNodeVersionKeyStringLookup(id, version))
		require.True(t, isMVCCUndoRecord(raw))
	}
	complete, err := encodeMVCCNodeRecord(&Node{ID: id, Labels: []string{"L"}, Properties: v1Props}, false)
	require.NoError(t, err)
	require.Less(t, len(rawVersionRecord(t, eng, eng.mvccNodeVersionKeyStringLookup(id, v1))), len(complete)/4)

	check := func(version MVCCVersion, labels []string, props map[string]any) {
		t.Helper()
		got, err := eng.GetNodeVisibleAt(id, version)
		require.NoError(t, err)
		require.Equal(t, labels, got.Labels)
		require.Equal(t, props, got.Properties)
	}
	check(v1, []string{"L"}, v1Props)
	check(v2, []string{"L", "M"}, v2Props)
	check(v3, []string{"L", "M"}, map[string]any{"a": int64(3)})

	// A delete archives the last version complete; older undo records still
	// rebuild from it.
	require.NoError(t, eng.DeleteNode(id))
	require.False(t, isMVCCUndoRecord(rawVersionRecord(t, eng, eng.mvccNodeVersionKeyStringLookup(id, v3))))
	check(v1, []string{"L"}, v1Props)
	check(v2, []string{"L", "M"}, v2Props)
	check(v3, []string{"L", "M"}, map[string]any{"a": int64(3)})

	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		record, at, err := eng.loadNodeMVCCRecordAtOrBeforeInTxn(txn, id, v2)
		require.NoError(t, err)
		require.Equal(t, v2, at)
		require.Equal(t, v2Props, record.Node.Properties)
		exact, err := eng.loadNodeMVCCRecordExactInTxn(txn, id, v1)
		require.NoError(t, err)
		require.Equal(t, v1Props, exact.Node.Properties)
		return nil
	}))
}

// Relationship history rebuilds the same way, metadata included.
func TestUndoHistoryRebuildsEdgeVersions(t *testing.T) {
	eng := createMVCCBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	id := EdgeID("test:e")
	require.NoError(t, eng.CreateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "K", Confidence: 0.5, Properties: map[string]any{"w": int64(1), "keep": "k"}}))
	v1 := edgeHeadVersion(t, eng, id)
	require.NoError(t, eng.UpdateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "K", Confidence: 0.75, Properties: map[string]any{"w": int64(2), "keep": "k", "extra": true}}))
	v2 := edgeHeadVersion(t, eng, id)

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	edge, err := tx.GetEdge(id)
	require.NoError(t, err)
	edge.Properties = map[string]any{"w": int64(3)}
	require.NoError(t, tx.UpdateEdge(edge))
	require.NoError(t, tx.Commit())

	require.True(t, isMVCCUndoRecord(rawVersionRecord(t, eng, eng.mvccEdgeVersionKeyStringLookup(id, v1))))
	got, err := eng.GetEdgeVisibleAt(id, v1)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"w": int64(1), "keep": "k"}, got.Properties)
	require.Equal(t, 0.5, got.Confidence)
	require.Equal(t, "K", got.Type)
	got, err = eng.GetEdgeVisibleAt(id, v2)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"w": int64(2), "keep": "k", "extra": true}, got.Properties)
	require.Equal(t, 0.75, got.Confidence)

	require.NoError(t, eng.DeleteEdge(id))
	got, err = eng.GetEdgeVisibleAt(id, v1)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"w": int64(1), "keep": "k"}, got.Properties)
}

// A transaction that updates a node twice archives against the state it
// leaves; one that updates and then deletes it archives the old version
// complete.
func TestUndoHistoryUsesTheCommitsFinalState(t *testing.T) {
	eng := createMVCCBadgerEngine(t)
	_, err := eng.CreateNode(&Node{ID: "test:twice", Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&Node{ID: "test:gone", Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	twiceV1 := nodeHeadVersion(t, eng, "test:twice")
	goneV1 := nodeHeadVersion(t, eng, "test:gone")

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for _, value := range []int64{2, 3} {
		node, err := tx.GetNode("test:twice")
		require.NoError(t, err)
		node.Properties = map[string]any{"v": value}
		require.NoError(t, tx.UpdateNode(node))
	}
	gone, err := tx.GetNode("test:gone")
	require.NoError(t, err)
	gone.Properties = map[string]any{"v": int64(2)}
	require.NoError(t, tx.UpdateNode(gone))
	require.NoError(t, tx.DeleteNode("test:gone"))
	require.NoError(t, tx.Commit())

	got, err := eng.GetNodeVisibleAt("test:twice", twiceV1)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"v": int64(1)}, got.Properties)
	require.False(t, isMVCCUndoRecord(rawVersionRecord(t, eng, eng.mvccNodeVersionKeyStringLookup("test:gone", goneV1))))
	got, err = eng.GetNodeVisibleAt("test:gone", goneV1)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"v": int64(1)}, got.Properties)
}

// A snapshot reader keeps its versions while later updates turn them into
// undo records.
func TestUndoHistorySnapshotReader(t *testing.T) {
	eng := createMVCCBadgerEngine(t)
	_, err := eng.CreateNode(&Node{ID: "test:s", Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	require.NoError(t, eng.UpdateNode(&Node{ID: "test:s", Labels: []string{"N"}, Properties: map[string]any{"v": int64(2)}}))

	reader, err := eng.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	require.NoError(t, reader.SetNamespace("test"))
	seen, err := reader.GetNode("test:s")
	require.NoError(t, err)
	require.Equal(t, int64(2), seen.Properties["v"])

	for _, value := range []int64{3, 4} {
		require.NoError(t, eng.UpdateNode(&Node{ID: "test:s", Labels: []string{"N"}, Properties: map[string]any{"v": value}}))
	}
	reader.snapshotPrefixNodeByID = nil
	seen, err = reader.GetNode("test:s")
	require.NoError(t, err)
	require.Equal(t, int64(2), seen.Properties["v"])
}

// An undo record whose chain can't be followed reads as not found, or as a
// chain error when its link doesn't point forward; pruning the oldest
// versions keeps the rest readable; without retention, complete records are
// archived.
func TestUndoHistoryChainsAndRetention(t *testing.T) {
	eng := createMVCCBadgerEngine(t)
	id := NodeID("test:c")
	_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	v1 := nodeHeadVersion(t, eng, id)
	require.NoError(t, eng.UpdateNode(&Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"v": int64(2)}}))
	v2 := nodeHeadVersion(t, eng, id)
	require.NoError(t, eng.UpdateNode(&Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"v": int64(3)}}))

	// Pruning to one historical version drops v1 and keeps v2 readable.
	_, err = eng.PruneMVCCVersions(context.Background(), MVCCPruneOptions{MaxVersionsPerKey: 1})
	require.NoError(t, err)
	got, err := eng.GetNodeVisibleAt(id, v2)
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Properties["v"])

	// The v2 record's next version gone: v2 can't be rebuilt.
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		head, err := eng.loadNodeMVCCHeadInTxn(txn, id)
		if err != nil {
			return err
		}
		undo := newMVCCNodeUndo(&Node{ID: id, Properties: map[string]any{"v": int64(2)}}, &Node{ID: id}, head.Version)
		undo.Next = MVCCVersion{CommitTimestamp: head.Version.CommitTimestamp, CommitSequence: head.Version.CommitSequence + 7}
		encoded, err := encodeMVCCNodeUndoRecord(undo, false)
		if err != nil {
			return err
		}
		return txn.Set(eng.mvccNodeVersionKeyStringLookup(id, v2), encoded)
	}))
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		_, err := eng.loadNodeMVCCRecordExactInTxn(txn, id, v2)
		require.ErrorIs(t, err, ErrNotFound)
		return nil
	}))

	// A link that doesn't point to a later version.
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		undo := newMVCCNodeUndo(&Node{ID: id}, &Node{ID: id}, v1)
		encoded, err := encodeMVCCNodeUndoRecord(undo, false)
		if err != nil {
			return err
		}
		return txn.Set(eng.mvccNodeVersionKeyStringLookup(id, v2), encoded)
	}))
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		_, err := eng.loadNodeMVCCRecordExactInTxn(txn, id, v2)
		require.ErrorIs(t, err, errMVCCUndoChain)
		return nil
	}))

	// Head-only retention archives complete records for a snapshot reader.
	plain, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer plain.Close()
	_, err = plain.CreateNode(&Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"v": int64(1)}})
	require.NoError(t, err)
	before := nodeHeadVersion(t, plain, id)
	reader, err := plain.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	require.NoError(t, reader.SetNamespace("test"))
	require.NoError(t, plain.UpdateNode(&Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{"v": int64(2)}}))
	require.False(t, isMVCCUndoRecord(rawVersionRecord(t, plain, plain.mvccNodeVersionKeyStringLookup(id, before))))
}

// Undo records rebuild a relationship whose next version was archived
// complete or is the live head, and report broken links.
func TestUndoHistoryEdgeChains(t *testing.T) {
	eng := createMVCCBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := eng.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	id := EdgeID("test:e")
	require.NoError(t, eng.CreateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "K", Properties: map[string]any{"w": int64(1)}}))
	v1 := edgeHeadVersion(t, eng, id)
	require.NoError(t, eng.UpdateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "K", Properties: map[string]any{"w": int64(2)}}))
	v2 := edgeHeadVersion(t, eng, id)
	require.NoError(t, eng.UpdateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "K", Properties: map[string]any{"w": int64(3)}}))

	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		record, at, err := eng.loadEdgeMVCCRecordAtOrBeforeInTxn(txn, id, v1)
		require.NoError(t, err)
		require.Equal(t, v1, at)
		require.Equal(t, int64(1), record.Edge.Properties["w"])
		return nil
	}))

	// v2's record replaced by a link past the head: not found.
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		head, err := eng.loadEdgeMVCCHeadInTxn(txn, id)
		if err != nil {
			return err
		}
		undo := newMVCCEdgeUndo(&Edge{ID: id, Type: "K"}, &Edge{ID: id}, MVCCVersion{CommitTimestamp: head.Version.CommitTimestamp, CommitSequence: head.Version.CommitSequence + 7})
		encoded, err := encodeMVCCEdgeUndoRecord(undo, false)
		if err != nil {
			return err
		}
		return txn.Set(eng.mvccEdgeVersionKeyStringLookup(id, v2), encoded)
	}))
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		_, err := eng.loadEdgeMVCCRecordExactInTxn(txn, id, v1)
		require.ErrorIs(t, err, ErrNotFound)
		return nil
	}))

	// A link that doesn't point forward.
	require.NoError(t, eng.withUpdate(func(txn *badger.Txn) error {
		encoded, err := encodeMVCCEdgeUndoRecord(newMVCCEdgeUndo(&Edge{ID: id}, &Edge{ID: id}, v1), false)
		if err != nil {
			return err
		}
		return txn.Set(eng.mvccEdgeVersionKeyStringLookup(id, v2), encoded)
	}))
	require.NoError(t, eng.withView(func(txn *badger.Txn) error {
		_, err := eng.loadEdgeMVCCRecordExactInTxn(txn, id, v2)
		require.ErrorIs(t, err, errMVCCUndoChain)
		return nil
	}))
}
