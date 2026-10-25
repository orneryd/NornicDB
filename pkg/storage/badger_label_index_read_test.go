package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestLabelIndexReadOwnerFallsBackAcrossReaders(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:target", Labels: []string{"Target"}},
		{ID: "test:other", Labels: []string{"Other"}},
	}))
	_, err = engine.CreateNode(&Node{ID: "other:foreign", Labels: []string{"Target"}})
	require.NoError(t, err)
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)

	got, err := engine.GetNodesByLabelInScope("test:", "Target")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, NodeID("test:target"), got[0].ID)

	count, err := engine.NodeCountByLabel("Target")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
	count, err = engine.NodeCountByLabelInNamespace("test", "Target")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	count, err = engine.countLabelIndexEntriesInNamespace("test", "Target")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)

	hasLabel, err := engine.HasLabelBatch([]NodeID{"test:target", "test:other", "other:foreign", "test:missing"}, "Target")
	require.NoError(t, err)
	require.True(t, hasLabel["test:target"])
	require.False(t, hasLabel["test:other"])
	require.True(t, hasLabel["other:foreign"])
	require.False(t, hasLabel["test:missing"])

	var snapshotIDs []NodeID
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		reader := engine.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", visibleRead, true)
		defer reader.Close()
		for reader.Next() {
			snapshotIDs = append(snapshotIDs, reader.NodeID())
		}
		return reader.Err()
	}))
	require.Equal(t, []NodeID{"test:other", "test:target"}, snapshotIDs)

	snapshotIDs = nil
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		reader := engine.newLabelIndexReader(context.Background(), txn, "test:", "Target", "test:other", visibleRead, true)
		defer reader.Close()
		for reader.Next() {
			snapshotIDs = append(snapshotIDs, reader.NodeID())
		}
		return reader.Err()
	}))
	require.Equal(t, []NodeID{"test:target"}, snapshotIDs)

	dictionary, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Empty(t, dictionary.Nodes)
	require.Empty(t, dictionary.MismatchedNodes)
}

func TestLabelIndexReadyCacheRespectsTransactionSnapshot(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NotZero(t, engine.labelIndex.readyAt.Load(), "open must cache the ready marker version")

	wipeLabelIndexAndMarker(t, engine) // marker only: keep the index range
	older, olderReadTs, err := engine.db.beginTxn(false)
	require.NoError(t, err)
	defer func() {
		older.Discard()
		engine.db.endRead(olderReadTs)
	}()
	ready, err := engine.labelIndexReadyInTxn(older)
	require.NoError(t, err)
	require.False(t, ready)

	require.NoError(t, engine.markEmptyLabelIndexReady())
	readyAt := engine.labelIndex.readyAt.Load()
	require.NotZero(t, readyAt)
	require.Less(t, older.ReadTs(), readyAt, "the old transaction predates the committed marker")

	ready, err = engine.labelIndexReadyInTxn(older)
	require.NoError(t, err)
	require.False(t, ready, "an older snapshot must keep using the complete fallback")

	current, currentReadTs, err := engine.db.beginTxn(false)
	require.NoError(t, err)
	defer func() {
		current.Discard()
		engine.db.endRead(currentReadTs)
	}()
	require.GreaterOrEqual(t, current.ReadTs(), readyAt)
	ready, err = engine.labelIndexReadyInTxn(current)
	require.NoError(t, err)
	require.True(t, ready, "new snapshots can use the ready index without re-reading the marker")
}

func TestReview1667ReadinessOwnerPropagatesSnapshotErrors(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	// Force the same-snapshot marker read so this malformed-transaction probe
	// still exercises error propagation instead of the ready-version fast path.
	engine.invalidateLabelIndexReadyReadTs()
	err = engine.withView(func(txn *badger.Txn) error {
		txn.Discard()
		reader := engine.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", visibleRead, false)
		defer reader.Close()
		require.False(t, reader.Next())
		require.Error(t, reader.Err())
		return nil
	})
	require.NoError(t, err)
}

func TestReview1667SnapshotFallbackStreamsScopedIDUnion(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:other", Labels: []string{"Target"}},
		{ID: "test:target", Labels: []string{"Target"}},
	}))
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(nodeIDForwardKey("test:other")); err != nil {
			return err
		}
		for _, id := range []NodeID{"test:missing", "test:zforward", "test:zzzzzzforward"} {
			if err := txn.Set(nodeIDForwardKey(id), []byte{1}); err != nil {
				return err
			}
		}
		if err := txn.Set(nodeKey("test:zbody"), []byte("unused by the ID stream")); err != nil {
			return err
		}
		return txn.Set(nodeKey("test:zzzzbody"), []byte("unused by the ID stream"))
	}))

	var ids []NodeID
	err = engine.withView(func(txn *badger.Txn) error {
		reader := engine.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", visibleRead, true)
		defer reader.Close()
		for reader.Next() {
			ids = append(ids, reader.NodeID())
		}
		return reader.Err()
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{
		"test:missing", "test:other", "test:target", "test:zbody", "test:zforward", "test:zzzzbody", "test:zzzzzzforward",
	}, ids)
}

func TestReview1667SnapshotFallbackSkipsEmptyPhysicalNodeKey(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: "test:a", Labels: []string{"Target"}}}))
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte{prefixNode}, nil)
	}))

	var ids []NodeID
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		reader := engine.newLabelIndexReader(context.Background(), txn, "", "Target", "", visibleRead, true)
		defer reader.Close()
		for reader.Next() {
			ids = append(ids, reader.NodeID())
		}
		return reader.Err()
	}))
	require.Equal(t, []NodeID{"test:a"}, ids, "an empty physical node key is skipped")
}

func TestReview1667DictionaryCheckReadsFallbackBodies(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: "test:known", Labels: []string{"Target"}}}))
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte{prefixNode}, []byte{0}); err != nil {
			return err
		}
		return txn.Set(nodeKey("test:orphan"), []byte("missing dictionary entry"))
	}))

	report, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Contains(t, report.Nodes, NodeID("test:orphan"))

	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(nodeKey("test:known"), []byte("not a node record"))
	}))
	_, err = engine.CheckIDDictionary()
	require.Error(t, err)
	_, err = engine.NodeCountByLabel("Target")
	require.Error(t, err, "fallback counts return a stored node decode failure")
}

func TestReview1667BodyFallbackSeeksAndPropagatesDecodeErrors(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.decayEnabled = true
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:first", Labels: []string{"Target"}},
		{ID: "test:second", Labels: []string{"Target"}},
	}))
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)

	var ids []NodeID
	err = engine.withView(func(txn *badger.Txn) error {
		reader := engine.newLabelIndexReader(context.Background(), txn, "test:", "Target", "test:first", visibleRead, false)
		defer reader.Close()
		for reader.Next() {
			ids = append(ids, reader.NodeID())
		}
		return reader.Err()
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:second"}, ids)

	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(nodeKey("test:"), []byte{0}); err != nil {
			return err
		}
		return txn.Set(nodeKey("test:broken"), []byte("not a node record"))
	}))
	err = engine.withView(func(txn *badger.Txn) error {
		reader := engine.newLabelIndexReader(context.Background(), txn, "test:", "Target", "", visibleRead, false)
		defer reader.Close()
		for reader.Next() {
		}
		return reader.Err()
	})
	require.Error(t, err)
}

func TestReview1667TransactionStreamCancellation(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:first", Labels: []string{"Present"}},
		{ID: "test:second", Labels: []string{"Present"}},
		{ID: "test:third", Labels: []string{"Present"}},
	}))
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = tx.StreamNodesWithOptions(ctx, StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Missing",
		ApplyDecayFilter: true,
	}, func(*Node) error { return nil })
	require.ErrorIs(t, err, context.Canceled)

	var ids []NodeID
	err = tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Present",
		ApplyDecayFilter: true,
	}, func(node *Node) error {
		ids = append(ids, node.ID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:first", "test:second", "test:third"}, ids)

	// The logical MVCC snapshot reader uses the same readiness-aware owner.
	tx.snapshotTx.Discard()
	tx.snapshotTx = nil
	tx.readTS = engine.currentMVCCReadVersion("test")
	ids = nil
	err = tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Present",
		ApplyDecayFilter: true,
	}, func(node *Node) error {
		ids = append(ids, node.ID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:first", "test:second", "test:third"}, ids)

	err = tx.StreamNodesWithOptions(ctx, StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Missing",
		ApplyDecayFilter: true,
	}, func(*Node) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestReview1667CancelledMissingLabelLogicalSnapshot(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:first", Labels: []string{"Present"}},
		{ID: "test:second", Labels: []string{"Present"}},
		{ID: "test:third", Labels: []string{"Present"}},
	}))
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
	tx.snapshotTx.Discard()
	tx.snapshotTx = nil
	tx.readTS = engine.currentMVCCReadVersion("test")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = tx.StreamNodesWithOptions(ctx, StreamNodesOptions{
		QueryNodes:       true,
		Prefix:           "test:",
		Label:            "Missing",
		ApplyDecayFilter: true,
	}, func(*Node) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestLabelIndexCountFallbackIncludesBufferedWrites(t *testing.T) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		LowMemory:           true,
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
		WriteBehindMaxOps:   100,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	existing := &Node{ID: "test:existing", Labels: []string{"Target"}}
	_, err = engine.CreateNode(existing)
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:durable", Labels: []string{"Target"}})
	require.NoError(t, err)
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)

	updated := *existing
	updated.Labels = []string{"Other"}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.UpdateGraphNode(&updated, ClientNodeEdits))
	require.NoError(t, tx.Commit())

	tx, err = engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "test:buffered", Labels: []string{"Target"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	count, err := engine.NodeCountByLabel("Target")
	require.NoError(t, err)
	require.EqualValues(t, 2, count, "fallback must shadow the durable update and include buffered nodes")
	require.NoError(t, engine.writeBehind.Flush())
}

func TestNodeHasLabelInTxnHandlesBodyAndTransactionErrors(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(nodeKey("test:broken"), []byte("not a node record"))
	}))

	err = engine.withView(func(txn *badger.Txn) error {
		matched, err := engine.nodeHasLabelInTxn(txn, "test:missing", "Target")
		require.NoError(t, err)
		require.False(t, matched)
		_, err = engine.nodeHasLabelInTxn(txn, "test:broken", "Target")
		require.Error(t, err)
		return nil
	})
	require.NoError(t, err)

	txn, readTs, err := engine.db.beginTxn(false)
	require.NoError(t, err)
	txn.Discard()
	_, err = engine.nodeHasLabelInTxn(txn, "test:missing", "Target")
	engine.db.endRead(readTs)
	require.Error(t, err)
	require.False(t, nodeHasLabel(nil, "Target"))
	require.Empty(t, nodeScopeForNamespace(""))
	require.Equal(t, "test:", nodeScopeForNamespace("test"))
}

func TestLabelIndexFallbackProjectedStreamPropagatesVisitorError(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: "test:target", Labels: []string{"Target"}}}))
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)

	want := errors.New("visitor failed")
	err = engine.StreamNodesByLabelProjectedInScope("test:", "Target", nil, func(*Node) error {
		return want
	})
	require.ErrorIs(t, err, want)
}

func TestConstraintLabelScanUsesCompleteFallback(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("target_key", "Target", "key"))
	_, err = engine.CreateNode(&Node{
		ID:         "test:target",
		Labels:     []string{"Target"},
		Properties: map[string]interface{}{"key": "existing"},
	})
	require.NoError(t, err)
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)

	err = engine.withView(func(txn *badger.Txn) error {
		return engine.scanForUniqueViolationInTxn(context.Background(), txn, "test", "Target", "key", "existing", "")
	})
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation)
}
