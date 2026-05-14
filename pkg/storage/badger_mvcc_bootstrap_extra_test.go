package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestMVCCBootstrapTime_Priority(t *testing.T) {
	created := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	updated := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	// UpdatedAt wins when set.
	require.Equal(t, updated, mvccBootstrapTime(created, updated))

	// Falls back to CreatedAt when UpdatedAt is zero.
	require.Equal(t, created, mvccBootstrapTime(created, time.Time{}))

	// Both zero → uses now (just verify it returns something current,
	// not zero, and in UTC).
	got := mvccBootstrapTime(time.Time{}, time.Time{})
	require.False(t, got.IsZero())
	require.Equal(t, time.UTC, got.Location())
}

func TestWriteNodeMVCCHeadForFreshCreateInTxn_FloorEqualsVersion(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	id := NodeID("test:fresh-1")
	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		v, err := be.allocateMVCCVersion(txn, time.Now().UTC())
		require.NoError(t, err)
		// Allocate a numID via the public head-key resolver so the head
		// write below has a numeric mapping to point at.
		if _, err := be.mvccNodeHeadKeyString(txn, id); err != nil {
			return err
		}
		require.NoError(t, be.writeNodeMVCCHeadForFreshCreateInTxn(txn, id, v))

		head, err := be.loadNodeMVCCHeadInTxn(txn, id)
		require.NoError(t, err)
		require.Equal(t, 0, head.Version.Compare(v),
			"persisted Version must equal the allocated version")
		require.Equal(t, 0, head.FloorVersion.Compare(v),
			"FloorVersion must equal Version on a fresh-create write")
		require.False(t, head.Tombstoned)
		return nil
	}))
}

func TestWriteEdgeMVCCHeadForFreshCreateInTxn_FloorEqualsVersion(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	id := EdgeID("test:fresh-edge-1")
	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		v, err := be.allocateMVCCVersion(txn, time.Now().UTC())
		require.NoError(t, err)
		if _, err := be.mvccEdgeHeadKeyString(txn, id); err != nil {
			return err
		}
		require.NoError(t, be.writeEdgeMVCCHeadForFreshCreateInTxn(txn, id, v))

		head, err := be.loadEdgeMVCCHeadInTxn(txn, id)
		require.NoError(t, err)
		require.Equal(t, 0, head.Version.Compare(v))
		require.Equal(t, 0, head.FloorVersion.Compare(v))
		require.False(t, head.Tombstoned)
		return nil
	}))
}

func TestBootstrapNodeMVCCFromCurrentStateInTxn_PopulatesHead(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	_, err := be.CreateNode(&Node{
		ID:        "test:bootstrap-1",
		Labels:    []string{"L"},
		CreatedAt: time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)

	// Delete the head record so bootstrap has work to do.
	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		key := be.mvccNodeHeadKeyStringLookup("test:bootstrap-1")
		require.NotNil(t, key)
		return txn.Delete(key)
	}))

	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		_, err := be.loadNodeMVCCHeadInTxn(txn, "test:bootstrap-1")
		require.ErrorIs(t, err, ErrNotFound)
		return nil
	}))

	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		return be.bootstrapNodeMVCCFromCurrentStateInTxn(txn)
	}))

	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		head, err := be.loadNodeMVCCHeadInTxn(txn, "test:bootstrap-1")
		require.NoError(t, err)
		require.False(t, head.Version.CommitTimestamp.IsZero())
		require.False(t, head.Tombstoned)
		return nil
	}))
}

func TestBootstrapEdgeMVCCFromCurrentStateInTxn_PopulatesHead(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	_, err := be.CreateNode(&Node{ID: "test:n-from", Labels: []string{"X"}})
	require.NoError(t, err)
	_, err = be.CreateNode(&Node{ID: "test:n-to", Labels: []string{"X"}})
	require.NoError(t, err)
	require.NoError(t, be.CreateEdge(&Edge{
		ID:        "test:e-1",
		StartNode: "test:n-from",
		EndNode:   "test:n-to",
		Type:      "REL",
	}))

	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		key := be.mvccEdgeHeadKeyStringLookup("test:e-1")
		require.NotNil(t, key)
		return txn.Delete(key)
	}))
	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		_, err := be.loadEdgeMVCCHeadInTxn(txn, "test:e-1")
		require.ErrorIs(t, err, ErrNotFound)
		return nil
	}))

	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		return be.bootstrapEdgeMVCCFromCurrentStateInTxn(txn)
	}))

	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		head, err := be.loadEdgeMVCCHeadInTxn(txn, "test:e-1")
		require.NoError(t, err)
		require.False(t, head.Version.CommitTimestamp.IsZero())
		require.False(t, head.Tombstoned)
		return nil
	}))
}

func TestBootstrapNodeMVCCFromCurrentStateInTxn_SkipsExistingHead(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	_, err := be.CreateNode(&Node{ID: "test:skip-1", Labels: []string{"L"}})
	require.NoError(t, err)

	var headBefore MVCCHead
	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		var err error
		headBefore, err = be.loadNodeMVCCHeadInTxn(txn, "test:skip-1")
		return err
	}))

	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		return be.bootstrapNodeMVCCFromCurrentStateInTxn(txn)
	}))

	var headAfter MVCCHead
	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		var err error
		headAfter, err = be.loadNodeMVCCHeadInTxn(txn, "test:skip-1")
		return err
	}))
	require.Equal(t, 0, headBefore.Version.Compare(headAfter.Version),
		"existing head must not be overwritten by re-bootstrap")
}

// TestApplyEdgeBootstrapBatch_SkipsExistingHead exercises the public-facing
// batch apply for an already-headed edge: head version must not move.
func TestApplyEdgeBootstrapBatch_SkipsExistingHead(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	_, err := be.CreateNode(&Node{ID: "test:n-from", Labels: []string{"X"}})
	require.NoError(t, err)
	_, err = be.CreateNode(&Node{ID: "test:n-to", Labels: []string{"X"}})
	require.NoError(t, err)
	edge := &Edge{
		ID:        "test:e-batch",
		StartNode: "test:n-from",
		EndNode:   "test:n-to",
		Type:      "REL",
	}
	require.NoError(t, be.CreateEdge(edge))

	var before MVCCHead
	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		var err error
		before, err = be.loadEdgeMVCCHeadInTxn(txn, "test:e-batch")
		return err
	}))

	require.NoError(t, be.applyEdgeBootstrapBatch([]*Edge{edge}))

	var after MVCCHead
	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		var err error
		after, err = be.loadEdgeMVCCHeadInTxn(txn, "test:e-batch")
		return err
	}))
	require.Equal(t, 0, before.Version.Compare(after.Version),
		"applyEdgeBootstrapBatch must skip edges that already have a head")
}

func TestApplyEdgeBootstrapBatch_NilAndEmpty(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	// Empty batch is a no-op.
	require.NoError(t, be.applyEdgeBootstrapBatch(nil))

	// Batch containing a nil entry is also a no-op (skipped).
	require.NoError(t, be.applyEdgeBootstrapBatch([]*Edge{nil}))
}

func TestBootstrapMVCCHeadsFromCurrentState_FullPath(t *testing.T) {
	be := NewMemoryEngine()
	t.Cleanup(func() { _ = be.Close() })

	_, err := be.CreateNode(&Node{ID: "test:b-n1", Labels: []string{"L"}})
	require.NoError(t, err)
	_, err = be.CreateNode(&Node{ID: "test:b-n2", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, be.CreateEdge(&Edge{
		ID: "test:b-e1", StartNode: "test:b-n1", EndNode: "test:b-n2", Type: "REL",
	}))

	require.NoError(t, be.db.Update(func(txn *badger.Txn) error {
		for _, id := range []NodeID{"test:b-n1", "test:b-n2"} {
			k := be.mvccNodeHeadKeyStringLookup(id)
			require.NoError(t, txn.Delete(k))
		}
		k := be.mvccEdgeHeadKeyStringLookup("test:b-e1")
		require.NoError(t, txn.Delete(k))
		return nil
	}))

	require.NoError(t, be.bootstrapMVCCHeadsFromCurrentState(context.Background()))

	require.NoError(t, be.db.View(func(txn *badger.Txn) error {
		_, err := be.loadNodeMVCCHeadInTxn(txn, "test:b-n1")
		require.NoError(t, err)
		_, err = be.loadNodeMVCCHeadInTxn(txn, "test:b-n2")
		require.NoError(t, err)
		_, err = be.loadEdgeMVCCHeadInTxn(txn, "test:b-e1")
		require.NoError(t, err)
		return nil
	}))
}
