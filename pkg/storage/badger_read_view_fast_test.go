package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestSnapshotOverlayIdleEpochAndNamespace(t *testing.T) {
	b := snapshotOverlayFixture(t)
	require.NoError(t, b.FlushWriteBehind())
	w := b.writeBehind
	before, idle := w.idleReadEpoch("test")
	require.True(t, idle)
	_, err := b.CreateNode(&Node{ID: "other:seed"})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("other"))
	_, err = tx.CreateNode(&Node{ID: "other:pending"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, idle = w.idleReadEpoch("test")
	require.True(t, idle)
	_, idle = w.idleReadEpoch("other")
	require.False(t, idle)
	require.NoError(t, b.FlushWriteBehind())
	after, idle := w.idleReadEpoch("test")
	require.True(t, idle)
	require.NotEqual(t, before, after, "idle publication must detect ABA")
}

func TestSnapshotOverlayCachedCaptureRetriesPublication(t *testing.T) {
	b := snapshotOverlayFixture(t)
	require.NoError(t, b.FlushWriteBehind())
	calls := 0
	var value int64
	view, err := b.captureReadView(nil, readViewOptions{scope: "test", cached: func(w *WriteBehindBuffer) (bool, error) {
		calls++
		if calls == 1 {
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			require.NoError(t, tx.SetNamespace("test"))
			require.NoError(t, tx.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"v": int64(9)}}))
			require.NoError(t, tx.Commit())
			require.NoError(t, b.FlushWriteBehind())
			value = 2
		} else {
			require.NotNil(t, w)
			node := b.nodeCache["test:a"]
			value = node.Properties["v"].(int64)
		}
		return true, nil
	}})
	require.NoError(t, err)
	require.Nil(t, view)
	require.Equal(t, 2, calls)
	require.Equal(t, int64(9), value)
}

func TestSnapshotOverlayLongViewReopensAndDoesNotPinClose(t *testing.T) {
	b := snapshotOverlayFixture(t)
	var overlay []*Node
	view, err := b.captureReadView(func(w *WriteBehindBuffer) { overlay, _ = w.allNodesOverlayLocked() }, readViewOptions{longLived: true})
	require.NoError(t, err)
	require.Nil(t, view.txn)
	require.Len(t, overlay, 2)
	require.NoError(t, b.FlushWriteBehind())
	for i := 0; i < 2; i++ {
		require.NoError(t, view.withView(func(txn *badger.Txn) error {
			_, err := txn.Get(edgeKey("test:r"))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			return nil
		}))
	}
	done := make(chan error, 1)
	go func() { done <- b.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("long view pins engine Close")
	}
	require.ErrorIs(t, view.withView(func(*badger.Txn) error { return nil }), ErrStorageClosed)
	view.Close()
	view.Close()
}

func TestSnapshotOverlayBusyCachedBatchAndAdjacency(t *testing.T) {
	b := snapshotOverlayFixture(t)
	// The committed part has hot caches; pending rows must shadow just their IDs.
	_, err := b.CreateNode(&Node{ID: "other:unrelated"})
	require.NoError(t, err)
	for _, read := range []func([]NodeID) (map[NodeID]*Node, error){b.BatchGetNodes, b.BatchGetNodesWithoutEmbeddings} {
		nodes, err := read([]NodeID{"test:a", "test:b", "test:new", "test:gone", "other:unrelated"})
		require.NoError(t, err)
		require.Len(t, nodes, 4)
		require.Equal(t, int64(2), nodes["test:a"].Properties["v"])
	}
	edges, err := b.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.Equal(t, EdgeID("test:r"), edges[0].ID)
}
