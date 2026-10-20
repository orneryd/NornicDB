package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestNamespaceColdMVCCAdmissionAfterFailedDrop(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	_, err := b.CreateNode(&Node{ID: "cold_peer:one"})
	require.NoError(t, err)
	op := NewNamespaceDrop(nil)
	require.NoError(t, b.PrepareNamespaceDrop(context.Background(), "cold_retired:", op))
	release, err := b.beginSchemaWriteContext(context.Background())
	require.NoError(t, err)
	err = op.commitLocked(context.Background())
	release()
	require.NoError(t, err)
	require.Error(t, op.physicalComplete(context.Background(), errors.New("interrupted cleanup")))
	// Snapshot sampling may read a cold namespace, but cannot publish its cache
	// or repair counters outside the namespace's lifecycle admission.
	_ = b.currentMVCCReadVersion("cold_retired")
	b.mvccByNamespaceMu.RLock()
	_, published := b.mvccByNamespace["cold_retired"]
	b.mvccByNamespaceMu.RUnlock()
	require.False(t, published, "read snapshot republished retired namespace state")
	require.ErrorIs(t, b.EnsureNamespaceMVCC("cold_retired"), ErrConflict)
	require.NoError(t, b.EnsureNamespaceMVCC("cold_peer"))
	_, err = b.CreateNode(&Node{ID: "cold_peer:two"})
	require.NoError(t, err)
	count, err := b.NodeCountByPrefix("cold_peer:")
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestNamespaceSuppressionCleanupRetainsOrigin(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	revision := b.namespaceLifetimeRevision()
	key := manualSuppressionKey(manualSuppressionNode, "side_retired:node")
	release, err := b.beginWrite("side_retired")
	require.NoError(t, err)
	err = b.withUpdate(func(txn *badger.Txn) error { return txn.Set(key, []byte{1}) })
	release()
	require.NoError(t, err)
	op := NewNamespaceDrop(nil)
	require.NoError(t, b.PrepareNamespaceDrop(context.Background(), "side_retired:", op))
	release, err = b.beginSchemaWriteContext(context.Background())
	require.NoError(t, err)
	err = op.commitLocked(context.Background())
	release()
	require.NoError(t, err)
	require.Error(t, op.physicalComplete(context.Background(), errors.New("interrupted cleanup")))
	require.ErrorIs(t, b.retireManualSuppressionKey(manualSuppressionNode, "side_retired:node", revision), ErrConflict)
	require.NoError(t, b.withView(func(txn *badger.Txn) error { _, err := txn.Get(key); return err }))
}
