package nornicdb

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingQueueStoreScanStopsAcrossLayouts(t *testing.T) {
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	managerConfig := multidb.DefaultConfig()
	managerConfig.IsolatedStores = db.IsolatedStoreConfig()
	require.NotNil(t, managerConfig.IsolatedStores)
	manager, err := multidb.NewDatabaseManager(t.Context(), db.GetBaseStorageForManager(), managerConfig)
	require.NoError(t, err)
	db.SetIsolatedStoresClose(manager.CloseIsolatedStores)
	db.SetEmbeddingStoreCatalog(manager)
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "dedicated"}))
	queue := db.embeddingQueueStorage().(*embeddingQueueStorage)
	for _, id := range []storage.NodeID{"nornic:failed", "dedicated:failed"} {
		engine, err := queue.storeForNode(id)
		require.NoError(t, err)
		_, err = engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Job"}})
		require.NoError(t, err)
		node, err := engine.GetNode(t.Context(), id)
		require.NoError(t, err)
		node.EmbedMeta = map[string]any{"embedding_failed": true}
		require.NoError(t, engine.(storage.EmbeddingSidecarUpdater).UpdateNodeEmbeddingSidecar(t.Context(), node))
	}

	count, err := queue.StreamParkedEmbeddingFailures(t.Context(), func(storage.NodeID, map[string]any) error { return nil })
	require.NoError(t, err)
	require.Equal(t, 2, count)
	count, err = queue.StreamParkedEmbeddingFailures(t.Context(), func(storage.NodeID, map[string]any) error {
		return storage.ErrIterationStopped
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	count, err = queue.StreamParkedEmbeddingFailures(t.Context(), func(storage.NodeID, map[string]any) error {
		return storage.ErrStorageClosed
	})
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	require.Equal(t, 1, count)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	count, err = queue.StreamParkedEmbeddingFailures(ctx, func(storage.NodeID, map[string]any) error {
		cancel()
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, count)
}
