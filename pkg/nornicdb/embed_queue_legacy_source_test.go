package nornicdb

import (
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEmbedWorkerLegacyStructuredSourceLifecycle(t *testing.T) {
	worker, queue, base := nodeSaveRetryFixture(t, "whole", 1, 1)
	original, err := queue.GetNode("node-000")
	require.NoError(t, err)
	original.Properties["id"] = "old"
	disk, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer disk.Close()
	disk.SetEmbeddingsEnabled(true)
	store := storage.NewNamespacedEngine(disk, "legacy-source")
	_, err = store.CreateNode(original)
	require.NoError(t, err)
	queue.orderedEmbeddingQueue.Engine = store
	var provider embed.Embedder = &retryMetadataPropertyEmbedder{mockEmbedder: base, key: "id"}
	worker.SetEmbedder(provider)
	require.True(t, worker.processNextBatch())
	makeNodeSaveRetriesReady(worker)
	require.True(t, worker.processNextBatch())
	require.Equal(t, 2, base.GetEmbedCount(), "a rejected save requests the provider again")
	current, err := store.GetNode(original.ID)
	require.NoError(t, err)
	require.NotEmpty(t, current.ChunkEmbeddings)
	current.Properties["id"] = "new"
	require.NoError(t, store.UpdateNode(current))
	stored, err := store.GetNode(original.ID)
	require.NoError(t, err)
	require.Empty(t, stored.ChunkEmbeddings, "legacy structured providers receive the full map, including metadata fields")
}
