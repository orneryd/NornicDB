package nornicdb

import (
	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestManagedGenerationDatabaseIsolationAndResume(t *testing.T) {
	engine := testfixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	engine.SetEmbeddingsEnabled(true)
	for _, id := range []storage.NodeID{"alpha:one", "beta:one"} {
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Document"}, Properties: map[string]any{"text": "Document to embed"}})
		require.NoError(t, err)
	}
	config := DefaultEmbedWorkerConfig()
	config.NumWorkers = 1
	config.DeferWorkerStart = true
	config.BatchDelay = 0
	worker := NewEmbedWorker(newMockEmbedder(), engine, config)
	t.Cleanup(worker.Close)
	db := &DB{baseStorage: engine, embedQueue: worker}
	worker.setEmbeddingAdmission(db.managedEmbeddingAllowed)
	db.SetDatabaseEmbeddingsEnabled("alpha", false)
	engine.SetEmbeddingLabelPolicy("alpha", nil, nil) // Label configuration must not clear the generation gate.
	require.False(t, engine.IsDatabaseEmbeddingsEnabled("alpha"))
	worker.StartWorkers()
	require.Eventually(t, func() bool {
		node, err := engine.GetNode(t.Context(), "beta:one")
		return err == nil && len(node.ChunkEmbeddings) > 0
	}, 5*time.Second, 10*time.Millisecond)
	node, err := engine.GetNode(t.Context(), "alpha:one")
	require.NoError(t, err)
	require.Empty(t, node.ChunkEmbeddings)
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: "alpha:two", Properties: map[string]any{"text": "Written while disabled"}})
	require.NoError(t, err)
	db.SetDatabaseEmbeddingsEnabled("alpha", true)
	require.Eventually(t, func() bool {
		for _, id := range []storage.NodeID{"alpha:one", "alpha:two"} {
			node, err := engine.GetNode(t.Context(), id)
			if err != nil || len(node.ChunkEmbeddings) == 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	db.SetDatabaseEmbeddingsEnabled("alpha", false)
	node, err = engine.GetNode(t.Context(), "alpha:one")
	require.NoError(t, err)
	require.NotEmpty(t, node.ChunkEmbeddings, "disabling must preserve existing vectors")
}

func TestManagedGenerationPausesRetryWithoutLosingIt(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 1, 1000)
	worker.processNextBatch()
	require.Len(t, worker.nodeRetries, 1)
	calls := provider.GetEmbedCount()
	makeNodeSaveRetriesReady(worker)
	worker.setEmbeddingAdmission(func(storage.NodeID) bool { return false })
	for i := 0; i < 3; i++ {
		worker.processNextBatch()
	}
	require.Equal(t, calls, provider.GetEmbedCount())
	require.Len(t, worker.nodeRetries, 1)
	require.Zero(t, worker.PendingEmbeddingsCount())
	require.GreaterOrEqual(t, worker.nodeEmbeddingRetryDelay(time.Minute), time.Minute)
	queue.rejections = 0
	worker.setEmbeddingAdmission(func(storage.NodeID) bool { return true })
	worker.processNextBatch()
	require.Equal(t, int64(1), worker.processed.Load())
	require.Empty(t, worker.nodeRetries)
}
