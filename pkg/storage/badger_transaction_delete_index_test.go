package storage_test

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_DeleteNode_FiresEventsAndClearsVectorIndex(t *testing.T) {
	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	svc := search.NewServiceWithDimensions(engine, 3)

	notifier, ok := interface{}(engine).(storage.StorageEventNotifier)
	require.True(t, ok)
	notifier.OnNodeCreated(func(node *storage.Node) { _ = svc.IndexNode(node) })
	notifier.OnNodeDeleted(func(nodeID storage.NodeID) { _ = svc.RemoveNode(nodeID) })

	node := &storage.Node{
		ID:              "db1:alpha",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	storage.WaitForListeners(engine)
	require.Equal(t, 1, svc.EmbeddingCount())

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode(node.ID))
	require.NoError(t, tx.Commit())

	storage.WaitForListeners(engine)
	require.Equal(t, 0, svc.EmbeddingCount())
}
