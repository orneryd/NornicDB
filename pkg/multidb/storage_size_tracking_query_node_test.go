package multidb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type fullReadOnlyEngine struct {
	storage.Engine
}

// TestSizeTrackingEngine_GetQueryNode: a database's engine forwards the
// query-node read (storage.QueryNodeReader) through the size-tracking
// wrapper: the node without its stored vectors, its embedding metadata kept.
// An inner engine without the read gives the full node.
func TestSizeTrackingEngine_GetQueryNode(t *testing.T) {
	manager, dbName := setupTestManager(t)
	store, err := manager.GetStorage(dbName)
	require.NoError(t, err)
	reader, ok := store.(storage.QueryNodeReader)
	require.True(t, ok, "%T", store)

	chunks := make([][]float32, 4)
	for c := range chunks {
		chunks[c] = make([]float32, 4000) // stored apart from the body
		chunks[c][c] = 1
	}
	id, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "t"}, ChunkEmbeddings: chunks})
	require.NoError(t, err)
	full, err := store.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, chunks, full.ChunkEmbeddings)

	node, err := reader.GetQueryNode(id)
	require.NoError(t, err)
	require.Equal(t, id, node.ID)
	require.Equal(t, "t", node.Properties["title"])
	require.True(t, node.HasChunkEmbeddings())

	wrapped := &sizeTrackingEngine{Engine: &fullReadOnlyEngine{Engine: store}}
	node, err = wrapped.GetQueryNode(id)
	require.NoError(t, err)
	require.Equal(t, chunks, node.ChunkEmbeddings)
}
