package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeleteNode_RemovesEmbeddings_WhenDeletedBeforeFlush(t *testing.T) {
	// Regression: with AsyncWritesEnabled, nodes can be created+embedded (and indexed)
	// while still buffered in AsyncEngine. If they are deleted before the async flush
	// commits them to the inner engine, no inner OnNodeDeleted callback fires, leaving
	// stale embeddings in the in-memory search index until manual clearing.
	db, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	defer db.Close()

	// Ensure search service exists for default DB.
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NotNil(t, svc)

	// Create a node that will be buffered in AsyncEngine (not necessarily flushed yet).
	localID := storage.NodeID("cache-only-delete-1")
	_, err = db.storage.CreateNode(&storage.Node{
		ID:              localID,
		Labels:          []string{"Test"},
		Properties:      map[string]any{"name": "cached"},
		ChunkEmbeddings: [][]float32{make([]float32, 1024)},
	})
	require.NoError(t, err)

	// Simulate "speculative" indexing (e.g., embed worker indexing before flush visibility).
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID:              localID,
		Labels:          []string{"Test"},
		Properties:      map[string]any{"name": "cached"},
		ChunkEmbeddings: [][]float32{make([]float32, 1024)},
	}))

	// Sanity: embedding count includes this node.
	require.GreaterOrEqual(t, db.EmbeddingCount(), 1)

	// Delete before flush commits to inner engine (AsyncEngine may satisfy from cache).
	require.NoError(t, db.storage.DeleteNode(localID))

	// Allow callback to run (it is synchronous, but keep a small buffer for timing).
	time.Sleep(10 * time.Millisecond)

	// Node embedding should be removed from vector index without requiring ClearAllEmbeddings.
	ctx := context.Background()
	results, err := db.Search(ctx, "cached", nil, 10)
	require.NoError(t, err)
	for _, r := range results {
		require.NotEqual(t, string(localID), string(r.Node.ID))
	}
}
