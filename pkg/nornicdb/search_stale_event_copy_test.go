package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A node's write-event copy without its embedding, queued after the embed
// worker's copy with it (a bulk write's creation event delivered after the
// node was already embedded), doesn't drop the node's vector: the flush
// indexes the stored node, which has it.
func TestSearchFlushKeepsEmbeddingOverAnOlderEventCopy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingDimensions = 3
	db, err := Open("", cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage)
	require.NoError(t, err)
	require.NoError(t, db.ensureSearchIndexesBuilt(ctx, db.defaultDatabaseName()))

	embedded := &storage.Node{
		ID:              "late",
		Labels:          []string{"Doc"},
		Properties:      map[string]any{"content": "embedded before its creation event was delivered"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
	}
	_, err = db.storage.CreateNode(embedded)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return svc.EmbeddingCount() == 1 }, 5*time.Second, 10*time.Millisecond)

	qualified := storage.NodeID(db.defaultDatabaseName() + ":late")
	db.indexNodeFromEvent(&storage.Node{ID: qualified, Labels: embedded.Labels, Properties: embedded.Properties, ChunkEmbeddings: embedded.ChunkEmbeddings})
	db.indexNodeFromEvent(&storage.Node{ID: qualified, Labels: embedded.Labels, Properties: embedded.Properties})

	require.Eventually(t, func() bool {
		entry := db.searchServices[db.defaultDatabaseName()]
		return entry != nil && !entry.hasPending()
	}, 5*time.Second, 10*time.Millisecond)
	db.bgWg.Wait()
	require.Equal(t, 1, svc.EmbeddingCount())
}
