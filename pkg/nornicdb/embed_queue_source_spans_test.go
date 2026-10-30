package nornicdb

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedWorkerPropertyOnlySourceLeavesFallbackUnconsumed(t *testing.T) {
	worker, queue, base := nodeSaveRetryFixture(t, "whole", 2, 1)
	for _, id := range []storage.NodeID{"node-000", "node-001"} {
		n, err := queue.GetNode(id)
		require.NoError(t, err)
		n.Properties["id"] = "old"
		n.Properties["text"] = "constant"
		require.NoError(t, queue.UpdateNode(storage.NodeUpdateRequest{After: n}))
	}
	structured := &retryMetadataPropertyBatchEmbedder{&retryMetadataPropertyEmbedder{mockEmbedder: base, key: "id", declared: true, textless: true}}
	provider := embed.NewCachedEmbedder(embed.NewTracedEmbedder(structured), 8)
	worker.SetEmbedder(provider)
	worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })
	require.True(t, worker.processNextBatch())
	makeNodeSaveRetriesReady(worker)
	require.True(t, worker.processNextBatch())
	require.True(t, worker.processNextBatch()) // Each due DLQ node requests a fresh result.
	require.Equal(t, int64(2), worker.processed.Load())
	for _, id := range []storage.NodeID{"node-000", "node-001"} {
		n, err := queue.GetNode(id)
		require.NoError(t, err)
		spans, _ := embeddingutil.StoredChunkSpans(n.EmbedMeta)
		require.Empty(t, spans, "property-only chunks cannot claim spans into unconsumed fallback text")
		n.Properties["text"] = "new fallback"
		require.NoError(t, queue.UpdateNode(storage.NodeUpdateRequest{After: n}))
		n, err = queue.GetNode(id)
		require.NoError(t, err)
		require.NotEmpty(t, n.ChunkEmbeddings, "fallback text was not consumed by this result")
	}
	require.Equal(t, 4, base.GetEmbedCount(), "both rejected nodes request the provider again")
}
