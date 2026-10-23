package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Review probe (test-only): the separately stored embedding replacement
// helper must invalidate its own warm node.
func reviewClaudeWarmLargeEmbeddingNode(t *testing.T, b *BadgerEngine) (NodeID, [][]float32) {
	t.Helper()
	id := NodeID(prefixTestID("review-claude-emb"))
	_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v"}, ChunkEmbeddings: makeLargeChunkEmbeddings()})
	require.NoError(t, err)
	_, err = b.GetNode(id)
	require.NoError(t, err)
	b.nodeCacheMu.RLock()
	_, warm := b.cachedNodeLocked(id, false, 0)
	b.nodeCacheMu.RUnlock()
	require.True(t, warm, "fixture node must be warm")
	replacement := makeLargeChunkEmbeddings()
	for _, chunk := range replacement {
		for i := range chunk {
			chunk[i] = 7
		}
	}
	return id, replacement
}

func TestReviewClaudeReplaceSeparateEmbeddingsDeclaredScope(t *testing.T) {
	b := createTestBadgerEngine(t)
	id, replacement := reviewClaudeWarmLargeEmbeddingNode(t, b)
	require.NoError(t, b.replaceSeparateEmbeddingChunks(id, replacement), "test-time scope guard verdict")
}

func TestReviewClaudeReplaceSeparateEmbeddingsProductionVisibility(t *testing.T) {
	saved := nodeCacheScopeCheck
	nodeCacheScopeCheck = nil // production has no write-set check
	t.Cleanup(func() { nodeCacheScopeCheck = saved })
	b := createTestBadgerEngine(t)
	id, replacement := reviewClaudeWarmLargeEmbeddingNode(t, b)
	require.NoError(t, b.replaceSeparateEmbeddingChunks(id, replacement))
	served, err := b.GetNode(id)
	require.NoError(t, err)
	b.nodeCacheMu.Lock()
	delete(b.nodeCache, id)
	delete(b.queryNodeCache, id)
	b.nodeCacheMu.Unlock()
	durable, err := b.GetNode(id)
	require.NoError(t, err)
	require.Equal(t, replacement, durable.ChunkEmbeddings, "sanity: replacement is durable")
	require.Equal(t, durable.ChunkEmbeddings, served.ChunkEmbeddings, "GetNode served a stale cached embedding after a durable replacement")
}
