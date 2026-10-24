package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A commit's cache update for a node still waiting for its embedding, run
// after the embed worker stored the embedding (a large transaction fires its
// cache updates one by one after the commit is visible), doesn't hide the
// embedding: reads still return it, and the node cache doesn't hold the
// written copy.
func TestCommittedCopyDoesNotHideAStoredEmbedding(t *testing.T) {
	for _, late := range []struct {
		name  string
		apply func(b *BadgerEngine, written *Node)
	}{
		{"created", func(b *BadgerEngine, written *Node) { b.cacheOnNodeCreated(written) }},
		{"updated", func(b *BadgerEngine, written *Node) { b.cacheOnNodeUpdatedWithOldNode(written, nil) }},
	} {
		t.Run(late.name, func(t *testing.T) {
			b := newSidecarTestBadger(t)
			b.SetEmbeddingsEnabled(true)
			now := time.Now().UTC().Truncate(time.Millisecond)
			node := sidecarTestNode(t, b, "late-cache", map[string]any{"title": "source text"}, now)
			written := CopyNode(node)
			require.True(t, sidecarPending(t, b, node.ID))

			embedded := embeddingWriteback(t, b, node.ID, [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(embedded))
			late.apply(b, written)

			got, err := b.GetNode(node.ID)
			require.NoError(t, err)
			require.Equal(t, [][]float32{{0.1, 0.2}}, got.ChunkEmbeddings)
		})
	}
}

// A committed payload invalidates cached reads for every embedding state;
// only a subsequent generation-checked storage read fills the node cache.
func TestWrittenNodeInvalidatesCacheForEveryEmbeddingState(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.SetEmbeddingsEnabled(true)
	cached := func(id NodeID) bool {
		b.nodeCacheMu.RLock()
		defer b.nodeCacheMu.RUnlock()
		_, ok := b.nodeCache[id]
		return ok
	}
	awaiting := &Node{ID: "test:awaiting", Labels: []string{"Doc"}, Properties: map[string]any{"title": "text"}}
	b.cacheFillNode(b.nodeCacheGen.current(), awaiting, b.nodeCacheCommitTs.Load())
	b.publishCommittedNodeFixture(awaiting)
	require.False(t, cached(awaiting.ID))

	embedded := &Node{ID: "test:embedded", Labels: []string{"Doc"}, Properties: map[string]any{"title": "text"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	b.cacheFillNode(b.nodeCacheGen.current(), embedded, b.nodeCacheCommitTs.Load())
	b.publishCommittedNodeFixture(embedded)
	require.False(t, cached(embedded.ID))

	b.SetEmbeddingsEnabled(false)
	b.cacheFillNode(b.nodeCacheGen.current(), awaiting, b.nodeCacheCommitTs.Load())
	b.publishCommittedNodeFixture(awaiting)
	require.False(t, cached(awaiting.ID))
}
