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

// A written node is never cached as written, whether it waits for its
// embedding or not: publication drops its cached copy, and the next read
// loads it from storage (F-PUBLISH).
func TestWrittenNodeDroppedFromCache(t *testing.T) {
	b := newSidecarTestBadger(t)
	b.SetEmbeddingsEnabled(true)
	cached := func(id NodeID) bool {
		b.nodeCacheMu.RLock()
		defer b.nodeCacheMu.RUnlock()
		_, ok := b.nodeCache[id]
		return ok
	}
	awaiting := &Node{ID: "test:awaiting", Labels: []string{"Doc"}, Properties: map[string]any{"title": "text"}}
	embedded := &Node{ID: "test:embedded", Labels: []string{"Doc"}, Properties: map[string]any{"title": "text"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	for _, enabled := range []bool{true, false} {
		b.SetEmbeddingsEnabled(enabled)
		for _, node := range []*Node{awaiting, embedded} {
			b.cacheStoreNode(node)
			require.True(t, cached(node.ID))
			b.cacheDropWrittenNode(node)
			require.False(t, cached(node.ID))
		}
	}
}
