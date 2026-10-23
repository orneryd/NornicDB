package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Commits publish their cache callbacks after durable visibility. An earlier
// callback may therefore arrive after the delete's callback. Replaying that
// ordering explicitly makes the concurrent search regression deterministic.
func TestLateNodeCachePublicationAfterDelete(t *testing.T) {
	for _, late := range []struct {
		name  string
		apply func(*BadgerEngine, *Node)
	}{
		{"create", func(b *BadgerEngine, n *Node) { b.cacheOnNodeCreatedWithCounts(n, false) }},
		{"update", func(b *BadgerEngine, n *Node) { b.cacheOnNodeUpdated(n) }},
		{"update-with-old", func(b *BadgerEngine, n *Node) { b.cacheOnNodeUpdatedWithOldNode(n, nil) }},
	} {
		t.Run(late.name, func(t *testing.T) {
			b := NewMemoryEngine()
			t.Cleanup(func() { _ = b.Close() })
			written := &Node{ID: "test:deleted", ChunkEmbeddings: [][]float32{{1, 0}}}
			_, err := b.CreateNode(written)
			require.NoError(t, err)
			require.NoError(t, b.DeleteNode(written.ID))
			late.apply(b.BadgerEngine, written)
			_, err = b.GetNode(written.ID)
			require.ErrorIs(t, err, ErrNotFound, "an earlier committed payload must not resurrect a deleted node in the read cache")
		})
	}
}
