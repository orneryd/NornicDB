package storage

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCacheKnownEmbeddingPartInvalidatesEveryAlias(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = b.Close() })
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}})
	owner := &Node{ID: "test:alias-owner", Properties: map[string]any{"v": int64(1)}, ChunkEmbeddings: [][]float32{{1}}}
	var key []byte
	for _, fixture := range nodeCacheEmbeddingKeyFixtures(owner.ID) {
		if fixture.ambiguous {
			key = fixture.key
			break
		}
	}
	candidates := embeddingKeyOwners(key)
	require.Len(t, candidates, 2)
	var alias NodeID
	for _, candidate := range candidates {
		if candidate.id != owner.ID {
			alias = candidate.id
		}
	}
	require.NotEmpty(t, alias)
	untouched := &Node{ID: "test:alias-untouched", Properties: map[string]any{"v": int64(7)}, ChunkEmbeddings: [][]float32{{1}}}
	for _, node := range []*Node{owner, {ID: alias, ChunkEmbeddings: [][]float32{{1}}}, untouched} {
		_, err := b.CreateNode(node)
		require.NoError(t, err)
		_, err = b.GetNode(node.ID)
		require.NoError(t, err)
	}
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	owner.Properties["v"] = int64(2)
	require.NoError(t, tx.UpdateNode(owner))
	tx.bufferSet(key, []byte{1})
	require.NoError(t, tx.Commit())
	b.nodeCacheMu.RLock()
	_, aliasEligible := b.cachedNodeLocked(alias, false, 0)
	_, unrelatedEligible := b.cachedNodeLocked(untouched.ID, false, 0)
	b.nodeCacheMu.RUnlock()
	require.False(t, aliasEligible, "a named legacy alias must not retain an obsolete payload")
	require.True(t, unrelatedEligible, "the complete candidate set must avoid the global fence")
	misses := atomic.LoadInt64(&b.cacheMisses)
	got, err := b.GetNode(untouched.ID)
	require.NoError(t, err)
	require.Equal(t, int64(7), got.Properties["v"])
	require.Equal(t, misses, atomic.LoadInt64(&b.cacheMisses))
}
