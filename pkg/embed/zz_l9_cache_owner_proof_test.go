package embed

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestL9CachedEmbeddingOwner(t *testing.T) {
	ctx := t.Context()
	cache := NewCachedEmbedder(&mockEmbedder{}, 4)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	first, err := cache.Embed(ctx, "same text")
	require.NoError(t, err)
	cache.mu.Lock()
	retained := cache.cache[hashText("same text")].Value.(*cacheEntry).embedding
	cache.mu.Unlock()
	require.Same(t, &retained[0], &first[0])
	safelyDelivered := slices.Clone(first)
	safelyDelivered[0] = 1000
	require.NotEqual(t, safelyDelivered[0], retained[0])
	first[0] = 2000
	next, err := cache.Embed(ctx, "same text")
	require.NoError(t, err)
	t.Logf("retained=%p first=%p next=%p value=%v detached=%v", &retained[0], &first[0], &next[0], next[0], safelyDelivered[0])
	require.Same(t, &retained[0], &next[0])
	require.EqualValues(t, 2000, next[0])
}
