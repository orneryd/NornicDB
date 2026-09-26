package cypher

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBoundedCacheKeepsCachingWhenFull pins the overflow policy: a full cache
// is cleared and keeps caching new keys, instead of refusing them.
func TestBoundedCacheKeepsCachingWhenFull(t *testing.T) {
	cache := newBoundedCache[string, int](4)
	for i := 0; i < 4; i++ {
		cache.put(strconv.Itoa(i), i)
	}
	value, ok := cache.get("3")
	require.True(t, ok)
	require.Equal(t, 3, value)

	cache.put("new", 42)
	value, ok = cache.get("new")
	require.True(t, ok, "a key added to a full cache is cached")
	require.Equal(t, 42, value)
	_, ok = cache.get("0")
	require.False(t, ok, "the full cache was cleared")

	// Past the limit, every new text is still cached.
	for i := 0; i < 20; i++ {
		key := "k" + strconv.Itoa(i)
		cache.put(key, i)
		_, ok := cache.get(key)
		require.True(t, ok, key)
	}
}
