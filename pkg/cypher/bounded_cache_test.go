package cypher

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBoundedCacheKeepsCachingWhenFull pins the overflow policy: entries are
// stored in 64 independently locked shards, every inserted key is immediately
// readable, and the total entry count never exceeds the cache limit.
func TestBoundedCacheKeepsCachingWhenFull(t *testing.T) {
	cache := newBoundedCache[string, int](boundedCacheShardCount, boundedCacheStringShardIndex)
	for i := 0; i < 4; i++ {
		cache.put(strconv.Itoa(i), i)
	}
	value, ok := cache.get("3")
	require.True(t, ok, "the most recently inserted key is cached")
	require.Equal(t, 3, value)

	total := func() int {
		n := 0
		for i := range cache.shards {
			cache.shards[i].mu.RLock()
			n += len(cache.shards[i].m)
			cache.shards[i].mu.RUnlock()
		}
		return n
	}

	// Filling well past the limit never grows past it, and every key added
	// to a full cache is still cached (its shard is cleared, then it is
	// inserted).
	for i := 0; i < 1000; i++ {
		key := "k" + strconv.Itoa(i)
		cache.put(key, i)
		_, ok := cache.get(key)
		require.True(t, ok, "a key added to a full cache is cached")
		require.LessOrEqual(t, total(), boundedCacheShardCount)
	}
}

// TestQueryTextCachesStayBounded pins #716: caches keyed by text that comes
// from queries (=~ patterns here) hold at most their limit, however many
// distinct texts a long-running server sees.
func TestQueryTextCachesStayBounded(t *testing.T) {
	for i := 0; i < 5000; i++ {
		_, err := GetCachedRegex("^pattern-" + strconv.Itoa(i) + "$")
		require.NoError(t, err)
	}
	size := 0
	for i := range regexCache.shards {
		regexCache.shards[i].mu.RLock()
		size += len(regexCache.shards[i].m)
		regexCache.shards[i].mu.RUnlock()
	}
	require.LessOrEqual(t, size, regexCache.limit)

	// A pattern seen after the cache cleared is compiled and cached again.
	re, err := GetCachedRegex("^pattern-1$")
	require.NoError(t, err)
	require.True(t, re.MatchString("pattern-1"))
}
