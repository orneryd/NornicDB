package search

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestServiceOptionsSearchResultCachePolicy(t *testing.T) {
	defaultService := NewServiceWithDimensionsAndBM25Engine(storage.NewMemoryEngine(), 3, "v2")
	require.NotNil(t, defaultService.resultCache)
	require.Equal(t, 1000, defaultService.resultCache.maxSize)
	require.Equal(t, 5*time.Minute, defaultService.resultCache.ttl)

	disabled := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{
		DatabaseID:               "alpha",
		SearchResultCacheEntries: 0,
		SearchResultCacheTTL:     time.Minute,
	})
	require.NotNil(t, disabled.resultCache)
	require.Equal(t, 0, disabled.resultCache.maxSize)

	configured := NewServiceWithDimensionsAndBM25EngineAndOptions(storage.NewMemoryEngine(), 3, "v2", &ServiceOptions{
		DatabaseID:               "beta",
		SearchResultCacheEntries: 12,
		SearchResultCacheTTL:     2 * time.Minute,
	})
	require.Equal(t, 12, configured.resultCache.maxSize)
	require.Equal(t, 2*time.Minute, configured.resultCache.ttl)
}

func TestSearchResultCacheResizeAndTTL(t *testing.T) {
	cache := newSearchResultCache(3, time.Hour)
	cache.Put("a", &SearchResponse{Query: "a"})
	cache.Put("b", &SearchResponse{Query: "b"})
	cache.Put("c", &SearchResponse{Query: "c"})
	require.NotNil(t, cache.Get("a"))

	cache.Resize(2)
	require.Nil(t, cache.Get("b"))
	require.NotNil(t, cache.Get("a"))
	require.NotNil(t, cache.Get("c"))

	cache.Resize(4)
	require.NotNil(t, cache.Get("a"))
	cache.Resize(0)
	require.Nil(t, cache.Get("a"))
	cache.Put("disabled", &SearchResponse{Query: "disabled"})
	require.Nil(t, cache.Get("disabled"))

	cache.Resize(2)
	cache.Put("old", &SearchResponse{Query: "old"})
	time.Sleep(5 * time.Millisecond)
	cache.SetTTL(time.Millisecond)
	require.Nil(t, cache.Get("old"))
}
