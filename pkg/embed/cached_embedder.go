// Package embed provides embedding generation with caching support.
//
// CachedEmbedder wraps any Embedder with an LRU cache to avoid redundant
// embedding computations. This provides significant performance improvements
// for repeated queries without any changes to existing code.
//
// Example:
//
//	// Wrap any embedder with caching
//	base := embed.NewOllama(nil)
//	cached := embed.NewCachedEmbedder(base, 10000) // Cache 10K embeddings
//
//	// Use exactly like the original - caching is transparent
//	vec, err := cached.Embed(ctx, "hello world")
//	vec2, err := cached.Embed(ctx, "hello world") // Cache hit!
//
// Performance:
//   - Cache hit: ~1µs (vs 50-200ms for actual embedding)
//   - Memory: ~4KB per cached embedding (1024 dims × 4 bytes)
//   - 10K cache = ~40MB memory
package embed

import (
	"container/list"
	"context"
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
)

// CachedEmbedder wraps an Embedder with LRU caching.
//
// The cache is keyed by FNV-1a hash of the input text, providing:
//   - Exact match caching (same text = same embedding)
//   - Efficient lookup (O(1) for cache hits)
//   - Bounded memory usage (LRU eviction)
//   - Fast hashing (FNV-1a is non-cryptographic but fast)
//
// Thread-safe: All methods can be called from multiple goroutines.
type CachedEmbedder struct {
	base Embedder

	mu      sync.RWMutex
	cache   map[string]*list.Element
	lru     *list.List
	maxSize int

	// Statistics
	hits   uint64
	misses uint64
}

// cacheEntry holds a cached embedding with its key
type cacheEntry struct {
	key       string
	embedding []float32
}

// NewCachedEmbedder wraps an existing embedder with LRU caching.
//
// Parameters:
//   - base: The underlying embedder to wrap
//   - maxSize: Maximum number of embeddings to cache (0 = 10000 default)
//
// Example:
//
//	// Wrap Ollama with 10K cache
//	ollama := embed.NewOllama(nil)
//	cached := embed.NewCachedEmbedder(ollama, 10000)
//
//	// Or use default cache size
//	cached = embed.NewCachedEmbedder(ollama, 0)
func NewCachedEmbedder(base Embedder, maxSize int) *CachedEmbedder {
	if maxSize <= 0 {
		maxSize = 10000 // Default: 10K embeddings (~40MB for 1024-dim)
	}

	return &CachedEmbedder{
		base:    base,
		cache:   make(map[string]*list.Element, maxSize),
		lru:     list.New(),
		maxSize: maxSize,
	}
}

// hashText creates a cache key from text content using FNV-1a.
// FNV-1a is a fast non-cryptographic hash suitable for cache keys.
func hashText(text string) string {
	h := fnv.New64a()
	h.Write([]byte(text))
	return strconv.FormatUint(h.Sum64(), 36)
}

// Embed generates or retrieves a cached embedding for the text.
//
// On cache hit, returns immediately without calling the underlying embedder.
// On cache miss, calls the base embedder and caches the result.
func (c *CachedEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	key := hashText(text)

	// Check cache first (read lock)
	c.mu.RLock()
	if elem, ok := c.cache[key]; ok {
		c.mu.RUnlock()
		atomic.AddUint64(&c.hits, 1)

		// Move to front (promote in LRU)
		c.mu.Lock()
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		c.mu.Unlock()

		return entry.embedding, nil
	}
	c.mu.RUnlock()

	atomic.AddUint64(&c.misses, 1)

	// Cache miss - generate embedding
	embedding, err := c.base.Embed(ctx, text)
	if err != nil {
		return nil, err
	}

	// Add to cache
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check (another goroutine might have added it)
	if elem, ok := c.cache[key]; ok {
		c.lru.MoveToFront(elem)
		return elem.Value.(*cacheEntry).embedding, nil
	}

	// Evict if at capacity
	for c.lru.Len() >= c.maxSize {
		c.evictOldest()
	}

	// Add new entry
	entry := &cacheEntry{key: key, embedding: embedding}
	elem := c.lru.PushFront(entry)
	c.cache[key] = elem

	return embedding, nil
}

// EmbedBatch generates embeddings for multiple texts with caching.
//
// Each text is checked against the cache individually. Only cache misses
// are sent to the underlying embedder.
func (c *CachedEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	var misses []int
	var missTexts []string

	// Check cache for each text
	for i, text := range texts {
		key := hashText(text)

		c.mu.RLock()
		if elem, ok := c.cache[key]; ok {
			entry := elem.Value.(*cacheEntry)
			results[i] = entry.embedding
			atomic.AddUint64(&c.hits, 1)
			c.mu.RUnlock()

			// Promote in LRU
			c.mu.Lock()
			c.lru.MoveToFront(elem)
			c.mu.Unlock()
		} else {
			c.mu.RUnlock()
			atomic.AddUint64(&c.misses, 1)
			misses = append(misses, i)
			missTexts = append(missTexts, text)
		}
	}

	// Generate embeddings for cache misses
	if len(missTexts) > 0 {
		embeddings, err := c.base.EmbedBatch(ctx, missTexts)
		if err != nil {
			return nil, err
		}

		// Add to results and cache
		c.mu.Lock()
		for j, embedding := range embeddings {
			i := misses[j]
			results[i] = embedding

			// Cache the result
			key := hashText(missTexts[j])
			if _, ok := c.cache[key]; !ok {
				for c.lru.Len() >= c.maxSize {
					c.evictOldest()
				}
				entry := &cacheEntry{key: key, embedding: embedding}
				elem := c.lru.PushFront(entry)
				c.cache[key] = elem
			}
		}
		c.mu.Unlock()
	}

	return results, nil
}

// Dimensions returns the embedding vector dimension.
func (c *CachedEmbedder) Dimensions() int {
	return c.base.Dimensions()
}

// Model returns the model name.
func (c *CachedEmbedder) Model() string {
	return c.base.Model()
}

// Stats returns cache statistics.
func (c *CachedEmbedder) Stats() CacheStats {
	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)

	c.mu.RLock()
	size := c.lru.Len()
	c.mu.RUnlock()

	total := hits + misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return CacheStats{
		Size:    size,
		MaxSize: c.maxSize,
		Hits:    hits,
		Misses:  misses,
		HitRate: hitRate,
	}
}

// CacheStats holds cache performance statistics.
type CacheStats struct {
	Size    int     `json:"size"`     // Current number of cached embeddings
	MaxSize int     `json:"max_size"` // Maximum cache capacity
	Hits    uint64  `json:"hits"`     // Number of cache hits
	Misses  uint64  `json:"misses"`   // Number of cache misses
	HitRate float64 `json:"hit_rate"` // Hit rate percentage (0-100)
}

// Clear removes all cached embeddings.
func (c *CachedEmbedder) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*list.Element, c.maxSize)
	c.lru.Init()
}

// evictOldest removes the least recently used entry.
// Caller must hold the write lock.
func (c *CachedEmbedder) evictOldest() {
	elem := c.lru.Back()
	if elem != nil {
		entry := elem.Value.(*cacheEntry)
		delete(c.cache, entry.key)
		c.lru.Remove(elem)
	}
}
