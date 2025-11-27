// Package cache provides query plan caching for NornicDB.
//
// Query plan caching avoids re-parsing identical Cypher queries,
// significantly improving throughput for repeated queries.
//
// Features:
// - LRU eviction for bounded memory
// - TTL expiration for stale plans
// - Thread-safe operations
// - Cache hit/miss statistics
//
// Usage:
//
//	cache := NewQueryCache(1000, 5*time.Minute)
//
//	// Check cache before parsing
//	if plan, ok := cache.Get(query); ok {
//		return plan // Cache hit
//	}
//
//	// Parse and cache
//	plan := parseQuery(query)
//	cache.Put(query, plan)
package cache

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// QueryCache is a thread-safe LRU cache for parsed query plans.
//
// The cache uses:
// - Hash map for O(1) lookups
// - Doubly-linked list for LRU ordering
// - TTL for automatic expiration
//
// Example:
//
//	cache := NewQueryCache(1000, 5*time.Minute)
//
//	// Try cache first
//	key := cache.Key(query, params)
//	if plan, ok := cache.Get(key); ok {
//		return plan.(*ParsedPlan)
//	}
//
//	// Parse and cache
//	plan := parseQuery(query)
//	cache.Put(key, plan)
type QueryCache struct {
	mu sync.RWMutex

	// Configuration
	maxSize int
	ttl     time.Duration
	enabled bool

	// LRU list and map
	list  *list.List
	items map[uint64]*list.Element

	// Statistics
	hits   uint64
	misses uint64
}

// cacheEntry holds a cached item with metadata.
type cacheEntry struct {
	key       uint64
	value     interface{}
	expiresAt time.Time
}

// NewQueryCache creates a new query cache.
//
// Parameters:
//   - maxSize: Maximum number of cached plans (LRU eviction when exceeded)
//   - ttl: Time-to-live for cached entries (0 = no expiration)
//
// Example:
//
//	// Cache up to 1000 plans for 5 minutes each
//	cache := NewQueryCache(1000, 5*time.Minute)
//
//	// Unlimited TTL (only LRU eviction)
//	cache = NewQueryCache(1000, 0)
func NewQueryCache(maxSize int, ttl time.Duration) *QueryCache {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &QueryCache{
		maxSize: maxSize,
		ttl:     ttl,
		enabled: true,
		list:    list.New(),
		items:   make(map[uint64]*list.Element, maxSize),
	}
}

// Key generates a cache key from query and parameters.
//
// The key is a fast hash suitable for map lookups.
// Same query with same params = same key.
func (c *QueryCache) Key(query string, params map[string]interface{}) uint64 {
	h := fnv.New64a()
	h.Write([]byte(query))

	// Include parameter keys (not values - they might differ)
	// This allows caching parameterized queries
	for k := range params {
		h.Write([]byte(k))
	}

	return h.Sum64()
}

// Get retrieves a cached plan if present and not expired.
//
// Returns (value, true) on cache hit, (nil, false) on miss.
// Moves the entry to front of LRU list on hit.
func (c *QueryCache) Get(key uint64) (interface{}, bool) {
	if !c.enabled {
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	c.mu.RLock()
	elem, ok := c.items[key]
	c.mu.RUnlock()

	if !ok {
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	entry := elem.Value.(*cacheEntry)

	// Check TTL
	if c.ttl > 0 && time.Now().After(entry.expiresAt) {
		// Expired - remove and return miss
		c.mu.Lock()
		c.removeElement(elem)
		c.mu.Unlock()
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	// Move to front (most recently used)
	c.mu.Lock()
	c.list.MoveToFront(elem)
	c.mu.Unlock()

	atomic.AddUint64(&c.hits, 1)
	return entry.value, true
}

// Put adds a plan to the cache.
//
// If the cache is full, the least recently used entry is evicted.
// If the key already exists, the value is updated.
func (c *QueryCache) Put(key uint64, value interface{}) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already exists
	if elem, ok := c.items[key]; ok {
		// Update existing entry
		entry := elem.Value.(*cacheEntry)
		entry.value = value
		if c.ttl > 0 {
			entry.expiresAt = time.Now().Add(c.ttl)
		}
		c.list.MoveToFront(elem)
		return
	}

	// Evict if at capacity
	for c.list.Len() >= c.maxSize {
		c.evictOldest()
	}

	// Add new entry
	entry := &cacheEntry{
		key:   key,
		value: value,
	}
	if c.ttl > 0 {
		entry.expiresAt = time.Now().Add(c.ttl)
	}

	elem := c.list.PushFront(entry)
	c.items[key] = elem
}

// Remove removes an entry from the cache.
func (c *QueryCache) Remove(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

// Clear removes all entries from the cache.
func (c *QueryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list.Init()
	c.items = make(map[uint64]*list.Element, c.maxSize)
}

// Len returns the number of cached entries.
func (c *QueryCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

// Stats returns cache statistics.
func (c *QueryCache) Stats() CacheStats {
	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)

	c.mu.RLock()
	size := c.list.Len()
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
	Size    int     // Current number of entries
	MaxSize int     // Maximum capacity
	Hits    uint64  // Number of cache hits
	Misses  uint64  // Number of cache misses
	HitRate float64 // Hit rate percentage (0-100)
}

// SetEnabled enables or disables the cache.
func (c *QueryCache) SetEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enabled = enabled

	if !enabled {
		c.list.Init()
		c.items = make(map[uint64]*list.Element, c.maxSize)
	}
}

// evictOldest removes the least recently used entry.
// Caller must hold the lock.
func (c *QueryCache) evictOldest() {
	elem := c.list.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

// removeElement removes an element from the cache.
// Caller must hold the lock.
func (c *QueryCache) removeElement(elem *list.Element) {
	c.list.Remove(elem)
	entry := elem.Value.(*cacheEntry)
	delete(c.items, entry.key)
}

// =============================================================================
// Global Query Cache (singleton for convenience)
// =============================================================================

var (
	globalQueryCache     *QueryCache
	globalQueryCacheOnce sync.Once
)

// GlobalQueryCache returns the global query cache instance.
//
// The global cache is lazily initialized with default settings.
// Use ConfigureGlobalCache to customize before first use.
func GlobalQueryCache() *QueryCache {
	globalQueryCacheOnce.Do(func() {
		globalQueryCache = NewQueryCache(1000, 5*time.Minute)
	})
	return globalQueryCache
}

// ConfigureGlobalCache configures the global query cache.
//
// Must be called before any Get/Put operations.
// Subsequent calls are no-ops.
func ConfigureGlobalCache(maxSize int, ttl time.Duration) {
	globalQueryCacheOnce.Do(func() {
		globalQueryCache = NewQueryCache(maxSize, ttl)
	})
}
