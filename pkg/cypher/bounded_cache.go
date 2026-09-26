package cypher

import "sync"

// boundedCache is a concurrency-safe cache for values derived from query text
// (keyword positions, parsed subquery shapes, predicate plans). It holds at
// most limit entries. When it is full, the next insert clears it and caching
// starts over, so a long-running server keeps caching the texts it currently
// sees: a cache that stopped growing would recompute every new text on every
// use once it had seen limit distinct texts.
type boundedCache[K comparable, V any] struct {
	mu    sync.RWMutex
	m     map[K]V
	limit int
}

// newBoundedCache returns an empty cache holding at most limit entries.
func newBoundedCache[K comparable, V any](limit int) *boundedCache[K, V] {
	return &boundedCache[K, V]{m: make(map[K]V, limit/4), limit: limit}
}

// get returns the cached value for key.
func (c *boundedCache[K, V]) get(key K) (V, bool) {
	c.mu.RLock()
	value, ok := c.m[key]
	c.mu.RUnlock()
	return value, ok
}

// put caches value for key, clearing the cache first when it is full.
func (c *boundedCache[K, V]) put(key K, value V) {
	c.mu.Lock()
	if len(c.m) >= c.limit {
		c.m = make(map[K]V, c.limit/4)
	}
	c.m[key] = value
	c.mu.Unlock()
}
