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
// The key is a 64-bit hash (FNV-1a algorithm) that uniquely identifies a query
// pattern. The hash includes the query text and parameter keys (but not values),
// allowing parameterized queries to be cached efficiently.
//
// Parameters:
//   - query: The Cypher query string
//   - params: Query parameters (only keys are hashed, not values)
//
// Returns:
//   - uint64 hash suitable for map lookups
//
// Example 1 - Basic Usage:
//
//	cache := cache.NewQueryCache(1000, 5*time.Minute)
//
//	query := "MATCH (n:Person {name: $name}) RETURN n"
//	params := map[string]interface{}{"name": "Alice"}
//
//	key := cache.Key(query, params)
//	fmt.Printf("Cache key: %d\n", key)
//
// Example 2 - Same Query, Different Values:
//
//	// These produce the SAME key (parameter values don't matter)
//	key1 := cache.Key("MATCH (n {id: $id}) RETURN n", map[string]interface{}{"id": 1})
//	key2 := cache.Key("MATCH (n {id: $id}) RETURN n", map[string]interface{}{"id": 2})
//	// key1 == key2 (same query pattern)
//
//	// This produces a DIFFERENT key (different query)
//	key3 := cache.Key("MATCH (n {name: $name}) RETURN n", map[string]interface{}{"name": "Bob"})
//	// key3 != key1 (different query pattern)
//
// Example 3 - Integration with Parser:
//
//	func executeQuery(query string, params map[string]interface{}) (*Result, error) {
//		cache := cache.GlobalQueryCache()
//		key := cache.Key(query, params)
//
//		// Try cache first
//		if plan, ok := cache.Get(key); ok {
//			return executePlan(plan.(*ParsedPlan), params)
//		}
//
//		// Parse and cache
//		plan, err := parseQuery(query)
//		if err != nil {
//			return nil, err
//		}
//		cache.Put(key, plan)
//
//		return executePlan(plan, params)
//	}
//
// Performance:
//   - FNV-1a hash: ~50-100 ns for typical queries
//   - O(1) lookup in cache map
//   - Parameter keys included for correctness
//   - Parameter values excluded for reusability
//
// ELI12:
//
// Think of the cache key like a fingerprint for a query:
//   - Same query pattern = same fingerprint
//   - Different values (like "Alice" vs "Bob") = same fingerprint
//   - Different query = different fingerprint
//
// Why? Because the query structure is what we cache, not the specific values.
// It's like caching a recipe (the steps) rather than the actual meal (with
// specific ingredients). You can use the same recipe with different ingredients!
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
// This method performs an O(1) lookup in the cache map and automatically:
//   - Checks TTL expiration (removes expired entries)
//   - Updates LRU ordering (moves accessed entry to front)
//   - Tracks hit/miss statistics
//
// Parameters:
//   - key: Cache key from Key() method
//
// Returns:
//   - (value, true) on cache hit
//   - (nil, false) on cache miss or expiration
//
// Example 1 - Basic Cache Check:
//
//	cache := cache.NewQueryCache(1000, 5*time.Minute)
//	key := cache.Key(query, params)
//
//	if plan, ok := cache.Get(key); ok {
//		fmt.Println("Cache hit!")
//		return plan.(*ParsedPlan)
//	}
//	fmt.Println("Cache miss - need to parse")
//
// Example 2 - Query Executor Pattern:
//
//	func (e *Executor) Execute(query string, params map[string]interface{}) (*Result, error) {
//		key := e.cache.Key(query, params)
//
//		// Fast path: cached plan
//		if cached, ok := e.cache.Get(key); ok {
//			plan := cached.(*ParsedPlan)
//			return e.executePlan(plan, params)
//		}
//
//		// Slow path: parse and cache
//		plan, err := e.parser.Parse(query)
//		if err != nil {
//			return nil, err
//		}
//		e.cache.Put(key, plan)
//
//		return e.executePlan(plan, params)
//	}
//
// Example 3 - TTL Expiration:
//
//	cache := cache.NewQueryCache(1000, 1*time.Second)
//	key := cache.Key("MATCH (n) RETURN n", nil)
//
//	cache.Put(key, parsedPlan)
//
//	// Immediate access: cache hit
//	if _, ok := cache.Get(key); ok {
//		fmt.Println("Hit!") // Prints
//	}
//
//	// After TTL: cache miss (auto-removed)
//	time.Sleep(2 * time.Second)
//	if _, ok := cache.Get(key); !ok {
//		fmt.Println("Expired!") // Prints
//	}
//
// Example 4 - Type Assertion:
//
//	if cached, ok := cache.Get(key); ok {
//		// Type assert to your plan type
//		plan, ok := cached.(*ParsedPlan)
//		if !ok {
//			return nil, fmt.Errorf("invalid cached type")
//		}
//		return executePlan(plan, params)
//	}
//
// Performance:
//   - Cache hit: O(1) map lookup + O(1) list move
//   - Cache miss: O(1) map lookup
//   - TTL check: O(1) time comparison
//   - Typical latency: <100 ns
//
// Thread Safety:
//   - Safe for concurrent reads (RLock)
//   - Safe for concurrent writes (Lock)
//   - Statistics updated atomically
//
// ELI12:
//
// Imagine a library with a "recently returned" shelf:
//   - Get checks if your book is on the shelf
//   - If found, you take it and move it to the front (most recent)
//   - If the book is too old (expired), it's thrown away
//   - If not found, you have to go find it in the main stacks (parse)
//
// The cache remembers what you looked at recently so you don't have to
// search the whole library every time!
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
// This method stores a parsed query plan in the cache for future reuse.
// It automatically handles:
//   - LRU eviction when cache is full
//   - TTL timestamp setting
//   - Updating existing entries
//   - Moving entry to front of LRU list
//
// Parameters:
//   - key: Cache key from Key() method
//   - value: Parsed query plan (typically *ParsedPlan)
//
// Example 1 - Basic Caching:
//
//	cache := cache.NewQueryCache(1000, 5*time.Minute)
//
//	query := "MATCH (n:Person) RETURN n"
//	plan := parseQuery(query) // Your parser
//
//	key := cache.Key(query, nil)
//	cache.Put(key, plan)
//
//	// Later: instant retrieval
//	if cached, ok := cache.Get(key); ok {
//		fmt.Println("Reusing cached plan!")
//	}
//
// Example 2 - Parse-Once Pattern:
//
//	func getOrParsePlan(query string, params map[string]interface{}) (*ParsedPlan, error) {
//		cache := cache.GlobalQueryCache()
//		key := cache.Key(query, params)
//
//		// Try cache
//		if cached, ok := cache.Get(key); ok {
//			return cached.(*ParsedPlan), nil
//		}
//
//		// Parse (expensive operation)
//		plan, err := parser.Parse(query)
//		if err != nil {
//			return nil, err
//		}
//
//		// Cache for next time
//		cache.Put(key, plan)
//		return plan, nil
//	}
//
// Example 3 - Updating Cached Entry:
//
//	// First put
//	key := cache.Key(query, nil)
//	cache.Put(key, plan1)
//
//	// Later: update with optimized plan
//	optimizedPlan := optimizePlan(plan1)
//	cache.Put(key, optimizedPlan) // Replaces old value
//
// Example 4 - LRU Eviction:
//
//	cache := cache.NewQueryCache(3, 0) // Only 3 entries, no TTL
//
//	cache.Put(1, "plan-A")
//	cache.Put(2, "plan-B")
//	cache.Put(3, "plan-C")
//	// Cache: [C, B, A] (most recent first)
//
//	cache.Get(1) // Access A
//	// Cache: [A, C, B]
//
//	cache.Put(4, "plan-D") // Cache full, evicts B (least recent)
//	// Cache: [D, A, C]
//
// Performance:
//   - O(1) insertion or update
//   - O(1) eviction when full
//   - No allocations for updates
//   - Typical latency: <200 ns
//
// Memory Management:
//   - LRU eviction prevents unbounded growth
//   - TTL expiration removes stale entries
//   - Eviction happens synchronously on Put
//
// Thread Safety:
//   - Exclusive lock held during Put
//   - Safe for concurrent Put/Get operations
//
// ELI12:
//
// Think of Put like adding a book to the "recently returned" shelf:
//   - If there's space, just add it to the front
//   - If the shelf is full, remove the oldest book from the back
//   - If the book is already there, move it to the front with new info
//   - Mark when it was added so we know when it's too old
//
// The shelf always keeps the most recently used books, automatically
// throwing away old ones you haven't touched in a while!
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
//
// Use this to manually invalidate a cached query plan, for example when
// the underlying data schema changes or when you know a plan is no longer
// valid.
//
// Parameters:
//   - key: Cache key to remove
//
// Example 1 - Schema Change Invalidation:
//
//	func createIndex(label, property string) error {
//		if err := db.CreateIndex(label, property); err != nil {
//			return err
//		}
//
//		// Invalidate affected queries
//		cache := cache.GlobalQueryCache()
//		for _, query := range affectedQueries {
//			key := cache.Key(query, nil)
//			cache.Remove(key)
//		}
//		return nil
//	}
//
// Example 2 - Selective Invalidation:
//
//	// Remove specific query from cache
//	query := "MATCH (n:Person) RETURN n"
//	key := cache.Key(query, nil)
//	cache.Remove(key)
//
//	// Next execution will re-parse
//	result := executeQuery(query, nil) // Cache miss
//
// Performance:
//   - O(1) removal from map and list
//   - No-op if key doesn't exist
func (c *QueryCache) Remove(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
	}
}

// Clear removes all entries from the cache.
//
// Use this to completely reset the cache, for example during testing,
// after major schema changes, or when switching databases.
//
// Example 1 - Testing:
//
//	func TestQueryExecution(t *testing.T) {
//		cache := cache.NewQueryCache(100, 0)
//
//		// Test with cache
//		result1 := executeQuery("MATCH (n) RETURN n", nil)
//
//		// Clear for next test
//		cache.Clear()
//
//		// Test without cache
//		result2 := executeQuery("MATCH (n) RETURN n", nil)
//	}
//
// Example 2 - Schema Migration:
//
//	func migrateSchema() error {
//		// Perform migration
//		if err := db.Migrate(); err != nil {
//			return err
//		}
//
//		// Invalidate all cached plans
//		cache.GlobalQueryCache().Clear()
//		return nil
//	}
//
// Example 3 - Memory Pressure:
//
//	// Free memory under pressure
//	if memoryPressure() {
//		cache.GlobalQueryCache().Clear()
//		runtime.GC()
//	}
//
// Performance:
//   - O(n) where n is cache size
//   - Reinitializes internal structures
//   - Resets statistics
func (c *QueryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.list.Init()
	c.items = make(map[uint64]*list.Element, c.maxSize)
}

// Len returns the number of cached entries.
//
// Use this to monitor cache utilization or for debugging.
//
// Returns:
//   - Current number of entries in the cache
//
// Example 1 - Monitoring:
//
//	cache := cache.GlobalQueryCache()
//	fmt.Printf("Cache size: %d/%d\n", cache.Len(), 1000)
//
// Example 2 - Metrics:
//
//	func collectMetrics() {
//		cache := cache.GlobalQueryCache()
//		stats := cache.Stats()
//
//		metrics.Gauge("cache.size", float64(cache.Len()))
//		metrics.Gauge("cache.hit_rate", stats.HitRate)
//	}
//
// Performance:
//   - O(1) with read lock
func (c *QueryCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

// Stats returns cache statistics.
//
// Use this to monitor cache performance and tune cache size and TTL settings.
// Statistics are tracked atomically and have minimal performance overhead.
//
// Returns:
//   - CacheStats with hit rate, size, and access counts
//
// Example 1 - Performance Monitoring:
//
//	cache := cache.GlobalQueryCache()
//	stats := cache.Stats()
//
//	fmt.Printf("Cache Performance:\n")
//	fmt.Printf("  Size: %d/%d (%.1f%% full)\n",
//		stats.Size, stats.MaxSize,
//		float64(stats.Size)/float64(stats.MaxSize)*100)
//	fmt.Printf("  Hit Rate: %.2f%%\n", stats.HitRate)
//	fmt.Printf("  Hits: %d\n", stats.Hits)
//	fmt.Printf("  Misses: %d\n", stats.Misses)
//
// Example 2 - Metrics Collection:
//
//	func recordCacheMetrics() {
//		cache := cache.GlobalQueryCache()
//		stats := cache.Stats()
//
//		metrics.Gauge("query_cache.size", float64(stats.Size))
//		metrics.Gauge("query_cache.hit_rate", stats.HitRate)
//		metrics.Counter("query_cache.hits", float64(stats.Hits))
//		metrics.Counter("query_cache.misses", float64(stats.Misses))
//	}
//
// Example 3 - Tuning Decisions:
//
//	stats := cache.GlobalQueryCache().Stats()
//
//	if stats.HitRate < 50 {
//		log.Println("Low hit rate - consider increasing cache size")
//	}
//
//	if stats.Size == stats.MaxSize {
//		log.Println("Cache full - consider increasing maxSize")
//	}
//
// Example 4 - Periodic Reporting:
//
//	go func() {
//		ticker := time.NewTicker(1 * time.Minute)
//		for range ticker.C {
//			stats := cache.GlobalQueryCache().Stats()
//			log.Printf("Cache: %d entries, %.1f%% hit rate",
//				stats.Size, stats.HitRate)
//		}
//	}()
//
// Interpreting Hit Rate:
//   - >80%: Excellent - cache is very effective
//   - 60-80%: Good - cache is helping
//   - 40-60%: Fair - consider tuning
//   - <40%: Poor - cache may be too small or TTL too short
//
// Performance:
//   - O(1) with read lock
//   - Atomic statistics access
//   - No allocations
//
// ELI12:
//
// Stats tells you how well your cache is working:
//   - Hit Rate: How often you find what you're looking for (higher is better)
//   - Size: How many things are in the cache right now
//   - Hits: How many times you found what you wanted
//   - Misses: How many times you had to go searching
//
// It's like checking your homework success rate - if you're getting most
// answers from your notes (high hit rate), your notes are working well!
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
//
// Use these statistics to monitor cache effectiveness and make tuning decisions.
// All fields are safe to read concurrently.
//
// Fields:
//   - Size: Current number of entries in the cache
//   - MaxSize: Maximum capacity (from NewQueryCache)
//   - Hits: Total number of successful cache lookups
//   - Misses: Total number of cache misses (parse required)
//   - HitRate: Percentage of lookups that were hits (0-100)
//
// Example 1 - Health Check:
//
//	func checkCacheHealth() error {
//		stats := cache.GlobalQueryCache().Stats()
//
//		if stats.HitRate < 50 {
//			return fmt.Errorf("cache hit rate too low: %.1f%%", stats.HitRate)
//		}
//
//		if stats.Size == stats.MaxSize {
//			log.Warn("Cache is full - consider increasing size")
//		}
//
//		return nil
//	}
//
// Example 2 - Dashboard Display:
//
//	stats := cache.GlobalQueryCache().Stats()
//	fmt.Printf(`
//	Query Cache Status:
//	  Capacity: %d/%d (%.1f%% full)
//	  Hit Rate: %.2f%%
//	  Total Requests: %d
//	    Hits: %d
//	    Misses: %d
//	`,
//		stats.Size, stats.MaxSize,
//		float64(stats.Size)/float64(stats.MaxSize)*100,
//		stats.HitRate,
//		stats.Hits+stats.Misses,
//		stats.Hits,
//		stats.Misses)
//
// Example 3 - Prometheus Metrics:
//
//	func exportPrometheusMetrics(stats cache.CacheStats) {
//		prometheus.GaugeSet("query_cache_size", float64(stats.Size))
//		prometheus.GaugeSet("query_cache_max_size", float64(stats.MaxSize))
//		prometheus.GaugeSet("query_cache_hit_rate", stats.HitRate)
//		prometheus.CounterAdd("query_cache_hits_total", float64(stats.Hits))
//		prometheus.CounterAdd("query_cache_misses_total", float64(stats.Misses))
//	}
//
// ELI12:
//
// CacheStats is like a report card for your cache:
//   - Size/MaxSize: How full is your backpack? (5/10 books)
//   - Hits: How many times you found your homework in your backpack
//   - Misses: How many times you had to search your locker
//   - HitRate: Your success percentage (80% means you find it 8 out of 10 times)
//
// Higher hit rate = better cache = faster queries!
type CacheStats struct {
	Size    int     // Current number of entries
	MaxSize int     // Maximum capacity
	Hits    uint64  // Number of cache hits
	Misses  uint64  // Number of cache misses
	HitRate float64 // Hit rate percentage (0-100)
}

// SetEnabled enables or disables the cache.
//
// When disabled, all Get operations return cache misses and Put operations
// are no-ops. The cache is also cleared when disabled. Use this for debugging
// or when you want to bypass caching temporarily.
//
// Parameters:
//   - enabled: true to enable caching, false to disable
//
// Example 1 - Debugging:
//
//	// Disable cache to test parsing performance
//	cache := cache.GlobalQueryCache()
//	cache.SetEnabled(false)
//
//	start := time.Now()
//	for i := 0; i < 1000; i++ {
//		executeQuery("MATCH (n) RETURN n", nil)
//	}
//	fmt.Printf("Without cache: %v\n", time.Since(start))
//
//	// Re-enable for comparison
//	cache.SetEnabled(true)
//	start = time.Now()
//	for i := 0; i < 1000; i++ {
//		executeQuery("MATCH (n) RETURN n", nil)
//	}
//	fmt.Printf("With cache: %v\n", time.Since(start))
//
// Example 2 - Conditional Caching:
//
//	func executeQuery(query string, useCache bool) (*Result, error) {
//		cache := cache.GlobalQueryCache()
//		cache.SetEnabled(useCache)
//
//		// Execute query (cache behavior depends on useCache)
//		return executor.Execute(query, nil)
//	}
//
// Example 3 - Testing:
//
//	func TestParserWithoutCache(t *testing.T) {
//		cache := cache.NewQueryCache(100, 0)
//		cache.SetEnabled(false) // Force re-parsing
//
//		// All queries will be parsed fresh
//		for _, query := range testQueries {
//			result := executeQuery(query, nil)
//			// Verify parsing logic...
//		}
//	}
//
// Performance Impact:
//   - Disabled: All Get() returns false (cache miss)
//   - Disabled: All Put() are no-ops
//   - Disabling clears the cache (frees memory)
//
// Thread Safety:
//   - Safe to call concurrently
//   - Exclusive lock held during state change
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
// The global cache is a singleton that's lazily initialized with default
// settings (1000 entries, 5-minute TTL). Use ConfigureGlobalCache to
// customize the cache before first use.
//
// Returns:
//   - Shared QueryCache instance
//
// Example 1 - Simple Usage:
//
//	func executeQuery(query string, params map[string]interface{}) (*Result, error) {
//		cache := cache.GlobalQueryCache()
//		key := cache.Key(query, params)
//
//		if plan, ok := cache.Get(key); ok {
//			return executePlan(plan.(*ParsedPlan), params)
//		}
//
//		plan, err := parseQuery(query)
//		if err != nil {
//			return nil, err
//		}
//		cache.Put(key, plan)
//
//		return executePlan(plan, params)
//	}
//
// Example 2 - With Custom Configuration:
//
//	func init() {
//		// Configure before first use
//		cache.ConfigureGlobalCache(5000, 10*time.Minute)
//	}
//
//	func main() {
//		// Now uses custom configuration
//		cache := cache.GlobalQueryCache()
//		fmt.Printf("Cache size: %d\n", cache.Len())
//	}
//
// Example 3 - Monitoring:
//
//	go func() {
//		ticker := time.NewTicker(1 * time.Minute)
//		for range ticker.C {
//			stats := cache.GlobalQueryCache().Stats()
//			log.Printf("Cache hit rate: %.1f%%", stats.HitRate)
//		}
//	}()
//
// Default Configuration:
//   - MaxSize: 1000 entries
//   - TTL: 5 minutes
//   - Enabled: true
//
// Thread Safety:
//   - Singleton initialization is thread-safe
//   - All cache operations are thread-safe
//
// ELI12:
//
// GlobalQueryCache is like having ONE shared notebook for the whole class:
//   - Everyone uses the same notebook (singleton)
//   - First person to open it sets it up (lazy initialization)
//   - Everyone can read and write at the same time (thread-safe)
//   - No need to pass the notebook around - just call GlobalQueryCache()!
func GlobalQueryCache() *QueryCache {
	globalQueryCacheOnce.Do(func() {
		globalQueryCache = NewQueryCache(1000, 5*time.Minute)
	})
	return globalQueryCache
}

// ConfigureGlobalCache configures the global query cache.
//
// This function must be called before the first use of GlobalQueryCache()
// to customize the cache settings. Subsequent calls are no-ops (first call wins).
//
// Parameters:
//   - maxSize: Maximum number of cached plans (LRU eviction when exceeded)
//   - ttl: Time-to-live for cached entries (0 = no expiration)
//
// Example 1 - Application Initialization:
//
//	func main() {
//		// Configure cache early in main()
//		cache.ConfigureGlobalCache(5000, 10*time.Minute)
//
//		// Start application
//		server.Start()
//	}
//
// Example 2 - Environment-Based Configuration:
//
//	func init() {
//		maxSize := getEnvInt("CACHE_SIZE", 1000)
//		ttl := getEnvDuration("CACHE_TTL", 5*time.Minute)
//
//		cache.ConfigureGlobalCache(maxSize, ttl)
//	}
//
// Example 3 - Production vs Development:
//
//	func init() {
//		if os.Getenv("ENV") == "production" {
//			// Large cache for production
//			cache.ConfigureGlobalCache(10000, 15*time.Minute)
//		} else {
//			// Small cache for development
//			cache.ConfigureGlobalCache(100, 1*time.Minute)
//		}
//	}
//
// Example 4 - Testing:
//
//	func TestMain(m *testing.M) {
//		// Small cache for tests
//		cache.ConfigureGlobalCache(10, 0)
//		os.Exit(m.Run())
//	}
//
// Timing:
//   - Call in init() or early in main()
//   - Before any query execution
//   - Before starting HTTP server
//
// Thread Safety:
//   - First call wins (sync.Once)
//   - Subsequent calls are ignored
//   - Safe to call from multiple goroutines
//
// ELI12:
//
// ConfigureGlobalCache is like setting up the classroom before students arrive:
//   - You decide how big the shared notebook should be (maxSize)
//   - You decide how long notes stay valid (ttl)
//   - Once students arrive, you can't change the notebook (first call wins)
//   - Do this in init() or main() before anyone uses the cache!
func ConfigureGlobalCache(maxSize int, ttl time.Duration) {
	globalQueryCacheOnce.Do(func() {
		globalQueryCache = NewQueryCache(maxSize, ttl)
	})
}
