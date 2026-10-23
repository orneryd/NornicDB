package cypher

import (
	"hash/maphash"
	"sync"
)

// boundedCache is a concurrency-safe cache for values derived from query text
// (keyword positions, parsed subquery shapes, predicate plans). It holds at
// most limit entries. When it is full, the next insert clears it and caching
// starts over, so a long-running server keeps caching the texts it currently
// sees: a cache that stopped growing would recompute every new text on every
// use once it had seen limit distinct texts.
//
// Entries live in 64 shards with independent locks: corpus-style literal
// statements insert a new text on every use, and one shared mutex made that
// 88% of all lock wait in the PR #1043 write-path profiles. Sharding keeps
// every hit (repeated texts still hit) while dividing the insert contention.
// The hash closure passed at construction keeps key hashing allocation-free
// (interface-boxing a key per lookup was 11% of alloc objects on the
// match-create-delete gate).
type boundedCache[K comparable, V any] struct {
	shards [boundedCacheShardCount]boundedCacheShard[K, V]
	limit  int
	hash   func(K) int
}

// boundedCacheShardCount is the number of independently locked shards.
const boundedCacheShardCount = 64

type boundedCacheShard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

var cacheKeyHashSeed = maphash.MakeSeed()

// boundedCacheStringShardIndex picks a shard for a string key without any
// allocation.
func boundedCacheStringShardIndex(s string) int {
	return int(maphash.String(cacheKeyHashSeed, s) % boundedCacheShardCount)
}

// Borrowed bytes use the same seed and shard as an equivalent string key.
func boundedCacheBytesShardIndex(s []byte) int {
	return int(maphash.Bytes(cacheKeyHashSeed, s) % boundedCacheShardCount)
}

// keywordIndexCacheKeyShardIndex picks a shard for a keyword index cache key
// without boxing it in an interface.
func keywordIndexCacheKeyShardIndex(k keywordIndexCacheKey) int {
	h := maphash.String(cacheKeyHashSeed, k.s) ^ maphash.String(cacheKeyHashSeed, k.keyword) ^ uint64(k.from)
	return int(h % boundedCacheShardCount)
}

// newBoundedCache returns an empty cache holding at most limit entries. hash
// must map each key to a shard index without allocating; use
// boundedCacheStringShardIndex for string keys.
func newBoundedCache[K comparable, V any](limit int, hash func(K) int) *boundedCache[K, V] {
	c := &boundedCache[K, V]{limit: limit, hash: hash}
	shardLimit := limit / boundedCacheShardCount
	for i := range c.shards {
		c.shards[i].m = make(map[K]V, shardLimit/4+1)
	}
	return c
}

// get returns the cached value for key.
func (c *boundedCache[K, V]) get(key K) (V, bool) {
	sh := &c.shards[c.hash(key)]
	sh.mu.RLock()
	value, ok := sh.m[key]
	sh.mu.RUnlock()
	return value, ok
}

// getMatchingFromShard reads a borrowed key without allocating its owned
// representation. The caller computes the normal shard and compares the full
// key. Each shard holds at most limit/64 entries.
func (c *boundedCache[K, V]) getMatchingFromShard(shard int, match func(K) bool) (V, bool) {
	sh := &c.shards[shard]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	for key, value := range sh.m {
		if match(key) {
			return value, true
		}
	}
	var zero V
	return zero, false
}

// put caches value for key, clearing the shard first when it is full.
func (c *boundedCache[K, V]) put(key K, value V) {
	sh := &c.shards[c.hash(key)]
	shardLimit := c.limit / boundedCacheShardCount
	sh.mu.Lock()
	if len(sh.m) >= shardLimit {
		sh.m = make(map[K]V, shardLimit/4+1)
	}
	sh.m[key] = value
	sh.mu.Unlock()
}
