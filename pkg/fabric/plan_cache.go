package fabric

import (
	"container/list"
	"sync"
)

// PlanCache stores immutable planned Fabric fragments for the exact accepted
// statement and admitted session database. Identity is not normalized here.
// It uses LRU eviction and is safe for concurrent use.
type PlanCache struct {
	mu      sync.RWMutex
	maxSize int
	cache   map[planCacheIdentity]*cachedPlan
	lru     *list.List
	lruMap  map[planCacheIdentity]*list.Element
	hits    int64
	misses  int64
}

type cachedPlan struct {
	key      planCacheIdentity
	fragment Fragment
}

// NewPlanCache creates a new Fabric plan cache.
func NewPlanCache(maxSize int) *PlanCache {
	if maxSize <= 0 {
		maxSize = 500
	}
	return &PlanCache{
		maxSize: maxSize,
		cache:   make(map[planCacheIdentity]*cachedPlan),
		lru:     list.New(),
		lruMap:  make(map[planCacheIdentity]*list.Element),
	}
}

// Get retrieves a cached fragment plan.
func (pc *PlanCache) Get(query, sessionDB string) (Fragment, bool) {
	key := planCacheKey(query, sessionDB)

	pc.mu.RLock()
	plan, ok := pc.cache[key]
	pc.mu.RUnlock()
	if !ok {
		pc.mu.Lock()
		pc.misses++
		pc.mu.Unlock()
		return nil, false
	}

	pc.mu.Lock()
	if elem, exists := pc.lruMap[key]; exists {
		pc.lru.MoveToFront(elem)
	}
	pc.hits++
	pc.mu.Unlock()
	return plan.fragment, true
}

// Put stores a fragment plan in cache.
func (pc *PlanCache) Put(query, sessionDB string, fragment Fragment) {
	if fragment == nil {
		return
	}
	key := planCacheKey(query, sessionDB)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if _, exists := pc.cache[key]; exists {
		return
	}

	for pc.lru.Len() >= pc.maxSize {
		if elem := pc.lru.Back(); elem != nil {
			plan := elem.Value.(*cachedPlan)
			delete(pc.cache, plan.key)
			delete(pc.lruMap, plan.key)
			pc.lru.Remove(elem)
		}
	}

	plan := &cachedPlan{key: key, fragment: fragment}
	pc.cache[key] = plan
	elem := pc.lru.PushFront(plan)
	pc.lruMap[key] = elem
}

// Clear removes all cached plans.
func (pc *PlanCache) Clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.cache = make(map[planCacheIdentity]*cachedPlan)
	pc.lru.Init()
	pc.lruMap = make(map[planCacheIdentity]*list.Element)
}

// Stats returns cache hit/miss counters and current size.
func (pc *PlanCache) Stats() (hits, misses int64, size int) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return pc.hits, pc.misses, len(pc.cache)
}

// planCacheIdentity keeps the two admitted identities separate, including any
// separator bytes. Credentials owns database identity semantics; the planner
// cache consumes that identity without case, whitespace or comment folding.
type planCacheIdentity struct {
	query, sessionDB string
}

func planCacheKey(query, sessionDB string) planCacheIdentity {
	return planCacheIdentity{query: query, sessionDB: sessionDB}
}
