package cypher

// clear empties the cache, for tests that must replay a statement cold.
func (c *boundedCache[K, V]) clear() {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		sh.m = make(map[K]V)
		sh.mu.Unlock()
	}
}

// clearExpressionCaches empties the per-expression plans and compiled
// programs, so a replay evaluates an expression exactly as its first,
// uncached evaluation did.
func clearExpressionCaches() {
	returnProjectionPlans.Lock()
	returnProjectionPlans.plans = make(map[string]*returnProjectionPlan)
	returnProjectionPlans.Unlock()
	rowValuePrograms.clear()
	rowPredicatePrograms.clear()
	rowPredicatePlans.clear()
	rowSubqueryPlans.clear()
	projectionExpressionCache.clear()
	pipelineRowWithPlans.clear()
	// The rewrite-free texts are global: a suite that filled them would make
	// a cold run evict an entry that its replays, now cached, never evict.
	rewriteFreeTexts.clear()
}
