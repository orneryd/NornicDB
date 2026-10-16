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
	returnProjectionPlans.plans.Clear()
	returnProjectionPlans.Unlock()
	rowValuePrograms.clear()
	rowPredicatePrograms.clear()
	rowPredicatePlans.clear()
	rowSubqueryPlans.clear()
	projectionExpressionCache.clear()
	pipelineRowWithPlans.clear()
}
