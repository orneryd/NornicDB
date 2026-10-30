package cypher

import "context"

// These conservative hints can over-invalidate caches. Execution admission and
// resolved statement modes use readStatementExecutionContract instead.
func (e *StorageExecutor) invalidateResultCacheForQuery(info *QueryInfo) {
	if info.IsWriteQuery && e.cache != nil {
		if len(info.Labels) > 0 {
			e.cache.InvalidateLabels(info.Labels)
		} else {
			e.cache.Invalidate()
		}
	}
}

func (e *StorageExecutor) invalidateCachesForQuery(info *QueryInfo, query string) {
	if info.IsWriteQuery && info.HasDelete && queryDeletesNodes(query) {
		e.invalidateNodeLookupCache()
	}
	e.invalidateResultCacheForQuery(info)
}

// canUseResultCache combines the one determinism decision with existing cache,
// transaction and visibility boundaries. Keys retain their typed parameters,
// database/principal scope, graph/search revisions and embedding generation.
func (e *StorageExecutor) canUseResultCache(ctx context.Context, info *QueryInfo, cypher string) bool {
	return !e.inTransaction(ctx) && info.IsReadOnly && e.StatementCacheSelectionValid() && e.cache != nil &&
		info.deterministicExecution(ctx, cypher) && !profileExecutionBypassesCache(ctx) && !e.queryPropertyDecayEnabled()
}

func (info *QueryInfo) deterministicExecution(ctx context.Context, text string) bool {
	policy := info.cachePolicy.Load()
	if policy == nil || policy.generation != globalProcedureRegistry.generation.Load() {
		policy = cachedExecutionPolicy(ctx, text)
		if ctx.Err() != nil {
			return false
		}
		info.cachePolicy.Store(policy)
	}
	return policy.allows(ctx, newExecutionBindings(ctx), 0)
}

func isCacheableReadQuery(cypher string) bool {
	return deterministicExecution(context.Background(), cypher, 0)
}

func cacheableReadQuery(cypher string, depth int) bool {
	return deterministicExecution(context.Background(), cypher, depth)
}
