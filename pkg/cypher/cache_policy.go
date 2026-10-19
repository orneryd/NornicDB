package cypher

import "context"

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
        policy = cachedExecutionPolicy(text)
        info.cachePolicy.Store(policy)
    }
    return policy.allows(&executionBindings{params: getParamsFromContext(ctx)}, 0)
}

func isCacheableReadQuery(cypher string) bool {
	return deterministicExecution(context.Background(), cypher, 0)
}

func cacheableReadQuery(cypher string, depth int) bool {
	return deterministicExecution(context.Background(), cypher, depth)
}
