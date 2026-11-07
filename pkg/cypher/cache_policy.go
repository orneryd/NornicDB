package cypher

import (
	"context"
)

// The write effect read here is the statement-effect owner's answer
// (QueryInfo.Writes); the remaining flags are cache hints that can
// over-invalidate. Execution admission and resolved statement modes read
// readStatementExecutionContract directly.
func (e *StorageExecutor) invalidateResultCacheForQuery(info *QueryInfo) {
	if info.Writes() && e.cache != nil {
		if len(info.Labels) > 0 {
			e.cache.InvalidateLabels(info.Labels)
		} else {
			e.cache.Invalidate()
		}
	}
}

// statementWrote reports whether completion must invalidate cached reads.
// Fixed writes conservatively invalidate even if they matched no rows. A runtime
// effect fixes no write of its own,
// so the answer comes from the resolved statement mode: the text it ran set that
// observation when it wrote, and nested procedures run inside this statement's
// scope. Nothing else may stand in for the resolved answer, because a runtime
// statement that resolved to reads leaves the cache as current as any read.
func (e *StorageExecutor) statementWrote(ctx context.Context, info *QueryInfo) bool {
	if !info.Writes() {
		return false
	}
	if !info.runtimeEffect() {
		return true
	}
	state := e.resolvedStatementModeOf(ctx)
	return state != nil && state.write.Load()
}

// invalidateCachesForQuery discards what this statement's resolved write can
// have staled. A statement that changed nothing keeps the cached reads of the
// graph it left untouched.
func (e *StorageExecutor) invalidateCachesForQuery(ctx context.Context, info *QueryInfo, query string) {
	if !e.statementWrote(ctx, info) {
		return
	}
	if info.HasDelete && queryDeletesNodes(query) {
		e.invalidateNodeLookupCache()
	}
	e.invalidateResultCacheForQuery(info)
}

// canUseResultCache combines the one determinism decision with existing cache,
// transaction and visibility boundaries. Keys retain their typed parameters,
// database/principal scope, graph/search revisions and embedding generation.
// The bound execution policy supplies the effect proof: a statement it reads as
// runtime work is admitted only when the resolved text is deterministic, and a
// nested write or unknown text refuses it there.
func (e *StorageExecutor) canUseResultCache(ctx context.Context, info *QueryInfo, cypher string) bool {
	return !e.inTransaction(ctx) && info.readResult() && e.StatementCacheSelectionValid() && e.cache != nil &&
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
