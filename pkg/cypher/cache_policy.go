package cypher

import (
	"context"
	"regexp"
	"strings"
)

// canUseResultCache applies the same transaction boundary to local and Fabric
// reads. Committed revisions cannot represent a transaction's pending writes,
// including an implicit transaction carried by context or a storage-scoped view.
func (e *StorageExecutor) canUseResultCache(ctx context.Context, info *QueryInfo, cypher string) bool {
	return !e.inTransaction(ctx) && info.IsReadOnly &&
		e.cache != nil && isCacheableReadQuery(cypher) && !profileExecutionBypassesCache(ctx) &&
		// Visibility changes with elapsed time and access metadata, independently
		// of graph-write revisions. Cache neither raw nor previously stripped rows.
		!e.queryPropertyDecayEnabled()
}

var (
	// inferCacheTrueRe enables db.infer cache only when the request explicitly opts in:
	//   CALL db.infer({..., cache: true})
	inferCacheTrueRe = regexp.MustCompile(`(?is)\bcache\s*:\s*true\b`)
	retrieveCallRe   = regexp.MustCompile(`(?is)\bCALL\s+DB\.RETRIEVE\s*\(`)
)

// isCacheableReadQuery returns true if it's safe to cache a read-only query result.
//
// Caching is correctness-preserving because we invalidate on writes, but some Cypher
// functions can vary over time (or be inherently non-deterministic) without any writes.
// Those queries should not be cached.
func isCacheableReadQuery(cypher string) bool {
	cypher = classificationText(cypher)
	upper := upperASCII(cypher)
	if strings.Contains(upper, "DB.STATS.") {
		return false
	}
	// SHOW commands list live state — running transactions, users, settings,
	// schema — that graph-write invalidation doesn't track, and TERMINATE
	// acts on it: neither is ever served from the result cache (#718, #531).
	if startsWithKeywordFold(strings.TrimSpace(cypher), "SHOW") || startsWithKeywordFold(strings.TrimSpace(cypher), "TERMINATE") {
		return false
	}

	// The GDS graph catalog (projections, gds.graph.list / exists,
	// gds.util.nodeProperty, the algorithms over a projection) changes
	// without a graph write, so the write invalidation can't keep a cached
	// answer right (#1161).
	if strings.Contains(upper, "GDS.") {
		return false
	}

	// Non-deterministic / time-sensitive builtins.
	// Keep this intentionally small and conservative; add more as we support them.
	if strings.Contains(upper, "RAND(") ||
		strings.Contains(upper, "RANDOMUUID(") ||
		strings.Contains(upper, "DATETIME(") ||
		strings.Contains(upper, "DATE(") ||
		strings.Contains(upper, "TIME(") ||
		strings.Contains(upper, "TIMESTAMP(") {
		return false
	}

	// db.retrieve can start, pull, or discard durable continuation cursors. Its
	// stateful lifecycle and owner checks must always reach the continuation
	// registry, including when qid/discard fields are hidden in parameters.
	// (The pattern needs the text DB.RETRIEVE, so any other statement skips
	// the regular expression.)
	if strings.Contains(upper, "DB.RETRIEVE") && retrieveCallRe.MatchString(cypher) {
		return false
	}

	// Inference is non-cacheable by default; allow explicit opt-in via request map.
	if strings.Contains(upper, "CALL DB.INFER(") {
		return inferCacheTrueRe.MatchString(cypher)
	}

	// A DBMS procedure reports the server's live state (its connections,
	// configuration, transaction log), which graph writes don't invalidate
	// and which can differ for each caller (dbms.listConnections lists only
	// the caller's own connections to a non-administrator): its result is
	// never served from the cache, which is shared by a database's callers.
	if strings.Contains(upper, "CALL") {
		for _, procedure := range registeredProceduresForCalls(cypher) {
			if procedure.Mode == ProcedureModeDBMS {
				return false
			}
		}
	}

	return true
}
