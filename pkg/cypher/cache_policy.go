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
		e.cache != nil && cacheableReadQueryWithRetrieval(cypher, 0, func(argument string) (bool, bool) {
		return cacheableBoundRetrievalRequest(ctx, argument)
	}) && !profileExecutionBypassesCache(ctx) &&
		// Visibility changes with elapsed time and access metadata, independently
		// of graph-write revisions. Cache neither raw nor previously stripped rows.
		!e.queryPropertyDecayEnabled()
}

var (
	// inferCacheTrueRe enables db.infer cache only when the request explicitly opts in:
	//   CALL db.infer({..., cache: true})
	inferCacheTrueRe = regexp.MustCompile(`(?is)\bcache\s*:\s*true\b`)
)

// isCacheableReadQuery returns true if it's safe to cache a read-only query result.
//
// Caching is correctness-preserving because we invalidate on writes, but some Cypher
// functions can vary over time (or be inherently non-deterministic) without any writes.
// Those queries should not be cached.
func isCacheableReadQuery(cypher string) bool {
	return cacheableReadQuery(cypher, 0)
}

func cacheableReadQuery(cypher string, depth int) bool {
	return cacheableReadQueryWithRetrieval(cypher, depth, nil)
}

// resolveRetrieval is used only after parameters are attached to the execution
// context. Row-bound/computed expressions remain outside the outer result cache;
// their one-shot searches still use the search service's existing cache.
func cacheableReadQueryWithRetrieval(cypher string, depth int, resolveRetrieval func(string) (bool, bool)) bool {
	if depth >= 16 {
		return false
	}
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

	// Stateful retrieval must reach the registry, including requests hidden
	// in parameters. Ordinary literal db.rretrieve searches keep their cache.
	if strings.Contains(upper, "RETRIEVE") && !cacheableRetrievalCalls(cypher, resolveRetrieval) {
		return false
	}

	// Inference is non-cacheable by default; allow explicit opt-in via request map.
	if strings.Contains(upper, "CALL DB.INFER(") {
		if !inferCacheTrueRe.MatchString(cypher) {
			return false
		}
	}

	// A DBMS procedure reports the server's live state (its connections,
	// configuration, transaction log), which graph writes don't invalidate
	// and which can differ for each caller (dbms.listConnections lists only
	// the caller's own connections to a non-administrator): its result is
	// never served from the cache, which is shared by a database's callers.
	// A user-defined procedure (CREATE PROCEDURE, apoc.custom) can be
	// replaced or dropped from any database, which writes nothing to this
	// database's graph: a query that calls one, itself or in a statement it
	// hands to a procedure such as apoc.cypher.run, isn't cached, so a call
	// after the replacement runs the new body.
	if strings.Contains(upper, "CALL") {
		if globalProcedureRegistry.callsUserProcedure(cypher) {
			return false
		}
		if strings.Contains(upper, "APOC.") && !cacheableDynamicStatements(cypher, depth) {
			return false
		}
		for _, procedure := range registeredProceduresForCalls(cypher) {
			if procedure.Mode == ProcedureModeDBMS {
				return false
			}
		}
	}

	return true
}

func cacheableRetrievalCalls(query string, resolve func(string) (bool, bool)) bool {
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets, opts.SkipBraces = false, false, false
	for from := 0; from < len(query); {
		at := keywordIndexFrom(query, "CALL", from, opts)
		if at < 0 {
			break
		}
		from = at + len("CALL")
		name, ok := procedureNameAt(query, queryGapEnd(query, from))
		if !ok {
			continue
		}
		switch lowerASCII(name) {
		case "db.retrieve":
			return false
		case "db.rretrieve":
			args := explicitProcedureArgumentTexts(query[at:])
			if len(args) != 1 {
				return false
			}
			if resolve != nil {
				if cacheable, known := resolve(args[0]); known {
					if !cacheable {
						return false
					}
					continue
				}
			}
			if isWholeCypherQuotedString(args[0]) {
				continue
			}
			stateful := false
			literal := visitStaticMapLiteralKeys(args[0], func(key string) {
				if isWholeCypherQuotedString(key) {
					key, _ = decodeCypherQuotedString(key)
				}
				switch key {
				case "qid", "discard", "n", "mode", "groupBy", "group_by", "rankedLimit", "ranked_limit":
					stateful = true
				}
			})
			if !literal || stateful {
				return false
			}
		}
	}
	return true
}

// cacheableDynamicStatements follows literal executable arguments through the
// same cache policy as the outer statement. Parameters and row expressions
// are resolved during execution, so their outer results cannot be reused.
func cacheableDynamicStatements(query string, depth int) bool {
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets, opts.SkipBraces = false, false, false
	for from := 0; from < len(query); {
		at := keywordIndexFrom(query, "CALL", from, opts)
		if at < 0 {
			break
		}
		from = at + len("CALL")
		name, ok := procedureNameAt(query, queryGapEnd(query, from))
		if !ok {
			continue
		}
		switch name = lowerASCII(name); name {
		case "apoc.cypher.run", "apoc.cypher.doitall":
			args := explicitProcedureArgumentTexts(query[at:])
			if len(args) == 0 || !isWholeCypherQuotedString(args[0]) {
				return false
			}
			statement, decoded := decodeCypherQuotedString(args[0])
			requirements := QueryPermissionRequirements(statement)
			if !decoded || requirements.Write || requirements.Schema || requirements.Admin || !cacheableReadQuery(statement, depth+1) {
				return false
			}
		default:
			if strings.HasPrefix(name, "apoc.cypher.") || strings.HasPrefix(name, "apoc.periodic.") ||
				strings.HasPrefix(name, "apoc.do.") || name == "apoc.when" || name == "apoc.case" ||
				name == "apoc.export.json.query" || name == "apoc.export.csv.query" {
				return false
			}
		}
	}
	return true
}
