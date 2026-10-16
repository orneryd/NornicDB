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
	return cacheableReadQuery(cypher, 0)
}

func cacheableReadQuery(cypher string, depth int) bool {
	if depth >= 16 {
		return false
	}
	cypher = classificationText(cypher)
	requirements := QueryPermissionRequirements(cypher)
	if requirements.Unclassified || requirements.Write || requirements.Schema || requirements.Admin || requirements.UpdatesDBMS {
		return false
	}
	cacheableFunctions := true
	_ = forEachSemanticCall(cypher, func(name, normalized string, _ int, procedure bool) error {
		// Catalog state belongs to the executing principal and changes without
		// a graph revision. Use decoded callable names for functions and
		// procedures, including quoted or spaced identifier components.
		if strings.HasPrefix(normalized, "gds.") {
			cacheableFunctions = false
		}
		if procedure {
			return nil
		}
		// An extension's privilege contract does not promise determinism or
		// principal-independent output. Always invoke its actual handler.
		if _, installed := pluginFunctionHandler(name); installed {
			cacheableFunctions = false
			return nil
		}
		if class, found := functionPrivilegeClass(name); found && class.Has(PrivilegeFileIO|PrivilegeSharedState|PrivilegeDBMS|PrivilegeWrite|PrivilegeSchema|PrivilegeAdmin) {
			cacheableFunctions = false
		}
		return nil
	})
	if !cacheableFunctions {
		return false
	}
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
			if procedure.Mode == ProcedureModeDBMS || procedure.Privilege.Has(PrivilegeFileIO|PrivilegeSharedState|PrivilegeDBMS) {
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
