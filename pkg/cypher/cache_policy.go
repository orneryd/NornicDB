package cypher

import (
	"regexp"
	"strings"
)

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
	upper := strings.ToUpper(cypher)
	if strings.HasPrefix(strings.TrimSpace(upper), "SHOW PROCEDURES") {
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

	// Inference is non-cacheable by default; allow explicit opt-in via request map.
	if strings.Contains(upper, "CALL DB.INFER(") {
		return inferCacheTrueRe.MatchString(cypher)
	}

	return true
}
