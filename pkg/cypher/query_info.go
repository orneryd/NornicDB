// Package cypher - Query analysis and AST capture.
package cypher

import (
	stderrors "errors"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// QueryInfo contains analyzed metadata extracted during query parsing.
// This is populated once and cached to avoid repeated string parsing.
type QueryInfo struct {
	// Query type flags - set during analysis
	HasMatch         bool
	HasOptionalMatch bool
	HasCreate        bool
	HasMerge         bool
	HasDelete        bool
	HasDetachDelete  bool
	HasSet           bool
	HasRemove        bool
	HasReturn        bool
	HasWith          bool
	HasUnwind        bool
	HasCall          bool
	HasExplain       bool
	HasProfile       bool
	HasShow          bool
	HasSchema        bool
	HasUnion         bool
	HasForeach       bool
	HasLoadCSV       bool
	HasShortestPath  bool
	HasOrderBy       bool
	HasLimit         bool
	HasSkip          bool
	HasAggregation   bool // COUNT, SUM, AVG, etc. (cached with conservative TTL)

	// First clause type for routing
	FirstClause ClauseType

	// Derived properties
	IsReadOnly      bool
	IsWriteQuery    bool
	IsSchemaQuery   bool
	IsCompoundQuery bool

	// Labels mentioned (for cache invalidation)
	Labels []string

	// Relationship types mentioned
	RelationshipTypes []string

	// The parsed AST clauses (if full parsing done)
	Clauses []Clause

	// Original query (normalized)
	NormalizedQuery string

	// Structured AST (lazily built, cached)
	// Use GetAST() to access - builds on first call
	ast      *AST
	astBuilt bool
	rawQuery string
	astMu    sync.RWMutex
}

// ClauseType represents the type of a Cypher clause
type ClauseType int

const (
	ClauseUnknown ClauseType = iota
	ClauseMatch
	ClauseCreate
	ClauseMerge
	ClauseDelete
	ClauseSet
	ClauseRemove
	ClauseReturn
	ClauseWith
	ClauseUnwind
	ClauseCall
	ClauseForeach
	ClauseLoadCSV
	ClauseShow
	ClauseDrop
	ClauseOptionalMatch
)

// QueryAnalyzer extracts query metadata with caching.
type QueryAnalyzer struct {
	cache   map[string]*QueryInfo
	raw     map[string]*QueryInfo
	cacheMu sync.RWMutex
	maxSize int
}

// NewQueryAnalyzer creates a new query analyzer with cache.
func NewQueryAnalyzer(maxSize int) *QueryAnalyzer {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &QueryAnalyzer{
		cache:   make(map[string]*QueryInfo),
		raw:     make(map[string]*QueryInfo),
		maxSize: maxSize,
	}
}

// Analyze extracts query information, using cache when available.
func (a *QueryAnalyzer) Analyze(cypher string) *QueryInfo {
	// Fast path: exact query text cache hit avoids normalization cost on hot loops.
	a.cacheMu.RLock()
	if info, ok := a.raw[cypher]; ok {
		a.cacheMu.RUnlock()
		return info
	}
	a.cacheMu.RUnlock()

	// Normalize for cache key
	normalized := normalizeQuery(cypher)

	// Check cache
	a.cacheMu.RLock()
	if info, ok := a.cache[normalized]; ok {
		a.cacheMu.RUnlock()
		return info
	}
	a.cacheMu.RUnlock()

	// Analyze query
	info := analyzeQuery(cypher)
	info.NormalizedQuery = normalized

	// Cache result
	a.cacheMu.Lock()
	// Simple eviction if at capacity
	if len(a.cache) >= a.maxSize {
		// Delete oldest (first found - not true LRU but simple and fast)
		for k := range a.cache {
			delete(a.cache, k)
			break
		}
	}
	if len(a.raw) >= a.maxSize {
		for k := range a.raw {
			delete(a.raw, k)
			break
		}
	}
	a.cache[normalized] = info
	a.raw[cypher] = info
	a.cacheMu.Unlock()

	return info
}

// ClearCache clears the analysis cache.
func (a *QueryAnalyzer) ClearCache() {
	a.cacheMu.Lock()
	a.cache = make(map[string]*QueryInfo)
	a.raw = make(map[string]*QueryInfo)
	a.cacheMu.Unlock()
}

// CacheSize returns current cache size.
func (a *QueryAnalyzer) CacheSize() int {
	a.cacheMu.RLock()
	defer a.cacheMu.RUnlock()
	return len(a.cache)
}

// GetAST returns the structured AST for the query.
// The AST is lazily built on first call and cached for subsequent calls.
// This is useful for LLM features that need structured query representation.
func (info *QueryInfo) GetAST() *AST {
	// Fast path: already built
	info.astMu.RLock()
	if info.astBuilt {
		ast := info.ast
		info.astMu.RUnlock()
		return ast
	}
	info.astMu.RUnlock()

	// Slow path: build AST
	info.astMu.Lock()
	defer info.astMu.Unlock()

	// Double-check after acquiring write lock
	if info.astBuilt {
		return info.ast
	}

	// Build AST
	builder := NewASTBuilder()
	ast, _ := builder.Build(info.rawQuery)
	info.ast = ast
	info.astBuilt = true

	return info.ast
}

// HasAST returns true if the AST has already been built.
func (info *QueryInfo) HasAST() bool {
	info.astMu.RLock()
	defer info.astMu.RUnlock()
	return info.astBuilt
}

// analyzeQuery performs the actual query analysis.
//
// SECURITY CONSIDERATIONS:
// This analyzer uses simple keyword detection for performance. It may produce
// FALSE POSITIVES (marking read-only queries as writes) but NOT false negatives.
// False positives can occur when keywords appear in:
//   - Property names: n.delete, n.create
//   - String literals: WHERE n.name = 'DELETE'
//   - Comments: // DELETE this later
//
// This is intentionally conservative - it's safe to treat a read as a write
// (performance penalty) but dangerous to treat a write as a read (stale data).
//
// The analyzer is used ONLY for caching optimization, NOT for access control.
// Actual query execution validates syntax and permissions independently.
func analyzeQuery(cypher string) *QueryInfo {
	info := &QueryInfo{
		rawQuery: cypher, // Store for lazy AST building
	}
	upper := strings.ToUpper(cypher)

	// Detect clause types using keyword search
	// This is O(n) per keyword but very fast for typical query lengths

	// Match clauses
	info.HasMatch = containsKeyword(upper, "MATCH")
	info.HasOptionalMatch = containsKeyword(upper, "OPTIONAL MATCH")

	// Write clauses
	info.HasCreate = containsKeyword(upper, "CREATE")
	info.HasMerge = containsKeyword(upper, "MERGE")
	info.HasDelete = containsKeyword(upper, "DELETE")
	info.HasDetachDelete = containsKeyword(upper, "DETACH DELETE")
	info.HasSet = containsKeyword(upper, "SET")
	info.HasRemove = containsKeyword(upper, "REMOVE")

	// Read/projection clauses
	info.HasReturn = containsKeyword(upper, "RETURN")
	info.HasWith = containsKeyword(upper, "WITH")
	info.HasUnwind = containsKeyword(upper, "UNWIND")
	info.HasOrderBy = containsKeyword(upper, "ORDER BY")
	info.HasLimit = containsKeyword(upper, "LIMIT")
	info.HasSkip = containsKeyword(upper, "SKIP")

	// Other clauses
	info.HasCall = containsKeyword(upper, "CALL")
	info.HasUnion = containsKeyword(upper, "UNION")
	info.HasForeach = containsKeyword(upper, "FOREACH")
	info.HasLoadCSV = containsKeyword(upper, "LOAD CSV")

	// Special handling
	info.HasExplain = strings.HasPrefix(upper, "EXPLAIN")
	info.HasProfile = strings.HasPrefix(upper, "PROFILE")
	info.HasShow = strings.HasPrefix(upper, "SHOW")
	info.HasSchema = containsKeyword(upper, "CREATE INDEX") ||
		containsKeyword(upper, "CREATE RANGE INDEX") ||
		containsKeyword(upper, "CREATE FULLTEXT INDEX") ||
		containsKeyword(upper, "CREATE VECTOR INDEX") ||
		containsKeyword(upper, "DROP INDEX") ||
		containsKeyword(upper, "CREATE CONSTRAINT") ||
		containsKeyword(upper, "DROP CONSTRAINT")

	// Path functions
	info.HasShortestPath = strings.Contains(upper, "SHORTESTPATH") ||
		strings.Contains(upper, "ALLSHORTESTPATHS")

	// Aggregation functions (COUNT/SUM/AVG/MIN/MAX/COLLECT).
	// These are cached with conservative TTL and invalidation on writes.
	info.HasAggregation = strings.Contains(upper, "COUNT(") ||
		strings.Contains(upper, "SUM(") ||
		strings.Contains(upper, "AVG(") ||
		strings.Contains(upper, "MIN(") ||
		strings.Contains(upper, "MAX(") ||
		strings.Contains(upper, "COLLECT(")

	// Determine first clause (for routing)
	info.FirstClause = detectFirstClause(upper)

	// Derive compound flags
	info.IsWriteQuery = info.HasCreate || info.HasMerge || info.HasDelete ||
		info.HasSet || info.HasRemove
	// For CALL, only "CALL db." procedures are read-only (schema introspection).
	// Other procedures like gds.graph.drop() may be writes.
	isDbCall := info.HasCall && strings.Contains(strings.ToUpper(cypher), "CALL DB.")
	info.IsReadOnly = !info.IsWriteQuery && !info.HasSchema &&
		(info.HasMatch || info.HasReturn || isDbCall || info.HasShow)
	info.IsSchemaQuery = info.HasSchema || info.HasShow

	// Count major clauses for compound detection
	clauseCount := 0
	if info.HasMatch {
		clauseCount++
	}
	if info.HasCreate {
		clauseCount++
	}
	if info.HasMerge {
		clauseCount++
	}
	if info.HasDelete {
		clauseCount++
	}
	info.IsCompoundQuery = clauseCount > 1

	// Extract labels for cache invalidation
	info.Labels = extractLabelsFromQuery(cypher)

	return info
}

// IsRetrySafeMergeCommitQuery reports whether a query's write shape is limited
// to a MERGE commit race that can be retried safely. MERGE may be combined
// with MATCH, OPTIONAL MATCH, SET, WITH, UNWIND, and RETURN, but side-effecting
// clauses such as CREATE, DELETE, REMOVE, FOREACH, LOAD CSV, or CALL make the
// statement non-retryable.
func IsRetrySafeMergeCommitQuery(info *QueryInfo) bool {
	if info == nil || !info.HasMerge {
		return false
	}
	if info.HasCreate || info.HasDelete || info.HasDetachDelete || info.HasRemove ||
		info.HasForeach || info.HasLoadCSV || info.HasCall || info.HasSchema ||
		info.HasShow || info.HasUnion {
		return false
	}
	return true
}

// MergeUniqueConflictIsRetrySafe reports whether a commit-time UNIQUE
// violation err of MERGE-only work (IsRetrySafeMergeCommitQuery) can succeed
// when retried: the violated value must come from a MERGE pattern, which the
// retry then matches. When a SET in one of the statements writes a violated
// property explicitly (x.k = …, or a map literal with key k), the clash comes
// from that SET, fails on every retry, and is reported as the constraint
// violation it is (MERGE (u:U {k: 7}) SET u.k = 5 against a stored k = 5, #657).
// A SET from a parameter map can't be inspected and keeps retrying, as before.
func MergeUniqueConflictIsRetrySafe(statements []string, err error) bool {
	var violation *storage.ConstraintViolationError
	if !stderrors.As(err, &violation) || violation == nil {
		return false
	}
	for _, statement := range statements {
		for _, property := range violation.Properties {
			if statementSetsProperty(statement, property) {
				return false
			}
		}
	}
	return true
}

// statementSetsProperty reports whether a SET, ON CREATE SET or ON MATCH SET
// of statement writes property explicitly.
func statementSetsProperty(statement, property string) bool {
	for offset := 0; offset < len(statement); {
		index := findKeywordIndexInContext(statement[offset:], "SET")
		if index < 0 {
			return false
		}
		body := statement[offset+index+len("SET"):]
		end := len(body)
		for _, keyword := range [...]string{"SET", "ON CREATE", "ON MATCH", "MERGE", "MATCH", "CREATE", "WITH", "RETURN", "UNWIND", "DELETE", "REMOVE"} {
			if next := findKeywordIndexInContext(body, keyword); next >= 0 && next < end {
				end = next
			}
		}
		for _, assignment := range splitTopLevelComma(body[:end]) {
			if setAssignmentWritesProperty(strings.TrimSpace(assignment), property) {
				return true
			}
		}
		offset += index + len("SET")
	}
	return false
}

// setAssignmentWritesProperty reports whether one SET assignment writes
// property: x.property = …, or x = / += a map literal with that key.
func setAssignmentWritesProperty(assignment, property string) bool {
	operator := strings.Index(assignment, "=")
	if operator <= 0 {
		return false
	}
	target := strings.TrimSpace(strings.TrimSuffix(assignment[:operator], "+"))
	if dot := strings.LastIndex(target, "."); dot >= 0 {
		return normalizePropertyKey(strings.TrimSpace(target[dot+1:])) == property
	}
	value := strings.TrimSpace(assignment[operator+1:])
	if !strings.HasPrefix(value, "{") || findMatchingDelimiter(value, 0, '{', '}') != len(value)-1 {
		return false
	}
	for _, pair := range splitTopLevelComma(value[1 : len(value)-1]) {
		if separator := findTopLevelMapKeyValueSeparator(pair); separator > 0 &&
			normalizePropertyKey(strings.TrimSpace(pair[:separator])) == property {
			return true
		}
	}
	return false
}

// containsKeyword checks if the query contains a keyword as a whole word.
// It searches all occurrences, not just the first (e.g., "ToDelete" won't
// block finding "DELETE" later in the query).
func containsKeyword(upper, keyword string) bool {
	searchFrom := 0
	for {
		idx := strings.Index(upper[searchFrom:], keyword)
		if idx < 0 {
			return false
		}
		idx += searchFrom // Adjust to absolute position

		// Check it's not part of a larger word
		isWordStart := idx == 0 || (!isAlphaNumericByte(upper[idx-1]) && upper[idx-1] != '_')
		end := idx + len(keyword)
		isWordEnd := end >= len(upper) || (!isAlphaNumericByte(upper[end]) && upper[end] != '_')

		if isWordStart && isWordEnd {
			return true
		}

		// Continue searching after this occurrence
		searchFrom = idx + 1
		if searchFrom >= len(upper) {
			return false
		}
	}
}

// detectFirstClause determines the first clause type.
func detectFirstClause(upper string) ClauseType {
	upper = strings.TrimSpace(upper)

	// Strip EXPLAIN/PROFILE prefix
	if strings.HasPrefix(upper, "EXPLAIN ") {
		upper = strings.TrimPrefix(upper, "EXPLAIN ")
		upper = strings.TrimSpace(upper)
	} else if strings.HasPrefix(upper, "PROFILE ") {
		upper = strings.TrimPrefix(upper, "PROFILE ")
		upper = strings.TrimSpace(upper)
	}

	switch {
	case strings.HasPrefix(upper, "MATCH"):
		return ClauseMatch
	case strings.HasPrefix(upper, "OPTIONAL MATCH"):
		return ClauseOptionalMatch
	case strings.HasPrefix(upper, "CREATE"):
		return ClauseCreate
	case strings.HasPrefix(upper, "MERGE"):
		return ClauseMerge
	case strings.HasPrefix(upper, "DELETE"), strings.HasPrefix(upper, "DETACH DELETE"):
		return ClauseDelete
	case strings.HasPrefix(upper, "RETURN"):
		return ClauseReturn
	case strings.HasPrefix(upper, "WITH"):
		return ClauseWith
	case strings.HasPrefix(upper, "UNWIND"):
		return ClauseUnwind
	case strings.HasPrefix(upper, "CALL"):
		return ClauseCall
	case strings.HasPrefix(upper, "LOAD CSV"):
		return ClauseLoadCSV
	case strings.HasPrefix(upper, "SHOW"):
		return ClauseShow
	default:
		return ClauseUnknown
	}
}
