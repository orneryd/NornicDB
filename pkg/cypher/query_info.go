// Package cypher - Query analysis and AST capture.
package cypher

import (
	stderrors "errors"
	"strconv"
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

// CommitStatement is one statement of a committing transaction with the
// parameters it ran with.
type CommitStatement struct {
	Query  string
	Params map[string]interface{}
}

// MergeUniqueConflictIsRetrySafe reports whether a commit-time UNIQUE
// violation err of MERGE-only work (IsRetrySafeMergeCommitQuery) can succeed
// when retried, so it is reported as a transient race: every value the
// statements' SET items (SET, ON CREATE SET, ON MATCH SET) write to a violated
// property must be the value a MERGE pattern of the statement keys on, which
// the retry then matches. A SET writing any other value clashes on every retry
// and is reported as the constraint violation it is (MERGE (u:U {k: 7})
// SET u.k = 5 against a stored k = 5, #657). SET x = / += $map is decided by
// the parameter map's keys and values. Whatever can't be decided statically (a
// computed value, a map from a variable, a statement the clause splitter can't
// split) is not retry-safe, so a real duplicate is never retried.
func MergeUniqueConflictIsRetrySafe(statements []CommitStatement, err error) bool {
	var violation *storage.ConstraintViolationError
	if !stderrors.As(err, &violation) || violation == nil {
		return false
	}
	for _, statement := range statements {
		items, mergeKeys, ok := mergeStatementSetItems(statement.Query)
		if !ok {
			return false
		}
		for _, item := range items {
			for _, property := range violation.Properties {
				if !setItemKeepsMergeKey(item, property, mergeKeys, statement.Params) {
					return false
				}
			}
		}
	}
	return true
}

// mergeStatementSetItems returns the SET items of statement (its SET clauses
// and its MERGE clauses' ON CREATE SET / ON MATCH SET lists) and the property
// expressions its MERGE patterns key on. ok is false when the statement can't
// be split into clauses.
func mergeStatementSetItems(statement string) (items []string, mergeKeys map[string][]string, ok bool) {
	clauses, ok := splitPipelineClauses(statement)
	if !ok {
		return nil, nil, false
	}
	for _, clause := range clauses {
		switch clause.kind {
		case pipelineClauseSet:
			items = append(items, splitSetAssignments(strings.TrimSpace(clause.text[len("SET"):]))...)
		case pipelineClauseMerge:
			pattern, onCreate, onMatch := splitMergeClauseActions(strings.TrimSpace(clause.text[len("MERGE"):]))
			items = append(items, splitSetAssignments(onCreate)...)
			items = append(items, splitSetAssignments(onMatch)...)
			mergeKeys = appendPatternPropertyExpressions(mergeKeys, pattern)
		}
	}
	return items, mergeKeys, true
}

// appendPatternPropertyExpressions adds the key: expression pairs of the
// property maps in pattern to keys.
func appendPatternPropertyExpressions(keys map[string][]string, pattern string) map[string][]string {
	for index := 0; index < len(pattern); index++ {
		switch pattern[index] {
		case '\'', '"':
			index = skipQuotedSemanticText(pattern, index) - 1
		case '{':
			closing := findMatchingDelimiter(pattern, index, '{', '}')
			if closing < 0 {
				return keys
			}
			for _, pair := range splitTopLevelComma(pattern[index+1 : closing]) {
				if separator := findTopLevelMapKeyValueSeparator(pair); separator > 0 {
					if keys == nil {
						keys = make(map[string][]string)
					}
					key := normalizePropertyKey(pair[:separator])
					keys[key] = append(keys[key], strings.TrimSpace(pair[separator+1:]))
				}
			}
			index = closing
		}
	}
	return keys
}

// setItemKeepsMergeKey reports whether the SET item can't cause a UNIQUE
// clash on property of its own: it doesn't write property, or it writes a
// value a MERGE pattern keys property on.
func setItemKeepsMergeKey(item, property string, mergeKeys map[string][]string, params map[string]interface{}) bool {
	_, setProperty, operator, right := splitSetAssignment(item)
	switch {
	case operator == ":":
		return true
	case operator == "":
		return false
	case setProperty != "":
		if normalizePropertyKey(setProperty) != property {
			return true
		}
		value, ok := staticCommitValue(right, params)
		return ok && isMergeKeyValue(value, mergeKeys[property], params)
	}
	var written map[string]interface{}
	switch {
	case strings.HasPrefix(right, "$") && isValidIdentifier(right[1:]):
		parameter, ok := params[right[1:]].(map[string]interface{})
		if !ok {
			return false
		}
		written = parameter
	case strings.HasPrefix(right, "{") && findMatchingDelimiter(right, 0, '{', '}') == len(right)-1:
		for _, pair := range splitTopLevelComma(right[1 : len(right)-1]) {
			separator := findTopLevelMapKeyValueSeparator(pair)
			if separator <= 0 || normalizePropertyKey(pair[:separator]) != property {
				continue
			}
			value, ok := staticCommitValue(pair[separator+1:], params)
			return ok && isMergeKeyValue(value, mergeKeys[property], params)
		}
		return true
	default:
		return false
	}
	value, writes := written[property]
	return !writes || isMergeKeyValue(value, mergeKeys[property], params)
}

// isMergeKeyValue reports whether value is one of the MERGE key expressions.
func isMergeKeyValue(value interface{}, expressions []string, params map[string]interface{}) bool {
	for _, expression := range expressions {
		if key, ok := staticCommitValue(expression, params); ok && samePropertyValue(key, value) {
			return true
		}
	}
	return false
}

// staticCommitValue is the value of a parameter or a scalar literal
// expression; ok is false for anything else.
func staticCommitValue(expression string, params map[string]interface{}) (interface{}, bool) {
	expression = strings.TrimSpace(expression)
	if strings.HasPrefix(expression, "$") && isValidIdentifier(expression[1:]) {
		value, ok := params[expression[1:]]
		return value, ok
	}
	if len(expression) >= 2 && (expression[0] == '\'' || expression[0] == '"') &&
		expression[len(expression)-1] == expression[0] && !strings.ContainsAny(expression[1:len(expression)-1], "\\'\"") {
		return expression[1 : len(expression)-1], true
	}
	if integer, err := strconv.ParseInt(expression, 10, 64); err == nil {
		return integer, true
	}
	if float, err := strconv.ParseFloat(expression, 64); err == nil {
		return float, true
	}
	switch strings.ToLower(expression) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return nil, false
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
