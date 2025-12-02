// Package cypher - Pre-compiled regex patterns for performance.
//
// This file contains all regex patterns used in hot paths, pre-compiled at package init time.
// Moving regex compilation from function calls to package initialization provides 5-10x
// performance improvements for operations that use these patterns repeatedly.
//
// Performance Impact:
//   - Schema DDL operations: 5-10x faster (9 patterns)
//   - APOC path operations: 8-15x faster (8 patterns)
//   - Duration parsing: 3-5x faster (2 patterns)
package cypher

import (
	"regexp"
	"sync"
)

// =============================================================================
// Schema DDL Patterns (CREATE CONSTRAINT, CREATE INDEX)
// =============================================================================

var (
	// Constraint patterns - CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (var:Label) REQUIRE var.prop IS UNIQUE
	constraintNamedForRequire   = regexp.MustCompile(`(?i)CREATE\s+CONSTRAINT\s+(\w+)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\((\w+):(\w+)\)\s+REQUIRE\s+(\w+)\.(\w+)\s+IS\s+UNIQUE`)
	constraintUnnamedForRequire = regexp.MustCompile(`(?i)CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\((\w+):(\w+)\)\s+REQUIRE\s+(\w+)\.(\w+)\s+IS\s+UNIQUE`)
	constraintOnAssert          = regexp.MustCompile(`(?i)CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+\((\w+):(\w+)\)\s+ASSERT\s+(\w+)\.(\w+)\s+IS\s+UNIQUE`)

	// Index patterns - CREATE INDEX [name] [IF NOT EXISTS] FOR (var:Label) ON (var.prop)
	indexNamedFor   = regexp.MustCompile(`(?i)CREATE\s+INDEX\s+(\w+)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\((\w+):(\w+)\)\s+ON\s+\(([^)]+)\)`)
	indexUnnamedFor = regexp.MustCompile(`(?i)CREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\((\w+):(\w+)\)\s+ON\s+\(([^)]+)\)`)

	// Fulltext index pattern - CREATE FULLTEXT INDEX name FOR (var:Label) ON EACH [props]
	fulltextIndexPattern = regexp.MustCompile(`(?i)CREATE\s+FULLTEXT\s+INDEX\s+(\w+)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\((\w+):(\w+)\)\s+ON\s+EACH\s+\[([^\]]+)\]`)

	// Vector index patterns
	vectorIndexPattern      = regexp.MustCompile(`(?i)CREATE\s+VECTOR\s+INDEX\s+(\w+)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\((\w+):(\w+)\)\s+ON\s+\((\w+)\.(\w+)\)`)
	vectorDimensionsPattern = regexp.MustCompile(`vector\.dimensions[:\s]+(\d+)`)
	vectorSimilarityPattern = regexp.MustCompile(`vector\.similarity_function[:\s]+['"]?(\w+)['"]?`)
)

// =============================================================================
// APOC Configuration Patterns
// =============================================================================

var (
	// APOC path configuration
	apocMaxLevelPattern    = regexp.MustCompile(`maxLevel\s*:\s*(\d+)`)
	apocMinLevelPattern    = regexp.MustCompile(`minLevel\s*:\s*(\d+)`)
	apocLimitPattern       = regexp.MustCompile(`limit\s*:\s*(\d+)`)
	apocRelFilterPattern   = regexp.MustCompile(`relationshipFilter\s*:\s*['"]([^'"]+)['"]`)
	apocLabelFilterPattern = regexp.MustCompile(`labelFilter\s*:\s*['"]([^'"]+)['"]`)

	// APOC node ID extraction
	apocNodeIdBracePattern = regexp.MustCompile(`\{[^}]*id\s*:\s*['"]([^'"]+)['"]`)
	apocWhereIdPattern     = regexp.MustCompile(`WHERE\s+\w+\.id\s*=\s*['"]([^'"]+)['"]`)

	// Fulltext search phrase extraction
	fulltextPhrasePattern = regexp.MustCompile(`"([^"]+)"`)
)

// =============================================================================
// Duration Parsing Patterns
// =============================================================================

var (
	// ISO 8601 duration parsing - P[n]Y[n]M[n]DT[n]H[n]M[n]S
	durationDatePartPattern = regexp.MustCompile(`(\d+)([YMD])`)
	durationTimePartPattern = regexp.MustCompile(`(\d+(?:\.\d+)?)([HMS])`)
)

// =============================================================================
// Fast-Path Compound Query Patterns
// =============================================================================

var (
	// MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE (a)-[r:Type]->(b) DELETE r
	// This pattern is used heavily in benchmarks and relationship tests.
	// Groups: 1=var1, 2=label1, 3=var2, 4=label2, 5=withVar1, 6=withVar2, 7=limit, 8=relVar, 9=relType, 10=delVar
	matchCreateDeleteRelPattern = regexp.MustCompile(
		`(?i)^\s*MATCH\s*\((\w+):(\w+)\)\s*,\s*\((\w+):(\w+)\)` +
			`\s*WITH\s+(\w+)\s*,\s*(\w+)` +
			`\s+LIMIT\s+(\d+)` +
			`\s+CREATE\s*\(\w+\)-\[(\w+):(\w+)\]->\(\w+\)` +
			`\s+DELETE\s+(\w+)\s*$`)

	// MATCH (p1:Person {id: 1}), (p2:Person {id: 2}) CREATE (p1)-[r:KNOWS]->(p2) DELETE r
	// LDBC-style pattern: property match without WITH/LIMIT
	// Groups: 1=var1, 2=label1, 3=prop1, 4=val1, 5=var2, 6=label2, 7=prop2, 8=val2, 9=relVar, 10=relType, 11=delVar
	matchPropCreateDeleteRelPattern = regexp.MustCompile(
		`(?i)^\s*MATCH\s*\((\w+):(\w+)\s*\{\s*(\w+)\s*:\s*(\d+)\s*\}\s*\)` +
			`\s*,\s*\((\w+):(\w+)\s*\{\s*(\w+)\s*:\s*(\d+)\s*\}\s*\)` +
			`\s+CREATE\s*\(\w+\)-\[(\w+):(\w+)\]->\(\w+\)` +
			`\s+DELETE\s+(\w+)\s*$`)
)

// =============================================================================
// Query Analysis Patterns (EXPLAIN/PROFILE)
// =============================================================================

var (
	// Variable-length path pattern: [*], [*1..3], [*..5]
	varLengthPathPattern = regexp.MustCompile(`\[\*\d*\.?\.\d*\]`)

	// Label extraction pattern: (n:Label) or (:Label)
	labelExtractPattern = regexp.MustCompile(`\(\w*:(\w+)`)

	// Aggregation function detection
	aggregationPattern = regexp.MustCompile(`(?i)(COUNT|SUM|AVG|MIN|MAX|COLLECT)\s*\(`)

	// LIMIT/SKIP extraction
	limitPattern = regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	skipPattern  = regexp.MustCompile(`(?i)SKIP\s+(\d+)`)

	// CALL procedure extraction
	callProcedurePattern = regexp.MustCompile(`(?i)CALL\s+([\w.]+)`)
)

// =============================================================================
// Parameter Substitution Pattern
// =============================================================================

var (
	// Parameter reference: $name or $name123
	parameterPattern = regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_]*)`)
)

// =============================================================================
// Aggregation Function Property Patterns
// =============================================================================

var (
	// Aggregation with property: COUNT(n.prop), SUM(n.prop), etc.
	countPropPattern   = regexp.MustCompile(`(?i)COUNT\((\w+)\.(\w+)\)`)
	sumPropPattern     = regexp.MustCompile(`(?i)SUM\((\w+)\.(\w+)\)`)
	avgPropPattern     = regexp.MustCompile(`(?i)AVG\((\w+)\.(\w+)\)`)
	minPropPattern     = regexp.MustCompile(`(?i)MIN\((\w+)\.(\w+)\)`)
	maxPropPattern     = regexp.MustCompile(`(?i)MAX\((\w+)\.(\w+)\)`)
	collectPropPattern = regexp.MustCompile(`(?i)COLLECT\((\w+)(?:\.(\w+))?\)`)

	// Aliases for compatibility (used in match.go)
	countPropRe = countPropPattern
	sumRe       = sumPropPattern
	avgRe       = avgPropPattern
	minRe       = minPropPattern
	maxRe       = maxPropPattern
	collectRe   = collectPropPattern

	// DISTINCT variants
	countDistinctPropPattern   = regexp.MustCompile(`(?i)COUNT\(\s*DISTINCT\s+(\w+)\.(\w+)\)`)
	collectDistinctPropPattern = regexp.MustCompile(`(?i)COLLECT\(\s*DISTINCT\s+(\w+)\.(\w+)\)`)

	// Aliases for DISTINCT variants
	countDistinctPropRe   = countDistinctPropPattern
	collectDistinctPropRe = collectDistinctPropPattern
)

// =============================================================================
// Path and Traversal Patterns
// =============================================================================

var (
	// Variable-length relationship: *1..3, *, *..5, *2..
	// Group 1: min hops (optional digits)
	// Group 2: max hops (optional digits after ..)
	varLengthRelPattern = regexp.MustCompile(`\*(\d*)(?:\.\.(\d*))?`)

	// Path pattern: (node1)-[rel]->(node2)
	pathPatternRe = regexp.MustCompile(`\(([^)]*)\)\s*(<?\-\[[^\]]*\]\-?>?)\s*\(([^)]*)\)`)

	// ID extraction: id(n)
	idFunctionPattern = regexp.MustCompile(`id\((\w+)\)`)
)

// =============================================================================
// Shortest Path Patterns
// =============================================================================

var (
	// shortestPath/allShortestPaths function call
	shortestPathFuncPattern = regexp.MustCompile(`(?i)(allShortestPaths|shortestPath)\s*\(\s*(\([^)]+\)\s*-\[.*?\]->\s*\([^)]+\)|\([^)]+\)\s*<-\[.*?\]-\s*\([^)]+\)|\([^)]+\)\s*-\[.*?\]-\s*\([^)]+\))\s*\)`)

	// Variable assignment: varName = shortestPath(...)
	shortestPathVarPattern = regexp.MustCompile(`(?i)(\w+)\s*=\s*(?:all)?shortestPath`)

	// MATCH clause extraction for shortest path
	matchClausePattern = regexp.MustCompile(`(?is)MATCH\s+(.+?)\s+MATCH`)
)

// =============================================================================
// CREATE Statement Patterns (pre-compiled for hot path optimization)
// =============================================================================

var (
	// MATCH keyword split pattern
	matchKeywordPattern = regexp.MustCompile(`(?i)\bMATCH\s+`)

	// CREATE keyword split pattern
	createKeywordPattern = regexp.MustCompile(`(?i)\bCREATE\s+`)

	// Forward relationship pattern: (a)-[r:TYPE {props}]->(b)
	// Captures full node content including inline definitions like (c:Label {props})
	relForwardPattern = regexp.MustCompile(`\(([^)]+)\)\s*-\[(\w*)(?::(\w+))?(?:\s*(\{[^}]*\}))?\]\s*->\s*\(([^)]+)\)`)

	// Reverse relationship pattern: (a)<-[r:TYPE {props}]-(b)
	// Captures full node content including inline definitions like (c:Label {props})
	relReversePattern = regexp.MustCompile(`\(([^)]+)\)\s*<-\[(\w*)(?::(\w+))?(?:\s*(\{[^}]*\}))?\]\s*-\s*\(([^)]+)\)`)

	// Node variable extraction: (varName:Label) or (varName)
	nodeVarPattern = regexp.MustCompile(`\((\w+)(?::\w+)?`)

	// Node variable with label: (varName:Label) or (varName)
	nodeVarLabelPattern = regexp.MustCompile(`\((\w+)(?::(\w+))?`)

	// Relationship variable extraction: [r:TYPE] or [r] or [:TYPE]
	relVarTypePattern = regexp.MustCompile(`\[(\w+)(?::(\w+))?\]`)
)

// =============================================================================
// Cache/Label Extraction Patterns
// =============================================================================

var (
	// Label extraction: :Label, (:Label), :Label:AnotherLabel
	// Used in cache.go for query analysis
	labelRegex = regexp.MustCompile(`:([A-Z][A-Za-z0-9_]*)`)
)

// =============================================================================
// Index Hint Patterns (USING INDEX, USING SCAN, USING JOIN)
// =============================================================================

var (
	// USING INDEX n:Label(property) or USING INDEX n:Label(prop1, prop2)
	indexHintPattern = regexp.MustCompile(`(?i)USING\s+INDEX\s+(\w+):(\w+)\s*\(\s*([^)]+)\s*\)`)

	// USING SCAN n:Label
	scanHintPattern = regexp.MustCompile(`(?i)USING\s+SCAN\s+(\w+):(\w+)`)

	// USING JOIN ON n
	joinHintPattern = regexp.MustCompile(`(?i)USING\s+JOIN\s+ON\s+(\w+)`)
)

// =============================================================================
// Dynamic Regex Cache (for user-provided patterns like =~ comparison)
// =============================================================================

// regexCache provides thread-safe caching of compiled regex patterns.
// Used for dynamic patterns like Cypher's =~ regex comparison operator.
var regexCache sync.Map // map[string]*regexp.Regexp

// GetCachedRegex returns a compiled regex for the pattern, using cache if available.
// This avoids re-compiling the same pattern on every =~ comparison.
func GetCachedRegex(pattern string) (*regexp.Regexp, error) {
	// Check cache first
	if cached, ok := regexCache.Load(pattern); ok {
		return cached.(*regexp.Regexp), nil
	}

	// Compile and cache
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// Store in cache (another goroutine might have stored it already, that's fine)
	regexCache.Store(pattern, re)
	return re, nil
}
