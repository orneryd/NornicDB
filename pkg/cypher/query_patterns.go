// Package cypher provides query pattern detection for optimization routing.
//
// This file identifies query patterns that can be executed more efficiently
// than the generic traversal algorithm. Pattern detection happens BEFORE
// execution, allowing the executor to route to specialized implementations.
//
// Supported patterns:
//   - Mutual Relationship: (a)-[:TYPE]->(b)-[:TYPE]->(a) - cycle back to start
//   - Incoming Count Aggregation: MATCH (x)<-[:TYPE]-(y) RETURN x, count(y)
//   - Edge Property Aggregation: RETURN avg(r.prop), count(r) GROUP BY node
//   - Large Result Set: Any traversal with LIMIT > 100
package cypher

import (
	"regexp"
	"strconv"
	"strings"
)

// QueryPattern identifies optimizable query structures
type QueryPattern int

const (
	// PatternGeneric is the default - use standard execution
	PatternGeneric QueryPattern = iota

	// PatternMutualRelationship detects (a)-[:T]->(b)-[:T]->(a) cycles
	// Optimized via single-pass edge set intersection
	PatternMutualRelationship

	// PatternIncomingCountAgg detects MATCH (x)<-[:T]-(y) RETURN x, count(y)
	// Optimized via single-pass edge counting
	PatternIncomingCountAgg

	// PatternOutgoingCountAgg detects MATCH (x)-[:T]->(y) RETURN x, count(y)
	// Optimized via single-pass edge counting
	PatternOutgoingCountAgg

	// PatternEdgePropertyAgg detects avg/sum/count on edge properties
	// Optimized via single-pass accumulation
	PatternEdgePropertyAgg

	// PatternLargeResultSet detects queries returning many rows (LIMIT > 100)
	// Optimized via batch node lookups and pre-allocation
	PatternLargeResultSet
)

// String returns a human-readable pattern name
func (p QueryPattern) String() string {
	switch p {
	case PatternMutualRelationship:
		return "MutualRelationship"
	case PatternIncomingCountAgg:
		return "IncomingCountAgg"
	case PatternOutgoingCountAgg:
		return "OutgoingCountAgg"
	case PatternEdgePropertyAgg:
		return "EdgePropertyAgg"
	case PatternLargeResultSet:
		return "LargeResultSet"
	default:
		return "Generic"
	}
}

// PatternInfo contains details about a detected pattern
type PatternInfo struct {
	Pattern      QueryPattern
	RelType      string   // Relationship type for the pattern (e.g., "FOLLOWS")
	StartVar     string   // Start node variable (e.g., "a")
	EndVar       string   // End node variable (e.g., "b")
	RelVar       string   // Relationship variable (e.g., "r")
	AggFunctions []string // Aggregation functions used (e.g., ["count", "avg"])
	AggProperty  string   // Property being aggregated (e.g., "rating")
	Limit        int      // LIMIT value if present
	GroupByVars  []string // Variables in implicit GROUP BY
}

// Pre-compiled patterns for detection
var (
	// Matches: (a)-[:TYPE]->(b)-[:TYPE]->(a) or (a)<-[:TYPE]-(b)<-[:TYPE]-(a)
	// Captures: startVar, relType, midVar, relType2, endVar
	mutualRelPattern = regexp.MustCompile(
		`(?i)\(\s*(\w+)(?::\w+)?\s*\)\s*` + // (a) or (a:Label)
			`(<)?-\[(?:\w+)?(?::(\w+))?\]->(?)?\s*` + // -[:TYPE]-> or <-[:TYPE]-
			`\(\s*(\w+)(?::\w+)?\s*\)\s*` + // (b) or (b:Label)
			`(<)?-\[(?:\w+)?(?::(\w+))?\]->(?)?\s*` + // -[:TYPE]-> or <-[:TYPE]-
			`\(\s*(\w+)(?::\w+)?\s*\)`, // (a) - same as start
	)

	// Matches incoming relationship with count: (x)<-[:TYPE]-(y) ... count(y)
	incomingCountPattern = regexp.MustCompile(
		`(?i)\(\s*(\w+)(?::\w+)?\s*\)\s*` + // (x)
			`<-\[(\w+)?(?::(\w+))?\]-\s*` + // <-[r:TYPE]-
			`\(\s*(\w+)(?::\w+)?\s*\)`, // (y)
	)

	// Matches outgoing relationship with count: (x)-[:TYPE]->(y) ... count(y)
	outgoingCountPattern = regexp.MustCompile(
		`(?i)\(\s*(\w+)(?::\w+)?\s*\)\s*` + // (x)
			`-\[(\w+)?(?::(\w+))?\]->\s*` + // -[r:TYPE]->
			`\(\s*(\w+)(?::\w+)?\s*\)`, // (y)
	)

	// Matches aggregation on relationship variable: avg(r.prop), sum(r.prop), count(r)
	edgeAggPattern = regexp.MustCompile(
		`(?i)(count|sum|avg|min|max)\s*\(\s*(\w+)(?:\.(\w+))?\s*\)`,
	)

	// Matches LIMIT clause for pattern detection
	patternLimitRegex = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)

	// Matches relationship variable in MATCH
	relVarPattern = regexp.MustCompile(`-\[(\w+)(?::\w+)?\]-`)
)

// DetectQueryPattern analyzes a Cypher query and returns pattern info
func DetectQueryPattern(query string) PatternInfo {
	info := PatternInfo{
		Pattern: PatternGeneric,
	}

	upperQuery := strings.ToUpper(query)

	// Don't optimize queries with WITH clause - they have complex aggregation
	// semantics that the optimized executors don't handle (aliases, collect, etc.)
	// Use word boundary check to avoid matching "STARTS WITH" or "ENDS WITH"
	if containsKeywordOutsideStrings(query, "WITH") {
		return info
	}

	// Extract LIMIT first (affects multiple patterns)
	if matches := patternLimitRegex.FindStringSubmatch(query); matches != nil {
		limit, _ := strconv.Atoi(matches[1])
		info.Limit = limit
	}

	// Check for mutual relationship pattern: (a)-[:T]->(b)-[:T]->(a)
	if info.Pattern == PatternGeneric && detectMutualRelationship(query, &info) {
		return info
	}

	// Check for incoming count aggregation
	if info.Pattern == PatternGeneric && strings.Contains(upperQuery, "COUNT(") {
		// Guardrails: the (in|out) count optimizers only support a very narrow query shape.
		// Do NOT route queries that:
		//   - have other aggregations (AVG/SUM/MIN/MAX/COLLECT)
		//   - have multiple relationship segments (chained traversals)
		//
		// Those queries should use the generic executor (and any traversal fast paths),
		// otherwise we can mis-route multi-hop queries and/or return incorrect columns.
		if !strings.Contains(upperQuery, "SUM(") &&
			!strings.Contains(upperQuery, "AVG(") &&
			!strings.Contains(upperQuery, "MIN(") &&
			!strings.Contains(upperQuery, "MAX(") &&
			!strings.Contains(upperQuery, "COLLECT(") {
			matchClause := extractMatchClause(query)
			if countRelationshipPatterns(matchClause) == 1 {
				if detectIncomingCountAgg(query, &info) {
					return info
				}
				if detectOutgoingCountAgg(query, &info) {
					return info
				}
			}
		}
	}

	// Check for edge property aggregation
	if info.Pattern == PatternGeneric && detectEdgePropertyAgg(query, &info) {
		return info
	}

	// Check for large result set (LIMIT > 100)
	if info.Limit > 100 && strings.Contains(upperQuery, "MATCH") {
		info.Pattern = PatternLargeResultSet
		return info
	}

	return info
}

func extractMatchClause(query string) string {
	upper := strings.ToUpper(query)
	matchIdx := strings.Index(upper, "MATCH")
	if matchIdx < 0 {
		return ""
	}
	end := len(query)
	for _, keyword := range []string{"WHERE", "RETURN", "WITH", "ORDER", "LIMIT", "SKIP"} {
		if idx := strings.Index(upper[matchIdx:], keyword); idx > 0 {
			if matchIdx+idx < end {
				end = matchIdx + idx
			}
		}
	}
	return query[matchIdx:end]
}

func countRelationshipPatterns(s string) int {
	inQuote := false
	quoteChar := byte(0)
	count := 0

	for i := 0; i < len(s); i++ {
		c := s[i]

		if (c == '\'' || c == '"') && (i == 0 || s[i-1] != '\\') {
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		}
		if inQuote {
			continue
		}

		if c != '[' {
			continue
		}

		// Relationship patterns are introduced by a preceding '-' (possibly with whitespace).
		j := i - 1
		for j >= 0 && isWhitespace(s[j]) {
			j--
		}
		if j >= 0 && s[j] == '-' {
			count++
		}
	}

	return count
}

// detectMutualRelationship checks for (a)-[:T]->(b)-[:T]->(a) pattern
func detectMutualRelationship(query string, info *PatternInfo) bool {
	// Simplified detection: look for pattern where start var == end var
	// Pattern: MATCH (a)-[:TYPE]->(b)-[:TYPE]->(a)

	upperQuery := strings.ToUpper(query)
	if !strings.Contains(upperQuery, "MATCH") {
		return false
	}

	// Extract MATCH clause
	matchIdx := strings.Index(upperQuery, "MATCH")
	if matchIdx < 0 {
		return false
	}

	// Find end of MATCH pattern (WHERE, RETURN, WITH, etc.)
	matchEnd := len(query)
	for _, keyword := range []string{"WHERE", "RETURN", "WITH", "ORDER", "LIMIT"} {
		idx := strings.Index(upperQuery[matchIdx:], keyword)
		if idx > 0 && matchIdx+idx < matchEnd {
			matchEnd = matchIdx + idx
		}
	}

	matchClause := query[matchIdx:matchEnd]

	// Look for two relationship patterns with same type going back to start
	// Simple heuristic: count node variables and check if first == last
	nodeVars := extractNodeVariables(matchClause)
	if len(nodeVars) >= 3 && nodeVars[0] == nodeVars[len(nodeVars)-1] {
		// Extract relationship type
		relType := extractRelationshipType(matchClause)
		if relType != "" {
			info.Pattern = PatternMutualRelationship
			info.StartVar = nodeVars[0]
			info.EndVar = nodeVars[1]
			info.RelType = relType
			return true
		}
	}

	return false
}

// detectIncomingCountAgg checks for (x)<-[:T]-(y) ... count(y) pattern
func detectIncomingCountAgg(query string, info *PatternInfo) bool {
	matches := incomingCountPattern.FindStringSubmatch(query)
	if matches == nil {
		return false
	}

	startVar := matches[1] // x
	relVar := matches[2]   // r (optional)
	relType := matches[3]  // TYPE
	endVar := matches[4]   // y

	// Check if count() is on the end variable (the one doing the incoming), and
	// only optimize the narrow "RETURN x.name, count(y)" shape that the optimized executor implements.
	upperQuery := strings.ToUpper(query)
	countPattern := "COUNT(" + strings.ToUpper(endVar)
	countStarPattern := "COUNT(*)"

	if (strings.Contains(upperQuery, countPattern) || strings.Contains(upperQuery, countStarPattern)) &&
		isReturnNameCountShape(query, startVar, endVar) {
		info.Pattern = PatternIncomingCountAgg
		info.StartVar = startVar
		info.EndVar = endVar
		info.RelVar = relVar
		info.RelType = relType
		info.AggFunctions = []string{"count"}
		return true
	}

	return false
}

// detectOutgoingCountAgg checks for (x)-[:T]->(y) ... count(y) pattern
func detectOutgoingCountAgg(query string, info *PatternInfo) bool {
	matches := outgoingCountPattern.FindStringSubmatch(query)
	if matches == nil {
		return false
	}

	startVar := matches[1] // x
	relVar := matches[2]   // r (optional)
	relType := matches[3]  // TYPE
	endVar := matches[4]   // y

	// Check if count() is on the end variable, and only optimize the narrow
	// "RETURN x.name, count(y)" shape that the optimized executor implements.
	upperQuery := strings.ToUpper(query)
	countPattern := "COUNT(" + strings.ToUpper(endVar)

	if strings.Contains(upperQuery, countPattern) && isReturnNameCountShape(query, startVar, endVar) {
		info.Pattern = PatternOutgoingCountAgg
		info.StartVar = startVar
		info.EndVar = endVar
		info.RelVar = relVar
		info.RelType = relType
		info.AggFunctions = []string{"count"}
		return true
	}

	return false
}

func isReturnNameCountShape(query string, startVar string, endVar string) bool {
	upperQuery := strings.ToUpper(query)
	returnIdx := strings.Index(upperQuery, "RETURN")
	if returnIdx < 0 {
		return false
	}

	returnPart := strings.TrimSpace(query[returnIdx+6:])

	// Strip ORDER BY / SKIP / LIMIT.
	end := len(returnPart)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnPart, keyword); idx >= 0 && idx < end {
			end = idx
		}
	}
	returnPart = strings.TrimSpace(returnPart[:end])
	if returnPart == "" {
		return false
	}

	parts := splitOutsideParens(returnPart, ',')
	if len(parts) != 2 {
		return false
	}

	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])

	// Handle "AS" aliases.
	if asIdx := strings.Index(strings.ToUpper(left), " AS "); asIdx > 0 {
		left = strings.TrimSpace(left[:asIdx])
	}
	if asIdx := strings.Index(strings.ToUpper(right), " AS "); asIdx > 0 {
		right = strings.TrimSpace(right[:asIdx])
	}

	// Require "startVar.name" (the optimized executor currently uses the "name" property).
	if !strings.EqualFold(left, startVar+".name") {
		return false
	}

	// Require COUNT(endVar) or COUNT(*).
	rightUpper := strings.ToUpper(strings.ReplaceAll(right, " ", ""))
	wantCountVar := "COUNT(" + strings.ToUpper(endVar) + ")"
	return rightUpper == wantCountVar || rightUpper == "COUNT(*)"
}

// detectEdgePropertyAgg checks for avg(r.prop), sum(r.prop) patterns
func detectEdgePropertyAgg(query string, info *PatternInfo) bool {
	// First, find relationship variable in MATCH
	relMatches := relVarPattern.FindStringSubmatch(query)
	if relMatches == nil {
		return false
	}
	relVar := relMatches[1]

	// Look for aggregation on this relationship variable
	aggMatches := edgeAggPattern.FindAllStringSubmatch(query, -1)
	if aggMatches == nil {
		return false
	}

	for _, match := range aggMatches {
		aggFunc := strings.ToLower(match[1]) // count, sum, avg, etc.
		varName := match[2]                  // variable name
		propName := match[3]                 // property name (optional for count)

		// Check if aggregation is on the relationship variable
		if strings.EqualFold(varName, relVar) {
			// Only use EdgePropertyAgg optimization for actual property aggregations
			// Simple count(r) without a property should use the regular path
			if propName == "" {
				return false // Let regular executeMatch handle simple count(r)
			}
			// Guardrail: only optimize the narrow "RETURN n.name, <agg>(r.prop)..." shape that
			// executeEdgePropertyAggOptimized implements (it hardcodes the node "name" property).
			if !isReturnEdgePropertyAggNameShape(query, relVar, propName) {
				return false
			}
			info.Pattern = PatternEdgePropertyAgg
			info.RelVar = relVar
			info.AggFunctions = append(info.AggFunctions, aggFunc)
			info.AggProperty = propName
			return true
		}
	}

	return false
}

func isReturnEdgePropertyAggNameShape(query string, relVar string, propName string) bool {
	upperQuery := strings.ToUpper(query)
	returnIdx := strings.Index(upperQuery, "RETURN")
	if returnIdx < 0 {
		return false
	}
	returnPart := strings.TrimSpace(query[returnIdx+6:])

	// Strip ORDER BY / SKIP / LIMIT.
	end := len(returnPart)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnPart, keyword); idx >= 0 && idx < end {
			end = idx
		}
	}
	returnPart = strings.TrimSpace(returnPart[:end])
	if returnPart == "" {
		return false
	}

	items := splitOutsideParens(returnPart, ',')
	if len(items) < 2 {
		return false
	}

	first := strings.TrimSpace(items[0])
	if asIdx := strings.Index(strings.ToUpper(first), " AS "); asIdx > 0 {
		first = strings.TrimSpace(first[:asIdx])
	}
	// Require "<var>.name" in first return position.
	dot := strings.LastIndex(first, ".")
	if dot < 0 || !strings.EqualFold(first[dot+1:], "name") {
		return false
	}

	wantAggPrefix := strings.ToUpper(relVar) + "."
	wantAggProp := strings.ToUpper(propName)

	// Remaining items must be aggregations over relVar.prop (optionally plus count(r)).
	for i := 1; i < len(items); i++ {
		item := strings.TrimSpace(items[i])
		if asIdx := strings.Index(strings.ToUpper(item), " AS "); asIdx > 0 {
			item = strings.TrimSpace(item[:asIdx])
		}

		u := strings.ToUpper(strings.ReplaceAll(item, " ", ""))
		switch {
		case strings.HasPrefix(u, "COUNT(") && strings.HasSuffix(u, ")"):
			// Allow COUNT(r) and COUNT(*).
			inner := strings.TrimSuffix(strings.TrimPrefix(u, "COUNT("), ")")
			if inner == "*" || strings.EqualFold(inner, relVar) {
				continue
			}
			return false

		case strings.HasPrefix(u, "SUM(") || strings.HasPrefix(u, "AVG(") || strings.HasPrefix(u, "MIN(") || strings.HasPrefix(u, "MAX("):
			open := strings.IndexByte(u, '(')
			close := strings.LastIndexByte(u, ')')
			if open < 0 || close < 0 || close <= open+1 {
				return false
			}
			inner := u[open+1 : close]
			if !strings.HasPrefix(inner, wantAggPrefix) {
				return false
			}
			if !strings.EqualFold(inner[len(wantAggPrefix):], wantAggProp) {
				return false
			}
			continue
		default:
			return false
		}
	}

	return true
}

// extractNodeVariables extracts node variable names from a MATCH pattern
func extractNodeVariables(matchClause string) []string {
	var vars []string
	// Simple extraction: find (varName) or (varName:Label) patterns
	nodePattern := regexp.MustCompile(`\(\s*(\w+)(?::\w+)?`)
	matches := nodePattern.FindAllStringSubmatch(matchClause, -1)
	for _, m := range matches {
		if m[1] != "" {
			vars = append(vars, m[1])
		}
	}
	return vars
}

// extractRelationshipType extracts the relationship type from a pattern
func extractRelationshipType(pattern string) string {
	// Match -[:TYPE]- or -[r:TYPE]-
	relTypePattern := regexp.MustCompile(`-\[(?:\w+)?:(\w+)\]-`)
	matches := relTypePattern.FindStringSubmatch(pattern)
	if matches != nil && len(matches) > 1 {
		return matches[1]
	}
	return ""
}

// IsOptimizable returns true if the pattern can be optimized
func (p PatternInfo) IsOptimizable() bool {
	return p.Pattern != PatternGeneric
}

// NeedsRelationshipTypeScan returns true if the optimization needs all edges of a type
func (p PatternInfo) NeedsRelationshipTypeScan() bool {
	switch p.Pattern {
	case PatternMutualRelationship, PatternIncomingCountAgg,
		PatternOutgoingCountAgg, PatternEdgePropertyAgg:
		return true
	default:
		return false
	}
}
