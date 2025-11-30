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
		if detectIncomingCountAgg(query, &info) {
			return info
		}
		if detectOutgoingCountAgg(query, &info) {
			return info
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

	// Check if count() is on the end variable (the one doing the incoming)
	upperQuery := strings.ToUpper(query)
	countPattern := "COUNT(" + strings.ToUpper(endVar)
	countStarPattern := "COUNT(*)"

	if strings.Contains(upperQuery, countPattern) || strings.Contains(upperQuery, countStarPattern) {
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

	// Check if count() is on the end variable
	upperQuery := strings.ToUpper(query)
	countPattern := "COUNT(" + strings.ToUpper(endVar)

	if strings.Contains(upperQuery, countPattern) {
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
			info.Pattern = PatternEdgePropertyAgg
			info.RelVar = relVar
			info.AggFunctions = append(info.AggFunctions, aggFunc)
			if propName != "" {
				info.AggProperty = propName
			}
			return true
		}
	}

	return false
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
