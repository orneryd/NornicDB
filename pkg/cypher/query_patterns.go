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
	"context"
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

// DetectQueryPattern analyzes a Cypher query and returns pattern info
func DetectQueryPattern(ctx context.Context, query string) PatternInfo {
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
	if limit, ok := ExtractLimit(query); ok {
		info.Limit = limit
	}

	// Check for mutual relationship pattern: (a)-[:T]->(b)-[:T]->(a)
	if info.Pattern == PatternGeneric && detectMutualRelationship(ctx, query, &info) {
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
				if detectIncomingCountAgg(ctx, query, &info) {
					return info
				}
				if detectOutgoingCountAgg(ctx, query, &info) {
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
	matchIdx := findKeywordIndex(query, "MATCH")
	if matchIdx < 0 {
		return ""
	}
	end := len(query)
	for _, keyword := range []string{"WHERE", "RETURN", "WITH", "ORDER", "LIMIT", "SKIP"} {
		if idx := findKeywordIndex(query[matchIdx:], keyword); idx > 0 {
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
func detectMutualRelationship(ctx context.Context, query string, info *PatternInfo) bool {
	matchClause := extractMatchClause(query)
	matchClause = strings.TrimSpace(matchClause)
	if matchClause == "" {
		return false
	}
	if strings.HasPrefix(strings.ToUpper(matchClause), "MATCH") {
		matchClause = strings.TrimSpace(matchClause[len("MATCH"):])
	}

	exec := &StorageExecutor{}
	match := exec.parseTraversalPattern(ctx, matchClause)
	if match == nil || !match.IsChained || len(match.Segments) != 2 {
		return false
	}
	first := match.Segments[0]
	second := match.Segments[1]
	if first.Relationship.Direction != "outgoing" || second.Relationship.Direction != "outgoing" {
		return false
	}
	if first.FromNode.variable == "" || first.ToNode.variable == "" || second.ToNode.variable == "" {
		return false
	}
	if first.FromNode.variable != second.ToNode.variable {
		return false
	}
	if first.ToNode.variable != second.FromNode.variable {
		return false
	}
	if len(first.Relationship.Types) == 0 || len(second.Relationship.Types) == 0 {
		return false
	}
	if !strings.EqualFold(first.Relationship.Types[0], second.Relationship.Types[0]) {
		return false
	}

	info.Pattern = PatternMutualRelationship
	info.StartVar = first.FromNode.variable
	info.EndVar = first.ToNode.variable
	info.RelType = first.Relationship.Types[0]
	return true
}

// detectIncomingCountAgg checks for (x)<-[:T]-(y) ... count(y) pattern
func detectIncomingCountAgg(ctx context.Context, query string, info *PatternInfo) bool {
	startVar, relVar, relType, endVar, ok := parseDirectionalCountPattern(ctx, extractMatchClause(query), true)
	if !ok {
		return false
	}

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
func detectOutgoingCountAgg(ctx context.Context, query string, info *PatternInfo) bool {
	startVar, relVar, relType, endVar, ok := parseDirectionalCountPattern(ctx, extractMatchClause(query), false)
	if !ok {
		return false
	}

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
	returnIdx := findKeywordIndex(query, "RETURN")
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

func parseDirectionalCountPattern(ctx context.Context, matchClause string, incoming bool) (string, string, string, string, bool) {
	matchClause = strings.TrimSpace(matchClause)
	if matchClause == "" {
		return "", "", "", "", false
	}
	if strings.HasPrefix(strings.ToUpper(matchClause), "MATCH") {
		matchClause = strings.TrimSpace(matchClause[len("MATCH"):])
	}
	if matchClause == "" {
		return "", "", "", "", false
	}

	exec := &StorageExecutor{}
	match := exec.parseTraversalPattern(ctx, matchClause)
	if match == nil || match.IsChained {
		return "", "", "", "", false
	}
	if incoming {
		if match.Relationship.Direction != "incoming" {
			return "", "", "", "", false
		}
	} else if match.Relationship.Direction != "outgoing" {
		return "", "", "", "", false
	}
	if match.StartNode.variable == "" || match.EndNode.variable == "" {
		return "", "", "", "", false
	}
	relType := ""
	if len(match.Relationship.Types) > 0 {
		relType = match.Relationship.Types[0]
	}
	return match.StartNode.variable, match.Relationship.Variable, relType, match.EndNode.variable, true
}

// detectEdgePropertyAgg checks for avg(r.prop), sum(r.prop) patterns
func detectEdgePropertyAgg(query string, info *PatternInfo) bool {
	// First, find relationship variable in MATCH
	matchClause := extractMatchClause(query)
	relVar := extractRelationshipVariable(matchClause)
	if relVar == "" {
		return false
	}

	exec := &StorageExecutor{}
	returnIdx := findKeywordIndex(query, "RETURN")
	if returnIdx < 0 {
		return false
	}
	returnItems := exec.parseReturnItems(strings.TrimSpace(query[returnIdx+len("RETURN"):]))
	if len(returnItems) < 2 {
		return false
	}

	for _, item := range returnItems[1:] {
		expr, _ := parseProjectionExprAlias(item.expr)
		u := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(expr), " ", ""))
		open := strings.IndexByte(u, '(')
		close := strings.LastIndexByte(u, ')')
		if open <= 0 || close <= open {
			continue
		}
		aggFunc := strings.ToLower(u[:open])
		inner := u[open+1 : close]

		if aggFunc == "count" {
			if inner == "*" || strings.EqualFold(inner, strings.ToUpper(relVar)) {
				continue
			}
			return false
		}
		if aggFunc != "sum" && aggFunc != "avg" && aggFunc != "min" && aggFunc != "max" {
			return false
		}
		wantPrefix := strings.ToUpper(relVar) + "."
		if !strings.HasPrefix(inner, wantPrefix) {
			return false
		}
		propName := inner[len(wantPrefix):]
		if propName == "" {
			return false
		}
		if info.AggProperty == "" {
			info.AggProperty = strings.ToLower(propName)
		} else if !strings.EqualFold(info.AggProperty, propName) {
			return false
		}
		info.AggFunctions = append(info.AggFunctions, aggFunc)
	}

	if info.AggProperty == "" {
		return false
	}
	if !isReturnEdgePropertyAggNameShape(query, relVar, info.AggProperty) {
		return false
	}
	info.Pattern = PatternEdgePropertyAgg
	info.RelVar = relVar

	return true
}

func isReturnEdgePropertyAggNameShape(query string, relVar string, propName string) bool {
	returnIdx := findKeywordIndex(query, "RETURN")
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
	for i := 0; i < len(matchClause); i++ {
		if matchClause[i] != '(' {
			continue
		}
		j := i + 1
		for j < len(matchClause) && isWhitespace(matchClause[j]) {
			j++
		}
		name, next, ok := scanIdentifierToken(matchClause, j)
		if !ok {
			continue
		}
		if next < len(matchClause) {
			for next < len(matchClause) && isWhitespace(matchClause[next]) {
				next++
			}
			if next < len(matchClause) && matchClause[next] != ':' && matchClause[next] != ')' && matchClause[next] != '{' {
				continue
			}
		}
		vars = append(vars, name)
	}
	return vars
}

// extractRelationshipType extracts the relationship type from a pattern
func extractRelationshipType(pattern string) string {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '[' {
			continue
		}
		inner, _, ok := extractBracketSectionQueryPattern(pattern[i:])
		if !ok {
			continue
		}
		if colonIdx := strings.Index(inner, ":"); colonIdx >= 0 {
			typePart := strings.TrimSpace(inner[colonIdx+1:])
			if propIdx := strings.Index(typePart, "{"); propIdx >= 0 {
				typePart = strings.TrimSpace(typePart[:propIdx])
			}
			if pipeIdx := strings.Index(typePart, "|"); pipeIdx >= 0 {
				typePart = strings.TrimSpace(typePart[:pipeIdx])
			}
			if typePart != "" {
				return typePart
			}
		}
	}
	return ""
}

func extractRelationshipVariable(pattern string) string {
	for i := 0; i < len(pattern); i++ {
		if pattern[i] != '[' {
			continue
		}
		inner, _, ok := extractBracketSectionQueryPattern(pattern[i:])
		if !ok {
			continue
		}
		inner = strings.TrimSpace(inner)
		if inner == "" {
			return ""
		}
		if colonIdx := strings.Index(inner, ":"); colonIdx >= 0 {
			name := strings.TrimSpace(inner[:colonIdx])
			if name != "" {
				return name
			}
			return ""
		}
		if inner[0] == '*' {
			return ""
		}
		name, _, ok := parseIdentifierTokenQueryPattern(inner)
		if ok {
			return name
		}
		return ""
	}
	return ""
}

func scanIdentifierToken(s string, start int) (string, int, bool) {
	if start < 0 || start >= len(s) {
		return "", start, false
	}
	if !isIdentifierStart(s[start]) {
		return "", start, false
	}
	i := start + 1
	for i < len(s) && isIdentifierPart(s[i]) {
		i++
	}
	return s[start:i], i, true
}

func isIdentifierStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentifierPart(c byte) bool {
	return isIdentifierStart(c) || (c >= '0' && c <= '9')
}

func parseIdentifierTokenQueryPattern(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	if s == "" || !isIdentifierStart(s[0]) {
		return "", "", false
	}
	i := 1
	for i < len(s) && isIdentifierPart(s[i]) {
		i++
	}
	return s[:i], s[i:], true
}

func extractBracketSectionQueryPattern(s string) (inside string, rest string, ok bool) {
	if !strings.HasPrefix(s, "[") {
		return "", "", false
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return s[1:i], s[i+1:], true
			}
		}
	}
	return "", "", false
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
