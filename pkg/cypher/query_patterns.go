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
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	return detectQueryPattern(ctx, inputBraceView(ctx, query, cyphertext.AcceptedSource))
}

func detectQueryPattern(ctx context.Context, view braceView) PatternInfo {
	ctx = withBraceView(ctx, view)
	query := view.text()
	info := PatternInfo{
		Pattern: PatternGeneric,
	}

	upperQuery := upperASCII(query)

	// Don't optimize queries with WITH clause - they have complex aggregation
	// semantics that the optimized executors don't handle (aliases, collect, etc.)
	// Use word boundary check to avoid matching "STARTS WITH" or "ENDS WITH"
	if containsKeywordOutsideStrings(view, "WITH") {
		return info
	}

	// Extract LIMIT first (affects multiple patterns)
	if _, limit, ok := decimalAfterKeyword(view, "LIMIT"); ok {
		info.Limit = limit
	}

	// Check for mutual relationship pattern: (a)-[:T]->(b)-[:T]->(a)
	if info.Pattern == PatternGeneric && detectMutualRelationship(ctx, view, &info) {
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
			matchClause := extractMatchClause(view)
			if countRelationshipPatterns(matchClause) == 1 {
				if detectIncomingCountAgg(ctx, view, &info) {
					return info
				}
				if detectOutgoingCountAgg(ctx, view, &info) {
					return info
				}
			}
		}
	}

	// Check for edge property aggregation
	if info.Pattern == PatternGeneric && detectEdgePropertyAgg(ctx, view, &info) {
		return info
	}

	// Check for large result set (LIMIT > 100)
	if info.Limit > 100 && strings.Contains(upperQuery, "MATCH") {
		info.Pattern = PatternLargeResultSet
		return info
	}

	return info
}

func extractMatchClause(view braceView) braceView {
	query := view.text()
	matchIdx := findKeywordIndex(view, "MATCH")
	if matchIdx < 0 {
		return braceView{}
	}
	end := len(query)
	for _, keyword := range []string{"WHERE", "RETURN", "WITH", "ORDER", "LIMIT", "SKIP"} {
		if idx := findKeywordIndex(view.window(matchIdx, len(query)), keyword); idx > 0 {
			if matchIdx+idx < end {
				end = matchIdx + idx
			}
		}
	}
	return view.window(matchIdx, end)
}

func countRelationshipPatterns(view braceView) int {
	s := view.text()
	count := 0
	for from := 0; from < len(s); {
		_, close := readRelationshipBracket(view, from, requireClosedRelationship)
		if close < 0 {
			break
		}
		count++
		from = close + 1
	}
	return count
}

// detectMutualRelationship checks for (a)-[:T]->(b)-[:T]->(a) pattern
func detectMutualRelationship(ctx context.Context, view braceView, info *PatternInfo) bool {
	matchClause := extractMatchClause(view)
	matchClause = matchClause.trim()
	if matchClause.empty() {
		return false
	}
	if strings.HasPrefix(upperASCII(matchClause.text()), "MATCH") {
		matchClause = matchClause.window(len("MATCH"), len(matchClause.text())).trim()
	}

	exec := &StorageExecutor{}
	match := exec.parseTraversalPatternStructure(ctx, matchClause)
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
	if first.Relationship.Types[0] != second.Relationship.Types[0] {
		return false
	}

	info.Pattern = PatternMutualRelationship
	info.StartVar = first.FromNode.variable
	info.EndVar = first.ToNode.variable
	info.RelType = first.Relationship.Types[0]
	return true
}

// detectIncomingCountAgg checks for (x)<-[:T]-(y) ... count(y) pattern
func detectIncomingCountAgg(ctx context.Context, view braceView, info *PatternInfo) bool {
	query := view.text()
	startVar, relVar, relType, endVar, ok := parseDirectionalCountPattern(ctx, extractMatchClause(view), true)
	if !ok {
		return false
	}

	// Check if count() is on the end variable (the one doing the incoming), and
	// only optimize the narrow "RETURN x.name, count(y)" shape that the optimized executor implements.
	upperQuery := upperASCII(query)
	countPattern := "COUNT(" + upperASCII(endVar)
	countStarPattern := "COUNT(*)"

	if (strings.Contains(upperQuery, countPattern) || strings.Contains(upperQuery, countStarPattern)) &&
		isReturnNameCountShape(view, startVar, endVar) {
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
func detectOutgoingCountAgg(ctx context.Context, view braceView, info *PatternInfo) bool {
	query := view.text()
	startVar, relVar, relType, endVar, ok := parseDirectionalCountPattern(ctx, extractMatchClause(view), false)
	if !ok {
		return false
	}

	// Check if count() is on the end variable, and only optimize the narrow
	// "RETURN x.name, count(y)" shape that the optimized executor implements.
	upperQuery := upperASCII(query)
	countPattern := "COUNT(" + upperASCII(endVar)

	if strings.Contains(upperQuery, countPattern) && isReturnNameCountShape(view, startVar, endVar) {
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

func isReturnNameCountShape(view braceView, startVar string, endVar string) bool {
	parts := returnShapeItems(view)
	if len(parts) != 2 {
		return false
	}

	left := trimReturnItemAlias(parts[0])
	right := trimReturnItemAlias(parts[1])

	// Require "startVar.name" (the optimized executor currently uses the "name" property).
	if !strings.EqualFold(left, startVar+".name") {
		return false
	}

	// Require COUNT(endVar) or COUNT(*).
	rightUpper := upperASCII(strings.ReplaceAll(right, " ", ""))
	wantCountVar := "COUNT(" + upperASCII(endVar) + ")"
	return rightUpper == wantCountVar || rightUpper == "COUNT(*)"
}

func parseDirectionalCountPattern(ctx context.Context, matchClause braceView, incoming bool) (string, string, string, string, bool) {
	matchClause = matchClause.trim()
	if matchClause.empty() {
		return "", "", "", "", false
	}
	if strings.HasPrefix(upperASCII(matchClause.text()), "MATCH") {
		matchClause = matchClause.window(len("MATCH"), len(matchClause.text())).trim()
	}
	if matchClause.empty() {
		return "", "", "", "", false
	}

	exec := &StorageExecutor{}
	match := exec.parseTraversalPatternStructure(ctx, matchClause)
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

// splitEdgeAggregationCall retains the existing first-open/last-close rule.
func splitEdgeAggregationCall(u string) (function string, inner string, ok bool) {
	open := strings.IndexByte(u, '(')
	close := strings.LastIndexByte(u, ')')
	if open <= 0 || close <= open {
		return "", "", false
	}
	return u[:open], u[open+1 : close], true
}

// edgeAggregationProperty owns aggregate argument validation; empty-property policy stays at callers.
func edgeAggregationProperty(function string, inner string, relVar string) (property string, valid bool) {
	switch function {
	case "COUNT":
		if inner == "*" || strings.EqualFold(inner, relVar) {
			return "", true
		}
		return "", false
	case "SUM", "AVG", "MIN", "MAX":
		wantPrefix := upperASCII(relVar) + "."
		if !strings.HasPrefix(inner, wantPrefix) {
			return "", false
		}
		return inner[len(wantPrefix):], true
	default:
		return "", false
	}
}

// detectEdgePropertyAgg checks for avg(r.prop), sum(r.prop) patterns
func detectEdgePropertyAgg(ctx context.Context, view braceView, info *PatternInfo) bool {
	query := view.text()
	// First, find relationship variable in MATCH
	matchClause := extractMatchClause(view)
	relVar := extractRelationshipVariable(matchClause.text())
	if relVar == "" {
		return false
	}

	exec := &StorageExecutor{}
	returnIdx := findKeywordIndex(view, "RETURN")
	if returnIdx < 0 {
		return false
	}
	returnItems := exec.parseReturnItems(ctx, view.window(returnIdx+len("RETURN"), len(query)).trim())
	if len(returnItems) < 2 {
		return false
	}

	for _, item := range returnItems[1:] {
		expr := readProjectionItemSource(ctx, item.view, projectionAdmitted).expression
		u := upperASCII(strings.ReplaceAll(strings.TrimSpace(expr), " ", ""))
		aggFunc, inner, ok := splitEdgeAggregationCall(u)
		if !ok {
			continue
		}

		propName, valid := edgeAggregationProperty(aggFunc, inner, relVar)
		if !valid {
			return false
		}
		if aggFunc == "COUNT" {
			continue
		}
		if propName == "" {
			return false
		}
		if info.AggProperty == "" {
			info.AggProperty = lowerASCII(propName)
		} else if !strings.EqualFold(info.AggProperty, propName) {
			return false
		}
		info.AggFunctions = append(info.AggFunctions, lowerASCII(aggFunc))
	}

	if info.AggProperty == "" {
		return false
	}
	if !isReturnEdgePropertyAggNameShape(view, relVar, info.AggProperty) {
		return false
	}
	info.Pattern = PatternEdgePropertyAgg
	info.RelVar = relVar

	return true
}

func isReturnEdgePropertyAggNameShape(view braceView, relVar string, propName string) bool {
	items := returnShapeItems(view)
	if len(items) < 2 {
		return false
	}

	first := trimReturnItemAlias(items[0])
	// Require "<var>.name" in first return position.
	dot := strings.LastIndex(first, ".")
	if dot < 0 || !strings.EqualFold(first[dot+1:], "name") {
		return false
	}

	wantAggProp := upperASCII(propName)

	// Remaining items must be aggregations over relVar.prop (optionally plus count(r)).
	for i := 1; i < len(items); i++ {
		item := trimReturnItemAlias(items[i])

		u := upperASCII(strings.ReplaceAll(item, " ", ""))
		switch {
		case strings.HasPrefix(u, "COUNT(") && strings.HasSuffix(u, ")"):
			aggFunc, inner, ok := splitEdgeAggregationCall(u)
			if !ok {
				return false
			}
			// Allow COUNT(r) and COUNT(*).
			if _, valid := edgeAggregationProperty(aggFunc, inner, relVar); !valid {
				return false
			}
			continue

		case strings.HasPrefix(u, "SUM(") || strings.HasPrefix(u, "AVG(") || strings.HasPrefix(u, "MIN(") || strings.HasPrefix(u, "MAX("):
			aggFunc, inner, ok := splitEdgeAggregationCall(u)
			if !ok || inner == "" {
				return false
			}
			prop, valid := edgeAggregationProperty(aggFunc, inner, relVar)
			if !valid {
				return false
			}
			if !strings.EqualFold(prop, wantAggProp) {
				return false
			}
			continue
		default:
			return false
		}
	}

	return true
}

// functionCallParenAt reports whether the '(' at text[open] opens a function
// call's arguments (size(kinds), n.f(x)) rather than a node pattern: it
// directly follows a name that isn't a clause keyword (MATCH(n), WHERE(...)
// open patterns and expressions).
func functionCallParenAt(text string, open int) bool {
	end := open
	start := end
	for start > 0 && cyphertext.NamePartAt(text, start-1) {
		start--
	}
	if start == end {
		return false
	}
	switch upperASCII(text[start:end]) {
	case "MATCH", "CREATE", "MERGE", "WHERE", "AND", "OR", "XOR", "NOT", "WITH", "RETURN", "UNWIND", "IN", "DELETE", "SET", "REMOVE", "FOREACH", "YIELD":
		return false
	}
	return true
}

// extractNodeVariables extracts node variable names from a MATCH pattern,
// plain or backtick-quoted (`n n`, as written). A parenthesis that opens a
// function call's arguments or an expression starting with a subquery
// (EXISTS { … } AND …) is not a node.
func extractNodeVariables(view braceView) []string {
	matchClause := view.text()
	var vars []string
	for i := 0; i < len(matchClause); i++ {
		if isCypherStringQuoteByte(matchClause[i]) {
			i = skipQuotedSemanticText(matchClause, i) - 1
			continue
		}
		if cyphertext.IsOpenBrace(matchClause[i]) {
			if end, found := view.close(i); found {
				i = end
				continue
			}
		}
		if dynamicLabelStartsAt(matchClause, i) {
			// $(e): a dynamic label's expression, not a node.
			if end := cyphertext.FindMatchingDelimiter(matchClause, i+strings.IndexByte(matchClause[i:], '('), '(', ')', cyphertext.OpeningDelimiter); end >= 0 {
				i = end
				continue
			}
		}
		if matchClause[i] != '(' || functionCallParenAt(matchClause, i) {
			continue
		}
		j := i + 1
		for j < len(matchClause) && cyphertext.IsWhitespaceAt(matchClause, j) {
			j++
		}
		if _, _, subquery := subqueryExpressionKeywordAt(view, j); subquery {
			continue
		}
		name, next, ok := scanSymbolicName(matchClause, j)
		if !ok {
			continue
		}
		if next < len(matchClause) {
			for next < len(matchClause) && cyphertext.IsWhitespaceAt(matchClause, next) {
				next++
			}
			if next < len(matchClause) && matchClause[next] != ':' && matchClause[next] != ')' && !cyphertext.IsOpenBrace(matchClause[next]) && matchClause[next] != '$' {
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
		inner, _, ok := extractBracketSection(pattern[i:])
		if !ok {
			continue
		}
		if colonIdx := strings.Index(inner, ":"); colonIdx >= 0 {
			typePart := strings.TrimSpace(inner[colonIdx+1:])
			if propIdx := firstLiveBrace(typePart); propIdx >= 0 {
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
		inner, _, ok := extractBracketSection(pattern[i:])
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
		name, _, ok := parseIdentifierToken(inner)
		if ok {
			return name
		}
		return ""
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
