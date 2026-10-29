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
	info := PatternInfo{
		Pattern: PatternGeneric,
	}

	upperQuery := upperASCII(query)

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
	if info.Pattern == PatternGeneric && detectEdgePropertyAgg(ctx, query, &info) {
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

// countRelationshipPatterns counts the relationship brackets of s, found by
// the shared bracket finder.
func countRelationshipPatterns(s string) int {
	count := 0
	for open, close := readRelationshipBracket(s, 0, requireClosedRelationship); open >= 0; open, close = readRelationshipBracket(s, close+1, requireClosedRelationship) {
		count++
	}
	return count
}

// optimizedTraversalPattern admits only declarations a scalar type scan can
// represent. It reads the owner's complete declaration before the traversal
// adapter projects it; that adapter deliberately does not retain predicates or
// invalid-type diagnostics. Alternatives, dynamic values, ranges and property
// predicates belong to the general executor.
func optimizedTraversalPattern(ctx context.Context, clause string) *TraversalMatch {
	pattern := strings.TrimSpace(clause)
	if end, ok := keywordSpanAt(pattern, 0, "MATCH"); ok {
		pattern = strings.TrimSpace(pattern[end:])
	}
	for rest := pattern; ; {
		hop, ok := readRelationshipHopPrefix(rest)
		if !ok || hop.incoming == hop.outgoing {
			return nil
		}
		d := hop.declaration
		if d.properties != "" || d.predicate != "" || d.hasLength || d.lengthInvalid || d.invalidType != "" || d.emptyType {
			return nil
		}
		if d.hasColon {
			if _, ok := d.singleLiteralType(); !ok {
				return nil
			}
		}
		if queryGapEnd(rest, hop.end) == len(rest) {
			break
		}
		if hop.toStart <= 0 {
			return nil
		}
		rest = rest[hop.toStart:]
	}
	exec := &StorageExecutor{}
	return exec.parseTraversalPatternStructure(ctx, pattern)
}

// Public PatternInfo fields are hints, not admission or retained syntax facts.
// Every physical entry re-derives the complete selection from the source.
func admitRelationshipOptimization(ctx context.Context, query string, kind QueryPattern) (PatternInfo, bool) {
	admitted := DetectQueryPattern(ctx, query)
	return admitted, admitted.Pattern == kind && kind != PatternGeneric
}

// detectMutualRelationship checks for (a)-[:T]->(b)-[:T]->(a) pattern
func detectMutualRelationship(ctx context.Context, query string, info *PatternInfo) bool {
	matchClause := extractMatchClause(query)
	matchClause = strings.TrimSpace(matchClause)
	if matchClause == "" {
		return false
	}
	if strings.HasPrefix(upperASCII(matchClause), "MATCH") {
		matchClause = strings.TrimSpace(matchClause[len("MATCH"):])
	}

	match := optimizedTraversalPattern(ctx, matchClause)
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
	// This physical executor looks up one literal type. Alternatives and
	// dynamic tokens must retain the generic executor's complete semantics.
	if first.Relationship.Types.len() != 1 || second.Relationship.Types.len() != 1 ||
		first.Relationship.Types.hasDynamic() || second.Relationship.Types.hasDynamic() ||
		first.Relationship.VariableLength || second.Relationship.VariableLength ||
		len(first.Relationship.Properties) != 0 || len(second.Relationship.Properties) != 0 {
		return false
	}
	if first.Relationship.Types.at(0).storageName() != second.Relationship.Types.at(0).storageName() {
		return false
	}

	info.Pattern = PatternMutualRelationship
	info.StartVar = first.FromNode.variable
	info.EndVar = first.ToNode.variable
	info.RelType = first.Relationship.Types.at(0).storageName()
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
	if isReturnNameCountShape(ctx, query, startVar, endVar) {
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
	if isReturnNameCountShape(ctx, query, startVar, endVar) {
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

func isReturnNameCountShape(ctx context.Context, query string, startVar, endVar string) bool {
	items := (&StorageExecutor{}).extractReturnItemsFromQuery(ctx, query)
	if len(items) != 2 {
		return false
	}
	variable, property, ok := parseVarPropertyRef(items[0].expr)
	if !ok || variable != startVar || property != "name" {
		return false
	}
	function, argument, ok := parseFunctionCallWS(items[1].expr)
	if !ok || !strings.EqualFold(function, "count") {
		return false
	}
	if strings.TrimSpace(argument) == "*" {
		return true
	}
	name, ok := isOneSymbolicName(argument)
	return ok && name == endVar
}

func parseDirectionalCountPattern(ctx context.Context, matchClause string, incoming bool) (string, string, string, string, bool) {
	matchClause = strings.TrimSpace(matchClause)
	if matchClause == "" {
		return "", "", "", "", false
	}
	if strings.HasPrefix(upperASCII(matchClause), "MATCH") {
		matchClause = strings.TrimSpace(matchClause[len("MATCH"):])
	}
	if matchClause == "" {
		return "", "", "", "", false
	}

	match := optimizedTraversalPattern(ctx, matchClause)
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
	if match.Relationship.Types.len() > 0 {
		relType = match.Relationship.Types.at(0).storageName()
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
func detectEdgePropertyAgg(ctx context.Context, query string, info *PatternInfo) bool {
	// First, find relationship variable in MATCH
	matchClause := extractMatchClause(query)
	match := optimizedTraversalPattern(ctx, matchClause)
	if match == nil || match.IsChained || match.Relationship.Direction != "outgoing" {
		return false
	}
	relVar := match.Relationship.Variable
	if relVar == "" || !isReturnGroupEndpoint(ctx, query, match.EndNode.variable) {
		return false
	}
	if match.Relationship.Types.len() == 1 {
		info.RelType = match.Relationship.Types.at(0).storageName()
	}

	items := (&StorageExecutor{}).extractReturnItemsFromQuery(ctx, query)
	if len(items) < 2 {
		return false
	}
	for _, item := range items[1:] {
		function, argument, ok := parseFunctionCallWS(item.expr)
		if !ok {
			return false
		}
		function = lowerASCII(function)
		if function == "count" {
			name, named := isOneSymbolicName(argument)
			if strings.TrimSpace(argument) != "*" && (!named || name != relVar) {
				return false
			}
		} else {
			if function != "sum" && function != "avg" && function != "min" && function != "max" {
				return false
			}
			variable, property, ok := parseVarPropertyRef(argument)
			if !ok || variable != relVar || property == "" {
				return false
			}
			if info.AggProperty != "" && info.AggProperty != property {
				return false
			}
			info.AggProperty = property
		}
		info.AggFunctions = append(info.AggFunctions, function)
	}
	if info.AggProperty == "" {
		return false
	}
	info.Pattern = PatternEdgePropertyAgg
	info.RelVar = relVar

	return true
}

func isReturnGroupEndpoint(ctx context.Context, query, variable string) bool {
	items := (&StorageExecutor{}).extractReturnItemsFromQuery(ctx, query)
	if len(items) == 0 || variable == "" {
		return false
	}
	actual, property, ok := parseVarPropertyRef(items[0].expr)
	return ok && actual == variable && property == "name"
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
// (EXISTS { … } AND …) is not a node, and neither is anything in a
// relationship's head (its type `x(y)`, a quote in `D]'Z`, its map) or a
// quoted name; a relationship's own WHERE predicate is read as written.
func extractNodeVariables(matchClause string) []string {
	var vars []string
	for i := 0; i < len(matchClause); i++ {
		switch c := matchClause[i]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			i = skipCypherQuotedText(matchClause, i, c) - 1
			continue
		case '[':
			if previous := trimRightIndex(matchClause, 0, i); previous > 0 && matchClause[previous-1] == '-' {
				if end := findMatchingBracket(matchClause, i); end >= 0 {
					// Resume at the relationship's own WHERE, if any.
					if declaration := relationshipDeclarationHead(matchClause[i+1 : end]); declaration.whereOffset >= 0 {
						i = i + 1 + declaration.whereOffset + len("WHERE") - 1
					} else {
						i = end
					}
					continue
				}
			}
		}
		if matchClause[i] == '{' {
			if end := cyphertext.FindMatchingDelimiter(matchClause, i, '{', '}', cyphertext.OpeningDelimiter); end >= 0 {
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
		if _, _, subquery := subqueryExpressionKeywordAt(matchClause, j); subquery {
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
			if next < len(matchClause) && matchClause[next] != ':' && matchClause[next] != ')' && matchClause[next] != '{' && matchClause[next] != '$' {
				continue
			}
		}
		vars = append(vars, name)
	}
	return vars
}

func extractRelationshipVariable(pattern string) string {
	if open, close := readRelationshipBracket(pattern, 0, requireClosedRelationship); open >= 0 {
		return relationshipDeclarationOf(pattern[open+1 : close]).variable
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
