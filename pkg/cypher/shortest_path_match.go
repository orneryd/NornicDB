package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// shortestPathMatch is a MATCH or OPTIONAL MATCH clause whose pattern is
// [p =] shortestPath(...) or allShortestPaths(...). It runs as a pipeline
// step like any other MATCH (#863): for each input row the endpoint node
// patterns are matched with the WHERE conjuncts that don't mention the path,
// the search runs once per start/end pair, and the path is bound, so ORDER
// BY, LIMIT, aggregation and later clauses apply to its rows as Neo4j's do.
type shortestPathMatch struct {
	findAll bool
	// others are the clause's other comma-separated patterns, matched first
	// one at a time; othersVariables are the variables they bind.
	others          []string
	othersVariables []string
	// startPattern and endPattern are the endpoint node patterns as written,
	// with a generated variable for an anonymous endpoint.
	startPattern, endPattern   string
	startVariable, endVariable string
	// pathVariable is the path's variable, or a generated one.
	pathVariable string
	traversal    *TraversalMatch
	// endpointWhere selects the endpoints; pathWhere mentions the path or its
	// relationship variable and constrains the search (Neo4j returns the
	// shortest path that satisfies it).
	endpointWhere, pathWhere string
	// selector is the pattern's path selector (ANY, SHORTEST), nil for
	// shortestPath and allShortestPaths (path_selector_match.go).
	selector *pathSelector
	// postWhere filters a selector's selected paths: the clause's WHERE
	// conjuncts that read the path, its relationships or its inner nodes.
	postWhere string
	// pattern is a selected pattern the search can't run (more than one
	// relationship, or none): it is matched whole, with pathWhere.
	pattern string
}

// parseShortestPathMatch reads the body of a MATCH clause (after MATCH or
// OPTIONAL MATCH). ok is false when no comma-separated part of its pattern is
// a shortestPath or allShortestPaths call.
func (e *StorageExecutor) parseShortestPathMatch(ctx context.Context, body string) (*shortestPathMatch, bool, error) {
	if indexASCIIFold(body, "shortestpath") < 0 {
		return nil, false, nil
	}
	pattern := strings.TrimSpace(body)
	where := ""
	if index := topLevelKeywordIndex(pattern, "WHERE"); index >= 0 {
		where = strings.TrimSpace(pattern[index+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:index])
	}
	var call, pathVariable string
	var others []string
	for _, part := range splitTopLevelComma(pattern) {
		part = strings.TrimSpace(part)
		variable := extractPathAssignmentVariable(part)
		candidate := part
		if variable != "" {
			candidate = strings.TrimSpace(part[strings.Index(part, "=")+1:])
		}
		if _, _, funcIdx, ok := extractShortestPathCall(candidate); call == "" && ok && funcIdx == 0 &&
			findMatchingParen(candidate, strings.IndexByte(candidate, '(')) == len(candidate)-1 {
			call, pathVariable = candidate, variable
			continue
		}
		others = append(others, part)
	}
	if call == "" {
		return nil, false, nil
	}
	funcName, inner, _, _ := extractShortestPathCall(call)
	if selector, predicate, terms, ok := splitPathSelectorTerm(where); ok {
		return e.parseSelectedPathMatch(ctx, inner, pathVariable, others, selector, predicate, terms)
	}
	startPattern, endPattern, ok := shortestPathEndpointPatterns(inner)
	traversal := e.parseTraversalPattern(ctx, inner)
	if !ok || traversal == nil || traversal.IsChained {
		return nil, true, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "ShortestPathSingleRelationship",
			localization.CypherMatchingShortestPathSingleRelationship(funcName))
	}
	if err := shortestPathPatternError(funcName, inner, traversal); err != nil {
		return nil, true, err
	}
	m := &shortestPathMatch{
		others:        others,
		findAll:       strings.EqualFold(funcName, "allShortestPaths"),
		startPattern:  startPattern,
		endPattern:    endPattern,
		startVariable: traversal.StartNode.variable,
		endVariable:   traversal.EndNode.variable,
		pathVariable:  pathVariable,
		traversal:     traversal,
	}
	if m.startVariable == "" {
		m.startVariable = generatedVariablePrefix + "sp_start"
		m.startPattern = "(" + m.startVariable + m.startPattern[1:]
		traversal.StartNode.variable = m.startVariable
	}
	if m.endVariable == "" {
		m.endVariable = generatedVariablePrefix + "sp_end"
		m.endPattern = "(" + m.endVariable + m.endPattern[1:]
		traversal.EndNode.variable = m.endVariable
	}
	if m.pathVariable == "" {
		m.pathVariable = generatedVariablePrefix + "sp_path"
	}
	traversal.PathVariable = m.pathVariable
	var endpointTerms, pathTerms []string
	for _, term := range splitTopLevelAndConjuncts(where) {
		if term = strings.TrimSpace(term); term == "" {
			continue
		}
		relationship := traversal.Relationship.Variable
		if referencesVariable(term, m.pathVariable) || (relationship != "" && referencesVariable(term, relationship)) {
			pathTerms = append(pathTerms, term)
		} else {
			endpointTerms = append(endpointTerms, term)
		}
	}
	m.endpointWhere = strings.Join(endpointTerms, " AND ")
	m.pathWhere = strings.Join(pathTerms, " AND ")
	m.setOthersVariables()
	return m, true, nil
}

// setOthersVariables lists the variables the clause's other patterns bind.
func (m *shortestPathMatch) setOthersVariables() {
	for _, part := range m.others {
		m.othersVariables = append(m.othersVariables, extractNodeVariables(part)...)
		m.othersVariables = append(m.othersVariables, extractRelationshipVariables(part)...)
		if variable := extractPathAssignmentVariable(part); variable != "" {
			m.othersVariables = append(m.othersVariables, variable)
		}
	}
}

// shortestPathPatternError is Neo4j's SyntaxError for a shortestPath or
// allShortestPaths pattern it doesn't support: a minimum length above 1, or
// relationship properties (pattern is the call's argument).
func shortestPathPatternError(funcName, pattern string, traversal *TraversalMatch) error {
	if traversal.Relationship.MinHops > 1 {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidShortestPathMinimalLength",
			localization.CypherMatchingShortestPathMinimalLength(funcName))
	}
	if len(traversal.Relationship.Properties) > 0 {
		properties := ""
		if open := strings.IndexByte(pattern, '['); open >= 0 {
			if close := findMatchingBracket(pattern, open); close > open {
				relationship := pattern[open:close]
				if brace := strings.IndexByte(relationship, '{'); brace >= 0 {
					properties = strings.TrimSpace(relationship[brace:])
				}
			}
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "ShortestPathRelationshipProperties",
			localization.CypherMatchingShortestPathRelationshipProperties(funcName, properties))
	}
	return nil
}

// shortestPathExpressionError is Neo4j's SyntaxError for a shortestPath or
// allShortestPaths call in an expression (query[wordStart:wordEnd] is the
// function name) with an anonymous endpoint: outside a MATCH pattern both
// endpoints must be bound variables. The statement rewrite checks every
// expression with it, so the statement fails whether or not a row reaches
// the call.
func shortestPathExpressionError(query string, wordStart, wordEnd, end int) error {
	name := query[wordStart:wordEnd]
	if !equalFoldASCII(name, "shortestPath") && !equalFoldASCII(name, "allShortestPaths") {
		return nil
	}
	open := skipASCIISpaces(query, wordEnd, end)
	if open >= end || query[open] != '(' {
		return nil
	}
	close := findMatchingParen(query[:end], open)
	if close < 0 {
		return nil
	}
	function := "shortestPath"
	if equalFoldASCII(name, "allShortestPaths") {
		function = "allShortestPaths"
	}
	startPattern, endPattern, ok := shortestPathEndpointPatterns(query[open+1 : close])
	if !ok {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "ShortestPathSingleRelationship",
			localization.CypherMatchingShortestPathSingleRelationship(function))
	}
	for _, endpoint := range []string{startPattern, endPattern} {
		if _, _, named := scanSymbolicName(endpoint, skipASCIISpaces(endpoint, 1, len(endpoint))); !named {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "ShortestPathUnboundNodes",
				localization.CypherMatchingShortestPathUnboundNodes(function))
		}
	}
	return nil
}

// shortestPathEndpointPatterns splits (start)-[rel]-(end) into its two node
// patterns; ok is false for any other shape (no relationship, or more than
// one).
func shortestPathEndpointPatterns(pattern string) (string, string, bool) {
	pattern = strings.TrimSpace(pattern)
	startEnd := -1
	if strings.HasPrefix(pattern, "(") {
		startEnd = findMatchingParen(pattern, 0)
	}
	rest := startEnd + 1
	if open := strings.IndexByte(pattern[rest:], '['); startEnd >= 0 && open >= 0 {
		if close := findMatchingBracket(pattern, rest+open); close > 0 {
			rest = close + 1
		}
	}
	endOpen := rest + strings.IndexByte(pattern[rest:], '(')
	if startEnd < 0 || endOpen < rest || findMatchingParen(pattern, endOpen) != len(pattern)-1 {
		return "", "", false
	}
	return pattern[:startEnd+1], pattern[endOpen:], true
}

// pipelineApplyShortestPathMatch runs a shortestPath MATCH for each row. An
// OPTIONAL MATCH keeps a row that finds no path, with the clause's new
// variables null.
func (e *StorageExecutor) pipelineApplyShortestPathMatch(ctx context.Context, rows []pipelineRow, m *shortestPathMatch, optional bool) ([]pipelineRow, error) {
	if m.selector != nil {
		return e.pipelineApplySelectedPathMatch(ctx, rows, m, optional)
	}
	out := make([]pipelineRow, 0, len(rows))
	for _, row := range rows {
		bases, err := e.shortestPathBases(ctx, m, row)
		if err != nil {
			return nil, err
		}
		var pairs []pipelineRow
		for _, base := range bases {
			found, err := e.shortestPathEndpointPairs(ctx, m, base)
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, found...)
		}
		found := 0
		for _, pair := range pairs {
			start, _ := pair[m.startVariable].(*storage.Node)
			end, _ := pair[m.endVariable].(*storage.Node)
			paths, err := e.shortestPathsBetween(ctx, m, start, end, pair)
			if err != nil {
				return nil, err
			}
			for _, path := range paths {
				out = append(out, e.bindShortestPath(ctx, m, pair, path))
				found++
			}
		}
		if optional && found == 0 {
			out = append(out, m.unmatchedRow(row))
		}
	}
	return out, nil
}

// shortestPathBases returns the rows the clause's other patterns bind for
// one input row, matched one at a time.
func (e *StorageExecutor) shortestPathBases(ctx context.Context, m *shortestPathMatch, row pipelineRow) ([]pipelineRow, error) {
	bases := []pipelineRow{row}
	for _, part := range m.others {
		var next []pipelineRow
		for _, base := range bases {
			matched, err := e.pipelineMatchRows(ctx, base, "MATCH "+part)
			if err != nil {
				return nil, err
			}
			next = append(next, matched...)
		}
		bases = next
	}
	return bases, nil
}

// bindShortestPath returns pair with the path, and the relationship
// variable when there is one, bound.
func (e *StorageExecutor) bindShortestPath(ctx context.Context, m *shortestPathMatch, pair pipelineRow, path PathResult) pipelineRow {
	bound := make(pipelineRow, len(pair)+2)
	for name, value := range pair {
		bound[name] = value
	}
	bound[m.pathVariable] = e.pathToMap(path)
	if relationship := m.traversal.Relationship.Variable; relationship != "" {
		bound[relationship] = e.evaluateExpressionWithPathContext(ctx, relationship, e.buildPathContext(path, m.traversal))
	}
	return bound
}

// unmatchedRow is an OPTIONAL MATCH's row for an input row that matches
// nothing: the clause's new variables are null.
func (m *shortestPathMatch) unmatchedRow(row pipelineRow) pipelineRow {
	names := append([]string{m.startVariable, m.endVariable, m.pathVariable}, m.othersVariables...)
	if m.traversal != nil {
		names = append(names, m.traversal.Relationship.Variable)
	}
	if m.pattern != "" {
		names = append(names, extractNodeVariables(m.pattern)...)
		names = append(names, extractRelationshipVariables(m.pattern)...)
	}
	bound := make(pipelineRow, len(row)+len(names))
	for name, value := range row {
		bound[name] = value
	}
	for _, name := range names {
		if _, exists := bound[name]; !exists && name != "" {
			bound[name] = nil
		}
	}
	return bound
}

// shortestPathEndpointPairs returns the rows that bind the clause's start
// and end for one input row. An endpoint written as a bare variable the row
// binds to a node is taken from the row; the others are matched, with the
// WHERE conjuncts that don't mention the path.
func (e *StorageExecutor) shortestPathEndpointPairs(ctx context.Context, m *shortestPathMatch, row pipelineRow) ([]pipelineRow, error) {
	var patterns []string
	for _, endpoint := range [][2]string{{m.startPattern, m.startVariable}, {m.endPattern, m.endVariable}} {
		node, bound := row[endpoint[1]].(*storage.Node)
		if bound && node != nil && strings.TrimSpace(endpoint[0][1:len(endpoint[0])-1]) == endpoint[1] {
			continue
		}
		patterns = append(patterns, endpoint[0])
	}
	if len(patterns) == 0 {
		if m.endpointWhere != "" && !e.evaluateWithWhereCondition(ctx, m.endpointWhere, map[string]interface{}(row)) {
			return nil, getExpressionFailure(ctx)
		}
		return []pipelineRow{row}, nil
	}
	clause := "MATCH " + strings.Join(patterns, ", ")
	if m.endpointWhere != "" {
		clause += " WHERE " + m.endpointWhere
	}
	return e.pipelineMatchRows(ctx, row, clause)
}

// pipelineMatchRows runs a MATCH clause for one row, for a step that
// matches part of its own pattern; a clause shape the pipeline can't run is
// an invalid pattern.
func (e *StorageExecutor) pipelineMatchRows(ctx context.Context, row pipelineRow, clause string) ([]pipelineRow, error) {
	rows, handled, err := e.pipelineApplyMatch(ctx, []pipelineRow{row}, clause)
	if err == nil && !handled {
		err = localizedError(localization.CypherMatchingPathPatternInvalid(strings.TrimSpace(clause[len("MATCH"):])), nil)
	}
	return rows, err
}

// shortestPathsBetween returns the shortest path (all of them for
// allShortestPaths) from start to end that satisfies the clause's path
// predicates, given the row's bindings. As in Neo4j:
//   - a start that is the end is an error unless the minimum length is 0,
//     which makes the path of length 0;
//   - without path predicates the breadth-first search answers directly;
//     with them, paths are tried by increasing length until one satisfies
//     them.
func (e *StorageExecutor) shortestPathsBetween(ctx context.Context, m *shortestPathMatch, start, end *storage.Node, row pipelineRow) ([]PathResult, error) {
	relationship := m.traversal.Relationship
	if start.ID == end.ID {
		if relationship.MinHops > 0 {
			return nil, localizedStatusError("Neo.DatabaseError.Statement.ExecutionFailed", "ShortestPathCommonEndNodes",
				localization.CypherMatchingShortestPathCommonEndNodes())
		}
		zero := []PathResult{{Nodes: []*storage.Node{start}}}
		return e.filterPathsByWhere(ctx, zero, m.traversal, m.pathWhere, map[string]interface{}(row)), nil
	}
	if m.pathWhere == "" {
		if m.findAll {
			return e.allShortestPaths(ctx, start, end, relationship.Types, relationship.Direction, relationship.MaxHops)
		}
		path, err := e.shortestPath(ctx, start, end, relationship.Types, relationship.Direction, relationship.MaxHops)
		if err != nil || path == nil {
			return nil, err
		}
		return []PathResult{*path}, nil
	}
	for length := max(relationship.MinHops, 1); length <= relationship.MaxHops; length++ {
		search := e.newTraversalContext(ctx, start, &relationship)
		search.minHops, search.maxHops = length, length
		search.endNodeID = end.ID
		paths := e.findPaths(search, start, []*storage.Node{start}, nil, 0, nil)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if paths = e.filterPathsByWhere(ctx, paths, m.traversal, m.pathWhere, map[string]interface{}(row)); len(paths) > 0 {
			if !m.findAll {
				paths = paths[:1]
			}
			return paths, nil
		}
		if search.deepest < length {
			// No path from start is this long, so none is longer.
			return nil, nil
		}
	}
	return nil, nil
}
