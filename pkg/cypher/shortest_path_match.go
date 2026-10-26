package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// shortestPathMatch is a MATCH or OPTIONAL MATCH clause whose pattern is
// [p =] shortestPath(...) or allShortestPaths(...). It runs as a pipeline
// step like any other MATCH (#863): for each input row the endpoint node
// patterns are matched with the WHERE conjuncts that don't mention the path,
// the selected path search (path_selector_search.go) runs once per
// start/end pair, and the path is bound, so ORDER BY, LIMIT, aggregation and
// later clauses apply to its rows as Neo4j's do.
type shortestPathMatch struct {
	findAll bool
	// others are the clause's other comma-separated patterns, matched first
	// one at a time; othersVariables are the variables they bind.
	others          []braceView
	othersVariables []string
	// startPattern and endPattern are the endpoint node patterns as written,
	// with a generated variable for an anonymous endpoint.
	startPattern, endPattern   braceView
	startVariable, endVariable string
	// pathVariable is the path's variable, or a generated one.
	pathVariable string
	traversal    *TraversalMatch
	// endpointWhere selects the endpoints; pathWhere mentions the path or its
	// relationship variable and constrains the search (Neo4j returns the
	// shortest path that satisfies it): it is the plan's predicate.
	endpointWhere, pathWhere braceView
	// selector is the pattern's path selector (ANY, SHORTEST), nil for
	// shortestPath and allShortestPaths (path_selector_match.go).
	selector *pathSelector
	// postWhere filters a selector's selected paths: the clause's WHERE
	// conjuncts that read the path, its relationships or its inner nodes.
	postWhere braceView
	// pattern is a selected pattern; plan is the compiled search of a
	// selected pattern or of a shortestPath pattern (path_selector_search.go).
	pattern braceView
	plan    *selectedPathPlan
}

// parseShortestPathMatch reads the body of a MATCH clause (after MATCH or
// OPTIONAL MATCH). ok is false when no comma-separated part of its pattern is
// a shortestPath or allShortestPaths call.
func (e *StorageExecutor) parseShortestPathMatch(ctx context.Context, body braceView) (*shortestPathMatch, bool, error) {
	if indexASCIIFold(body.text(), "shortestpath") < 0 {
		return nil, false, nil
	}
	pattern := body.trim()
	where := pattern.window(0, 0)
	if index := topLevelKeywordIndex(pattern, "WHERE"); index >= 0 {
		where = pattern.window(index+len("WHERE"), len(pattern.text())).trim()
		pattern = pattern.window(0, index).trim()
	}
	var call braceView
	var pathVariable string
	var others []braceView
	for _, part := range pattern.commaViews() {
		part = part.trim()
		variable := extractPathAssignmentVariable(part.text())
		candidate := part
		if variable != "" {
			candidate = part.window(strings.Index(part.text(), "=")+1, len(part.text())).trim()
		}
		if _, _, funcIdx, ok := extractShortestPathCall(candidate); call.empty() && ok && funcIdx == 0 &&
			findMatchingParen(candidate.text(), strings.IndexByte(candidate.text(), '(')) == len(candidate.text())-1 {
			call, pathVariable = candidate, variable
			continue
		}
		others = append(others, part)
	}
	if call.empty() {
		return nil, false, nil
	}
	funcName, inner, _, _ := extractShortestPathCall(call)
	if selector, predicate, terms, ok := splitPathSelectorTerm(where); ok {
		return e.parseSelectedPathMatchContext(ctx, inner, pathVariable, others, selector, predicate, terms)
	}
	startPattern, endPattern, ok := shortestPathEndpointPatterns(inner)
	if !ok {
		return nil, true, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "ShortestPathSingleRelationship",
			localization.CypherMatchingShortestPathSingleRelationship(funcName))
	}
	// Planning reads only endpoint syntax. Their property expressions belong
	// to shortestPathEndpointPairs, after the incoming row is bound.
	startHead, _ := splitNodePatternSources(startPattern)
	endHead, _ := splitNodePatternSources(endPattern)
	relationship := inner.window(len(startPattern.text()), len(inner.text())-len(endPattern.text()))
	traversal := e.parseTraversalPatternStructure(ctx, inner.assembleCode([]braceView{startHead.wrap("(", ")"), relationship, endHead.wrap("(", ")")}, ""))
	if err := getExpressionFailure(ctx); err != nil {
		return nil, true, err
	}
	if traversal == nil || traversal.IsChained {
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
		m.startPattern = m.startPattern.window(1, len(m.startPattern.text())).wrap("("+m.startVariable, "")
		traversal.StartNode.variable = m.startVariable
	}
	if m.endVariable == "" {
		m.endVariable = generatedVariablePrefix + "sp_end"
		m.endPattern = m.endPattern.window(1, len(m.endPattern.text())).wrap("("+m.endVariable, "")
		traversal.EndNode.variable = m.endVariable
	}
	if m.pathVariable == "" {
		m.pathVariable = generatedVariablePrefix + "sp_path"
	}
	traversal.PathVariable = m.pathVariable
	var endpointTerms, pathTerms []braceView
	for _, term := range splitTopLevelAndConjuncts(where) {
		if term = term.trim(); term.empty() {
			continue
		}
		relationship := traversal.Relationship.Variable
		if referencesVariable(term.text(), m.pathVariable) || (relationship != "" && referencesVariable(term.text(), relationship)) {
			pathTerms = append(pathTerms, term)
		} else {
			endpointTerms = append(endpointTerms, term)
		}
	}
	m.endpointWhere = body.assembleCode(endpointTerms, " AND ")
	m.pathWhere = body.assembleCode(pathTerms, " AND ")
	if m.plan, ok = shortestPathPlanContext(ctx, inner, startPattern, endPattern, m.startVariable, m.endVariable, m.pathVariable, m.pathWhere); !ok {
		if ctx.Err() != nil {
			return nil, true, ctx.Err()
		}
		return nil, true, errSelectedPathPattern(inner)
	}
	m.setOthersVariables()
	return m, true, nil
}

// shortestPathPlan compiles a shortestPath pattern, (start)-[...]-(end) with
// the endpoint node patterns startPattern and endPattern, for the selected
// path search between its endpoints: the endpoints are matched first, so
// the plan takes them as the bare variables, and predicate (the WHERE
// conjuncts that read the path) constrains the paths selected from.
func shortestPathPlanContext(ctx context.Context, pattern, startPattern, endPattern braceView, startVariable, endVariable, pathVariable string, predicate braceView) (*selectedPathPlan, bool) {
	pattern = pattern.trim()
	relationship := pattern.window(len(startPattern.text()), len(pattern.text())-len(endPattern.text()))
	plan, _, ok := compileSelectedPathPlanContext(ctx, pattern.assembleCode([]braceView{pattern.generated("(" + startVariable + ")"), relationship, pattern.generated("(" + endVariable + ")")}, ""), pathVariable, predicate, nil)
	return plan, ok
}

// setOthersVariables lists the variables the clause's other patterns bind.
func (m *shortestPathMatch) setOthersVariables() {
	for _, part := range m.others {
		m.othersVariables = append(m.othersVariables, extractNodeVariables(matchPatternStructure(part))...)
		m.othersVariables = append(m.othersVariables, extractRelationshipVariables(matchPatternStructure(part))...)
		if variable := extractPathAssignmentVariable(part.text()); variable != "" {
			m.othersVariables = append(m.othersVariables, variable)
		}
	}
}

// shortestPathPatternError is Neo4j's SyntaxError for a shortestPath or
// allShortestPaths pattern it doesn't support: a minimum length above 1, or
// relationship properties (pattern is the call's argument).
func shortestPathPatternError(funcName string, view braceView, traversal *TraversalMatch) error {
	pattern := view.text()
	if traversal.Relationship.MinHops > 1 {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidShortestPathMinimalLength",
			localization.CypherMatchingShortestPathMinimalLength(funcName))
	}
	if traversal.StartNode.variable != "" && traversal.StartNode.variable == traversal.EndNode.variable {
		return errShortestCommonEndpoints()
	}
	if len(traversal.Relationship.Properties) > 0 {
		properties := ""
		if open := strings.IndexByte(pattern, '['); open >= 0 {
			if close := findMatchingBracket(pattern, open); close > open {
				relationship := view.window(open, close)
				if brace, _, _, found := relationship.firstBrace(); found {
					properties = strings.TrimSpace(relationship.text()[brace:])
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
func shortestPathExpressionError(view braceView, wordStart, wordEnd, end int) error {
	query := view.text()
	name := query[wordStart:wordEnd]
	if !functionNameStartsAt(query, wordStart) {
		return nil
	}
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
	startPattern, endPattern, ok := shortestPathEndpointPatterns(view.window(open+1, close))
	if !ok {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "ShortestPathSingleRelationship",
			localization.CypherMatchingShortestPathSingleRelationship(function))
	}
	for _, pattern := range []braceView{startPattern, endPattern} {
		endpoint := pattern.text()
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
func shortestPathEndpointPatterns(view braceView) (braceView, braceView, bool) {
	view = view.trim()
	pattern := view.text()
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
		return braceView{}, braceView{}, false
	}
	return view.window(0, startEnd+1), view.window(endOpen, len(pattern)), true
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
			out = append(out, paths...)
			found += len(paths)
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
			matched, err := e.pipelineMatchRows(ctx, base, part.wrap("MATCH ", ""))
			if err != nil {
				return nil, err
			}
			next = append(next, matched...)
		}
		bases = next
	}
	return bases, nil
}

// unmatchedRow is an OPTIONAL MATCH's row for an input row that matches
// nothing: the clause's new variables are null.
func (m *shortestPathMatch) unmatchedRow(row pipelineRow) pipelineRow {
	names := append([]string{m.startVariable, m.endVariable, m.pathVariable}, m.othersVariables...)
	if m.traversal != nil {
		names = append(names, m.traversal.Relationship.Variable)
	}
	if !m.pattern.empty() {
		names = append(names, extractNodeVariables(matchPatternStructure(m.pattern))...)
		names = append(names, extractRelationshipVariables(matchPatternStructure(m.pattern))...)
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
	var patterns []braceView
	for _, endpoint := range []struct {
		pattern  braceView
		variable string
	}{{m.startPattern, m.startVariable}, {m.endPattern, m.endVariable}} {
		node, bound := row[endpoint.variable].(*storage.Node)
		text := endpoint.pattern.text()
		if bound && node != nil && strings.TrimSpace(text[1:len(text)-1]) == endpoint.variable {
			continue
		}
		patterns = append(patterns, endpoint.pattern)
	}
	if len(patterns) == 0 {
		if !m.endpointWhere.empty() && !e.evaluateWithWhereCondition(ctx, m.endpointWhere, map[string]interface{}(row)) {
			return nil, getExpressionFailure(ctx)
		}
		return []pipelineRow{row}, nil
	}
	base := braceView{syntax: m.startPattern.syntax, source: m.startPattern.source.OriginalView()}
	clause := base.assembleCode(patterns, ", ").wrap("MATCH ", "")
	if !m.endpointWhere.empty() {
		clause = base.assembleCode([]braceView{clause, m.endpointWhere.wrap(" WHERE ", "")}, "")
	}
	return e.pipelineMatchRows(ctx, row, clause)
}

// pipelineMatchRows runs a MATCH clause for one row, for a step that
// matches part of its own pattern; a clause shape the pipeline can't run is
// an invalid pattern.
func (e *StorageExecutor) pipelineMatchRows(ctx context.Context, row pipelineRow, clause braceView) ([]pipelineRow, error) {
	rows, handled, err := e.pipelineApplyMatch(ctx, []pipelineRow{row}, clause)
	if err == nil && !handled {
		err = localizedError(localization.CypherMatchingPathPatternInvalid(strings.TrimSpace(clause.text()[len("MATCH"):])), nil)
	}
	return rows, err
}

// shortestPathsBetween returns the rows of the shortest path (all of them
// for allShortestPaths) from start to end that satisfies the clause's path
// predicates, given the row's bindings: the selected path search between
// the pair (path_selector_search.go). Identical variable names are rejected
// when the pattern is read. Different names holding the same node may take
// a zero-length path when the lower bound is zero.
func (e *StorageExecutor) shortestPathsBetween(ctx context.Context, m *shortestPathMatch, start, end *storage.Node, row pipelineRow) ([]pipelineRow, error) {
	if variable := m.traversal.Relationship.Variable; variable != "" {
		if _, bound := row[variable]; bound {
			return nil, errBoundShortestRelationships(shortestPathFunctionName(m.findAll))
		}
	}
	if start.ID == end.ID && m.traversal.Relationship.MinHops > 0 {
		return nil, errShortestCommonEndpoints()
	}
	maxLength, ok := e.selectedPathLimit(ctx, m.plan, row)
	if !ok {
		return nil, getExpressionFailure(ctx)
	}
	rows := e.searchSelectedPaths(ctx, m.plan, m.plan.forward, false, row, maxLength, start, end, nil, 1, m.findAll)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return rows, getExpressionFailure(ctx)
}

// errBoundShortestRelationships is Neo4j 5.26.30's diagnostic for a function
// pattern that reuses a relationship binding from an earlier pattern.
func errBoundShortestRelationships(function string) error {
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "BoundRelationships", localization.CypherMatchingBoundShortestRelationships(function))
}

// validateShortestCallBindings checks calls against the incoming semantic
// scope. Subqueries are checked by their own scope walk, and selector calls
// inserted by the rewrite have the selector's equality semantics instead.
func validateShortestCallBindings(view braceView, scope matchSemanticScope) error {
	text := view.text()
	if indexASCIIFold(text, "shortestpath") < 0 {
		return nil
	}
	if where := topLevelKeywordIndex(view, "WHERE"); where >= 0 {
		if _, _, _, selected := splitPathSelectorTerm(view.window(where+len("WHERE"), len(text)).trim()); selected {
			return nil
		}
	}
	view = view.mask(maskSubqueryBodies(view))
	text = view.text()
	for at := 0; at < len(text); at++ {
		if isCypherQuoteByte(text[at]) {
			at = skipCypherQuotedText(text, at, text[at]) - 1
			continue
		}
		if !cyphertext.NameStartAt(text, at) || !functionNameStartsAt(text, at) || at > 0 && text[at-1] == '$' {
			continue
		}
		end := at + 1
		end = cyphertext.NamePartEnd(text, end)
		word := text[at:end]
		if equalFoldASCII(word, "shortestPath") || equalFoldASCII(word, "allShortestPaths") {
			_, pattern, index, ok := extractShortestPathCall(view.window(at, len(text)))
			if ok && index == 0 {
				if items, ok := parsePathPatternItems(pattern, false); ok && len(items) == 3 && items[1].kind == pathPatternRelationship {
					if items[0].variable != "" && items[0].variable == items[2].variable && items[1].min <= 1 {
						return errShortestCommonEndpoints()
					}
					if _, bound := scope[items[1].variable]; items[1].variable != "" && bound {
						return errBoundShortestRelationships(shortestPathFunctionName(equalFoldASCII(word, "allShortestPaths")))
					}
				}
			}
		}
		at = end - 1
	}
	return nil
}

func errShortestCommonEndpoints() error {
	return localizedStatusError("Neo.DatabaseError.Statement.ExecutionFailed", "ShortestPathCommonEndNodes", localization.CypherMatchingShortestPathCommonEndNodes())
}

func shortestPathFunctionName(all bool) string {
	if all {
		return "allShortestPaths"
	}
	return "shortestPath"
}
