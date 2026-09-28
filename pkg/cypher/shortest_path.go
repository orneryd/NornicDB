// Shortest path Cypher syntax support for NornicDB.
// Implements shortestPath() and allShortestPaths() functions.
//
// Syntax:
//   MATCH p = shortestPath((start)-[*]-(end)) RETURN p
//   MATCH p = allShortestPaths((start)-[*]-(end)) RETURN p

package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// bfsCancelCheckMask is the bitmask used to check ctx.Done() periodically
// inside hot BFS loops. Checking on every iteration is measurable overhead
// for traversals over large fan-out graphs; checking once every 256 dequeues
// is enough to react to client disconnects/server shutdown within a few ms
// while keeping the per-iteration cost negligible. Power of two so the
// compiler turns the modulo into a single AND.
const bfsCancelCheckMask = 0xFF

// parseShortestPathQuery parses queries with shortestPath() or allShortestPaths()
func (e *StorageExecutor) parseShortestPathQuery(ctx context.Context, cypher string) (*ShortestPathQuery, error) {
	query := &ShortestPathQuery{
		// shortestPath terminates when the BFS frontier is exhausted, so the
		// only reason to cap depth is to bound worst-case work. Using the
		// shared sentinel keeps `[*]` semantics consistent with the rest of
		// the variable-length parser.
		maxHops:        VarLengthUnboundedMaxHops,
		originalCypher: cypher,
	}

	funcName, pattern, funcIdx, ok := extractShortestPathCall(cypher)
	if !ok {
		return nil, localizedError(localization.CypherMatchingShortestPathQueryExpected(), nil)
	}
	query.findAll = strings.EqualFold(funcName, "allShortestPaths")
	if pattern == "" {
		return nil, localizedError(localization.CypherMatchingShortestPathSyntaxInvalid(), nil)
	}
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil {
		return nil, localizedError(localization.CypherMatchingPathPatternInvalid(pattern), nil)
	}
	query.startNode = match.StartNode
	query.endNode = match.EndNode
	query.relTypes = match.Relationship.Types
	query.direction = match.Relationship.Direction
	if match.Relationship.MaxHops > 0 {
		query.maxHops = match.Relationship.MaxHops
	}
	query.pathVariable = extractShortestPathPathVariable(cypher, funcIdx)

	// Extract WHERE clause if present
	whereIdx := findKeywordIndexInContext(cypher, "WHERE")
	returnIdx := findKeywordIndexInContext(cypher, "RETURN")
	if whereIdx > 0 && whereIdx < returnIdx {
		query.whereClause = strings.TrimSpace(cypher[whereIdx+5 : returnIdx])
	}

	// Extract RETURN clause
	if returnIdx > 0 {
		query.returnClause = strings.TrimSpace(cypher[returnIdx+6:])
	}

	// Resolve variable bindings from the preceding MATCH clause, if present.
	e.resolveShortestPathVariables(ctx, query, match.StartNode.variable, match.EndNode.variable, extractPreviousMatchClause(cypher, funcIdx))

	return query, nil
}

// resolveShortestPathVariables resolves variable references from the MATCH clause
func (e *StorageExecutor) resolveShortestPathVariables(ctx context.Context, query *ShortestPathQuery, startVar, endVar, matchClause string) {
	if strings.TrimSpace(matchClause) == "" {
		return
	}

	// Parse node patterns from the MATCH clause
	// Pattern: (var:Label {props}), (var2:Label2 {props2})
	nodePatterns := e.splitNodePatterns(matchClause)

	varBindings := make(map[string]nodePatternInfo)
	for _, np := range nodePatterns {
		info := e.parseNodePattern(ctx, np)
		if info.variable != "" {
			varBindings[info.variable] = info
		}
	}

	// Check if startVar is a variable reference (no labels/props in shortestPath pattern)
	if len(query.startNode.labels) == 0 && len(query.startNode.properties) == 0 {
		// It's a variable reference - look it up
		if binding, ok := varBindings[startVar]; ok {
			// Find the actual node
			query.startVarBinding = e.findNodeByPattern(binding)
		}
	}

	// Check if endVar is a variable reference
	if len(query.endNode.labels) == 0 && len(query.endNode.properties) == 0 {
		// It's a variable reference - look it up
		if binding, ok := varBindings[endVar]; ok {
			// Find the actual node
			query.endVarBinding = e.findNodeByPattern(binding)
		}
	}
}

func extractShortestPathCall(cypher string) (string, string, int, bool) {
	upperCypher := upperASCII(cypher)
	// Upper-case literals avoid upperASCII(funcName) allocations on every
	// statement; the compiler places this constant slice in static data.
	for _, upperFunc := range []string{"ALLSHORTESTPATHS", "SHORTESTPATH"} {
		searchStart := 0
		for searchStart < len(cypher) {
			idx := strings.Index(upperCypher[searchStart:], upperFunc)
			if idx < 0 {
				break
			}
			idx += searchStart
			if idx > 0 && isWordChar(cypher[idx-1]) {
				searchStart = idx + 1
				continue
			}
			openParen := idx + len(upperFunc)
			for openParen < len(cypher) && (cypher[openParen] == ' ' || cypher[openParen] == '\t' || cypher[openParen] == '\n' || cypher[openParen] == '\r') {
				openParen++
			}
			if openParen >= len(cypher) || cypher[openParen] != '(' {
				searchStart = idx + 1
				continue
			}
			closeParen := findMatchingParen(cypher, openParen)
			if closeParen < 0 {
				if upperFunc == "ALLSHORTESTPATHS" {
					return "allShortestPaths", "", idx, false
				}
				return "shortestPath", "", idx, false
			}
			if upperFunc == "ALLSHORTESTPATHS" {
				return "allShortestPaths", strings.TrimSpace(cypher[openParen+1 : closeParen]), idx, true
			}
			return "shortestPath", strings.TrimSpace(cypher[openParen+1 : closeParen]), idx, true
		}
	}
	return "", "", -1, false
}

func extractShortestPathPathVariable(cypher string, funcIdx int) string {
	matchIdx := lastKeywordIndexBefore(cypher, "MATCH", funcIdx)
	if matchIdx < 0 {
		return ""
	}
	clause := strings.TrimSpace(cypher[matchIdx+len("MATCH") : funcIdx])
	eqIdx := strings.LastIndex(clause, "=")
	if eqIdx <= 0 {
		return ""
	}
	left := strings.TrimSpace(clause[:eqIdx])
	if !isValidIdentifier(left) {
		return ""
	}
	return left
}

func extractPreviousMatchClause(cypher string, beforeIdx int) string {
	currentMatchIdx := lastKeywordIndexBefore(cypher, "MATCH", beforeIdx)
	if currentMatchIdx < 0 {
		return ""
	}
	previousMatchIdx := lastKeywordIndexBefore(cypher, "MATCH", currentMatchIdx)
	if previousMatchIdx < 0 {
		return ""
	}
	return strings.TrimSpace(cypher[previousMatchIdx+len("MATCH") : currentMatchIdx])
}

// findNodeByPattern finds a node matching the given pattern.
//
// Hot path for shortestPath start/end resolution: this runs twice per
// shortestPath request. The previous implementation always full-scanned
// `GetNodesByLabel` and linear-filtered, which on label populations of a
// few thousand fetched and decoded every node body — adding a fixed cost
// per request that scaled with total label size, not path length. When a
// property index covers any of the requested properties, the schema
// lookup returns the candidate IDs directly so we only fetch the few
// nodes whose value actually matches.
func (e *StorageExecutor) findNodeByPattern(pattern nodePatternInfo) *storage.Node {
	if len(pattern.labels) > 0 && len(pattern.properties) > 0 {
		if schema := e.storage.GetSchema(); schema != nil {
			label := pattern.labels[0]
			for _, prop := range mergePropertyNamesSorted(pattern.properties) {
				if _, ok := schema.GetPropertyIndex(label, prop); !ok {
					continue
				}
				ids := schema.PropertyIndexLookup(label, prop, pattern.properties[prop])
				for _, id := range ids {
					n, err := e.storage.GetNode(id)
					if err != nil || n == nil {
						continue
					}
					if e.nodeMatchesProps(n, pattern.properties) {
						return n
					}
				}
				// Property is indexed; the index lookup is authoritative
				// for this (label, prop) pair, so no fallback scan.
				return nil
			}
		}
	}

	var candidates []*storage.Node
	if len(pattern.labels) > 0 {
		candidates, _ = e.storage.GetNodesByLabel(pattern.labels[0])
	} else {
		candidates, _ = e.storage.AllNodes()
	}

	for _, node := range candidates {
		if e.nodeMatchesProps(node, pattern.properties) {
			return node
		}
	}

	return nil
}

// ShortestPathQuery represents a parsed shortest path query
type ShortestPathQuery struct {
	pathVariable    string
	startNode       nodePatternInfo
	endNode         nodePatternInfo
	startVarBinding *storage.Node // Resolved node from MATCH clause (if variable reference)
	endVarBinding   *storage.Node // Resolved node from MATCH clause (if variable reference)
	relTypes        []string
	direction       string
	maxHops         int
	findAll         bool // true for allShortestPaths, false for shortestPath
	whereClause     string
	returnClause    string
	originalCypher  string // Full original query for MATCH clause parsing
}

// executeShortestPathQuery executes a shortestPath or allShortestPaths query.
// ctx is checked between start/end pairs and inside the BFS so the caller can
// abandon expensive traversals on client disconnect or server shutdown.
func (e *StorageExecutor) executeShortestPathQuery(ctx context.Context, query *ShortestPathQuery) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Resolve start and end nodes from variable bindings (from MATCH clause)
	// If startNode/endNode has a variable reference but no labels/props, it references
	// a node from the preceding MATCH clause. We need to find those nodes.

	var startNodes []*storage.Node
	var endNodes []*storage.Node

	// Check if we have concrete node patterns or just variable references
	startHasPattern := len(query.startNode.labels) > 0 || len(query.startNode.properties) > 0
	endHasPattern := len(query.endNode.labels) > 0 || len(query.endNode.properties) > 0

	startIsVarRef := !startHasPattern && query.startNode.variable != ""
	endIsVarRef := !endHasPattern && query.endNode.variable != ""

	if startIsVarRef && query.startVarBinding != nil {
		startNodes = []*storage.Node{query.startVarBinding}
	} else if startIsVarRef {
		// Variable referenced but couldn't be resolved against the previous
		// MATCH clause — refuse rather than silently scanning every node and
		// running BFS from each one (which produces a multi-second hang on
		// any non-trivial graph).
		return nil, localizedError(localization.CypherMatchingShortestPathStartVariableUnresolved(query.startNode.variable), nil)
	} else if len(query.startNode.labels) > 0 {
		startNodes, _ = e.storage.GetNodesByLabel(query.startNode.labels[0])
		if len(query.startNode.properties) > 0 {
			var filtered []*storage.Node
			for _, n := range startNodes {
				if e.nodeMatchesProps(n, query.startNode.properties) {
					filtered = append(filtered, n)
				}
			}
			startNodes = filtered
		}
	} else {
		startNodes, _ = e.storage.AllNodes()
	}

	if endIsVarRef && query.endVarBinding != nil {
		endNodes = []*storage.Node{query.endVarBinding}
	} else if endIsVarRef {
		return nil, localizedError(localization.CypherMatchingShortestPathEndVariableUnresolved(query.endNode.variable), nil)
	} else if len(query.endNode.labels) > 0 {
		endNodes, _ = e.storage.GetNodesByLabel(query.endNode.labels[0])
		if len(query.endNode.properties) > 0 {
			var filtered []*storage.Node
			for _, n := range endNodes {
				if e.nodeMatchesProps(n, query.endNode.properties) {
					filtered = append(filtered, n)
				}
			}
			endNodes = filtered
		}
	} else {
		endNodes, _ = e.storage.AllNodes()
	}

	// Find paths between all start/end node pairs
	var allPaths []PathResult
	for _, start := range startNodes {
		for _, end := range endNodes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if start.ID == end.ID {
				continue // Skip same node
			}

			if query.findAll {
				paths, err := e.allShortestPaths(ctx, start, end, query.relTypes, query.direction, query.maxHops)
				if err != nil {
					return nil, err
				}
				allPaths = append(allPaths, paths...)
			} else {
				path, err := e.shortestPath(ctx, start, end, query.relTypes, query.direction, query.maxHops)
				if err != nil {
					return nil, err
				}
				if path != nil {
					allPaths = append(allPaths, *path)
				}
			}
		}
	}

	// Build result
	if query.returnClause != "" {
		// Parse return items
		returnItems := e.parseReturnItems(query.returnClause)

		for _, item := range returnItems {
			if item.alias != "" {
				result.Columns = append(result.Columns, item.alias)
			} else {
				result.Columns = append(result.Columns, item.expr)
			}
		}

		// Build rows from paths. Every item is evaluated with the same path
		// context the MATCH traversal routes use (path variable, start and end
		// node), so length(p) + 1, size(nodes(p)) or nodes(p)[0].id are full
		// expressions rather than a bare path function.
		for _, path := range allPaths {
			row := make([]interface{}, len(returnItems))
			pathContext := e.buildPathContext(path, &TraversalMatch{
				StartNode:    query.startNode,
				EndNode:      query.endNode,
				PathVariable: query.pathVariable,
			})

			for i, item := range returnItems {
				if item.expr == query.pathVariable {
					row[i] = e.pathToMap(path)
					continue
				}
				row[i] = e.evaluateExpressionWithPathContext(ctx, item.expr, pathContext)
			}

			result.Rows = append(result.Rows, row)
		}
	} else {
		// Return paths directly
		result.Columns = []string{query.pathVariable}
		for _, path := range allPaths {
			result.Rows = append(result.Rows, []interface{}{e.pathToMap(path)})
		}
	}

	return result, nil
}

// executeOptionalShortestPath handles anchored shortestPath forms:
//
//	MATCH (a:ZSP {id:1}) OPTIONAL MATCH p = shortestPath((a)-[:ZS*]->(c:ZSP {id:3})) RETURN ...
//
// The initial MATCH clause provides the seed rows; each seed row that binds a
// bare start/end variable runs one BFS and contributes either a path row or a
// null row, preserving OPTIONAL MATCH semantics. Patterned endpoints resolve
// via the schema-aware findNodeByPattern once per statement.
func (e *StorageExecutor) executeOptionalShortestPath(ctx context.Context, cypher string) (*ExecuteResult, error) {
	params := getParamsFromContext(ctx)
	if params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	funcName, pattern, funcIdx, ok := extractShortestPathCall(cypher)
	if !ok {
		return nil, localizedError(localization.CypherMatchingShortestPathQueryExpected(), nil)
	}
	findAll := strings.EqualFold(funcName, "allShortestPaths")

	optMatchIdx := findKeywordIndexInContext(cypher, "OPTIONAL MATCH")
	if optMatchIdx < 0 {
		return nil, localizedError(localization.CypherResidualOptionalMatchNotFound(truncateQuery(cypher, 80)), nil)
	}

	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil {
		return nil, localizedError(localization.CypherMatchingPathPatternInvalid(pattern), nil)
	}

	maxHops := VarLengthUnboundedMaxHops
	if match.Relationship.MaxHops > 0 {
		maxHops = match.Relationship.MaxHops
	}
	pathVariable := extractShortestPathPathVariable(cypher, funcIdx)

	returnIdx := findKeywordIndexInContext(cypher, "RETURN")
	if returnIdx <= funcIdx {
		returnIdx = -1
	}
	var whereClause string
	if whereIdx := findKeywordIndexInContext(cypher, "WHERE"); whereIdx > optMatchIdx && (returnIdx < 0 || whereIdx < returnIdx) {
		whereClause = strings.TrimSpace(cypher[whereIdx+5 : returnIdx])
	}
	returnClause := ""
	if returnIdx > 0 {
		returnClause = strings.TrimSpace(cypher[returnIdx+6:])
	}

	startHasPattern := len(match.StartNode.labels) > 0 || len(match.StartNode.properties) > 0
	endHasPattern := len(match.EndNode.labels) > 0 || len(match.EndNode.properties) > 0

	// Seed the row space from the initial MATCH clause. Bare variables in the
	// traversal pattern are resolved per row; patterned endpoints resolve once.
	var seedColumns []string
	if !startHasPattern && match.StartNode.variable != "" {
		seedColumns = append(seedColumns, match.StartNode.variable)
	}
	if !endHasPattern && match.EndNode.variable != "" {
		seedColumns = append(seedColumns, match.EndNode.variable)
	}

	var startByPattern, endByPattern *storage.Node
	if startHasPattern {
		startByPattern = e.findNodeByPattern(match.StartNode)
	}
	if endHasPattern {
		endByPattern = e.findNodeByPattern(match.EndNode)
	}

	rows := [][]interface{}{{nil}}
	if len(seedColumns) > 0 {
		initial := strings.TrimSpace(cypher[:optMatchIdx])
		seedResult, err := e.executeInternal(ctx, initial+" RETURN "+strings.Join(seedColumns, ", "), nil)
		if err != nil {
			return nil, err
		}
		if len(seedResult.Rows) > 0 {
			rows = seedResult.Rows
		}
	}

	// Bound columns carry *storage.Node values keyed by the seed column names.
	buildResult := func(columnNames []string, builtRows [][]interface{}) *ExecuteResult {
		return &ExecuteResult{Columns: columnNames, Rows: builtRows, Stats: &QueryStats{}}
	}

	var result *ExecuteResult
	if returnClause != "" {
		returnItems := e.parseReturnItems(returnClause)
		columnNames := make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				columnNames[i] = item.alias
			} else {
				columnNames[i] = item.expr
			}
		}
		traversal := &TraversalMatch{
			StartNode:    match.StartNode,
			EndNode:      match.EndNode,
			PathVariable: pathVariable,
		}
		builtRows := make([][]interface{}, 0, len(rows))
		for _, seed := range rows {
			bound := map[string]interface{}{}
			for i, name := range seedColumns {
				bound[name] = seed[i]
			}
			start, end := startByPattern, endByPattern
			if !startHasPattern && match.StartNode.variable != "" {
				start, _ = bound[match.StartNode.variable].(*storage.Node)
			}
			if !endHasPattern && match.EndNode.variable != "" {
				end, _ = bound[match.EndNode.variable].(*storage.Node)
			}
			if start == nil || end == nil || start.ID == end.ID {
				nullCtx := PathContext{nodes: map[string]*storage.Node{}, rels: map[string]*storage.Edge{}, paths: map[string]*PathResult{}}
				for i, name := range seedColumns {
					if node, ok := seed[i].(*storage.Node); ok {
						nullCtx.nodes[name] = node
					}
				}
				builtRows = append(builtRows, e.buildOptionalShortestPathRow(ctx, returnItems, pathVariable, nil, nullCtx))
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var paths []PathResult
			if findAll {
				found, err := e.allShortestPaths(ctx, start, end, match.Relationship.Types, match.Relationship.Direction, maxHops)
				if err != nil {
					return nil, err
				}
				paths = found
			} else {
				found, err := e.shortestPath(ctx, start, end, match.Relationship.Types, match.Relationship.Direction, maxHops)
				if err != nil {
					return nil, err
				}
				if found != nil {
					paths = append(paths, *found)
				}
			}
			if len(paths) == 0 {
				nullCtx := PathContext{nodes: map[string]*storage.Node{}, rels: map[string]*storage.Edge{}, paths: map[string]*PathResult{}}
				for i, name := range seedColumns {
					if node, ok := seed[i].(*storage.Node); ok {
						nullCtx.nodes[name] = node
					}
				}
				builtRows = append(builtRows, e.buildOptionalShortestPathRow(ctx, returnItems, pathVariable, nil, nullCtx))
				continue
			}
			for _, path := range paths {
				pathContext := e.buildPathContext(path, traversal)
				row := e.buildOptionalShortestPathRow(ctx, returnItems, pathVariable, &path, pathContext)
				if whereClause != "" && !isTruthy(e.evaluateExpressionWithPathContext(ctx, whereClause, pathContext)) {
					continue
				}
				builtRows = append(builtRows, row)
			}
		}
		result = buildResult(columnNames, builtRows)
	} else {
		// Neo4j rejects a query that concludes with OPTIONAL MATCH.
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "OptionalMatchConcludesQuery", "Query cannot conclude with OPTIONAL MATCH")
	}
	return result, nil
}

// buildOptionalShortestPathRow projects one row for an optional shortestPath:
// the path variable itself projects the path map, everything else evaluates
// against the path context (length(p), p IS NULL, nodes(p), arithmetic, ...).
func (e *StorageExecutor) buildOptionalShortestPathRow(ctx context.Context, items []returnItem, pathVariable string, path *PathResult, pathContext PathContext) []interface{} {
	row := make([]interface{}, len(items))
	for i, item := range items {
		if item.expr == pathVariable {
			if path != nil {
				row[i] = e.pathToMap(*path)
			}
			continue
		}
		row[i] = e.evaluateExpressionWithPathContext(ctx, item.expr, pathContext)
	}
	return row
}

// evaluateShortestPathValue evaluates shortestPath(...) / allShortestPaths(...)
// in expression position for one row: bare endpoint variables resolve against
// the row's node bindings, patterned endpoints via the schema-aware lookup.
// It shares the same BFS machinery as the clause handlers so MATCH, OPTIONAL
// MATCH, and value forms all traverse identically. handled=false lets the
// ordinary evaluator report its own error when the argument is not a
// traversal pattern.
func (e *StorageExecutor) evaluateShortestPathValue(ctx context.Context, funcName, pattern string, nodes map[string]*storage.Node) (interface{}, bool) {
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil {
		return nil, false
	}
	startHasPattern := len(match.StartNode.labels) > 0 || len(match.StartNode.properties) > 0
	endHasPattern := len(match.EndNode.labels) > 0 || len(match.EndNode.properties) > 0

	var start, end *storage.Node
	if !startHasPattern && match.StartNode.variable != "" {
		start = nodes[match.StartNode.variable]
	} else if startHasPattern {
		start = e.findNodeByPattern(match.StartNode)
	}
	if !endHasPattern && match.EndNode.variable != "" {
		end = nodes[match.EndNode.variable]
	} else if endHasPattern {
		end = e.findNodeByPattern(match.EndNode)
	}
	if start == nil || end == nil || start.ID == end.ID {
		return nil, true
	}

	maxHops := VarLengthUnboundedMaxHops
	if match.Relationship.MaxHops > 0 {
		maxHops = match.Relationship.MaxHops
	}

	if strings.EqualFold(funcName, "allShortestPaths") {
		paths, err := e.allShortestPaths(ctx, start, end, match.Relationship.Types, match.Relationship.Direction, maxHops)
		if err != nil {
			return nil, true
		}
		values := make([]interface{}, 0, len(paths))
		for _, path := range paths {
			values = append(values, e.pathToMap(path))
		}
		return values, true
	}

	path, err := e.shortestPath(ctx, start, end, match.Relationship.Types, match.Relationship.Direction, maxHops)
	if err != nil {
		return nil, true
	}
	if path == nil {
		return nil, true
	}
	return e.pathToMap(*path), true
}

func nodeToValue(n *storage.Node) interface{} {
	props := make(map[string]interface{}, len(n.Properties))
	for k, v := range n.Properties {
		props[k] = v
	}
	return map[string]interface{}{
		"elementId":  string(n.ID),
		"labels":     labelStringSlice(n.Labels),
		"properties": props,
	}
}

func labelStringSlice(in []string) []interface{} {
	out := make([]interface{}, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

// pathToMap converts a PathResult to a map representation
func (e *StorageExecutor) pathToMap(path PathResult) map[string]interface{} {
	nodes := make([]interface{}, len(path.Nodes))
	for i, n := range path.Nodes {
		nodes[i] = n
	}

	rels := make([]interface{}, len(path.Relationships))
	for i, r := range path.Relationships {
		rels[i] = e.edgeToMap(r)
	}

	return map[string]interface{}{
		"_pathResult":   path,
		"nodes":         nodes,
		"relationships": rels,
		"length":        int64(path.Length),
	}
}

// pathValueParts returns the nodes and relationships of a path value. Path
// values come in two map shapes - pathToMap's ("relationships" holds
// relationship maps) and the path-context evaluator's ("rels" holds
// relationships) - and both carry the PathResult under "_pathResult", which
// is authoritative: nodes are *storage.Node and relationships *storage.Edge
// whichever shape the value has. The "nodes" / "rels" keys are read only when
// "_pathResult" is absent. hasNodes / hasRelationships report whether the
// value carried them.
func pathValueParts(path map[string]interface{}) (nodes, relationships []interface{}, hasNodes, hasRelationships bool) {
	var result *PathResult
	switch typed := path["_pathResult"].(type) {
	case PathResult:
		result = &typed
	case *PathResult:
		result = typed
	}
	if result != nil {
		nodes = make([]interface{}, len(result.Nodes))
		for index, node := range result.Nodes {
			nodes[index] = node
		}
		relationships = make([]interface{}, len(result.Relationships))
		for index, relationship := range result.Relationships {
			relationships[index] = relationship
		}
		return nodes, relationships, true, true
	}
	if raw, found := path["nodes"]; found {
		nodes, hasNodes = toAnySlice(raw), true
	}
	if raw, found := path["rels"]; found {
		relationships, hasRelationships = toAnySlice(raw), true
	}
	return nodes, relationships, hasNodes, hasRelationships
}

// isShortestPathQuery checks if a query uses shortestPath or allShortestPaths
func isShortestPathQuery(cypher string) bool {
	upper := upperASCII(cypher)
	return strings.Contains(upper, "SHORTESTPATH") || strings.Contains(upper, "ALLSHORTESTPATHS")
}

// isShortestPathClause reports whether the shortestPath/allShortestPaths call
// lives inside a MATCH clause rather than in a RETURN/WITH projection. The
// clause-level executor claims only clause forms; value forms project per row
// through the shared expression evaluator.
func isShortestPathClause(cypher string) bool {
	_, _, funcIdx, ok := extractShortestPathCall(cypher)
	if !ok {
		return false
	}
	matchIdx := lastKeywordIndexBefore(cypher, "MATCH", funcIdx)
	if matchIdx < 0 {
		return false
	}
	returnIdx := lastKeywordIndexBefore(cypher, "RETURN", funcIdx)
	withIdx := lastKeywordIndexBefore(cypher, "WITH", funcIdx)
	return matchIdx > returnIdx && matchIdx > withIdx
}
