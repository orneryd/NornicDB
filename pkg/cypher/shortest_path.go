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
	upperCypher := strings.ToUpper(cypher)
	for _, funcName := range []string{"allShortestPaths", "shortestPath"} {
		upperFunc := strings.ToUpper(funcName)
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
			openParen := idx + len(funcName)
			for openParen < len(cypher) && (cypher[openParen] == ' ' || cypher[openParen] == '\t' || cypher[openParen] == '\n' || cypher[openParen] == '\r') {
				openParen++
			}
			if openParen >= len(cypher) || cypher[openParen] != '(' {
				searchStart = idx + 1
				continue
			}
			closeParen := findMatchingParen(cypher, openParen)
			if closeParen < 0 {
				return funcName, "", idx, false
			}
			return funcName, strings.TrimSpace(cypher[openParen+1 : closeParen]), idx, true
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

// isShortestPathQuery checks if a query uses shortestPath or allShortestPaths
func isShortestPathQuery(cypher string) bool {
	upper := strings.ToUpper(cypher)
	return strings.Contains(upper, "SHORTESTPATH") || strings.Contains(upper, "ALLSHORTESTPATHS")
}
