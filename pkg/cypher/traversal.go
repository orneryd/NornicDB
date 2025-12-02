// Package cypher provides graph traversal operations for NornicDB.
// This file implements relationship pattern matching, variable-length paths,
// and shortest path algorithms for Neo4j-compatible traversal queries.

package cypher

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// PathResult represents a path through the graph
type PathResult struct {
	Nodes         []*storage.Node
	Relationships []*storage.Edge
	Length        int
}

// TraversalContext holds state during graph traversal
type TraversalContext struct {
	startNode   *storage.Node
	endNode     *storage.Node
	relTypes    []string // Allowed relationship types (empty = any)
	direction   string   // "outgoing", "incoming", "both"
	minHops     int
	maxHops     int
	visited     map[string]bool
	paths       []PathResult
	nodeCache   map[storage.NodeID]*storage.Node // Cache for batch-fetched nodes
	limit       int                              // OPTIMIZATION: Early termination limit (0 = no limit)
	resultCount int                              // Count of results found so far
}

// RelationshipPattern represents a parsed relationship pattern
type RelationshipPattern struct {
	Variable   string   // r in [r:TYPE]
	Types      []string // TYPE in [r:TYPE|OTHER]
	Direction  string   // "outgoing" (-[r]->), "incoming" (<-[r]-), "both" (-[r]-)
	MinHops    int      // min in [*min..max]
	MaxHops    int      // max in [*min..max]
	Properties map[string]interface{}
}

// parseRelationshipPattern parses patterns like -[r:TYPE {props}]->
func (e *StorageExecutor) parseRelationshipPattern(pattern string) *RelationshipPattern {
	result := &RelationshipPattern{
		Direction:  "both",
		MinHops:    1,
		MaxHops:    1,
		Properties: make(map[string]interface{}),
	}

	// Determine direction
	if strings.HasPrefix(pattern, "<-") {
		result.Direction = "incoming"
		pattern = pattern[2:]
	} else if strings.HasPrefix(pattern, "-") {
		pattern = pattern[1:]
	}

	if strings.HasSuffix(pattern, "->") {
		result.Direction = "outgoing"
		pattern = pattern[:len(pattern)-2]
	} else if strings.HasSuffix(pattern, "-") {
		pattern = pattern[:len(pattern)-1]
	}

	// Extract [r:TYPE {props}] part
	if strings.HasPrefix(pattern, "[") && strings.HasSuffix(pattern, "]") {
		inner := pattern[1 : len(pattern)-1]

		// Check for variable length: [*], [*2], [*1..3], [*2..], [*..5]
		if strings.Contains(inner, "*") {
			if matches := varLengthRelPattern.FindStringSubmatch(inner); matches != nil {
				hasRange := strings.Contains(matches[0], "..") // Check if .. is present

				if matches[1] != "" {
					result.MinHops, _ = strconv.Atoi(matches[1])
				} else {
					result.MinHops = 1
				}

				if matches[2] != "" {
					result.MaxHops, _ = strconv.Atoi(matches[2])
				} else if hasRange {
					// *2.. or *.. means unbounded max
					result.MaxHops = 100 // High number for unbounded
				} else if matches[1] != "" {
					// *2 means exactly 2 hops
					result.MaxHops = result.MinHops
				} else {
					// * means any length (1 to default max)
					result.MaxHops = 10
				}
			}
			// Remove variable length part
			inner = varLengthRelPattern.ReplaceAllString(inner, "")
		}

		// Parse variable and types: r:TYPE|OTHER
		if colonIdx := strings.Index(inner, ":"); colonIdx >= 0 {
			result.Variable = strings.TrimSpace(inner[:colonIdx])
			typesPart := inner[colonIdx+1:]

			// Check for properties
			if propsIdx := strings.Index(typesPart, "{"); propsIdx >= 0 {
				result.Properties = e.parseProperties(typesPart[propsIdx:])
				typesPart = typesPart[:propsIdx]
			}

			// Split by | for multiple types
			for _, t := range strings.Split(typesPart, "|") {
				t = strings.TrimSpace(t)
				if t != "" {
					result.Types = append(result.Types, t)
				}
			}
		} else if strings.TrimSpace(inner) != "" {
			result.Variable = strings.TrimSpace(inner)
		}
	}

	return result
}

// executeMatchWithRelationships handles MATCH queries with relationship patterns
func (e *StorageExecutor) executeMatchWithRelationships(pattern string, whereClause string, returnItems []returnItem) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Set up columns from return items
	for _, item := range returnItems {
		if item.alias != "" {
			result.Columns = append(result.Columns, item.alias)
		} else {
			result.Columns = append(result.Columns, item.expr)
		}
	}

	// Parse the pattern: (a:Label)-[r:TYPE]->(b:Label)
	matches := e.parseTraversalPattern(pattern)
	if matches == nil {
		return result, fmt.Errorf("invalid traversal pattern: %s", pattern)
	}

	// Execute traversal
	paths := e.traverseGraph(matches)

	// Apply WHERE clause filter if present
	if whereClause != "" {
		paths = e.filterPathsByWhere(paths, matches, whereClause)
	}

	// Pre-compute upper-case expressions and aggregation flags ONCE for all items
	// This avoids repeated strings.ToUpper() calls in loops (major performance win)
	upperExprs := make([]string, len(returnItems))
	isAggFlags := make([]bool, len(returnItems))
	for i, item := range returnItems {
		upperExprs[i] = strings.ToUpper(item.expr)
		isAggFlags[i] = strings.HasPrefix(upperExprs[i], "COUNT(") ||
			strings.HasPrefix(upperExprs[i], "SUM(") ||
			strings.HasPrefix(upperExprs[i], "AVG(") ||
			strings.HasPrefix(upperExprs[i], "MIN(") ||
			strings.HasPrefix(upperExprs[i], "MAX(") ||
			strings.HasPrefix(upperExprs[i], "COLLECT(")
	}

	// Check if this is an aggregation query
	hasAggregation := false
	for _, isAgg := range isAggFlags {
		if isAgg {
			hasAggregation = true
			break
		}
	}

	// Handle aggregation queries
	if hasAggregation {
		// Check if there are non-aggregation columns (implicit GROUP BY)
		hasGrouping := false
		for _, isAgg := range isAggFlags {
			if !isAgg {
				hasGrouping = true
				break
			}
		}

		// If no grouping, return single aggregated row
		if !hasGrouping {
			row := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				upperExpr := upperExprs[i] // Use pre-computed

				switch {
				case strings.HasPrefix(upperExpr, "COUNT("):
					row[i] = int64(len(paths))

				case strings.HasPrefix(upperExpr, "COLLECT("):
					collected := make([]interface{}, 0, len(paths))
					inner := item.expr[8 : len(item.expr)-1]
					for _, path := range paths {
						context := e.buildPathContext(path, matches)
						val := e.evaluateExpressionWithContext(inner, context.nodes, context.rels)
						collected = append(collected, val)
					}
					row[i] = collected

				default:
					if len(paths) > 0 {
						context := e.buildPathContext(paths[0], matches)
						row[i] = e.evaluateExpressionWithContext(item.expr, context.nodes, context.rels)
					} else {
						row[i] = nil
					}
				}
			}
			result.Rows = append(result.Rows, row)
			return result, nil
		}

		// GROUP BY: group paths by non-aggregation column values
		groups := make(map[string][]PathResult)
		groupKeys := make(map[string][]interface{})

		for _, path := range paths {
			context := e.buildPathContext(path, matches)
			keyParts := make([]interface{}, 0)

			// Build group key from non-aggregation columns
			for i, item := range returnItems {
				if !isAggFlags[i] { // Use pre-computed flag
					val := e.evaluateExpressionWithContext(item.expr, context.nodes, context.rels)
					keyParts = append(keyParts, val)
				}
			}

			key := fmt.Sprintf("%v", keyParts)
			groups[key] = append(groups[key], path)
			if _, exists := groupKeys[key]; !exists {
				groupKeys[key] = keyParts
			}
		}

		// Build result rows for each group
		for key, groupPaths := range groups {
			row := make([]interface{}, len(returnItems))
			keyIdx := 0

			for i, item := range returnItems {
				upperExpr := upperExprs[i] // Use pre-computed

				if !isAggFlags[i] { // Use pre-computed flag
					// Non-aggregated column - use group key value
					row[i] = groupKeys[key][keyIdx]
					keyIdx++
					continue
				}

				// Aggregation function - aggregate over this group
				switch {
				case strings.HasPrefix(upperExpr, "COUNT("):
					row[i] = int64(len(groupPaths))

				case strings.HasPrefix(upperExpr, "COLLECT("):
					collected := make([]interface{}, 0, len(groupPaths))
					inner := item.expr[8 : len(item.expr)-1]
					for _, path := range groupPaths {
						context := e.buildPathContext(path, matches)
						val := e.evaluateExpressionWithContext(inner, context.nodes, context.rels)
						collected = append(collected, val)
					}
					row[i] = collected

				default:
					if len(groupPaths) > 0 {
						context := e.buildPathContext(groupPaths[0], matches)
						row[i] = e.evaluateExpressionWithContext(item.expr, context.nodes, context.rels)
					} else {
						row[i] = nil
					}
				}
			}
			result.Rows = append(result.Rows, row)
		}
		return result, nil
	}

	// Build result rows (non-aggregation)
	for _, path := range paths {
		row := make([]interface{}, len(returnItems))
		context := e.buildPathContext(path, matches)

		for i, item := range returnItems {
			row[i] = e.evaluateExpressionWithContext(item.expr, context.nodes, context.rels)
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// TraversalMatch represents a parsed traversal pattern
type TraversalMatch struct {
	StartNode    nodePatternInfo
	EndNode      nodePatternInfo
	Relationship RelationshipPattern
}

// parseTraversalPattern parses (a:Label)-[r:TYPE]->(b:Label) style patterns
// Uses a state machine instead of regex to properly handle parentheses in property values
func (e *StorageExecutor) parseTraversalPattern(pattern string) *TraversalMatch {
	// Try regex first for simple patterns (faster)
	matches := pathPatternRe.FindStringSubmatch(pattern)
	if matches != nil {
		// Verify the regex matched the ENTIRE pattern - if not, it got confused by special chars
		// The full match (matches[0]) should equal the trimmed pattern
		fullMatch := strings.TrimSpace(matches[0])
		trimmedPattern := strings.TrimSpace(pattern)

		if fullMatch == trimmedPattern {
			// Regex captured the complete pattern
			startNode := e.parseNodePatternFromString(matches[1])
			endNode := e.parseNodePatternFromString(matches[3])

			return &TraversalMatch{
				StartNode:    startNode,
				Relationship: *e.parseRelationshipPattern(matches[2]),
				EndNode:      endNode,
			}
		}
		// Regex matched only a partial pattern (e.g., stopped at ')' inside a quoted string)
		// Fall through to state machine parsing
	}

	// Fall back to state-machine parsing for complex patterns with special chars
	return e.parseTraversalPatternStateMachine(pattern)
}

// parseTraversalPatternStateMachine parses patterns with a state machine
// to properly handle parentheses and special characters inside quoted property values.
func (e *StorageExecutor) parseTraversalPatternStateMachine(pattern string) *TraversalMatch {
	// Find the boundaries of (startNode), -[rel]->, and (endNode)
	// respecting quotes and nested parentheses

	// Find start node: first balanced parentheses
	startIdx := strings.Index(pattern, "(")
	if startIdx < 0 {
		return nil
	}
	startEnd := findMatchingParen(pattern, startIdx)
	if startEnd < 0 {
		return nil
	}
	startNodeStr := pattern[startIdx+1 : startEnd]

	// Find relationship: -[...]-> or <-[...]-
	relStart := strings.Index(pattern[startEnd:], "[")
	if relStart < 0 {
		return nil
	}
	relStart += startEnd
	relEnd := strings.Index(pattern[relStart:], "]")
	if relEnd < 0 {
		return nil
	}
	relEnd += relStart

	// Extract full relationship pattern including arrows
	relPatternStart := startEnd + 1
	relPatternEnd := relEnd + 1
	// Include the arrow after ]
	for relPatternEnd < len(pattern) && (pattern[relPatternEnd] == '-' || pattern[relPatternEnd] == '>') {
		relPatternEnd++
	}
	relStr := pattern[relPatternStart:relPatternEnd]

	// Find end node: next balanced parentheses after relationship
	endStart := strings.Index(pattern[relEnd:], "(")
	if endStart < 0 {
		return nil
	}
	endStart += relEnd
	endEnd := findMatchingParen(pattern, endStart)
	if endEnd < 0 {
		return nil
	}
	endNodeStr := pattern[endStart+1 : endEnd]

	return &TraversalMatch{
		StartNode:    e.parseNodePatternFromString(startNodeStr),
		Relationship: *e.parseRelationshipPattern(relStr),
		EndNode:      e.parseNodePatternFromString(endNodeStr),
	}
}

// findMatchingParen finds the index of the closing paren that matches the opening paren at startIdx.
// Respects quoted strings so that ')' inside quotes is not treated as a closing paren.
func findMatchingParen(s string, startIdx int) int {
	if startIdx >= len(s) || s[startIdx] != '(' {
		return -1
	}

	depth := 0
	inQuote := false
	quoteChar := byte(0)

	for i := startIdx; i < len(s); i++ {
		c := s[i]

		// Handle quotes
		if (c == '\'' || c == '"') && (i == 0 || s[i-1] != '\\') {
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			continue
		}

		// Only count parens when not in a quote
		if !inQuote {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 {
					return i
				}
			}
		}
	}

	return -1 // No matching paren found
}

// parseNodePatternFromString parses n:Label {props} from a string
func (e *StorageExecutor) parseNodePatternFromString(s string) nodePatternInfo {
	info := nodePatternInfo{
		properties: make(map[string]interface{}),
	}

	s = strings.TrimSpace(s)

	// Check for properties
	if propsIdx := strings.Index(s, "{"); propsIdx >= 0 {
		info.properties = e.parseProperties(s[propsIdx:])
		s = s[:propsIdx]
	}

	// Check for labels
	if colonIdx := strings.Index(s, ":"); colonIdx >= 0 {
		info.variable = strings.TrimSpace(s[:colonIdx])
		labelsStr := s[colonIdx+1:]
		for _, label := range strings.Split(labelsStr, ":") {
			label = strings.TrimSpace(label)
			if label != "" {
				info.labels = append(info.labels, label)
			}
		}
	} else {
		info.variable = strings.TrimSpace(s)
	}

	return info
}

// traverseGraph executes the traversal and returns all matching paths
func (e *StorageExecutor) traverseGraph(match *TraversalMatch) []PathResult {
	// Get starting nodes
	var startNodes []*storage.Node
	if len(match.StartNode.labels) > 0 {
		startNodes, _ = e.storage.GetNodesByLabel(match.StartNode.labels[0])
	} else {
		startNodes = e.storage.GetAllNodes()
	}

	// Filter by properties
	if len(match.StartNode.properties) > 0 {
		var filtered []*storage.Node
		for _, n := range startNodes {
			if e.nodeMatchesProps(n, match.StartNode.properties) {
				filtered = append(filtered, n)
			}
		}
		startNodes = filtered
	}

	// OPTIMIZATION: Use parallel traversal for large start node sets
	// Threshold is MinBatchSize (default 200) - goroutine overhead hurts small traversals
	config := GetParallelConfig()
	if config.Enabled && len(startNodes) >= config.MinBatchSize {
		return e.traverseGraphParallel(match, startNodes, config)
	}

	return e.traverseGraphSequential(match, startNodes)
}

// traverseGraphSequential performs sequential traversal from start nodes
func (e *StorageExecutor) traverseGraphSequential(match *TraversalMatch, startNodes []*storage.Node) []PathResult {
	var results []PathResult

	for _, startNode := range startNodes {
		ctx := &TraversalContext{
			startNode: startNode,
			relTypes:  match.Relationship.Types,
			direction: match.Relationship.Direction,
			minHops:   match.Relationship.MinHops,
			maxHops:   match.Relationship.MaxHops,
			visited:   make(map[string]bool),
			nodeCache: make(map[storage.NodeID]*storage.Node),
		}

		paths := e.findPaths(ctx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode)
		results = append(results, paths...)
	}

	return results
}

// traverseGraphParallel performs parallel traversal from multiple start nodes
// Each goroutine gets its own TraversalContext to avoid data races
func (e *StorageExecutor) traverseGraphParallel(match *TraversalMatch, startNodes []*storage.Node, config ParallelConfig) []PathResult {
	numWorkers := config.MaxWorkers
	if numWorkers > len(startNodes) {
		numWorkers = len(startNodes)
	}

	// Channel for collecting results from workers
	type workerResult struct {
		paths []PathResult
	}
	resultsChan := make(chan workerResult, numWorkers)

	// Divide start nodes among workers
	chunkSize := (len(startNodes) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(startNodes) {
			break
		}
		if end > len(startNodes) {
			end = len(startNodes)
		}

		wg.Add(1)
		go func(workerNodes []*storage.Node) {
			defer wg.Done()

			var workerPaths []PathResult
			for _, startNode := range workerNodes {
				// Each goroutine gets its own context (no shared state)
				ctx := &TraversalContext{
					startNode: startNode,
					relTypes:  match.Relationship.Types,
					direction: match.Relationship.Direction,
					minHops:   match.Relationship.MinHops,
					maxHops:   match.Relationship.MaxHops,
					visited:   make(map[string]bool),
					nodeCache: make(map[storage.NodeID]*storage.Node),
				}

				paths := e.findPaths(ctx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode)
				workerPaths = append(workerPaths, paths...)
			}

			resultsChan <- workerResult{paths: workerPaths}
		}(startNodes[start:end])
	}

	// Close channel when all workers done
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	var allResults []PathResult
	for wr := range resultsChan {
		allResults = append(allResults, wr.paths...)
	}

	return allResults
}

// findPaths performs DFS to find all paths matching the pattern
func (e *StorageExecutor) findPaths(
	ctx *TraversalContext,
	currentNode *storage.Node,
	pathNodes []*storage.Node,
	pathEdges []*storage.Edge,
	depth int,
	endPattern *nodePatternInfo,
) []PathResult {
	var results []PathResult

	// OPTIMIZATION: Early termination if limit reached
	if ctx.limit > 0 && ctx.resultCount >= ctx.limit {
		return results
	}

	// Check if current path meets minimum length and endpoint requirements
	if depth >= ctx.minHops {
		if e.matchesEndPattern(currentNode, endPattern) {
			results = append(results, PathResult{
				Nodes:         append([]*storage.Node{}, pathNodes...),
				Relationships: append([]*storage.Edge{}, pathEdges...),
				Length:        depth,
			})
			ctx.resultCount++ // Track for early termination

			// Check again after adding result
			if ctx.limit > 0 && ctx.resultCount >= ctx.limit {
				return results
			}
		}
	}

	// Stop if we've reached max depth
	if depth >= ctx.maxHops {
		return results
	}

	// Get edges based on direction
	var edges []*storage.Edge
	switch ctx.direction {
	case "outgoing":
		edges, _ = e.storage.GetOutgoingEdges(currentNode.ID)
	case "incoming":
		edges, _ = e.storage.GetIncomingEdges(currentNode.ID)
	case "both":
		outgoing, _ := e.storage.GetOutgoingEdges(currentNode.ID)
		incoming, _ := e.storage.GetIncomingEdges(currentNode.ID)
		edges = append(outgoing, incoming...)
	}

	// Traverse each edge
	for _, edge := range edges {
		// Check relationship type filter
		if len(ctx.relTypes) > 0 {
			found := false
			for _, t := range ctx.relTypes {
				if edge.Type == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		// Get next node
		var nextNodeID storage.NodeID
		if ctx.direction == "outgoing" || (ctx.direction == "both" && edge.StartNode == currentNode.ID) {
			nextNodeID = edge.EndNode
		} else {
			nextNodeID = edge.StartNode
		}

		// Avoid cycles
		if ctx.visited[string(nextNodeID)] {
			continue
		}

		// OPTIMIZATION: Use node cache to avoid repeated lookups
		nextNode := ctx.nodeCache[nextNodeID]
		if nextNode == nil {
			var err error
			nextNode, err = e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}
			ctx.nodeCache[nextNodeID] = nextNode
		}

		// Mark as visited
		ctx.visited[string(nextNodeID)] = true

		// Recurse with optimized path copying (pre-allocate exact size)
		newPathNodes := make([]*storage.Node, len(pathNodes)+1)
		copy(newPathNodes, pathNodes)
		newPathNodes[len(pathNodes)] = nextNode

		newPathEdges := make([]*storage.Edge, len(pathEdges)+1)
		copy(newPathEdges, pathEdges)
		newPathEdges[len(pathEdges)] = edge

		subPaths := e.findPaths(ctx, nextNode, newPathNodes, newPathEdges, depth+1, endPattern)
		results = append(results, subPaths...)

		// Unmark for other paths
		ctx.visited[string(nextNodeID)] = false
	}

	return results
}

// matchesEndPattern checks if a node matches the end pattern requirements
func (e *StorageExecutor) matchesEndPattern(node *storage.Node, pattern *nodePatternInfo) bool {
	if pattern == nil {
		return true
	}

	// Check labels
	if len(pattern.labels) > 0 {
		for _, reqLabel := range pattern.labels {
			found := false
			for _, nodeLabel := range node.Labels {
				if nodeLabel == reqLabel {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}

	// Check properties
	return e.nodeMatchesProps(node, pattern.properties)
}

// PathContext holds node/relationship mappings for expression evaluation
type PathContext struct {
	nodes map[string]*storage.Node
	rels  map[string]*storage.Edge
}

// buildPathContext creates a context for evaluating expressions over a path
func (e *StorageExecutor) buildPathContext(path PathResult, match *TraversalMatch) PathContext {
	ctx := PathContext{
		nodes: make(map[string]*storage.Node),
		rels:  make(map[string]*storage.Edge),
	}

	// Map start node
	if match.StartNode.variable != "" && len(path.Nodes) > 0 {
		ctx.nodes[match.StartNode.variable] = path.Nodes[0]
	}

	// Map end node
	if match.EndNode.variable != "" && len(path.Nodes) > 1 {
		ctx.nodes[match.EndNode.variable] = path.Nodes[len(path.Nodes)-1]
	}

	// Map relationship
	if match.Relationship.Variable != "" && len(path.Relationships) > 0 {
		ctx.rels[match.Relationship.Variable] = path.Relationships[0]
	}

	return ctx
}

// shortestPath finds the shortest path between two nodes
func (e *StorageExecutor) shortestPath(startNode, endNode *storage.Node, relTypes []string, direction string, maxHops int) *PathResult {
	if startNode == nil || endNode == nil {
		return nil
	}

	// BFS for shortest path
	type queueItem struct {
		node *storage.Node
		path PathResult
	}

	queue := []queueItem{{
		node: startNode,
		path: PathResult{
			Nodes:         []*storage.Node{startNode},
			Relationships: []*storage.Edge{},
			Length:        0,
		},
	}}

	visited := map[string]bool{string(startNode.ID): true}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.path.Length >= maxHops {
			continue
		}

		// Get edges based on direction
		var edges []*storage.Edge
		switch direction {
		case "outgoing":
			edges, _ = e.storage.GetOutgoingEdges(current.node.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(current.node.ID)
		default:
			outgoing, _ := e.storage.GetOutgoingEdges(current.node.ID)
			incoming, _ := e.storage.GetIncomingEdges(current.node.ID)
			edges = append(outgoing, incoming...)
		}

		for _, edge := range edges {
			// Filter by relationship type
			if len(relTypes) > 0 {
				found := false
				for _, t := range relTypes {
					if edge.Type == t {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Get next node
			var nextNodeID storage.NodeID
			if direction == "outgoing" || (direction == "both" && edge.StartNode == current.node.ID) {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			if visited[string(nextNodeID)] {
				continue
			}

			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			newPath := PathResult{
				Nodes:         append(append([]*storage.Node{}, current.path.Nodes...), nextNode),
				Relationships: append(append([]*storage.Edge{}, current.path.Relationships...), edge),
				Length:        current.path.Length + 1,
			}

			// Check if we've reached the end
			if nextNodeID == endNode.ID {
				return &newPath
			}

			visited[string(nextNodeID)] = true
			queue = append(queue, queueItem{node: nextNode, path: newPath})
		}
	}

	return nil // No path found
}

// allShortestPaths finds all shortest paths between two nodes
func (e *StorageExecutor) allShortestPaths(startNode, endNode *storage.Node, relTypes []string, direction string, maxHops int) []PathResult {
	if startNode == nil || endNode == nil {
		return nil
	}

	var results []PathResult
	shortestLen := -1

	// BFS for all shortest paths
	type queueItem struct {
		node *storage.Node
		path PathResult
	}

	queue := []queueItem{{
		node: startNode,
		path: PathResult{
			Nodes:         []*storage.Node{startNode},
			Relationships: []*storage.Edge{},
			Length:        0,
		},
	}}

	// Track visited at each depth
	visitedDepth := map[string]int{string(startNode.ID): 0}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// If we've found a path, don't explore beyond that length
		if shortestLen >= 0 && current.path.Length >= shortestLen {
			continue
		}

		if current.path.Length >= maxHops {
			continue
		}

		// Get edges
		var edges []*storage.Edge
		switch direction {
		case "outgoing":
			edges, _ = e.storage.GetOutgoingEdges(current.node.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(current.node.ID)
		default:
			outgoing, _ := e.storage.GetOutgoingEdges(current.node.ID)
			incoming, _ := e.storage.GetIncomingEdges(current.node.ID)
			edges = append(outgoing, incoming...)
		}

		for _, edge := range edges {
			// Filter by type
			if len(relTypes) > 0 {
				found := false
				for _, t := range relTypes {
					if edge.Type == t {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			var nextNodeID storage.NodeID
			if direction == "outgoing" || (direction == "both" && edge.StartNode == current.node.ID) {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			// Allow revisit if at same depth (for multiple paths)
			prevDepth, seen := visitedDepth[string(nextNodeID)]
			if seen && prevDepth < current.path.Length+1 {
				continue
			}

			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			newPath := PathResult{
				Nodes:         append(append([]*storage.Node{}, current.path.Nodes...), nextNode),
				Relationships: append(append([]*storage.Edge{}, current.path.Relationships...), edge),
				Length:        current.path.Length + 1,
			}

			// Check if we've reached the end
			if nextNodeID == endNode.ID {
				if shortestLen < 0 {
					shortestLen = newPath.Length
				}
				if newPath.Length == shortestLen {
					results = append(results, newPath)
				}
				continue
			}

			visitedDepth[string(nextNodeID)] = current.path.Length + 1
			queue = append(queue, queueItem{node: nextNode, path: newPath})
		}
	}

	return results
}

// getRelType gets the type of a relationship - used for type(r) function
func (e *StorageExecutor) getRelType(relID storage.EdgeID) string {
	edge, err := e.storage.GetEdge(relID)
	if err != nil || edge == nil {
		return ""
	}
	return edge.Type
}

// filterPathsByWhere filters paths based on a WHERE clause condition.
// This evaluates conditions like "i.name = 'value'" against each path's context.
func (e *StorageExecutor) filterPathsByWhere(paths []PathResult, matches *TraversalMatch, whereClause string) []PathResult {
	if whereClause == "" {
		return paths
	}

	var filtered []PathResult
	for _, path := range paths {
		context := e.buildPathContext(path, matches)
		if e.evaluateWhereOnPath(whereClause, context) {
			filtered = append(filtered, path)
		}
	}
	return filtered
}

// evaluateWhereOnPath evaluates a WHERE condition against a path context.
// Handles conditions like: i.name = 'value', e.score < 90, etc.
func (e *StorageExecutor) evaluateWhereOnPath(whereClause string, context PathContext) bool {
	upperClause := strings.ToUpper(whereClause)

	// Handle AND conditions
	if idx := strings.Index(upperClause, " AND "); idx > 0 {
		left := strings.TrimSpace(whereClause[:idx])
		right := strings.TrimSpace(whereClause[idx+5:])
		return e.evaluateWhereOnPath(left, context) && e.evaluateWhereOnPath(right, context)
	}

	// Handle OR conditions
	if idx := strings.Index(upperClause, " OR "); idx > 0 {
		left := strings.TrimSpace(whereClause[:idx])
		right := strings.TrimSpace(whereClause[idx+4:])
		return e.evaluateWhereOnPath(left, context) || e.evaluateWhereOnPath(right, context)
	}

	// Handle comparison operators: =, <>, <, >, <=, >=
	operators := []string{"<>", "<=", ">=", "=", "<", ">"}
	for _, op := range operators {
		if idx := strings.Index(whereClause, op); idx > 0 {
			leftExpr := strings.TrimSpace(whereClause[:idx])
			rightExpr := strings.TrimSpace(whereClause[idx+len(op):])

			leftVal := e.evaluateExpressionWithContext(leftExpr, context.nodes, context.rels)
			rightVal := e.evaluatePathValue(rightExpr)

			return e.compareValues(leftVal, rightVal, op)
		}
	}

	// Handle CONTAINS
	if idx := strings.Index(upperClause, " CONTAINS "); idx > 0 {
		leftExpr := strings.TrimSpace(whereClause[:idx])
		rightExpr := strings.TrimSpace(whereClause[idx+10:])

		leftVal := e.evaluateExpressionWithContext(leftExpr, context.nodes, context.rels)
		rightVal := e.evaluatePathValue(rightExpr)

		leftStr, lok := leftVal.(string)
		rightStr, rok := rightVal.(string)
		if lok && rok {
			return strings.Contains(leftStr, rightStr)
		}
		return false
	}

	// Handle IS NULL / IS NOT NULL
	if strings.HasSuffix(upperClause, " IS NOT NULL") {
		expr := strings.TrimSpace(whereClause[:len(whereClause)-12])
		val := e.evaluateExpressionWithContext(expr, context.nodes, context.rels)
		return val != nil
	}
	if strings.HasSuffix(upperClause, " IS NULL") {
		expr := strings.TrimSpace(whereClause[:len(whereClause)-8])
		val := e.evaluateExpressionWithContext(expr, context.nodes, context.rels)
		return val == nil
	}

	return true // Default: pass through
}

// evaluatePathValue parses a literal value from a WHERE clause expression.
func (e *StorageExecutor) evaluatePathValue(expr string) interface{} {
	expr = strings.TrimSpace(expr)

	// Handle quoted strings
	if len(expr) >= 2 {
		first, last := expr[0], expr[len(expr)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
			return expr[1 : len(expr)-1]
		}
	}

	// Handle numbers
	if i, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f
	}

	// Handle booleans
	if strings.EqualFold(expr, "true") {
		return true
	}
	if strings.EqualFold(expr, "false") {
		return false
	}

	return expr
}

// compareValues compares two values using the given operator.
func (e *StorageExecutor) compareValues(left, right interface{}, op string) bool {
	// Handle nil cases
	if left == nil || right == nil {
		if op == "=" {
			return left == right
		}
		if op == "<>" {
			return left != right
		}
		return false
	}

	// String comparison
	leftStr, leftIsStr := left.(string)
	rightStr, rightIsStr := right.(string)
	if leftIsStr && rightIsStr {
		switch op {
		case "=":
			return leftStr == rightStr
		case "<>":
			return leftStr != rightStr
		case "<":
			return leftStr < rightStr
		case ">":
			return leftStr > rightStr
		case "<=":
			return leftStr <= rightStr
		case ">=":
			return leftStr >= rightStr
		}
	}

	// Numeric comparison - convert to float64 for comparison
	leftNum, leftOk := toFloat64(left)
	rightNum, rightOk := toFloat64(right)
	if leftOk && rightOk {
		switch op {
		case "=":
			return leftNum == rightNum
		case "<>":
			return leftNum != rightNum
		case "<":
			return leftNum < rightNum
		case ">":
			return leftNum > rightNum
		case "<=":
			return leftNum <= rightNum
		case ">=":
			return leftNum >= rightNum
		}
	}

	// Fallback: string comparison
	return fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right) && op == "="
}
