package cypher

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// APOC Path Procedures (CRITICAL for Mimir graph traversal)
// ========================================

// callApocPathSubgraphNodes implements apoc.path.subgraphNodes
// Syntax: CALL apoc.path.subgraphNodes(startNode, {maxLevel: n, relationshipFilter: 'TYPE'})
//
// This is the primary graph traversal procedure used by Mimir for:
//   - Knowledge graph exploration
//   - Relationship discovery
//   - Context gathering from connected nodes
//
// Config Parameters:
//   - maxLevel: Maximum traversal depth (default: 3)
//   - relationshipFilter: Filter by relationship types (e.g., "RELATES_TO|CONTAINS")
//   - labelFilter: Filter by node labels (e.g., "+Memory|-Archive")
//   - minLevel: Minimum traversal depth before returning results
//   - limit: Maximum number of nodes to return
//   - bfs: Use breadth-first search (default: true)
//
// Relationship Filter Syntax:
//   - "TYPE" - Match relationships of type TYPE in either direction
//   - ">TYPE" - Match outgoing relationships of type TYPE
//   - "<TYPE" - Match incoming relationships of type TYPE
//   - "TYPE1|TYPE2" - Match multiple types
//
// Label Filter Syntax:
//   - "+Label" - Only include nodes with Label
//   - "-Label" - Exclude nodes with Label
//   - "/Label" - Terminate traversal at nodes with Label (end nodes)
func (e *StorageExecutor) callApocPathSubgraphNodes(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"node"},
		Rows:    [][]interface{}{},
	}

	// Parse configuration and start node
	config := e.parseApocPathConfig(cypher)
	startNodeID := e.extractStartNodeID(cypher)

	// Get starting node(s)
	var startNodes []*storage.Node
	if startNodeID == "*" {
		// Special case: traverse from all nodes (when no specific start node)
		allNodes, err := e.storage.AllNodes()
		if err != nil {
			return nil, err
		}
		startNodes = allNodes
	} else if startNodeID != "" {
		if node, err := e.storage.GetNode(storage.NodeID(startNodeID)); err == nil && node != nil {
			startNodes = append(startNodes, node)
		}
	} else {
		// If no start node at all (parameter reference), return empty
		return result, nil
	}

	if len(startNodes) == 0 {
		return result, nil
	}

	// BFS traversal
	visited := make(map[string]bool)
	var resultNodes []*storage.Node

	for _, startNode := range startNodes {
		nodes := e.bfsTraversal(startNode, config, visited)
		resultNodes = append(resultNodes, nodes...)
	}

	// Apply limit if specified
	if config.limit > 0 && len(resultNodes) > config.limit {
		resultNodes = resultNodes[:config.limit]
	}

	// Convert to result rows
	for _, node := range resultNodes {
		result.Rows = append(result.Rows, []interface{}{node})
	}

	return result, nil
}

// apocPathConfig holds parsed APOC path configuration
type apocPathConfig struct {
	maxLevel          int
	minLevel          int
	relationshipTypes []string
	direction         string // "both", "outgoing", "incoming"
	includeLabels     []string
	excludeLabels     []string
	terminateLabels   []string
	limit             int
	bfs               bool
}

// parseApocPathConfig extracts configuration from APOC path calls
func (e *StorageExecutor) parseApocPathConfig(cypher string) apocPathConfig {
	config := apocPathConfig{
		maxLevel:  3,
		minLevel:  0,
		direction: "both",
		bfs:       true,
		limit:     0, // No limit
	}

	// Find config object { ... }
	configStart := strings.Index(cypher, "{")
	configEnd := strings.LastIndex(cypher, "}")
	if configStart == -1 || configEnd == -1 || configEnd <= configStart {
		return config
	}

	configStr := cypher[configStart+1 : configEnd]

	// Parse maxLevel using pre-compiled pattern from regex_patterns.go
	if match := apocMaxLevelPattern.FindStringSubmatch(configStr); len(match) > 1 {
		if level, err := strconv.Atoi(match[1]); err == nil && level > 0 {
			config.maxLevel = level
		}
	}

	// Parse minLevel using pre-compiled pattern
	if match := apocMinLevelPattern.FindStringSubmatch(configStr); len(match) > 1 {
		if level, err := strconv.Atoi(match[1]); err == nil {
			config.minLevel = level
		}
	}

	// Parse limit using pre-compiled pattern
	if match := apocLimitPattern.FindStringSubmatch(configStr); len(match) > 1 {
		if limit, err := strconv.Atoi(match[1]); err == nil {
			config.limit = limit
		}
	}

	// Parse relationshipFilter using pre-compiled pattern
	if match := apocRelFilterPattern.FindStringSubmatch(configStr); len(match) > 1 {
		filterStr := match[1]
		config.relationshipTypes, config.direction = parseRelationshipFilter(filterStr)
	}

	// Parse labelFilter using pre-compiled pattern
	if match := apocLabelFilterPattern.FindStringSubmatch(configStr); len(match) > 1 {
		filterStr := match[1]
		config.includeLabels, config.excludeLabels, config.terminateLabels = parseLabelFilter(filterStr)
	}

	// Parse bfs
	if strings.Contains(configStr, "bfs: false") || strings.Contains(configStr, "bfs:false") {
		config.bfs = false
	}

	return config
}

// parseRelationshipFilter parses a relationship filter string
func parseRelationshipFilter(filter string) (types []string, direction string) {
	direction = "both"

	// Handle direction prefix
	if strings.HasPrefix(filter, ">") {
		direction = "outgoing"
		filter = filter[1:]
	} else if strings.HasPrefix(filter, "<") {
		direction = "incoming"
		filter = filter[1:]
	}

	// Split by | for multiple types
	for _, t := range strings.Split(filter, "|") {
		t = strings.TrimSpace(t)
		if t != "" && t != ">" && t != "<" {
			types = append(types, t)
		}
	}

	return types, direction
}

// parseLabelFilter parses a label filter string
func parseLabelFilter(filter string) (include, exclude, terminate []string) {
	parts := strings.Split(filter, "|")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if strings.HasPrefix(part, "+") {
			include = append(include, part[1:])
		} else if strings.HasPrefix(part, "-") {
			exclude = append(exclude, part[1:])
		} else if strings.HasPrefix(part, "/") {
			terminate = append(terminate, part[1:])
		}
	}

	return include, exclude, terminate
}

// extractStartNodeID extracts the starting node ID from the CALL statement
func (e *StorageExecutor) extractStartNodeID(cypher string) string {
	// Look for node variable in MATCH clause
	// Pattern: MATCH (varName:Label {id: 'value'}) or MATCH (varName) WHERE varName.id = 'value'
	// Uses pre-compiled patterns from regex_patterns.go

	// Try to find a MATCH pattern with id property
	if match := apocNodeIdBracePattern.FindStringSubmatch(cypher); len(match) > 1 {
		return match[1]
	}

	// Try to find WHERE clause with id
	if match := apocWhereIdPattern.FindStringSubmatch(cypher); len(match) > 1 {
		return match[1]
	}

	// Try to find $nodeId parameter (would need to be substituted)
	if strings.Contains(cypher, "$nodeId") || strings.Contains(cypher, "$startNode") {
		return ""
	}

	// Return special marker for "traverse all" when no specific ID found
	return "*"
}

// bfsTraversal performs breadth-first traversal from a start node
func (e *StorageExecutor) bfsTraversal(startNode *storage.Node, config apocPathConfig, globalVisited map[string]bool) []*storage.Node {
	var results []*storage.Node

	// Queue: (node, level)
	type queueItem struct {
		node  *storage.Node
		level int
	}
	queue := []queueItem{{node: startNode, level: 0}}

	// Track visited for this traversal
	visited := make(map[string]bool)
	visited[string(startNode.ID)] = true

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		node := item.node
		level := item.level

		// Check if we should include this node
		if level >= config.minLevel && !globalVisited[string(node.ID)] {
			// Check label filters
			if passesLabelFilter(node, config.includeLabels, config.excludeLabels) {
				results = append(results, node)
				globalVisited[string(node.ID)] = true
			}
		}

		// Check if we should terminate at this node
		if isTerminateNode(node, config.terminateLabels) {
			continue
		}

		// Check if we've reached max level
		if level >= config.maxLevel {
			continue
		}

		// Get edges based on direction
		var edges []*storage.Edge
		switch config.direction {
		case "outgoing":
			edges, _ = e.storage.GetOutgoingEdges(node.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(node.ID)
		default: // "both"
			out, _ := e.storage.GetOutgoingEdges(node.ID)
			in, _ := e.storage.GetIncomingEdges(node.ID)
			edges = append(out, in...)
		}

		// Process each edge
		for _, edge := range edges {
			// Check relationship type filter
			if len(config.relationshipTypes) > 0 {
				found := false
				for _, t := range config.relationshipTypes {
					if edge.Type == t {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Get the other node
			var nextNodeID storage.NodeID
			if edge.StartNode == node.ID {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			// Skip if already visited
			if visited[string(nextNodeID)] {
				continue
			}
			visited[string(nextNodeID)] = true

			// Get the node and add to queue
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err == nil && nextNode != nil {
				queue = append(queue, queueItem{node: nextNode, level: level + 1})
			}
		}
	}

	return results
}

// passesLabelFilter checks if a node passes the label filter
func passesLabelFilter(node *storage.Node, include, exclude []string) bool {
	// Check exclude labels first
	for _, excLabel := range exclude {
		for _, nodeLabel := range node.Labels {
			if nodeLabel == excLabel {
				return false
			}
		}
	}

	// If no include labels specified, pass
	if len(include) == 0 {
		return true
	}

	// Check include labels
	for _, incLabel := range include {
		for _, nodeLabel := range node.Labels {
			if nodeLabel == incLabel {
				return true
			}
		}
	}

	return false
}

// isTerminateNode checks if traversal should terminate at this node
func isTerminateNode(node *storage.Node, terminateLabels []string) bool {
	for _, termLabel := range terminateLabels {
		for _, nodeLabel := range node.Labels {
			if nodeLabel == termLabel {
				return true
			}
		}
	}
	return false
}

// callApocPathExpand implements apoc.path.expand
// Syntax: CALL apoc.path.expand(startNode, relationshipFilter, labelFilter, minLevel, maxLevel)
//
// Returns paths (sequences of nodes and relationships) from the start node.
// Unlike subgraphNodes which returns just nodes, expand returns complete paths.
func (e *StorageExecutor) callApocPathExpand(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"path"},
		Rows:    [][]interface{}{},
	}

	// Parse parameters: (startNode, relationshipFilter, labelFilter, minLevel, maxLevel)
	params := e.parseApocPathExpandParams(cypher)
	if params.startNode == nil {
		// No start node found, return empty result
		return result, nil
	}

	// Build config from parameters
	config := apocPathConfig{
		maxLevel:          params.maxLevel,
		minLevel:          params.minLevel,
		relationshipTypes: params.relationshipTypes,
		direction:         params.direction,
		includeLabels:     params.includeLabels,
		excludeLabels:     params.excludeLabels,
		terminateLabels:   params.terminateLabels,
		limit:             0, // No limit for expand
		bfs:               true,
	}

	// Perform BFS traversal with path tracking
	paths := e.bfsPathTraversal(params.startNode, config)

	// Convert paths to result format
	for _, path := range paths {
		result.Rows = append(result.Rows, []interface{}{e.pathToMap(path)})
	}

	return result, nil
}

// apocPathExpandParams holds parsed parameters for apoc.path.expand
type apocPathExpandParams struct {
	startNode         *storage.Node
	relationshipTypes []string
	direction         string
	includeLabels     []string
	excludeLabels     []string
	terminateLabels   []string
	minLevel          int
	maxLevel          int
}

// parseApocPathExpandParams parses positional parameters from apoc.path.expand call
// Syntax: CALL apoc.path.expand(startNode, relationshipFilter, labelFilter, minLevel, maxLevel)
func (e *StorageExecutor) parseApocPathExpandParams(cypher string) apocPathExpandParams {
	params := apocPathExpandParams{
		minLevel:  1,
		maxLevel:  1,
		direction: "both",
	}

	// Find the procedure call and extract parameters
	upper := strings.ToUpper(cypher)
	callIdx := strings.Index(upper, "APOC.PATH.EXPAND")
	if callIdx == -1 {
		return params
	}

	// Find opening parenthesis
	rest := cypher[callIdx:]
	parenIdx := strings.Index(rest, "(")
	if parenIdx == -1 {
		return params
	}

	// Find matching closing parenthesis
	parenContent := rest[parenIdx+1:]
	depth := 1
	endIdx := -1
	for i, c := range parenContent {
		if c == '(' {
			depth++
		} else if c == ')' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}
	if endIdx == -1 {
		return params
	}

	// Split parameters carefully (respecting quotes and nested structures)
	paramStr := parenContent[:endIdx]
	parts := splitParamsCarefully(paramStr)

	// Parse startNode (first parameter)
	if len(parts) > 0 {
		firstParam := strings.TrimSpace(parts[0])
		// Try to extract node ID from MATCH clause first
		startNodeID := e.extractStartNodeID(cypher)
		if startNodeID != "" && startNodeID != "*" {
			if node, err := e.storage.GetNode(storage.NodeID(startNodeID)); err == nil && node != nil {
				params.startNode = node
			}
		} else if firstParam != "" && firstParam != "null" {
			// If no ID found, try to find node by variable name from MATCH clause
			// Look for pattern: MATCH (varName:Label {id: 'value'})
			varName := strings.Trim(firstParam, "'\"")
			if node := e.findNodeByVariableInMatch(cypher, varName); node != nil {
				params.startNode = node
			}
		}
	}

	// Parse relationshipFilter (second parameter)
	if len(parts) > 1 {
		relFilter := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		if relFilter != "" && relFilter != "null" {
			params.relationshipTypes, params.direction = parseRelationshipFilter(relFilter)
		}
	}

	// Parse labelFilter (third parameter)
	if len(parts) > 2 {
		labelFilter := strings.Trim(strings.TrimSpace(parts[2]), "'\"")
		if labelFilter != "" && labelFilter != "null" {
			params.includeLabels, params.excludeLabels, params.terminateLabels = parseLabelFilter(labelFilter)
		}
	}

	// Parse minLevel (fourth parameter)
	if len(parts) > 3 {
		minLevelStr := strings.TrimSpace(parts[3])
		if minLevel, err := strconv.Atoi(minLevelStr); err == nil {
			params.minLevel = minLevel
		}
	}

	// Parse maxLevel (fifth parameter)
	if len(parts) > 4 {
		maxLevelStr := strings.TrimSpace(parts[4])
		if maxLevel, err := strconv.Atoi(maxLevelStr); err == nil {
			params.maxLevel = maxLevel
		}
	}

	return params
}

// findNodeByVariableInMatch finds a node by variable name from a MATCH clause
func (e *StorageExecutor) findNodeByVariableInMatch(cypher, varName string) *storage.Node {
	// Look for MATCH clause with this variable
	// Pattern: MATCH (varName:Label {id: 'value'}) or MATCH (varName:Label {prop: 'value'})
	matchPattern := regexp.MustCompile(`(?i)MATCH\s*\(` + regexp.QuoteMeta(varName) + `[^)]*\)`)
	matches := matchPattern.FindStringSubmatch(cypher)
	if len(matches) == 0 {
		return nil
	}

	// Extract the node pattern
	nodePattern := matches[0]

	// Try to extract ID from {id: 'value'} pattern
	if idMatch := apocNodeIdBracePattern.FindStringSubmatch(nodePattern); len(idMatch) > 1 {
		if node, err := e.storage.GetNode(storage.NodeID(idMatch[1])); err == nil && node != nil {
			return node
		}
	}

	// Try to find node by parsing the pattern
	nodeInfo := e.parseNodePattern(nodePattern)
	if len(nodeInfo.labels) > 0 {
		candidates, _ := e.storage.GetNodesByLabel(nodeInfo.labels[0])
		for _, node := range candidates {
			if e.nodeMatchesProps(node, nodeInfo.properties) {
				return node
			}
		}
	}

	return nil
}

// bfsPathTraversal performs breadth-first traversal with path tracking
func (e *StorageExecutor) bfsPathTraversal(startNode *storage.Node, config apocPathConfig) []PathResult {
	var results []PathResult

	// Queue item tracks the path taken to reach this node
	type pathQueueItem struct {
		path  PathResult
		level int
	}

	// Start with path containing just the start node
	initialPath := PathResult{
		Nodes:         []*storage.Node{startNode},
		Relationships: []*storage.Edge{},
		Length:        0,
	}
	queue := []pathQueueItem{{path: initialPath, level: 0}}

	// Track visited nodes to avoid cycles
	visited := make(map[string]bool)
	visited[string(startNode.ID)] = true

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		currentPath := item.path
		level := item.level
		currentNode := currentPath.Nodes[len(currentPath.Nodes)-1]

		// Check if we should include this path
		if level >= config.minLevel && level <= config.maxLevel {
			// Check label filters on the end node
			if passesLabelFilter(currentNode, config.includeLabels, config.excludeLabels) {
				// Create a copy of the path for results
				pathCopy := PathResult{
					Nodes:         make([]*storage.Node, len(currentPath.Nodes)),
					Relationships: make([]*storage.Edge, len(currentPath.Relationships)),
					Length:        currentPath.Length,
				}
				copy(pathCopy.Nodes, currentPath.Nodes)
				copy(pathCopy.Relationships, currentPath.Relationships)
				results = append(results, pathCopy)
			}
		}

		// Check if we should terminate at this node
		if isTerminateNode(currentNode, config.terminateLabels) {
			continue
		}

		// Check if we've reached max level
		if level >= config.maxLevel {
			continue
		}

		// Get edges based on direction
		var edges []*storage.Edge
		switch config.direction {
		case "outgoing":
			edges, _ = e.storage.GetOutgoingEdges(currentNode.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(currentNode.ID)
		default: // "both"
			out, _ := e.storage.GetOutgoingEdges(currentNode.ID)
			in, _ := e.storage.GetIncomingEdges(currentNode.ID)
			edges = append(out, in...)
		}

		// Process each edge
		for _, edge := range edges {
			// Check relationship type filter
			if len(config.relationshipTypes) > 0 {
				found := false
				for _, t := range config.relationshipTypes {
					if edge.Type == t {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Get the other node
			var nextNodeID storage.NodeID
			if edge.StartNode == currentNode.ID {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			// Skip if already visited in this path (avoid cycles)
			// But allow revisiting nodes if they're reached via different paths
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			// Create new path by extending current path
			newPath := PathResult{
				Nodes:         make([]*storage.Node, len(currentPath.Nodes)+1),
				Relationships: make([]*storage.Edge, len(currentPath.Relationships)+1),
				Length:        currentPath.Length + 1,
			}
			copy(newPath.Nodes, currentPath.Nodes)
			copy(newPath.Relationships, currentPath.Relationships)
			newPath.Nodes[len(newPath.Nodes)-1] = nextNode
			newPath.Relationships[len(newPath.Relationships)-1] = edge

			// Add to queue
			queue = append(queue, pathQueueItem{path: newPath, level: level + 1})
		}
	}

	return results
}

// callApocPathSpanningTree implements apoc.path.spanningTree
// Syntax: CALL apoc.path.spanningTree(startNode, {maxLevel: n, relationshipFilter: 'TYPE', ...})
//
// Returns a spanning tree from the start node - a minimal tree that connects all reachable
// nodes without creating cycles. The tree is represented as a list of relationships.
//
// Config Parameters:
//   - maxLevel: Maximum traversal depth (default: -1 for unlimited)
//   - minLevel: Minimum traversal depth before returning results (default: 0)
//   - relationshipFilter: Filter by relationship types (e.g., "RELATES_TO|CONTAINS")
//   - labelFilter: Filter by node labels (e.g., "+Memory|-Archive")
//   - limit: Maximum number of relationships to return
//   - bfs: Use breadth-first search (default: true, DFS if false)
//
// Returns: List of relationships that form the spanning tree
func (e *StorageExecutor) callApocPathSpanningTree(cypher string) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{"path"},
		Rows:    [][]interface{}{},
	}

	// Parse configuration and start node
	config := e.parseApocPathConfig(cypher)
	if config.maxLevel == 3 { // Default from parseApocPathConfig
		config.maxLevel = -1 // For spanning tree, default to unlimited
	}
	startNodeID := e.extractStartNodeID(cypher)

	// Get starting node
	if startNodeID == "" || startNodeID == "*" {
		// Spanning tree requires a specific start node
		return result, fmt.Errorf("apoc.path.spanningTree requires a specific start node")
	}

	startNode, err := e.storage.GetNode(storage.NodeID(startNodeID))
	if err != nil || startNode == nil {
		return result, nil
	}

	// Build spanning tree using BFS or DFS
	var treeEdges []*storage.Edge
	if config.bfs {
		treeEdges = e.bfsSpanningTree(startNode, config)
	} else {
		treeEdges = e.dfsSpanningTree(startNode, config)
	}

	// Apply limit if specified
	if config.limit > 0 && len(treeEdges) > config.limit {
		treeEdges = treeEdges[:config.limit]
	}

	// Convert edges to path format
	// Each path contains the edge and its connected nodes
	for _, edge := range treeEdges {
		// Get the nodes
		startNodeObj, _ := e.storage.GetNode(edge.StartNode)
		endNodeObj, _ := e.storage.GetNode(edge.EndNode)

		if startNodeObj != nil && endNodeObj != nil {
			path := map[string]interface{}{
				"nodes": []interface{}{
					startNodeObj,
					endNodeObj,
				},
				"relationships": []interface{}{
					map[string]interface{}{
						"_edgeId":    string(edge.ID),
						"type":       edge.Type,
						"properties": edge.Properties,
						"startNode":  string(edge.StartNode),
						"endNode":    string(edge.EndNode),
					},
				},
				"length": 1,
			}
			result.Rows = append(result.Rows, []interface{}{path})
		}
	}

	return result, nil
}

// bfsSpanningTree builds a spanning tree using breadth-first search
func (e *StorageExecutor) bfsSpanningTree(startNode *storage.Node, config apocPathConfig) []*storage.Edge {
	var treeEdges []*storage.Edge
	visited := make(map[string]bool)

	// Queue: (node, level, parentEdge)
	type queueItem struct {
		node       *storage.Node
		level      int
		parentEdge *storage.Edge
	}
	queue := []queueItem{{node: startNode, level: 0, parentEdge: nil}}
	visited[string(startNode.ID)] = true

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		node := item.node
		level := item.level

		// Add the edge that got us here (if any) and if level > minLevel
		// Note: edges connect level N to level N+1, so check level > minLevel not level >= minLevel
		if item.parentEdge != nil && level > config.minLevel {
			treeEdges = append(treeEdges, item.parentEdge)
		}

		// Check if we should terminate at this node
		if isTerminateNode(node, config.terminateLabels) {
			continue
		}

		// Check if we've reached max level
		if config.maxLevel >= 0 && level >= config.maxLevel {
			continue
		}

		// Get edges based on direction
		var edges []*storage.Edge
		switch config.direction {
		case "outgoing":
			edges, _ = e.storage.GetOutgoingEdges(node.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(node.ID)
		default: // "both"
			out, _ := e.storage.GetOutgoingEdges(node.ID)
			in, _ := e.storage.GetIncomingEdges(node.ID)
			edges = append(out, in...)
		}

		// Process each edge
		for _, edge := range edges {
			// Check relationship type filter
			if len(config.relationshipTypes) > 0 {
				found := false
				for _, t := range config.relationshipTypes {
					if edge.Type == t {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Get the other node
			var nextNodeID storage.NodeID
			if edge.StartNode == node.ID {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			// Skip if already visited (no cycles in spanning tree)
			if visited[string(nextNodeID)] {
				continue
			}
			visited[string(nextNodeID)] = true

			// Get the node
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			// Check label filters
			if !passesLabelFilter(nextNode, config.includeLabels, config.excludeLabels) {
				continue
			}

			// Add to queue with this edge
			queue = append(queue, queueItem{
				node:       nextNode,
				level:      level + 1,
				parentEdge: edge,
			})
		}
	}

	return treeEdges
}

// dfsSpanningTree builds a spanning tree using depth-first search
func (e *StorageExecutor) dfsSpanningTree(startNode *storage.Node, config apocPathConfig) []*storage.Edge {
	var treeEdges []*storage.Edge
	visited := make(map[string]bool)

	// Stack: (node, level, parentEdge)
	type stackItem struct {
		node       *storage.Node
		level      int
		parentEdge *storage.Edge
	}
	stack := []stackItem{{node: startNode, level: 0, parentEdge: nil}}
	visited[string(startNode.ID)] = true

	for len(stack) > 0 {
		// Pop from stack
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node := item.node
		level := item.level

		// Add the edge that got us here (if any) and if level > minLevel
		// Note: edges connect level N to level N+1, so check level > minLevel not level >= minLevel
		if item.parentEdge != nil && level > config.minLevel {
			treeEdges = append(treeEdges, item.parentEdge)
		}

		// Check if we should terminate at this node
		if isTerminateNode(node, config.terminateLabels) {
			continue
		}

		// Check if we've reached max level
		if config.maxLevel >= 0 && level >= config.maxLevel {
			continue
		}

		// Get edges based on direction
		var edges []*storage.Edge
		switch config.direction {
		case "outgoing":
			edges, _ = e.storage.GetOutgoingEdges(node.ID)
		case "incoming":
			edges, _ = e.storage.GetIncomingEdges(node.ID)
		default: // "both"
			out, _ := e.storage.GetOutgoingEdges(node.ID)
			in, _ := e.storage.GetIncomingEdges(node.ID)
			edges = append(out, in...)
		}

		// Process each edge (in reverse for DFS to maintain order)
		for i := len(edges) - 1; i >= 0; i-- {
			edge := edges[i]

			// Check relationship type filter
			if len(config.relationshipTypes) > 0 {
				found := false
				for _, t := range config.relationshipTypes {
					if edge.Type == t {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Get the other node
			var nextNodeID storage.NodeID
			if edge.StartNode == node.ID {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			// Skip if already visited (no cycles in spanning tree)
			if visited[string(nextNodeID)] {
				continue
			}
			visited[string(nextNodeID)] = true

			// Get the node
			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			// Check label filters
			if !passesLabelFilter(nextNode, config.includeLabels, config.excludeLabels) {
				continue
			}

			// Push to stack with this edge
			stack = append(stack, stackItem{
				node:       nextNode,
				level:      level + 1,
				parentEdge: edge,
			})
		}
	}

	return treeEdges
}
