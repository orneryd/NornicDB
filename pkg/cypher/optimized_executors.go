// Package cypher provides optimized query executors for specific patterns.
//
// These executors implement specialized algorithms that are significantly faster
// than generic traversal for certain query patterns. Each executor is designed
// for a specific pattern detected by DetectQueryPattern().
//
// Performance characteristics:
//   - MutualRelationship: O(E) instead of O(N * D²)
//   - IncomingCountAgg: O(E) instead of O(N * separate_calls)
//   - EdgePropertyAgg: O(E) single-pass accumulation
//   - LargeResultSet: Batch node lookups, pre-allocation
package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ExecuteOptimized attempts to execute a query using an optimized path.
// Returns (result, true) if optimization was applied, (nil, false) otherwise.
func (e *StorageExecutor) ExecuteOptimized(ctx context.Context, query string, patternInfo PatternInfo) (*ExecuteResult, bool) {
	switch patternInfo.Pattern {
	case PatternMutualRelationship:
		result, err := e.executeMutualRelationshipOptimized(ctx, query, patternInfo)
		if err == nil {
			return result, true
		}
		// Fall through to generic on error

	case PatternIncomingCountAgg:
		result, err := e.executeIncomingCountOptimized(ctx, query, patternInfo)
		if err == nil {
			return result, true
		}

	case PatternOutgoingCountAgg:
		result, err := e.executeOutgoingCountOptimized(ctx, query, patternInfo)
		if err == nil {
			return result, true
		}

	case PatternEdgePropertyAgg:
		result, err := e.executeEdgePropertyAggOptimized(ctx, query, patternInfo)
		if err == nil {
			return result, true
		}

	case PatternLargeResultSet:
		// Large result set optimization is applied within existing traversal
		// by using batch node lookups - handled in traversal.go
		return nil, false
	}

	return nil, false
}

// =============================================================================
// Mutual Relationship Optimization
// Pattern: (a)-[:TYPE]->(b)-[:TYPE]->(a)
// Algorithm: Single pass - build edge set, then find reverse pairs
// =============================================================================

func (e *StorageExecutor) executeMutualRelationshipOptimized(ctx context.Context, query string, info PatternInfo) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{info.StartVar + ".name", info.EndVar + ".name"},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Get all edges (we'll filter by type)
	allEdges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	// Build edge set: key = "source:target" for edges of the right type
	// Also build reverse lookup for quick pair finding
	edgeSet := make(map[string]bool)
	edgeList := make([]*storage.Edge, 0)
	normalizedType := strings.ToLower(info.RelType)

	for _, edge := range allEdges {
		if strings.ToLower(edge.Type) == normalizedType {
			key := string(edge.StartNode) + ":" + string(edge.EndNode)
			edgeSet[key] = true
			edgeList = append(edgeList, edge)
		}
	}

	// Find mutual pairs: for each edge A→B, check if B→A exists
	seenPairs := make(map[string]bool) // Avoid duplicates
	nodeCache := make(map[storage.NodeID]*storage.Node)

	for _, edge := range edgeList {
		reverseKey := string(edge.EndNode) + ":" + string(edge.StartNode)
		if edgeSet[reverseKey] {
			// Found mutual relationship!
			// Use ordered pair to avoid duplicates (smaller ID first)
			var pairKey string
			if edge.StartNode < edge.EndNode {
				pairKey = string(edge.StartNode) + ":" + string(edge.EndNode)
			} else {
				pairKey = string(edge.EndNode) + ":" + string(edge.StartNode)
			}

			if !seenPairs[pairKey] {
				seenPairs[pairKey] = true

				// Get node properties
				startNode := e.getNodeCached(edge.StartNode, nodeCache)
				endNode := e.getNodeCached(edge.EndNode, nodeCache)

				if startNode != nil && endNode != nil {
					startName := e.getPropertyString(startNode, "name")
					endName := e.getPropertyString(endNode, "name")
					result.Rows = append(result.Rows, []interface{}{startName, endName})
				}
			}
		}
	}

	// Apply LIMIT from pattern info
	if info.Limit > 0 && len(result.Rows) > info.Limit {
		result.Rows = result.Rows[:info.Limit]
	}

	return result, nil
}

// =============================================================================
// Incoming Count Aggregation Optimization
// Pattern: MATCH (x)<-[:TYPE]-(y) RETURN x.prop, count(y)
// Algorithm: Single pass through all edges, build count map
// =============================================================================

func (e *StorageExecutor) executeIncomingCountOptimized(ctx context.Context, query string, info PatternInfo) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{info.StartVar + ".name", "count"},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Get all edges
	allEdges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	// Build count map: EndNode → count of incoming edges of this type
	incomingCount := make(map[storage.NodeID]int64)
	normalizedType := strings.ToLower(info.RelType)

	for _, edge := range allEdges {
		if normalizedType == "" || strings.ToLower(edge.Type) == normalizedType {
			incomingCount[edge.EndNode]++
		}
	}

	// Convert to result rows
	type countRow struct {
		nodeID storage.NodeID
		count  int64
	}
	rows := make([]countRow, 0, len(incomingCount))
	for nodeID, count := range incomingCount {
		rows = append(rows, countRow{nodeID: nodeID, count: count})
	}

	// Sort by count descending (common for "top followers" queries)
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].count > rows[j].count
	})

	// Apply LIMIT
	limit := len(rows)
	if info.Limit > 0 && info.Limit < limit {
		limit = info.Limit
	}

	// Build result with node properties
	nodeCache := make(map[storage.NodeID]*storage.Node)
	for i := 0; i < limit; i++ {
		row := rows[i]
		node := e.getNodeCached(row.nodeID, nodeCache)
		if node != nil {
			name := e.getPropertyString(node, "name")
			result.Rows = append(result.Rows, []interface{}{name, row.count})
		}
	}

	return result, nil
}

// =============================================================================
// Outgoing Count Aggregation Optimization
// Pattern: MATCH (x)-[:TYPE]->(y) RETURN x.prop, count(y)
// Algorithm: Single pass through all edges, build count map
// =============================================================================

func (e *StorageExecutor) executeOutgoingCountOptimized(ctx context.Context, query string, info PatternInfo) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{info.StartVar + ".name", "count"},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Get all edges
	allEdges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	// Build count map: StartNode → count of outgoing edges of this type
	outgoingCount := make(map[storage.NodeID]int64)
	normalizedType := strings.ToLower(info.RelType)

	for _, edge := range allEdges {
		if normalizedType == "" || strings.ToLower(edge.Type) == normalizedType {
			outgoingCount[edge.StartNode]++
		}
	}

	// Convert to result rows
	type countRow struct {
		nodeID storage.NodeID
		count  int64
	}
	rows := make([]countRow, 0, len(outgoingCount))
	for nodeID, count := range outgoingCount {
		rows = append(rows, countRow{nodeID: nodeID, count: count})
	}

	// Sort by count descending
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].count > rows[j].count
	})

	// Apply LIMIT
	limit := len(rows)
	if info.Limit > 0 && info.Limit < limit {
		limit = info.Limit
	}

	// Build result with node properties
	nodeCache := make(map[storage.NodeID]*storage.Node)
	for i := 0; i < limit; i++ {
		row := rows[i]
		node := e.getNodeCached(row.nodeID, nodeCache)
		if node != nil {
			name := e.getPropertyString(node, "name")
			result.Rows = append(result.Rows, []interface{}{name, row.count})
		}
	}

	return result, nil
}

// =============================================================================
// Edge Property Aggregation Optimization
// Pattern: MATCH ()-[r:TYPE]->() RETURN avg(r.prop), count(r)
// Algorithm: Single pass accumulation
// =============================================================================

type edgeAggStats struct {
	sum      float64
	count    int64
	min      float64
	max      float64
	hasValue bool
}

func (e *StorageExecutor) executeEdgePropertyAggOptimized(ctx context.Context, query string, info PatternInfo) (*ExecuteResult, error) {
	// This optimization handles queries like:
	// MATCH (c)-[r:REVIEWED]->(p) RETURN p.name, avg(r.rating), count(r)
	// GROUP BY p (the end node)

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Get all edges
	allEdges, err := e.storage.AllEdges()
	if err != nil {
		return nil, err
	}

	// Build aggregation map: EndNode → stats
	aggMap := make(map[storage.NodeID]*edgeAggStats)

	for _, edge := range allEdges {
		// Get property value
		propVal, exists := edge.Properties[info.AggProperty]
		if !exists {
			continue
		}

		// Convert to float64
		var numVal float64
		switch v := propVal.(type) {
		case float64:
			numVal = v
		case int64:
			numVal = float64(v)
		case int:
			numVal = float64(v)
		default:
			continue
		}

		// Get or create stats
		stats, exists := aggMap[edge.EndNode]
		if !exists {
			stats = &edgeAggStats{min: numVal, max: numVal}
			aggMap[edge.EndNode] = stats
		}

		// Update stats
		stats.sum += numVal
		stats.count++
		stats.hasValue = true
		if numVal < stats.min {
			stats.min = numVal
		}
		if numVal > stats.max {
			stats.max = numVal
		}
	}

	// Build columns based on aggregation functions
	result.Columns = append(result.Columns, "name")
	for _, fn := range info.AggFunctions {
		result.Columns = append(result.Columns, fn)
	}

	// Convert to result rows
	type aggRow struct {
		nodeID storage.NodeID
		stats  *edgeAggStats
	}
	rows := make([]aggRow, 0, len(aggMap))
	for nodeID, stats := range aggMap {
		if stats.hasValue {
			rows = append(rows, aggRow{nodeID: nodeID, stats: stats})
		}
	}

	// Sort by avg descending (common pattern)
	sort.Slice(rows, func(i, j int) bool {
		avgI := rows[i].stats.sum / float64(rows[i].stats.count)
		avgJ := rows[j].stats.sum / float64(rows[j].stats.count)
		return avgI > avgJ
	})

	// Apply LIMIT
	limit := len(rows)
	if info.Limit > 0 && info.Limit < limit {
		limit = info.Limit
	}

	// Build result with node properties
	nodeCache := make(map[storage.NodeID]*storage.Node)
	for i := 0; i < limit; i++ {
		row := rows[i]
		node := e.getNodeCached(row.nodeID, nodeCache)
		if node == nil {
			continue
		}

		resultRow := []interface{}{e.getPropertyString(node, "name")}
		for _, fn := range info.AggFunctions {
			switch fn {
			case "count":
				resultRow = append(resultRow, row.stats.count)
			case "sum":
				resultRow = append(resultRow, row.stats.sum)
			case "avg":
				resultRow = append(resultRow, row.stats.sum/float64(row.stats.count))
			case "min":
				resultRow = append(resultRow, row.stats.min)
			case "max":
				resultRow = append(resultRow, row.stats.max)
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	return result, nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// getNodeCached retrieves a node, using cache to avoid repeated lookups
func (e *StorageExecutor) getNodeCached(id storage.NodeID, cache map[storage.NodeID]*storage.Node) *storage.Node {
	if node, exists := cache[id]; exists {
		return node
	}
	node, err := e.storage.GetNode(id)
	if err != nil {
		return nil
	}
	cache[id] = node
	return node
}

// getPropertyString extracts a string property from a node
func (e *StorageExecutor) getPropertyString(node *storage.Node, prop string) string {
	if node == nil || node.Properties == nil {
		return ""
	}
	if val, exists := node.Properties[prop]; exists {
		return fmt.Sprintf("%v", val)
	}
	return ""
}

// BatchGetNodes retrieves multiple nodes in a single operation
// Used for large result set optimization
func (e *StorageExecutor) BatchGetNodes(ids []storage.NodeID) map[storage.NodeID]*storage.Node {
	result := make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		if node, err := e.storage.GetNode(id); err == nil && node != nil {
			result[id] = node
		}
	}
	return result
}
