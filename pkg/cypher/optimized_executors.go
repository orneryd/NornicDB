// Package cypher provides optimized query executors for specific patterns.
//
// These executors implement specialized algorithms that are significantly faster
// than generic traversal for certain query patterns. Each executor is designed
// for a specific pattern detected by DetectQueryPattern(ctx).
//
// Performance characteristics:
//   - MutualRelationship: O(E) instead of O(N * D²)
//   - IncomingCountAgg: O(E) instead of O(N * separate_calls)
//   - EdgePropertyAgg: delegates to the clause pipeline (shared owner)
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
		// Parse RETURN clause to get actual column names/aliases
		returnItems := e.extractReturnItemsFromQuery(ctx, query)
		columns := e.buildColumnsFromReturnItems(returnItems)

		result := &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{},
			Stats:   &QueryStats{},
		}

		// Get edges by type directly (MUCH faster than AllEdges + filter)
		// This uses the edge type index for O(edges_of_type) instead of O(all_edges)
		edgeList, err := e.storage.GetEdgesByType(ctx, patternInfo.RelType)
		if err != nil {
			return nil, false
		}

		// Build edge set for O(1) reverse lookup
		edgeSet := make(map[string]bool, len(edgeList))
		for _, edge := range edgeList {
			key := string(edge.StartNode) + ":" + string(edge.EndNode)
			edgeSet[key] = true
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
					startNode := e.getNodeCached(ctx, edge.StartNode, nodeCache)
					endNode := e.getNodeCached(ctx, edge.EndNode, nodeCache)

					if startNode != nil && endNode != nil {
						startName := e.getPropertyString(startNode, "name")
						endName := e.getPropertyString(endNode, "name")
						result.Rows = append(result.Rows, []interface{}{startName, endName})
					}
				}
			}
		}

		// Apply LIMIT from pattern info
		if patternInfo.Limit > 0 && len(result.Rows) > patternInfo.Limit {
			result.Rows = result.Rows[:patternInfo.Limit]
		}

		return result, true

	case PatternIncomingCountAgg, PatternOutgoingCountAgg:
		result, err := e.executeRelationshipCountOptimized(ctx, query, patternInfo)
		if err == nil {
			return result, true
		}

	case PatternEdgePropertyAgg:
		// The detector-supported edge-property aggregate shape runs on the
		// shared clause pipeline, which owns grouping, null-only groups,
		// projection width and aggregate typing. A shape the pipeline
		// declines or fails stays unhandled so callers keep the general path.
		outcome := e.executePipeline(ctx, query)
		if outcome.handled() {
			return outcome.result, true
		}

	case PatternLargeResultSet:
		// Large result set optimization is applied within existing traversal
		// by using batch node lookups - handled in traversal.go
		return nil, false
	}

	return nil, false
}

// =============================================================================
// Relationship Count Aggregation Optimization
// Pattern: MATCH (x)<-[:TYPE]-(y) or (x)-[:TYPE]->(y), grouping by x
// Algorithm: Single pass through all edges, build count map
// =============================================================================

func (e *StorageExecutor) executeRelationshipCountOptimized(ctx context.Context, query string, info PatternInfo) (*ExecuteResult, error) {
	// Parse RETURN clause to get actual column names/aliases
	returnItems := e.extractReturnItemsFromQuery(ctx, query)
	columns := e.buildColumnsFromReturnItems(returnItems)

	result := &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Get all edges
	allEdges, err := e.storage.AllEdges(ctx)
	if err != nil {
		return nil, err
	}

	// Both directed count patterns share aggregation, ordering and projection;
	// only the grouped endpoint differs.
	counts := make(map[storage.NodeID]int64)
	relType := info.RelType

	for _, edge := range allEdges {
		if relType == "" || edge.Type == relType {
			nodeID := edge.EndNode
			if info.Pattern == PatternOutgoingCountAgg {
				nodeID = edge.StartNode
			}
			counts[nodeID]++
		}
	}

	// Convert to result rows
	type countRow struct {
		nodeID storage.NodeID
		count  int64
	}
	rows := make([]countRow, 0, len(counts))
	for nodeID, count := range counts {
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
		node := e.getNodeCached(ctx, row.nodeID, nodeCache)
		if node != nil {
			name := e.getPropertyString(node, "name")
			result.Rows = append(result.Rows, []interface{}{name, row.count})
		}
	}

	return result, nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// extractReturnItemsFromQuery extracts RETURN items from a Cypher query
func (e *StorageExecutor) extractReturnItemsFromQuery(ctx context.Context, query string) []returnItem {
	returnIdx := findKeywordIndex(ctx, query, "RETURN")
	if returnIdx == -1 {
		return nil
	}

	returnPart := strings.TrimSpace(query[returnIdx+6:])
	return e.parseReturnItems(ctx, returnPart)
}

// getNodeCached retrieves a node, using cache to avoid repeated lookups
func (e *StorageExecutor) getNodeCached(ctx context.Context, id storage.NodeID, cache map[storage.NodeID]*storage.Node) *storage.Node {
	if node, exists := cache[id]; exists {
		return node
	}
	node, err := e.getQueryNode(ctx, e.storage, id)
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
func (e *StorageExecutor) BatchGetNodes(ctx context.Context, ids []storage.NodeID) map[storage.NodeID]*storage.Node {
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	result := make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		node, err := e.storage.GetNode(ctx, id)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				recordExpressionFailure(ctx, canceled)
				return nil
			}
			continue
		}
		if node != nil {
			result[id] = node
		}
	}
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	return result
}
