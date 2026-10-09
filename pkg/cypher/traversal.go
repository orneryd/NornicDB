// Package cypher provides graph traversal operations for NornicDB.
// This file implements relationship pattern matching, variable-length paths,
// and shortest path algorithms for Neo4j-compatible traversal queries.

package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// VarLengthUnboundedMaxHops is the depth cap applied when a variable-length
// relationship pattern is written without an explicit upper bound — for
// example `[*]`, `[*..]`, or `[*N..]`. The previous defaults (10 and 100) were
// surprising in practice: BFS traversals silently returned no rows on graphs
// whose actual diameter exceeded the cap. This sentinel is large enough that
// it acts effectively unbounded for any realistic graph (BFS terminates when
// the frontier is exhausted) while still keeping the field a plain int so all
// downstream `>=` comparisons stay correct.
const VarLengthUnboundedMaxHops = 1 << 24 // ~16.7M

// PathResult represents a path through the graph
type PathResult struct {
	Nodes          []*storage.Node
	Relationships  []*storage.Edge
	Length         int
	SegmentLengths []int
}

// TraversalContext holds state during graph traversal
type TraversalContext struct {
	startNode        *storage.Node
	endNode          *storage.Node
	relTypes         []string // Allowed relationship types (empty = any)
	relTypeSet       map[string]struct{}
	relProperties    map[string]interface{}
	direction        string // "outgoing", "incoming", "both"
	minHops          int
	maxHops          int
	usedEdges        map[storage.EdgeID]bool
	paths            []PathResult
	nodeCache        map[storage.NodeID]*storage.Node // Cache for batch-fetched nodes
	limit            int                              // OPTIMIZATION: Early termination limit (0 = no limit)
	resultCount      int                              // Count of results found so far
	temporalViewport TemporalViewport
	temporalChecker  temporalCurrentNodeChecker
	// cancelCtx is consulted periodically inside findPaths so callers can
	// abandon long traversals on client disconnect / server shutdown. It is
	// allowed to be nil for legacy call sites that pass through Background.
	cancelCtx context.Context
	// findPathsCalls counts entries into the recursive search; used to amortise
	// the ctx.Err() probe (one check per N calls).
	findPathsCalls int
	// endNodeID, when set, is the only node a path may end at (a shortestPath
	// pair, #863).
	endNodeID storage.NodeID
	// deepest is the greatest depth the search reached.
	deepest int
	// endpointsNeedOnlyExist is set when nothing in the statement reads the
	// nodes the traversal reaches (traversalEndpointsNeedOnlyExist).
	endpointsNeedOnlyExist bool
	// relationshipsNeedOnlyHeaders is set when nothing in the statement reads
	// the relationships' properties (traversalRelationshipsNeedOnlyHeaders).
	relationshipsNeedOnlyHeaders bool
}

// traversalEndpointsNeedOnlyExist reports whether the nodes a traversal
// reaches are needed only to exist and be visible: the end node is anonymous
// with no labels or property map, and no path variable exposes the nodes.
// The traversal then asks storage whether each one is visible
// (storage.RelationshipEndpointChecker) instead of reading it.
func traversalEndpointsNeedOnlyExist(match *TraversalMatch) bool {
	return !match.IsChained && match.PathVariable == "" && match.EndNode.variable == "" &&
		len(match.EndNode.labels) == 0 && len(match.EndNode.properties) == 0
}

// traversalRelationshipsNeedOnlyHeaders reports whether a traversal needs
// its relationships only for their ID, type and endpoints: the relationship
// is anonymous with no property map and no path variable exposes it. The
// traversal then lists them through storage.EdgeHeaderReader, which can
// answer without reading the relationship records.
func traversalRelationshipsNeedOnlyHeaders(match *TraversalMatch) bool {
	return !match.IsChained && match.PathVariable == "" && match.Relationship.Variable == "" && len(match.Relationship.Properties) == 0
}

// traversalEdges lists the relationships a traversal step expands from node
// in direction ("outgoing" or "incoming"): headers when the statement needs
// no relationship properties and storage can list them, full relationships
// otherwise.
func (e *StorageExecutor) traversalEdges(ctx *TraversalContext, nodeID storage.NodeID, outgoing bool) []*storage.Edge {
	if reader, ok := e.storage.(storage.EdgeHeaderReader); ok && ctx.relationshipsNeedOnlyHeaders {
		var edges []*storage.Edge
		var answered bool
		var err error
		if outgoing {
			edges, answered, err = reader.OutgoingEdgeHeaders(nodeID)
		} else {
			edges, answered, err = reader.IncomingEdgeHeaders(nodeID)
		}
		if err != nil && ctx.cancelCtx != nil {
			recordExpressionFailure(ctx.cancelCtx, err)
			return nil
		}
		if answered {
			return edges
		}
	}
	var edges []*storage.Edge
	var err error
	if outgoing {
		edges, err = e.storage.GetOutgoingEdges(nodeID)
	} else {
		edges, err = e.storage.GetIncomingEdges(nodeID)
	}
	if err != nil && ctx.cancelCtx != nil {
		recordExpressionFailure(ctx.cancelCtx, err)
	}
	return edges
}

func buildRelTypeSet(relTypes []string) map[string]struct{} {
	if len(relTypes) <= 1 {
		return nil
	}
	set := make(map[string]struct{}, len(relTypes))
	for _, relType := range relTypes {
		set[relType] = struct{}{}
	}
	return set
}

// RelationshipPattern represents a parsed relationship pattern
type RelationshipPattern struct {
	Variable       string   // r in [r:TYPE]
	Types          []string // TYPE in [r:TYPE|OTHER]
	Direction      string   // "outgoing" (-[r]->), "incoming" (<-[r]-), "both" (-[r]-)
	MinHops        int      // min in [*min..max]
	MaxHops        int      // max in [*min..max]
	VariableLength bool     // whether the pattern explicitly uses `*`
	Properties     map[string]interface{}
}

// parseRelationshipPattern parses patterns like -[r:TYPE {props}]->
func (e *StorageExecutor) parseRelationshipPattern(ctx context.Context, pattern string) *RelationshipPattern {
	result := &RelationshipPattern{
		Direction:  "both",
		MinHops:    1,
		MaxHops:    1,
		Properties: make(map[string]interface{}),
	}

	// Determine direction from both ends before trimming either marker. A
	// relationship carrying arrowheads at both ends is traversable in either
	// direction; neither marker may overwrite the other.
	hasIncomingArrow := strings.HasPrefix(pattern, "<-")
	hasOutgoingArrow := strings.HasSuffix(pattern, "->")
	switch {
	case hasIncomingArrow && hasOutgoingArrow:
		result.Direction = "both"
	case hasIncomingArrow:
		result.Direction = "incoming"
	case hasOutgoingArrow:
		result.Direction = "outgoing"
	}
	if hasIncomingArrow {
		pattern = pattern[2:]
	} else if strings.HasPrefix(pattern, "-") {
		pattern = pattern[1:]
	}
	if hasOutgoingArrow {
		pattern = pattern[:len(pattern)-2]
	} else if strings.HasSuffix(pattern, "-") {
		pattern = pattern[:len(pattern)-1]
	}

	// Extract [r:TYPE {props}] part
	if strings.HasPrefix(pattern, "[") && strings.HasSuffix(pattern, "]") {
		inner := pattern[1 : len(pattern)-1]

		// Check for variable length: [*], [*2], [*1..3], [*2..], [*..5]. A
		// * in a backticked type name is part of the name (#879).
		if varLengthStart := indexOutsideQuotes(inner, '*'); varLengthStart >= 0 {
			result.VariableLength = true
			varLengthEnd := varLengthStart + 1
			for varLengthEnd < len(inner) {
				ch := inner[varLengthEnd]
				if (ch < '0' || ch > '9') && ch != '.' {
					break
				}
				varLengthEnd++
			}
			spec := inner[varLengthStart+1 : varLengthEnd]
			hasRange := strings.Contains(spec, "..")
			switch {
			case spec == "":
				// `[*]` — fully unbounded.
				result.MinHops = 1
				result.MaxHops = VarLengthUnboundedMaxHops
			case hasRange:
				parts := strings.SplitN(spec, "..", 2)
				result.MinHops = 1
				if parts[0] != "" {
					result.MinHops, _ = strconv.Atoi(parts[0])
				}
				if parts[1] != "" {
					result.MaxHops, _ = strconv.Atoi(parts[1])
				} else {
					// `[*N..]` — open-ended upper bound.
					result.MaxHops = VarLengthUnboundedMaxHops
				}
			default:
				result.MinHops, _ = strconv.Atoi(spec)
				result.MaxHops = result.MinHops
			}
			inner = strings.TrimSpace(inner[:varLengthStart] + inner[varLengthEnd:])
		}

		// Property maps are valid with or without a relationship type, e.g.
		// [r {name: 'value'}] and [r:TYPE {name: 'value'}]. Remove the map
		// before interpreting the remaining declaration as variable/type text.
		if propsIdx := indexByteOutsideBackticks(inner, '{'); propsIdx >= 0 {
			result.Properties = e.parseProperties(ctx, inner[propsIdx:])
			inner = strings.TrimSpace(inner[:propsIdx])
		}

		// Parse variable and types: r:TYPE|OTHER
		if colonIdx := strings.Index(inner, ":"); colonIdx >= 0 {
			result.Variable = strings.TrimSpace(inner[:colonIdx])
			typesPart := inner[colonIdx+1:]

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
func (e *StorageExecutor) executeMatchWithRelationships(ctx context.Context, pattern string, whereClause string, returnItems []returnItem) (*ExecuteResult, error) {
	return e.executeMatchWithRelationshipsWithPath(ctx, pattern, whereClause, returnItems, nil, "", -1)
}

// executeMatchWithRelationshipsWithPath handles MATCH queries with relationship patterns and optional path variable
func (e *StorageExecutor) executeMatchWithRelationshipsWithPath(ctx context.Context, pattern string, whereClause string, returnItems []returnItem, seedNodes []*storage.Node, pathVariable string, earlyLimit int) (*ExecuteResult, error) {
	return e.executeMatchWithRelationshipsWithPathSeeded(ctx, pattern, whereClause, returnItems, seedNodes, nil, pathVariable, earlyLimit)
}

// executeMatchWithRelationshipsWithPathSeeded is executeMatchWithRelationshipsWithPath
// with an additional endSeedNodes parameter. When a pipeline row already binds
// the pattern's end-node variable (but not its start-node variable), the
// caller passes that node here so the traversal starts from the known
// endpoint and walks backwards, instead of expanding the pattern over every
// relationship in the store and joining the bound value afterward. seedNodes
// (start-side) takes priority when both are supplied.
func (e *StorageExecutor) executeMatchWithRelationshipsWithPathSeeded(ctx context.Context, pattern string, whereClause string, returnItems []returnItem, seedNodes []*storage.Node, endSeedNodes []*storage.Node, pathVariable string, earlyLimit int) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Parse the pattern: (a:Label)-[r:TYPE]->(b:Label)
	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil {
		return result, localizedError(localization.CypherMatchingTraversalPatternInvalid(pattern), nil)
	}
	returnItems = expandTraversalWildcardReturnItems(returnItems, matches, pathVariable)
	for _, item := range returnItems {
		result.Columns = append(result.Columns, item.column())
	}

	// Store the path variable for path functions (relationships(path), nodes(path), length(path))
	if pathVariable != "" {
		matches.PathVariable = pathVariable
	}
	if earlyLimit == 0 {
		return result, nil
	}
	if earlyLimit > 0 && whereClause == "" {
		matches.TraversalLimit = earlyLimit
	}

	// Fast path: MATCH ()-[r(:TYPE|...)?]->() RETURN count(r|*) [AS ...]
	//
	// This pattern appears in real workloads (and Northwind benchmarks). Routing it
	// through full traversal materializes all edges/paths, which is unnecessary for
	// pure counts and can be orders of magnitude slower.
	if whereClause == "" && pathVariable == "" && !matches.IsChained && len(returnItems) == 1 &&
		matches.StartNode.variable == "" && len(matches.StartNode.labels) == 0 && len(matches.StartNode.properties) == 0 &&
		matches.EndNode.variable == "" && len(matches.EndNode.labels) == 0 && len(matches.EndNode.properties) == 0 &&
		matches.Relationship.MinHops == 1 && matches.Relationship.MaxHops == 1 &&
		len(matches.Relationship.Properties) == 0 {
		if count, ok, err := e.tryFastRelationshipCount(matches, returnItems[0]); ok {
			if err != nil {
				return nil, err
			}
			result.Rows = [][]interface{}{{count}}
			return result, nil
		}
	}

	// Fast path: relationship aggregations and chained joins (Northwind-style query shapes).
	//
	// This avoids full path materialization for common patterns like:
	//   - MATCH (c)<-[:PART_OF]-(p) RETURN c.name, count(p)
	//   - MATCH (s)-[:SUPPLIES]->(p)-[:PART_OF]->(c) RETURN s.name, c.name, count(p)
	//   - MATCH (c)-[:PURCHASED]->(o)-[:ORDERS]->(p)-[:PART_OF]->(cat) RETURN c.name, cat.name, count(DISTINCT o)
	if whereClause == "" && pathVariable == "" {
		if rows, ok, err := e.tryFastRelationshipAggregations(matches, returnItems); ok {
			if err != nil {
				return nil, err
			}
			result.Rows = rows
			return result, nil
		}
	}

	// OPTIMIZATION: If WHERE clause filters by id(startNode) = value, filter start nodes before traversal
	// This avoids traversing from all nodes when we only need one specific node
	var optimizedStartNodes []*storage.Node
	usedPropertyIndex := false
	if len(seedNodes) > 0 {
		optimizedStartNodes = seedNodes
		usedPropertyIndex = true
	} else if whereClause != "" {
		// Direct element/id equality seek on start variable:
		// MATCH (o)-[:R]->(t) WHERE elementId(o) = '...'
		// This avoids traversing from all start nodes for single-node lookups.
		if matches.StartNode.variable != "" {
			if nodes, used, idxErr := e.tryCollectNodesFromIDEqualityCompound(ctx, matches.StartNode, whereClause, nil); idxErr == nil && used {
				optimizedStartNodes = nodes
				usedPropertyIndex = true
			}
		}

		// Prefer indexed start-node pruning for simple property predicates.
		if matches.StartNode.variable != "" && len(optimizedStartNodes) == 0 {
			if nodes, used, idxErr := e.tryCollectNodesFromPropertyIndex(ctx, matches.StartNode, whereClause); idxErr == nil && used {
				optimizedStartNodes = nodes
				usedPropertyIndex = true
			}
			if !usedPropertyIndex {
				if nodes, used, idxErr := e.tryCollectNodesFromPropertyIndexNotNull(matches.StartNode, whereClause); idxErr == nil && used {
					optimizedStartNodes = nodes
					usedPropertyIndex = true
				}
			}
			// BUG FIX: a start-node `WHERE var.prop IN [...]` / `IN $param`
			// predicate previously fell through every branch above (none of
			// them recognize IN-lists) straight to the O(all-label-nodes)
			// scan below, even though match.go/clauses.go/executor_mutations.go
			// already use this exact index-seek for plain node MATCH and
			// single-clause DELETE. Reuse it here so a relationship-pattern
			// traversal anchored by an IN-list seeds from the property index
			// instead of scanning every node of the label.
			if !usedPropertyIndex {
				if nodes, used, idxErr := e.tryCollectNodesFromPropertyIndexInCompound(ctx, matches.StartNode, whereClause, getParamsFromContext(ctx)); idxErr == nil && used {
					optimizedStartNodes = nodes
					usedPropertyIndex = true
				}
			}
			// Fallback optimization: even when no property index exists, pre-prune start
			// nodes for simple start-variable predicates before traversing relationships.
			// This preserves semantics while avoiding expensive traversal from irrelevant
			// start nodes in patterns like:
			//   MATCH (n:Label)-[:R]->(m) WHERE n.prop = 'x' RETURN ...
			if !usedPropertyIndex {
				if nodes, used, scanErr := e.tryCollectNodesFromStartPropertyScan(ctx, matches.StartNode, whereClause); scanErr == nil && used {
					optimizedStartNodes = nodes
				}
			}
		}

	}

	// Execute traversal with optimized start nodes if available
	var paths []PathResult
	if endSeededPaths, endSeeded := e.traverseFromEndSeeds(ctx, matches, optimizedStartNodes, endSeedNodes, whereClause); endSeeded {
		// Final even when empty: a bound end node with no qualifying path must
		// not fall through to the store-wide scan below.
		paths = endSeededPaths
	} else if len(optimizedStartNodes) > 0 {
		// Use optimized start nodes for non-chained traversal. Chained traversal uses
		// segment-aware expansion; feeding it through traverseGraphSequential can break
		// semantics because it ignores segment topology.
		if matches.IsChained && len(matches.Segments) > 1 {
			paths = e.traverseChainedGraph(ctx, matches, optimizedStartNodes)
		} else {
			viewport, _ := TemporalViewportFromContext(ctx)
			checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
			paths = e.traverseGraphSequential(ctx, matches, optimizedStartNodes, viewport, checker)
		}
		// Still need to apply WHERE clause filter (in case there are other conditions)
		if whereClause != "" {
			paths = e.filterPathsByWhere(ctx, paths, matches, whereClause, nil)
		}
	} else if matches.TraversalLimit > 0 && !matches.IsChained && len(matches.StartNode.properties) == 0 {
		viewport, _ := TemporalViewportFromContext(ctx)
		checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
		var streamed bool
		var streamErr error
		paths, streamed, streamErr = e.traverseGraphWithStreamingStartNodes(ctx, matches, viewport, checker)
		if streamErr != nil {
			return nil, streamErr
		}
		if !streamed {
			paths = e.traverseGraph(ctx, matches)
		}
	} else {
		// Normal traversal from all matching nodes
		paths = e.traverseGraph(ctx, matches)
		// Apply WHERE clause filter if present
		if whereClause != "" {
			paths = e.filterPathsByWhere(ctx, paths, matches, whereClause, nil)
		}
	}
	if matches.StartNode.variable != "" && matches.StartNode.variable == matches.EndNode.variable {
		filtered := paths[:0]
		for _, path := range paths {
			if len(path.Nodes) > 0 && path.Nodes[0].ID == path.Nodes[len(path.Nodes)-1].ID {
				filtered = append(filtered, path)
			}
		}
		paths = filtered
	}
	for _, item := range returnItems {
		if !isAggregateFunc(item.expr) && containsAggregateFunc(item.expr) {
			rows := make([]traversalOptRow, 0, len(paths))
			for _, path := range paths {
				pathContext := e.buildPathContext(path, matches)
				rows = append(rows, traversalOptRow{values: e.pathContextValues(pathContext)})
			}
			aggregated, err := e.aggregateTraversalOptionalRows(ctx, rows, returnItems)
			if err != nil {
				return nil, err
			}
			result.Rows = aggregated
			return result, nil
		}
	}

	// Pre-compute upper-case expressions and aggregation flags ONCE for all items
	// This avoids repeated upperASCII() calls in loops (major performance win)
	upperExprs := make([]string, len(returnItems))
	isAggFlags := make([]bool, len(returnItems))
	for i, item := range returnItems {
		upperExprs[i] = upperASCII(item.expr)
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
				switch {
				case isAggregateFuncName(item.expr, "count"):
					row[i] = e.aggregatePathCount(ctx, paths, matches, item.expr)

				case isAggregateFuncName(item.expr, "sum"):
					row[i] = e.aggregatePathSum(ctx, paths, matches, extractFuncInner(item.expr))

				case isAggregateFuncName(item.expr, "avg"):
					row[i] = e.aggregatePathAvg(ctx, paths, matches, extractFuncInner(item.expr))

				case isAggregateFuncName(item.expr, "min"):
					row[i] = e.aggregatePathMinMax(ctx, paths, matches, extractFuncInner(item.expr), false)

				case isAggregateFuncName(item.expr, "max"):
					row[i] = e.aggregatePathMinMax(ctx, paths, matches, extractFuncInner(item.expr), true)

				case isAggregateFuncName(item.expr, "collect") && startsWithDistinctArgument(extractFuncInner(item.expr)):
					row[i] = e.aggregatePathCollect(ctx, paths, matches, item.expr, true)

				case isAggregateFuncName(item.expr, "collect"):
					row[i] = e.aggregatePathCollect(ctx, paths, matches, item.expr, false)

				default:
					if len(paths) > 0 {
						context := e.buildPathContext(paths[0], matches)
						row[i] = e.evaluateExpressionWithPathContext(ctx, item.expr, context)
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
					val := e.evaluateExpressionWithPathContext(ctx, item.expr, context)
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
				if !isAggFlags[i] { // Use pre-computed flag
					// Non-aggregated column - use group key value
					row[i] = groupKeys[key][keyIdx]
					keyIdx++
					continue
				}

				// Aggregation function - aggregate over this group
				switch {
				case isAggregateFuncName(item.expr, "count"):
					row[i] = e.aggregatePathCount(ctx, groupPaths, matches, item.expr)

				case isAggregateFuncName(item.expr, "sum"):
					row[i] = e.aggregatePathSum(ctx, groupPaths, matches, extractFuncInner(item.expr))

				case isAggregateFuncName(item.expr, "avg"):
					row[i] = e.aggregatePathAvg(ctx, groupPaths, matches, extractFuncInner(item.expr))

				case isAggregateFuncName(item.expr, "min"):
					row[i] = e.aggregatePathMinMax(ctx, groupPaths, matches, extractFuncInner(item.expr), false)

				case isAggregateFuncName(item.expr, "max"):
					row[i] = e.aggregatePathMinMax(ctx, groupPaths, matches, extractFuncInner(item.expr), true)

				case isAggregateFuncName(item.expr, "collect") && startsWithDistinctArgument(extractFuncInner(item.expr)):
					row[i] = e.aggregatePathCollect(ctx, groupPaths, matches, item.expr, true)

				case isAggregateFuncName(item.expr, "collect"):
					row[i] = e.aggregatePathCollect(ctx, groupPaths, matches, item.expr, false)

				default:
					if len(groupPaths) > 0 {
						context := e.buildPathContext(groupPaths[0], matches)
						row[i] = e.evaluateExpressionWithPathContext(ctx, item.expr, context)
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
	e.appendTraversalRows(ctx, result, paths, matches, returnItems, earlyLimit)

	return result, nil
}

func expandTraversalWildcardReturnItems(items []returnItem, match *TraversalMatch, pathVariable string) []returnItem {
	if len(items) != 1 || strings.TrimSpace(items[0].expr) != "*" || match == nil {
		return items
	}
	names := make(map[string]struct{})
	bind := func(name string) {
		if name = strings.TrimSpace(name); name != "" && !isGeneratedVariable(name) {
			names[name] = struct{}{}
		}
	}
	bind(pathVariable)
	bind(match.StartNode.variable)
	bind(match.EndNode.variable)
	bind(match.Relationship.Variable)
	for _, node := range match.IntermediateNodes {
		bind(node.variable)
	}
	for _, segment := range match.Segments {
		bind(segment.FromNode.variable)
		bind(segment.ToNode.variable)
		bind(segment.Relationship.Variable)
	}
	columns := make([]string, 0, len(names))
	for name := range names {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	expanded := make([]returnItem, len(columns))
	for index, name := range columns {
		expanded[index] = returnItem{expr: name, alias: name}
	}
	return expanded
}

func (e *StorageExecutor) appendTraversalRows(ctx context.Context, result *ExecuteResult, paths []PathResult, matches *TraversalMatch, returnItems []returnItem, rowLimit int) {
	for _, path := range paths {
		if rowLimit >= 0 && len(result.Rows) >= rowLimit {
			break
		}
		row := make([]interface{}, len(returnItems))
		context := e.buildPathContext(path, matches)

		for i, item := range returnItems {
			if isLengthPathExpr(item.expr) {
				row[i] = int64(context.pathLength)
			} else {
				row[i] = e.evaluateExpressionWithPathContext(ctx, item.expr, context)
			}
		}
		result.Rows = append(result.Rows, row)
	}
}

func (e *StorageExecutor) tryExecuteTraversalEndSeedOrderLimit(ctx context.Context, pattern string, whereClause string, returnItems []returnItem, pathVariable string, orderExpr string, limit int) (*ExecuteResult, bool, error) {
	if limit <= 0 || strings.TrimSpace(orderExpr) == "" {
		return nil, false, nil
	}

	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil || matches.IsChained {
		return nil, false, nil
	}
	if pathVariable != "" {
		matches.PathVariable = pathVariable
	}
	if strings.TrimSpace(matches.EndNode.variable) == "" || len(matches.EndNode.labels) == 0 {
		return nil, false, nil
	}

	orderSpecs := e.parseNodeOrderSpecs(orderExpr, matches.EndNode.variable)
	if len(orderSpecs) == 0 {
		return nil, false, nil
	}

	seedWhere := e.extractTraversalSeedWhereClause(whereClause, matches.EndNode.variable, matches.StartNode.variable)
	if strings.TrimSpace(seedWhere) == "" && strings.TrimSpace(whereClause) != "" {
		return nil, false, nil
	}

	seedLimit := topKSeedLimit(limit)
	seedNodes, used, err := e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, matches.EndNode, seedWhere, orderExpr, seedLimit)
	if err != nil {
		return nil, true, err
	}
	if !used {
		if seedNodes, used, err = e.tryCollectNodesFromPropertyIndexNotNullOrderLimit(ctx, matches.EndNode, seedWhere, orderExpr, seedLimit); err != nil {
			return nil, true, err
		}
		if !used {
			return nil, false, nil
		}
	}

	result := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
	for _, item := range returnItems {
		result.Columns = append(result.Columns, item.column())
	}
	if len(seedNodes) == 0 {
		e.markTraversalEndSeedTopKUsed()
		return result, true, nil
	}

	reversed := reverseTraversalMatch(matches)
	if reversed == nil {
		return nil, false, nil
	}

	paths := make([]PathResult, 0, util.SafePreallocCap(len(seedNodes)))
	for _, endNode := range seedNodes {
		reversedPaths := e.traverseFromNode(ctx, endNode, reversed)
		for _, reversedPath := range reversedPaths {
			path := reversePathResult(reversedPath)
			if whereClause != "" && !e.evaluateWhereOnPath(ctx, whereClause, e.buildPathContext(path, matches)) {
				continue
			}
			paths = append(paths, path)
			if len(paths) >= limit {
				e.appendTraversalRows(ctx, result, paths, matches, returnItems, limit)
				e.markTraversalEndSeedTopKUsed()
				return result, true, nil
			}
		}
	}

	if len(paths) < limit && len(seedNodes) >= seedLimit {
		return nil, false, nil
	}

	e.appendTraversalRows(ctx, result, paths, matches, returnItems, limit)
	e.markTraversalEndSeedTopKUsed()
	return result, true, nil
}

func (e *StorageExecutor) extractTraversalSeedWhereClause(whereClause string, seedVar string, blockedVars ...string) string {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return ""
	}
	parts := splitTopLevelAndConjuncts(clause)
	selected := make([]string, 0, len(parts))
	for _, part := range parts {
		term := unwrapOuterParens(strings.TrimSpace(part))
		if term == "" {
			continue
		}
		if !referencesTraversalVariable(term, seedVar) {
			continue
		}
		blocked := false
		for _, blockedVar := range blockedVars {
			if blockedVar == "" {
				continue
			}
			if referencesTraversalVariable(term, blockedVar) {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}
		selected = append(selected, term)
	}
	return strings.Join(selected, " AND ")
}

func referencesTraversalVariable(expr string, variable string) bool {
	if variable == "" {
		return false
	}
	trimmed := strings.TrimSpace(expr)
	if strings.Contains(trimmed, variable+".") || strings.Contains(lowerASCII(trimmed), "id("+lowerASCII(variable)+")") || strings.Contains(lowerASCII(trimmed), "elementid("+lowerASCII(variable)+")") {
		return true
	}
	return false
}

// traverseFromEndSeeds walks a non-chained pattern backwards from nodes
// already bound to its end variable, when no start-side seed exists. Rather
// than expanding the pattern over every relationship in the store and joining
// the bound value afterward (O(|E|)), it reverses the pattern, traverses from
// each known endpoint (O(degree)), flips each path back and applies the WHERE
// clause. seeded reports whether this path ran; when it did, the returned
// paths are final even if empty. The caller is responsible for having checked
// the end node's own labels and inline properties.
func (e *StorageExecutor) traverseFromEndSeeds(ctx context.Context, matches *TraversalMatch, startSeedNodes, endSeedNodes []*storage.Node, whereClause string) (paths []PathResult, seeded bool) {
	if len(startSeedNodes) > 0 || len(endSeedNodes) == 0 || matches.IsChained {
		return nil, false
	}
	reversed := reverseTraversalMatch(matches)
	if reversed == nil {
		return nil, false
	}
	paths = make([]PathResult, 0, util.SafePreallocCap(len(endSeedNodes)))
	for _, endNode := range endSeedNodes {
		for _, reversedPath := range e.traverseFromNode(ctx, endNode, reversed) {
			paths = append(paths, reversePathResult(reversedPath))
		}
	}
	if whereClause != "" {
		paths = e.filterPathsByWhere(ctx, paths, matches, whereClause, nil)
	}
	return paths, true
}

func reverseTraversalMatch(match *TraversalMatch) *TraversalMatch {
	if match == nil || match.IsChained {
		return nil
	}
	reversed := *match
	reversed.StartNode = match.EndNode
	reversed.EndNode = match.StartNode
	reversed.Relationship = match.Relationship
	switch match.Relationship.Direction {
	case "outgoing":
		reversed.Relationship.Direction = "incoming"
	case "incoming":
		reversed.Relationship.Direction = "outgoing"
	default:
		reversed.Relationship.Direction = match.Relationship.Direction
	}
	return &reversed
}

func reversePathResult(path PathResult) PathResult {
	reversedNodes := make([]*storage.Node, len(path.Nodes))
	for i := range path.Nodes {
		reversedNodes[len(path.Nodes)-1-i] = path.Nodes[i]
	}
	reversedEdges := make([]*storage.Edge, len(path.Relationships))
	for i := range path.Relationships {
		reversedEdges[len(path.Relationships)-1-i] = path.Relationships[i]
	}
	return PathResult{Nodes: reversedNodes, Relationships: reversedEdges, Length: path.Length}
}

func topKSeedLimit(limit int) int {
	seedLimit, ok := util.SafeIntProduct(limit, 4)
	if !ok {
		seedLimit = int(^uint(0) >> 1)
	}
	if seedLimit < 200 {
		seedLimit = 200
	}
	return seedLimit
}

func (e *StorageExecutor) tryCollectTraversalStartSeedOrderNodes(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	orderExpr string,
	limit int,
) ([]*storage.Node, bool, error) {
	if limit <= 0 || strings.TrimSpace(orderExpr) == "" || strings.TrimSpace(nodePattern.variable) == "" || len(nodePattern.labels) == 0 {
		return nil, false, nil
	}

	seedNodes, used, err := e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePattern, whereClause, orderExpr, topKSeedLimit(limit))
	if err != nil || !used {
		return nil, used, err
	}

	if len(seedNodes) > limit {
		if topK, ok := e.selectTopKNodesByOrder(seedNodes, nodePattern.variable, orderExpr, limit); ok {
			seedNodes = topK
		}
	}

	return seedNodes, true, nil
}

func (e *StorageExecutor) tryExecuteTraversalStartSeedOrderLimit(ctx context.Context, pattern string, whereClause string, returnItems []returnItem, pathVariable string, orderExpr string, limit int) (*ExecuteResult, bool, error) {
	if limit <= 0 || strings.TrimSpace(orderExpr) == "" {
		return nil, false, nil
	}

	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil || matches.IsChained == false && strings.TrimSpace(matches.StartNode.variable) == "" {
		return nil, false, nil
	}
	if pathVariable != "" {
		matches.PathVariable = pathVariable
	}

	seedNodes, used, err := e.tryCollectTraversalStartSeedOrderNodes(ctx, matches.StartNode, whereClause, orderExpr, limit)
	if err != nil {
		return nil, true, err
	}
	if !used {
		return nil, false, nil
	}

	result, err := e.executeMatchWithRelationshipsWithPath(ctx, pattern, whereClause, returnItems, seedNodes, pathVariable, -1)
	if err != nil {
		return nil, true, err
	}
	e.markTraversalStartSeedTopKUsed()
	return result, true, nil
}

// tryCollectNodesFromStartPropertyScan applies start-node predicate pruning for
// traversal patterns without requiring an index. It only handles simple predicates
// on the traversal start variable:
//   - <startVar>.<prop> = <value>
//   - <startVar>.<prop> IS NOT NULL
func (e *StorageExecutor) tryCollectNodesFromStartPropertyScan(ctx context.Context, nodePattern nodePatternInfo, whereClause string) ([]*storage.Node, bool, error) {
	if strings.TrimSpace(nodePattern.variable) == "" || strings.TrimSpace(whereClause) == "" {
		return nil, false, nil
	}
	for _, reference := range semanticExpressionReferences(whereClause) {
		if strings.SplitN(reference, ".", 2)[0] != nodePattern.variable {
			return nil, false, nil
		}
	}
	nodes, _, err := e.collectPipelineInitialNodeCandidates(ctx, nodePattern, whereClause, pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
	if err != nil {
		return nil, true, err
	}
	values := pipelineNodeRow(ctx, nodePattern.variable, nil)
	filtered := nodes[:0]
	for _, node := range nodes {
		values[nodePattern.variable] = node
		if e.evaluateRowPredicate(ctx, whereClause, values) {
			filtered = append(filtered, node)
		}
	}
	return filtered, true, nil
}

func (e *StorageExecutor) tryFastRelationshipCount(matches *TraversalMatch, item returnItem) (count int64, ok bool, err error) {
	upper := upperASCII(strings.TrimSpace(item.expr))
	if !strings.HasPrefix(upper, "COUNT(") || !strings.HasSuffix(upper, ")") {
		return 0, false, nil
	}

	arg := strings.TrimSpace(item.expr[len("COUNT(") : len(item.expr)-1])
	argUpper := upperASCII(strings.TrimSpace(arg))

	// Only handle COUNT(*) or COUNT(<relationship var>).
	if argUpper != "*" && !strings.EqualFold(strings.TrimSpace(arg), matches.Relationship.Variable) {
		return 0, false, nil
	}

	// A repeated endpoint variable requires self-loops only; the counters can't
	// express that filter, so the general path handles it.
	if matches.StartNode.variable != "" && matches.StartNode.variable == matches.EndNode.variable {
		return 0, false, nil
	}

	// Inline endpoint property maps are value-level filters the counters can't
	// express (e.g. MATCH ({id:'x'})-[r:T]->() RETURN count(r)); the general
	// path handles them.
	if len(matches.StartNode.properties) > 0 || len(matches.EndNode.properties) > 0 {
		return 0, false, nil
	}

	// One-labeled-endpoint shapes: answered from the positional (label, type)
	// counters, matching Neo4j's RelationshipCountFromCountStore planning rule
	// (at most one endpoint labeled, no other predicates).
	if len(matches.StartNode.labels) > 0 || len(matches.EndNode.labels) > 0 {
		if len(matches.StartNode.labels) > 1 || len(matches.EndNode.labels) > 1 ||
			len(matches.StartNode.properties) > 0 || len(matches.EndNode.properties) > 0 ||
			matches.Relationship.Direction == "both" || len(matches.Relationship.Types) == 0 {
			return 0, false, nil
		}
		var label string
		startTier := false
		switch {
		case matches.Relationship.Direction == "outgoing" && len(matches.StartNode.labels) == 1:
			label, startTier = matches.StartNode.labels[0], true
		case matches.Relationship.Direction == "outgoing" && len(matches.EndNode.labels) == 1:
			label = matches.EndNode.labels[0]
		case matches.Relationship.Direction == "incoming" && len(matches.StartNode.labels) == 1:
			label = matches.StartNode.labels[0]
		case matches.Relationship.Direction == "incoming" && len(matches.EndNode.labels) == 1:
			label, startTier = matches.EndNode.labels[0], true
		default:
			return 0, false, nil
		}
		var total int64
		for _, t := range matches.Relationship.Types {
			var n int64
			var err error
			if startTier {
				n, err = e.storage.EdgeCountByStartLabel(label, t)
			} else {
				n, err = e.storage.EdgeCountByEndLabel(label, t)
			}
			if err != nil {
				return 0, true, err
			}
			total += n
		}
		return total, true, nil
	}

	if matches.Relationship.Direction == "both" {
		return e.countUndirectedRelationshipMatches(matches.Relationship.Types)
	}

	// No type filter: use storage.EdgeCount() (O(1) for most engines).
	if len(matches.Relationship.Types) == 0 {
		n, err := e.storage.EdgeCount()
		return n, true, err
	}

	// Type filter(s): every engine maintains a per-type counter (issue #638),
	// so typed counts are answered by O(1) point reads — no engine falls back
	// to edge materialization.
	var total int64
	for _, t := range matches.Relationship.Types {
		n, err := e.storage.EdgeCountByType(t)
		if err != nil {
			return 0, true, err
		}
		total += n
	}
	return total, true, nil
}

// countUndirectedRelationshipMatches returns Cypher pattern cardinality rather
// than physical edge cardinality. A non-loop relationship matches once from
// each endpoint, while a self-loop has only one distinct orientation.
func (e *StorageExecutor) countUndirectedRelationshipMatches(types []string) (int64, bool, error) {
	var candidates []*storage.Edge
	if len(types) == 0 {
		edges, err := e.storage.AllEdges()
		if err != nil {
			return 0, true, err
		}
		candidates = edges
	} else {
		seen := make(map[storage.EdgeID]struct{})
		for _, relationshipType := range types {
			edges, err := e.storage.GetEdgesByType(relationshipType)
			if err != nil {
				return 0, true, err
			}
			for _, edge := range edges {
				if edge == nil {
					continue
				}
				if _, exists := seen[edge.ID]; exists {
					continue
				}
				seen[edge.ID] = struct{}{}
				candidates = append(candidates, edge)
			}
		}
	}

	var count int64
	for _, edge := range candidates {
		if edge == nil {
			continue
		}
		count++
		if edge.StartNode != edge.EndNode {
			count++
		}
	}
	return count, true, nil
}

// isLengthPathExpr checks if an expression is length(path) for some path variable
func isLengthPathExpr(expr string) bool {
	return matchFuncStartAndSuffix(expr, "length") && strings.Contains(lowerASCII(expr), "path")
}

func (e *StorageExecutor) aggregatePathSum(ctx context.Context, paths []PathResult, matches *TraversalMatch, inner string) interface{} {
	var sumInt int64
	sumFloat := 0.0
	hasFloat := false
	hasValue := false

	for _, path := range paths {
		context := e.buildPathContext(path, matches)
		val := e.evaluateExpressionWithPathContext(ctx, inner, context)
		if val == nil {
			continue
		}
		hasValue = true
		switch v := val.(type) {
		case int64:
			sumInt += v
			sumFloat += float64(v)
		case int:
			sumInt += int64(v)
			sumFloat += float64(v)
		default:
			if f, ok := toFloat64(v); ok {
				hasFloat = true
				sumFloat += f
			}
		}
	}

	if !hasValue {
		return int64(0)
	}
	if hasFloat {
		return sumFloat
	}
	return sumInt
}

func (e *StorageExecutor) aggregatePathAvg(ctx context.Context, paths []PathResult, matches *TraversalMatch, inner string) interface{} {
	sum := 0.0
	count := 0
	for _, path := range paths {
		context := e.buildPathContext(path, matches)
		val := e.evaluateExpressionWithPathContext(ctx, inner, context)
		if f, ok := toFloat64(val); ok {
			sum += f
			count++
		}
	}
	if count == 0 {
		return nil
	}
	return sum / float64(count)
}

func (e *StorageExecutor) aggregatePathMinMax(ctx context.Context, paths []PathResult, matches *TraversalMatch, inner string, wantMax bool) interface{} {
	var best interface{}
	hasBest := false
	for _, path := range paths {
		context := e.buildPathContext(path, matches)
		val := e.evaluateExpressionWithPathContext(ctx, inner, context)
		if val == nil {
			continue
		}
		if !hasBest {
			best = val
			hasBest = true
			continue
		}
		if wantMax {
			if compareForSort(best, val) {
				best = val
			}
			continue
		}
		if compareForSort(val, best) {
			best = val
		}
	}
	if !hasBest {
		return nil
	}
	return best
}

func (e *StorageExecutor) aggregatePathCount(ctx context.Context, paths []PathResult, matches *TraversalMatch, expr string) int64 {
	inner := strings.TrimSpace(extractFuncInner(expr))
	if inner == "*" {
		return int64(len(paths))
	}
	inner, distinct := cutDistinctArgument(inner)
	seen := make(map[string]struct{}, len(paths))
	var count int64
	for _, path := range paths {
		pathContext := e.buildPathContext(path, matches)
		value := e.evaluateExpressionWithPathContext(ctx, inner, pathContext)
		if value == nil {
			continue
		}
		if distinct {
			key := joinedValueKey(value)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
		}
		count++
	}
	return count
}

func (e *StorageExecutor) aggregatePathCollect(ctx context.Context, paths []PathResult, matches *TraversalMatch, expr string, distinct bool) interface{} {
	inner, suffix, _ := extractFuncArgsWithSuffix(expr, "collect")
	if distinct {
		inner, _ = cutDistinctArgument(inner)
	}

	collected := make([]interface{}, 0, len(paths))
	if distinct {
		seen := make(map[string]struct{}, len(paths))
		for _, path := range paths {
			context := e.buildPathContext(path, matches)
			val := e.evaluateExpressionWithPathContext(ctx, inner, context)
			if val == nil {
				continue
			}
			key := fmt.Sprintf("%v", val)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			collected = append(collected, val)
		}
	} else {
		for _, path := range paths {
			context := e.buildPathContext(path, matches)
			collected = append(collected, e.evaluateExpressionWithPathContext(ctx, inner, context))
		}
	}
	if suffix == "" {
		return collected
	}
	return e.applyArraySuffix(collected, suffix)
}

// TraversalMatch represents a parsed traversal pattern
type TraversalMatch struct {
	StartNode    nodePatternInfo
	EndNode      nodePatternInfo
	Relationship RelationshipPattern
	// For chained patterns like (a)-[:R1]->(b)-[:R2]->(c), we store intermediate segments
	IntermediateNodes []nodePatternInfo
	Segments          []TraversalSegment // All segments in the chain
	IsChained         bool               // True if this is a multi-segment pattern
	PathVariable      string             // Variable name for path assignment (e.g., "path" in "path = (a)-[r]-(b)")
	TraversalLimit    int                // Early traversal cap for LIMIT-only shapes (0 = disabled)
}

// TraversalSegment represents one segment in a chained pattern
type TraversalSegment struct {
	FromNode     nodePatternInfo
	ToNode       nodePatternInfo
	Relationship RelationshipPattern
}

// parseTraversalPattern parses (a:Label)-[r:TYPE]->(b:Label) style patterns
// Also handles chained patterns like (a)<-[:R1]-(b)-[:R2]->(c)
// Uses a state machine instead of regex to properly handle parentheses in property values
func (e *StorageExecutor) parseTraversalPattern(ctx context.Context, pattern string) *TraversalMatch {
	pattern = normalizeAnonymousTraversalRelationships(pattern)
	// First check if this is a chained pattern (has multiple relationship segments)
	if e.isChainedPattern(pattern) {
		return e.parseChainedTraversalPattern(ctx, pattern)
	}
	return e.parseTraversalPatternStateMachine(ctx, pattern)
}

func normalizeAnonymousTraversalRelationships(pattern string) string {
	var normalized strings.Builder
	normalized.Grow(len(pattern) + 8)
	quote := byte(0)
	for index := 0; index < len(pattern); {
		character := pattern[index]
		if quote != 0 {
			normalized.WriteByte(character)
			if character == quote && !isBackslashEscaped(pattern, index) {
				quote = 0
			}
			index++
			continue
		}
		if character == '\'' || character == '"' || character == '`' {
			quote = character
			normalized.WriteByte(character)
			index++
			continue
		}
		switch {
		case strings.HasPrefix(pattern[index:], "<-->"):
			normalized.WriteString("-[]-")
			index += len("<-->")
		case strings.HasPrefix(pattern[index:], "-->"):
			normalized.WriteString("-[]->")
			index += len("-->")
		case strings.HasPrefix(pattern[index:], "<--"):
			normalized.WriteString("<-[]-")
			index += len("<--")
		case strings.HasPrefix(pattern[index:], "--"):
			normalized.WriteString("-[]-")
			index += len("--")
		default:
			normalized.WriteByte(character)
			index++
		}
	}
	return normalized.String()
}

// isChainedPattern checks if a pattern has multiple relationship segments
// e.g., (a)<-[:R1]-(b)-[:R2]->(c) or (a)-[:R1]->(b)-[:R2]->(c)
func (e *StorageExecutor) isChainedPattern(pattern string) bool {
	// Count relationship patterns by counting ]-
	// A chained pattern has at least 2 relationship segments
	count := 0
	// Reverse iteration: order doesn't matter for counting
	for i := len(pattern) - 2; i >= 0; i-- {
		if pattern[i] == ']' && (pattern[i+1] == '-' || pattern[i+1] == '>') {
			count++
		}
	}
	return count >= 2
}

// parseChainedTraversalPattern parses multi-segment patterns like (a)<-[:R1]-(b)-[:R2]->(c)
func (e *StorageExecutor) parseChainedTraversalPattern(ctx context.Context, pattern string) *TraversalMatch {
	result := &TraversalMatch{
		IsChained:         true,
		Segments:          []TraversalSegment{},
		IntermediateNodes: []nodePatternInfo{},
	}

	// Parse segments by splitting on relationship boundaries
	// We need to find each (node)-[rel]->(node) or (node)<-[rel]-(node) segment

	// Use state machine to properly handle nested parens and quotes
	type segmentPart struct {
		nodeStr string
		relStr  string
	}

	var parts []segmentPart
	var currentNode strings.Builder
	var currentRel strings.Builder
	inNode := false
	inRel := false
	parenDepth := 0
	bracketDepth := 0
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(pattern); i++ {
		c := pattern[i]

		// Handle quotes
		if (c == '\'' || c == '"') && !isBackslashEscaped(pattern, i) {
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
		}

		if inQuote {
			if inNode {
				currentNode.WriteByte(c)
			} else if inRel {
				currentRel.WriteByte(c)
			}
			continue
		}

		switch c {
		case '(':
			if parenDepth == 0 {
				inNode = true
			}
			parenDepth++
			if inNode {
				currentNode.WriteByte(c)
			}
		case ')':
			parenDepth--
			if inNode {
				currentNode.WriteByte(c)
			}
			if parenDepth == 0 {
				inNode = false
			}
		case '[':
			if bracketDepth == 0 {
				inRel = true
			}
			bracketDepth++
			if inRel {
				currentRel.WriteByte(c)
			}
		case ']':
			if inRel {
				currentRel.WriteByte(c)
			}
			bracketDepth--
			if bracketDepth == 0 {
				inRel = false
				// Capture the direction arrows after ]
				for j := i + 1; j < len(pattern) && (pattern[j] == '-' || pattern[j] == '>'); j++ {
					currentRel.WriteByte(pattern[j])
					i = j
				}
				// Save this part
				parts = append(parts, segmentPart{
					nodeStr: currentNode.String(),
					relStr:  currentRel.String(),
				})
				currentNode.Reset()
				currentRel.Reset()
			}
		case '<', '-':
			if !inNode && !inRel && parenDepth == 0 && bracketDepth == 0 {
				// Direction marker before relationship - add to next rel
				currentRel.WriteByte(c)
			} else if inNode {
				currentNode.WriteByte(c)
			} else if inRel {
				currentRel.WriteByte(c)
			}
		default:
			if inNode {
				currentNode.WriteByte(c)
			} else if inRel {
				currentRel.WriteByte(c)
			}
		}
	}

	// Capture final node if present
	if currentNode.Len() > 0 {
		parts = append(parts, segmentPart{
			nodeStr: currentNode.String(),
			relStr:  "",
		})
	}

	// Now convert parts to segments
	if len(parts) < 2 {
		return nil // Not enough parts for a valid pattern
	}

	// Parse all nodes first
	var allNodes []nodePatternInfo
	for _, part := range parts {
		nodeStr := strings.Trim(part.nodeStr, "()")
		allNodes = append(allNodes, e.parseNodePatternFromString(ctx, nodeStr))
	}

	// Build segments
	for i := 0; i < len(parts)-1; i++ {
		relStr := parts[i].relStr
		if relStr == "" {
			continue
		}

		segment := TraversalSegment{
			FromNode:     allNodes[i],
			ToNode:       allNodes[i+1],
			Relationship: *e.parseRelationshipPattern(ctx, relStr),
		}
		result.Segments = append(result.Segments, segment)
	}

	// Set start and end nodes
	if len(allNodes) > 0 {
		result.StartNode = allNodes[0]
	}
	if len(allNodes) > 1 {
		result.EndNode = allNodes[len(allNodes)-1]
	}

	// Intermediate nodes are all nodes except first and last
	if len(allNodes) > 2 {
		result.IntermediateNodes = allNodes[1 : len(allNodes)-1]
	}

	// For backward compatibility, set single Relationship to first segment
	if len(result.Segments) > 0 {
		result.Relationship = result.Segments[0].Relationship
	}

	return result
}

// parseTraversalPatternStateMachine parses patterns with a state machine
// to properly handle parentheses and special characters inside quoted property values.
func (e *StorageExecutor) parseTraversalPatternStateMachine(ctx context.Context, pattern string) *TraversalMatch {
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
	relEnd := findMatchingBracket(pattern, relStart)
	if relEnd < 0 {
		return nil
	}

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
		StartNode:    e.parseNodePatternFromString(ctx, startNodeStr),
		Relationship: *e.parseRelationshipPattern(ctx, relStr),
		EndNode:      e.parseNodePatternFromString(ctx, endNodeStr),
	}
}

func findMatchingBracket(s string, startIdx int) int {
	return findMatchingDelimiter(s, startIdx, '[', ']')
}

// findMatchingParen finds the index of the closing paren that matches the
// opening paren at startIdx. It shares the comment- and quote-aware matcher
// so a ')' inside a string, backtick name or comment never closes early.
func findMatchingParen(s string, startIdx int) int {
	return findMatchingDelimiter(s, startIdx, '(', ')')
}

// parseNodePatternFromString parses n:Label {props} from a string
func (e *StorageExecutor) parseNodePatternFromString(ctx context.Context, s string) nodePatternInfo {
	return e.parseNodePattern(ctx, s)
}

// traverseGraph executes the traversal and returns all matching paths
func (e *StorageExecutor) traverseGraph(ctx context.Context, match *TraversalMatch) []PathResult {
	viewport, _ := TemporalViewportFromContext(ctx)
	checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
	// Handle chained patterns differently (multi-segment traversal)
	if match.IsChained && len(match.Segments) > 1 {
		return e.traverseChainedGraph(ctx, match, nil)
	}

	startNodes, _, err := e.collectPipelineInitialNodeCandidates(ctx, match.StartNode, "", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}

	// Filter indexed candidates through the shared complete pattern check.
	if len(match.StartNode.properties) > 0 || len(match.StartNode.labels) > 0 {
		var filtered []*storage.Node
		for _, n := range startNodes {
			if pipelineNodeMatchesPattern(n, match.StartNode) {
				filtered = append(filtered, n)
			}
		}
		startNodes = filtered
	}

	// OPTIMIZATION: Use parallel traversal for large start node sets
	// Threshold is MinBatchSize (default 200) - goroutine overhead hurts small traversals
	config := GetParallelConfig()
	// Keep traversal single-threaded when early LIMIT short-circuiting is active so
	// we can stop globally once enough paths are found.
	if config.Enabled && len(startNodes) >= config.MinBatchSize && match.TraversalLimit <= 0 {
		return e.traverseGraphParallel(ctx, match, startNodes, config, viewport, checker)
	}

	return e.traverseGraphSequential(ctx, match, startNodes, viewport, checker)
}

func (e *StorageExecutor) traverseGraphWithStreamingStartNodes(
	ctx context.Context,
	match *TraversalMatch,
	viewport TemporalViewport,
	checker temporalCurrentNodeChecker,
) ([]PathResult, bool, error) {
	store := e.getStorage(ctx)
	labels := match.StartNode.labels
	var stream func(func(*storage.Node) error) error
	if len(labels) > 0 {
		reader, ok := store.(storage.ProjectedLabelNodeReader)
		if !ok {
			return nil, false, nil
		}
		stream = func(visit func(*storage.Node) error) error {
			return reader.StreamNodesByLabelProjected(labels[0], nil, visit)
		}
	} else {
		reader, ok := store.(storage.StreamingEngine)
		if !ok {
			return nil, false, nil
		}
		stream = func(visit func(*storage.Node) error) error {
			return reader.StreamNodes(ctx, visit)
		}
	}

	remaining := match.TraversalLimit
	hideSystemNodes := shouldHideSystemNodes(store)
	var paths []PathResult
	err := stream(func(node *storage.Node) error {
		if node == nil || (hideSystemNodes && isSystemNode(node)) {
			return nil
		}
		if len(labels) > 0 && !mergeNodeHasLabels(node, labels) {
			return nil
		}
		visible, err := nodeVisibleInTemporalViewport(node, viewport, checker)
		if err != nil {
			return err
		}
		if !visible {
			return nil
		}

		limitedMatch := *match
		limitedMatch.TraversalLimit = remaining
		found := e.traverseGraphSequential(ctx, &limitedMatch, []*storage.Node{node}, viewport, checker)
		paths = append(paths, found...)
		remaining -= len(found)
		if remaining <= 0 {
			return storage.ErrIterationStopped
		}
		return nil
	})
	if err != nil && err != storage.ErrIterationStopped {
		return nil, true, err
	}
	return paths, true, nil
}

// traverseGraphSequential performs sequential traversal from start nodes
func (e *StorageExecutor) traverseGraphSequential(ctx context.Context, match *TraversalMatch, startNodes []*storage.Node, viewport TemporalViewport, checker temporalCurrentNodeChecker) []PathResult {
	var results []PathResult
	remaining := -1
	if match.TraversalLimit > 0 {
		remaining = match.TraversalLimit
	}

	for _, startNode := range startNodes {
		if remaining == 0 {
			break
		}
		ctxLimit := 0
		if remaining > 0 {
			ctxLimit = remaining
		}
		traversalCtx := &TraversalContext{
			startNode:                    startNode,
			relTypes:                     match.Relationship.Types,
			relTypeSet:                   buildRelTypeSet(match.Relationship.Types),
			relProperties:                match.Relationship.Properties,
			direction:                    match.Relationship.Direction,
			minHops:                      match.Relationship.MinHops,
			maxHops:                      match.Relationship.MaxHops,
			usedEdges:                    make(map[storage.EdgeID]bool),
			nodeCache:                    make(map[storage.NodeID]*storage.Node),
			limit:                        ctxLimit,
			temporalViewport:             viewport,
			temporalChecker:              checker,
			cancelCtx:                    ctx,
			endpointsNeedOnlyExist:       traversalEndpointsNeedOnlyExist(match),
			relationshipsNeedOnlyHeaders: traversalRelationshipsNeedOnlyHeaders(match),
		}

		paths := e.findPaths(traversalCtx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode)
		results = append(results, paths...)
		if remaining > 0 {
			remaining -= len(paths)
		}
		if ctx != nil && ctx.Err() != nil {
			break
		}
	}

	return results
}

// traverseGraphParallel performs parallel traversal from multiple start nodes
// Each goroutine gets its own TraversalContext to avoid data races
func (e *StorageExecutor) traverseGraphParallel(ctx context.Context, match *TraversalMatch, startNodes []*storage.Node, config ParallelConfig, viewport TemporalViewport, checker temporalCurrentNodeChecker) []PathResult {
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
				if ctx != nil && ctx.Err() != nil {
					break
				}
				// Each goroutine gets its own traversal state (no shared state)
				traversalCtx := &TraversalContext{
					startNode:                    startNode,
					relTypes:                     match.Relationship.Types,
					relTypeSet:                   buildRelTypeSet(match.Relationship.Types),
					relProperties:                match.Relationship.Properties,
					direction:                    match.Relationship.Direction,
					minHops:                      match.Relationship.MinHops,
					maxHops:                      match.Relationship.MaxHops,
					usedEdges:                    make(map[storage.EdgeID]bool),
					nodeCache:                    make(map[storage.NodeID]*storage.Node),
					temporalViewport:             viewport,
					temporalChecker:              checker,
					cancelCtx:                    ctx,
					endpointsNeedOnlyExist:       traversalEndpointsNeedOnlyExist(match),
					relationshipsNeedOnlyHeaders: traversalRelationshipsNeedOnlyHeaders(match),
				}

				paths := e.findPaths(traversalCtx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode)
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

// traverseChainedGraph handles multi-segment patterns like (a)<-[:R1]-(b)-[:R2]->(c)
// It traverses each segment sequentially, joining results where intermediate nodes match
func (e *StorageExecutor) traverseChainedGraph(ctx context.Context, match *TraversalMatch, seedNodes []*storage.Node) []PathResult {
	if len(match.Segments) == 0 {
		return nil
	}

	// Start with first segment
	firstSeg := match.Segments[0]
	simpleMatch := &TraversalMatch{
		StartNode:    firstSeg.FromNode,
		EndNode:      firstSeg.ToNode,
		Relationship: firstSeg.Relationship,
	}
	// Get initial paths from first segment
	var currentPaths []PathResult
	if len(seedNodes) > 0 {
		for _, startNode := range seedNodes {
			currentPaths = append(currentPaths, e.traverseFromNode(ctx, startNode, simpleMatch)...)
		}
	} else {
		currentPaths = e.traverseGraph(ctx, simpleMatch)
	}
	repeats := chainRepeatedNodes(match.Segments)
	kept := currentPaths[:0]
	for _, path := range currentPaths {
		path.SegmentLengths = []int{len(path.Relationships)}
		if repeats.consistent(path, 1) {
			kept = append(kept, path)
		}
	}
	currentPaths = kept

	// For each subsequent segment, extend paths
	for segIdx := 1; segIdx < len(match.Segments); segIdx++ {
		seg := match.Segments[segIdx]
		var extendedPaths []PathResult

		for _, path := range currentPaths {
			if len(path.Nodes) == 0 {
				continue
			}

			// The last node in current path should be the start of next segment
			lastNode := path.Nodes[len(path.Nodes)-1]

			// Create a match for this segment starting from the last node
			segMatch := &TraversalMatch{
				StartNode:    seg.FromNode,
				EndNode:      seg.ToNode,
				Relationship: seg.Relationship,
			}

			// Traverse from the last node
			segPaths := e.traverseFromNode(ctx, lastNode, segMatch)

			// Join paths: combine current path with each segment path
			for _, segPath := range segPaths {
				if pathResultsReuseRelationship(path, segPath) {
					continue
				}
				// Create extended path
				extended := PathResult{
					Nodes:          make([]*storage.Node, 0, len(path.Nodes)+len(segPath.Nodes)-1),
					Relationships:  make([]*storage.Edge, 0, len(path.Relationships)+len(segPath.Relationships)),
					Length:         path.Length + segPath.Length,
					SegmentLengths: append(append([]int{}, path.SegmentLengths...), len(segPath.Relationships)),
				}

				// Add all nodes from current path
				extended.Nodes = append(extended.Nodes, path.Nodes...)

				// Add nodes from segment path (skip first node as it's the same as last node of current)
				if len(segPath.Nodes) > 1 {
					extended.Nodes = append(extended.Nodes, segPath.Nodes[1:]...)
				}

				// Add all relationships
				extended.Relationships = append(extended.Relationships, path.Relationships...)
				extended.Relationships = append(extended.Relationships, segPath.Relationships...)

				if repeats.consistent(extended, segIdx+1) {
					extendedPaths = append(extendedPaths, extended)
				}
			}
		}

		currentPaths = extendedPaths
	}

	return currentPaths
}

// chainNodeRepeats lists, for each node position of a chain (position j
// ends segment j-1; position 0 starts the chain), the earlier position that
// names the same variable, or -1. A variable named twice in one chain, as b
// in (a)-[:R]->(b)-[:S]->(b), binds one node: Neo4j matches only the paths
// whose two positions are that node. It is nil when no variable repeats.
type chainNodeRepeats []int

func chainRepeatedNodes(segments []TraversalSegment) chainNodeRepeats {
	variable := func(position int) string {
		if position == 0 {
			return segments[0].FromNode.variable
		}
		return segments[position-1].ToNode.variable
	}
	var repeats chainNodeRepeats
	for position := 1; position <= len(segments); position++ {
		name := variable(position)
		if name == "" {
			continue
		}
		for earlier := 0; earlier < position; earlier++ {
			if variable(earlier) != name {
				continue
			}
			if repeats == nil {
				repeats = make(chainNodeRepeats, len(segments)+1)
				for index := range repeats {
					repeats[index] = -1
				}
			}
			repeats[position] = earlier
			break
		}
	}
	return repeats
}

// consistent reports whether path, which has its first position segments
// (path.SegmentLengths), binds the node at position to the node at the
// earlier position of the same variable.
func (repeats chainNodeRepeats) consistent(path PathResult, position int) bool {
	if repeats == nil || repeats[position] < 0 {
		return true
	}
	return chainPositionNode(path, position).ID == chainPositionNode(path, repeats[position]).ID
}

func chainPositionNode(path PathResult, position int) *storage.Node {
	offset := 0
	for _, length := range path.SegmentLengths[:position] {
		offset += length
	}
	return path.Nodes[offset]
}

func pathResultsReuseRelationship(left, right PathResult) bool {
	for _, leftRelationship := range left.Relationships {
		for _, rightRelationship := range right.Relationships {
			if leftRelationship != nil && rightRelationship != nil && leftRelationship.ID == rightRelationship.ID {
				return true
			}
		}
	}
	return false
}

// traverseFromNode traverses from a specific node rather than finding start nodes by label
func (e *StorageExecutor) traverseFromNode(traversalCtx context.Context, startNode *storage.Node, match *TraversalMatch) []PathResult {
	// Verify the start node matches the expected pattern (labels and properties)
	if len(match.StartNode.labels) > 0 {
		found := false
		for _, label := range startNode.Labels {
			for _, requiredLabel := range match.StartNode.labels {
				if label == requiredLabel {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return nil
		}
	}

	if len(match.StartNode.properties) > 0 {
		if !e.nodeMatchesProps(startNode, match.StartNode.properties) {
			return nil
		}
	}

	ctx := e.newTraversalContext(traversalCtx, startNode, &match.Relationship)
	return e.findPaths(ctx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode)
}

// newTraversalContext is the depth-first search state for relationship
// pattern rel from startNode: its types, properties, direction and length
// bounds, the statement's temporal viewport, and traversalCtx for
// cancellation.
func (e *StorageExecutor) newTraversalContext(traversalCtx context.Context, startNode *storage.Node, rel *RelationshipPattern) *TraversalContext {
	ctx := &TraversalContext{
		startNode:     startNode,
		relTypes:      rel.Types,
		relTypeSet:    buildRelTypeSet(rel.Types),
		relProperties: rel.Properties,
		direction:     rel.Direction,
		minHops:       rel.MinHops,
		maxHops:       rel.MaxHops,
		usedEdges:     make(map[storage.EdgeID]bool),
		nodeCache:     make(map[storage.NodeID]*storage.Node),
		cancelCtx:     traversalCtx,
	}
	if viewport, ok := TemporalViewportFromContext(traversalCtx); ok {
		ctx.temporalViewport = viewport
		if checker, canCheck := e.getStorage(traversalCtx).(temporalCurrentNodeChecker); canCheck {
			ctx.temporalChecker = checker
		}
	}
	return ctx
}

// loadTraversalEndpointNode resolves a traversal endpoint using the same
// tolerance as the existing traversal path: if the adjacency entry points at a
// node that no longer loads, the candidate path is skipped rather than failing
// the whole query. This gives callers one place to share the current dangling-
// edge semantics.
func (e *StorageExecutor) loadTraversalEndpointNode(ctx *TraversalContext, nextNodeID storage.NodeID) (*storage.Node, bool) {
	nextNode := ctx.nodeCache[nextNodeID]
	if nextNode == nil && ctx.endpointsNeedOnlyExist && !ctx.temporalViewport.Enabled() {
		if checker, ok := e.storage.(storage.RelationshipEndpointChecker); ok {
			if visible, answered := checker.RelationshipEndpointVisible(nextNodeID); answered {
				if !visible {
					return nil, false
				}
				nextNode = &storage.Node{ID: nextNodeID}
				ctx.nodeCache[nextNodeID] = nextNode
			}
		}
	}
	if nextNode == nil {
		var err error
		nextNode, err = e.storage.GetNode(nextNodeID)
		if err != nil || nextNode == nil {
			return nil, false
		}
		ctx.nodeCache[nextNodeID] = nextNode
	}
	visible, err := nodeVisibleInTemporalViewport(nextNode, ctx.temporalViewport, ctx.temporalChecker)
	if err != nil || !visible {
		return nil, false
	}
	return nextNode, true
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

	// Cancellation probe — amortised across recursive entries to keep the
	// per-call cost negligible. When ctx.cancelCtx is canceled we unwind the
	// recursion immediately by returning whatever we've gathered so far.
	if ctx.cancelCtx != nil {
		ctx.findPathsCalls++
		if ctx.findPathsCalls&bfsCancelCheckMask == 0 {
			if ctx.cancelCtx.Err() != nil {
				return results
			}
		}
	}

	// OPTIMIZATION: Early termination if limit reached
	if ctx.limit > 0 && ctx.resultCount >= ctx.limit {
		return results
	}

	if depth > ctx.deepest {
		ctx.deepest = depth
	}

	// Check if current path meets minimum length and endpoint requirements
	if depth >= ctx.minHops && (ctx.endNodeID == "" || currentNode.ID == ctx.endNodeID) {
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
		edges = e.traversalEdges(ctx, currentNode.ID, true)
	case "incoming":
		edges = e.traversalEdges(ctx, currentNode.ID, false)
	case "both":
		var err error
		edges, err = undirectedIncidentEdges(e.storage, currentNode.ID)
		if err != nil && ctx.cancelCtx != nil {
			recordExpressionFailure(ctx.cancelCtx, err)
			return results
		}
	}

	// Traverse each edge
	for _, edge := range edges {
		// A Cypher path may revisit a node, but it cannot reuse a relationship.
		// This also prevents an undirected expansion from walking the same edge
		// immediately back in the opposite direction.
		if ctx.usedEdges[edge.ID] {
			continue
		}
		// Check relationship type filter
		if len(ctx.relTypes) > 0 {
			if len(ctx.relTypes) == 1 {
				if edge.Type != ctx.relTypes[0] {
					continue
				}
			} else {
				if _, ok := ctx.relTypeSet[edge.Type]; !ok {
					continue
				}
			}
		}
		if len(ctx.relProperties) > 0 && !e.edgeMatchesProps(edge, ctx.relProperties) {
			continue
		}

		// Get next node
		var nextNodeID storage.NodeID
		if ctx.direction == "outgoing" || (ctx.direction == "both" && edge.StartNode == currentNode.ID) {
			nextNodeID = edge.EndNode
		} else {
			nextNodeID = edge.StartNode
		}

		nextNode, ok := e.loadTraversalEndpointNode(ctx, nextNodeID)
		if !ok {
			continue
		}

		ctx.usedEdges[edge.ID] = true

		// Recurse with optimized path copying (pre-allocate exact size)
		nextNodesLen, ok := util.SafeIntAdd(len(pathNodes), 1)
		if !ok {
			ctx.usedEdges[edge.ID] = false
			continue
		}
		newPathNodes := make([]*storage.Node, nextNodesLen)
		copy(newPathNodes, pathNodes)
		newPathNodes[len(pathNodes)] = nextNode

		nextEdgesLen, ok := util.SafeIntAdd(len(pathEdges), 1)
		if !ok {
			ctx.usedEdges[edge.ID] = false
			continue
		}
		newPathEdges := make([]*storage.Edge, nextEdgesLen)
		copy(newPathEdges, pathEdges)
		newPathEdges[len(pathEdges)] = edge

		subPaths := e.findPaths(ctx, nextNode, newPathNodes, newPathEdges, depth+1, endPattern)
		results = append(results, subPaths...)

		// Unmark for sibling paths.
		ctx.usedEdges[edge.ID] = false
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
	nodes        map[string]*storage.Node
	rels         map[string]*storage.Edge
	pathLength   int                    // Length of the path for length(path) function
	paths        map[string]*PathResult // Full paths by variable name for relationships(path), nodes(path)
	allPathEdges []*storage.Edge        // All edges in the path (for variable-length patterns)
	allPathNodes []*storage.Node        // All nodes in the path
}

// pathContextValues returns the path's variables as row values: nodes and
// relationships as themselves, named paths as the pipeline's path maps.
func (e *StorageExecutor) pathContextValues(pathCtx PathContext) map[string]interface{} {
	values := make(map[string]interface{}, len(pathCtx.nodes)+len(pathCtx.rels)+len(pathCtx.paths))
	for name, node := range pathCtx.nodes {
		if node != nil {
			values[name] = node
		}
	}
	for name, relationship := range pathCtx.rels {
		if relationship != nil {
			values[name] = relationship
		}
	}
	for name, path := range pathCtx.paths {
		if path != nil {
			values[name] = e.pathContextRowValue(path)
		}
	}
	return values
}

// relationshipOnlyPath reports whether a path context entry is a
// variable-length relationship variable: the context keeps it as a
// PathResult holding only the relationships.
func relationshipOnlyPath(path *PathResult) bool {
	return len(path.Nodes) == 0
}

// relationshipListValue is a variable-length relationship variable's Cypher
// value: its relationships in path order.
func relationshipListValue(relationships []*storage.Edge) []interface{} {
	values := make([]interface{}, len(relationships))
	for index, relationship := range relationships {
		values[index] = relationship
	}
	return values
}

// pathContextEntryValue is a path context entry's value for the path-context
// expression evaluator: a variable-length relationship variable is its list
// of relationships, a named path the evaluator's path map.
func pathContextEntryValue(path *PathResult) interface{} {
	if relationshipOnlyPath(path) {
		return relationshipListValue(path.Relationships)
	}
	return map[string]interface{}{
		"_pathResult": path,
		"length":      path.Length,
		"nodes":       path.Nodes,
		"rels":        path.Relationships,
	}
}

// pathContextRowValue is a path context entry's value for the row
// evaluator: a variable-length relationship variable is its list of
// relationships, a named path the pipeline's path map (#882).
func (e *StorageExecutor) pathContextRowValue(path *PathResult) interface{} {
	if relationshipOnlyPath(path) {
		return relationshipListValue(path.Relationships)
	}
	return e.pathToMap(*path)
}

// buildPathContext creates a context for evaluating expressions over a path
func (e *StorageExecutor) buildPathContext(path PathResult, match *TraversalMatch) PathContext {
	ctx := PathContext{
		nodes:        make(map[string]*storage.Node),
		rels:         make(map[string]*storage.Edge),
		paths:        make(map[string]*PathResult),
		pathLength:   path.Length,        // Store path length for length(path) function
		allPathEdges: path.Relationships, // Store all edges for relationships(path)
		allPathNodes: path.Nodes,         // Store all nodes for nodes(path)
	}

	// Map the path variable if present (for path functions like relationships(path), nodes(path))
	if match.PathVariable != "" {
		pathCopy := path // Make a copy to store pointer
		ctx.paths[match.PathVariable] = &pathCopy
	}

	// Map start node
	if match.StartNode.variable != "" && len(path.Nodes) > 0 {
		ctx.nodes[match.StartNode.variable] = path.Nodes[0]
	}

	// Map end node
	if match.EndNode.variable != "" && len(path.Nodes) > 0 {
		ctx.nodes[match.EndNode.variable] = path.Nodes[len(path.Nodes)-1]
	}

	// For chained patterns, map intermediate nodes
	// Path nodes layout: [startNode, intermediate1, intermediate2, ..., endNode]
	// IntermediateNodes layout: [intermediate1, intermediate2, ...]
	if match.IsChained && len(match.IntermediateNodes) > 0 {
		nodeOffset := 0
		for i, intermediateInfo := range match.IntermediateNodes {
			if i < len(path.SegmentLengths) {
				nodeOffset += path.SegmentLengths[i]
			} else {
				nodeOffset++
			}
			if intermediateInfo.variable != "" {
				pathIdx := nodeOffset
				if pathIdx < len(path.Nodes) {
					ctx.nodes[intermediateInfo.variable] = path.Nodes[pathIdx]
				}
			}
		}

		// Also map relationships from each segment
		relationshipOffset := 0
		for i, seg := range match.Segments {
			segmentLength := 1
			if i < len(path.SegmentLengths) {
				segmentLength = path.SegmentLengths[i]
			}
			segmentEnd := relationshipOffset + segmentLength
			if segmentEnd > len(path.Relationships) {
				segmentEnd = len(path.Relationships)
			}
			if seg.Relationship.Variable != "" {
				if seg.Relationship.VariableLength {
					relationships := append([]*storage.Edge{}, path.Relationships[relationshipOffset:segmentEnd]...)
					ctx.paths[seg.Relationship.Variable] = &PathResult{Relationships: relationships}
				} else if relationshipOffset < segmentEnd {
					ctx.rels[seg.Relationship.Variable] = path.Relationships[relationshipOffset]
				}
			}
			relationshipOffset = segmentEnd
		}
	} else {
		// Map relationship (single segment)
		if match.Relationship.Variable != "" {
			if match.Relationship.VariableLength {
				relationships := append([]*storage.Edge{}, path.Relationships...)
				ctx.paths[match.Relationship.Variable] = &PathResult{Relationships: relationships}
			} else if len(path.Relationships) > 0 {
				ctx.rels[match.Relationship.Variable] = path.Relationships[0]
			}
		}
	}

	return ctx
}

// shortestPath finds the shortest path between two nodes.
// ctx is checked periodically inside the BFS so the caller can cancel a
// long-running traversal (client disconnect, server shutdown).
//
// Implementation notes:
//
//   - When the storage chain implements AdjacentEdgesEngine, BFS fetches both
//     directions in a single underlying view per frontier node. Profiling
//     showed ~64% of per-request CPU was in Badger view-transaction setup;
//     halving the open-count is the dominant win.
//   - The frontier stores parent pointers (predecessor edge + node ID) only,
//     not full path slices. Old behavior copied O(L) nodes/edges per
//     discovered neighbor — for paths of length ~50 that's tens of thousands
//     of unused allocations because BFS visits far more nodes than it ends
//     up keeping. We rebuild the path once, after the end is hit, fetching
//     the materialized node bodies in a single BatchGetNodes call.
func (e *StorageExecutor) shortestPath(ctx context.Context, startNode, endNode *storage.Node, relTypes []string, direction string, maxHops int) (*PathResult, error) {
	if startNode == nil || endNode == nil {
		return nil, nil
	}

	relTypeSet := buildRelTypeSet(relTypes)

	preds := map[storage.NodeID]bfsPredecessor{startNode.ID: {}}
	queue := []storage.NodeID{startNode.ID}

	adj, hasAdj := e.storage.(storage.AdjacentEdgesEngine)

	for head := 0; head < len(queue); head++ {
		if head&bfsCancelCheckMask == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		currentID := queue[head]
		currentDepth := preds[currentID].depth
		if currentDepth >= maxHops {
			continue
		}

		var outgoing, incoming []*storage.Edge
		switch direction {
		case "outgoing":
			outgoing, _ = e.storage.GetOutgoingEdges(currentID)
		case "incoming":
			incoming, _ = e.storage.GetIncomingEdges(currentID)
		default:
			if hasAdj {
				outgoing, incoming, _ = adj.GetAdjacentEdges(currentID)
			} else {
				outgoing, _ = e.storage.GetOutgoingEdges(currentID)
				incoming, _ = e.storage.GetIncomingEdges(currentID)
			}
		}

		expand := func(edge *storage.Edge, fromOutgoing bool) (done bool) {
			if len(relTypes) == 1 {
				if edge.Type != relTypes[0] {
					return false
				}
			} else if len(relTypes) > 1 {
				if _, ok := relTypeSet[edge.Type]; !ok {
					return false
				}
			}

			var nextNodeID storage.NodeID
			switch {
			case direction == "outgoing":
				nextNodeID = edge.EndNode
			case direction == "incoming":
				nextNodeID = edge.StartNode
			case fromOutgoing:
				nextNodeID = edge.EndNode
			default:
				nextNodeID = edge.StartNode
			}
			if _, seen := preds[nextNodeID]; seen {
				return false
			}
			preds[nextNodeID] = bfsPredecessor{parent: currentID, edge: edge, depth: currentDepth + 1}
			if nextNodeID == endNode.ID {
				return true
			}
			queue = append(queue, nextNodeID)
			return false
		}

		for _, edge := range outgoing {
			if expand(edge, true) {
				return e.reconstructShortestPath(ctx, startNode, endNode, preds)
			}
		}
		for _, edge := range incoming {
			if expand(edge, false) {
				return e.reconstructShortestPath(ctx, startNode, endNode, preds)
			}
		}
	}

	return nil, nil // No path found
}

// bfsPredecessor records, for every node BFS visits, the edge it was
// discovered through and the parent NodeID. We rebuild the path by walking
// these pointers from end → start once the goal is reached.
type bfsPredecessor struct {
	parent storage.NodeID
	edge   *storage.Edge
	depth  int
}

// reconstructShortestPath walks the predecessor map from end → start and
// materializes the *Node bodies for every node on the path in a single
// BatchGetNodes call. The startNode/endNode bodies are used as-is when
// available (the executor already had to fetch them to start BFS).
func (e *StorageExecutor) reconstructShortestPath(ctx context.Context, startNode, endNode *storage.Node, preds map[storage.NodeID]bfsPredecessor) (*PathResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var revIDs []storage.NodeID
	var revEdges []*storage.Edge
	cur := endNode.ID
	for cur != startNode.ID {
		p := preds[cur]
		revIDs = append(revIDs, cur)
		revEdges = append(revEdges, p.edge)
		cur = p.parent
		if cur == "" {
			return nil, nil
		}
	}
	revIDs = append(revIDs, startNode.ID)

	// Reverse path order: start → end.
	pathIDs := make([]storage.NodeID, len(revIDs))
	for i, id := range revIDs {
		pathIDs[len(revIDs)-1-i] = id
	}
	pathEdges := make([]*storage.Edge, len(revEdges))
	for i, edge := range revEdges {
		pathEdges[len(revEdges)-1-i] = edge
	}

	nodeBodies, err := e.storage.BatchGetNodes(pathIDs)
	if err != nil {
		return nil, err
	}
	pathNodes := make([]*storage.Node, len(pathIDs))
	for i, id := range pathIDs {
		switch id {
		case startNode.ID:
			pathNodes[i] = startNode
		case endNode.ID:
			pathNodes[i] = endNode
		default:
			n, ok := nodeBodies[id]
			if !ok || n == nil {
				return nil, nil
			}
			pathNodes[i] = n
		}
	}

	return &PathResult{
		Nodes:         pathNodes,
		Relationships: pathEdges,
		Length:        len(pathEdges),
	}, nil
}

// allShortestPaths finds all shortest paths between two nodes.
// ctx is checked periodically inside the BFS so the caller can cancel.
func (e *StorageExecutor) allShortestPaths(ctx context.Context, startNode, endNode *storage.Node, relTypes []string, direction string, maxHops int) ([]PathResult, error) {
	if startNode == nil || endNode == nil {
		return nil, nil
	}

	relTypeSet := buildRelTypeSet(relTypes)

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
	visitedDepth := map[storage.NodeID]int{startNode.ID: 0}

	for head := 0; head < len(queue); head++ {
		if head&bfsCancelCheckMask == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		current := queue[head]

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
			edges, _ = undirectedIncidentEdges(e.storage, current.node.ID)
		}

		for _, edge := range edges {
			// Filter by type
			if len(relTypes) > 0 {
				if len(relTypes) == 1 {
					if edge.Type != relTypes[0] {
						continue
					}
				} else {
					if _, ok := relTypeSet[edge.Type]; !ok {
						continue
					}
				}
			}

			var nextNodeID storage.NodeID
			if direction == "outgoing" || (direction == "both" && edge.StartNode == current.node.ID) {
				nextNodeID = edge.EndNode
			} else {
				nextNodeID = edge.StartNode
			}

			// Allow revisit if at same depth (for multiple paths)
			prevDepth, seen := visitedDepth[nextNodeID]
			if seen && prevDepth < current.path.Length+1 {
				continue
			}

			nextNode, err := e.storage.GetNode(nextNodeID)
			if err != nil || nextNode == nil {
				continue
			}

			newNodes := make([]*storage.Node, len(current.path.Nodes)+1)
			copy(newNodes, current.path.Nodes)
			newNodes[len(current.path.Nodes)] = nextNode

			newRels := make([]*storage.Edge, len(current.path.Relationships)+1)
			copy(newRels, current.path.Relationships)
			newRels[len(current.path.Relationships)] = edge

			newPath := PathResult{
				Nodes:         newNodes,
				Relationships: newRels,
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

			visitedDepth[nextNodeID] = current.path.Length + 1
			queue = append(queue, queueItem{node: nextNode, path: newPath})
		}
	}

	return results, nil
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
// This evaluates conditions like "i.name = 'value'" against each path's
// context. extra merges the row's other bindings into the context, so a
// predicate can also reference seed-row variables beyond the path endpoints
// (#581: WHERE length(p) > limit.n).
func (e *StorageExecutor) filterPathsByWhere(ctx context.Context, paths []PathResult, matches *TraversalMatch, whereClause string, extra map[string]interface{}) []PathResult {
	if whereClause == "" {
		return paths
	}

	var filtered []PathResult
	for _, path := range paths {
		context := e.buildPathContext(path, matches)
		for name, value := range extra {
			switch v := value.(type) {
			case *storage.Node:
				if v != nil {
					context.nodes[name] = v
				}
			case *storage.Edge:
				if v != nil {
					context.rels[name] = v
				}
			}
		}
		if e.evaluateWhereOnPath(ctx, whereClause, context) {
			filtered = append(filtered, path)
		}
	}
	return filtered
}

// evaluateWhereOnPath evaluates a WHERE condition against a path context.
// Handles conditions like: i.name = 'value', e.score < 90, etc.
func (e *StorageExecutor) evaluateWhereOnPath(ctx context.Context, whereClause string, pathCtx PathContext) bool {
	values := e.pathContextValues(pathCtx)
	bindParameterRow(ctx, values)
	return e.evaluateRowPredicate(ctx, strings.TrimSpace(whereClause), values)
}

func (e *StorageExecutor) pathSubqueryMatches(ctx context.Context, outer PathContext, subquery string) bool {
	subquery = strings.TrimSpace(subquery)
	if !hasPrefixFold(subquery, "MATCH ") {
		return false
	}
	pattern := strings.TrimSpace(subquery[len("MATCH"):])
	innerWhere := ""
	if whereIdx := findKeywordIndex(pattern, "WHERE"); whereIdx > 0 {
		innerWhere = strings.TrimSpace(pattern[whereIdx+5:])
		pattern = strings.TrimSpace(pattern[:whereIdx])
	}

	if looksLikeRowRelationshipPattern(pattern) {
		matches := e.parseTraversalPattern(ctx, pattern)
		if matches == nil {
			return false
		}
		var paths []PathResult
		if seed := outer.nodes[matches.StartNode.variable]; seed != nil {
			if matches.IsChained && len(matches.Segments) > 1 {
				paths = e.traverseChainedGraph(ctx, matches, []*storage.Node{seed})
			} else {
				paths = e.traverseFromNode(ctx, seed, matches)
			}
		} else {
			paths = e.traverseGraph(ctx, matches)
		}
		for _, path := range paths {
			inner := e.buildPathContext(path, matches)
			correlated := true
			for name, node := range outer.nodes {
				if bound, exists := inner.nodes[name]; exists {
					if bound == nil || node == nil || bound.ID != node.ID {
						correlated = false
						break
					}
					continue
				}
				inner.nodes[name] = node
			}
			if !correlated {
				continue
			}
			for name, rel := range outer.rels {
				if bound, exists := inner.rels[name]; exists {
					if bound == nil || rel == nil || bound.ID != rel.ID {
						correlated = false
						break
					}
					continue
				}
				inner.rels[name] = rel
			}
			if !correlated {
				continue
			}
			if innerWhere == "" || e.evaluateRowPredicate(ctx, innerWhere, pipelineRowFromPathContext(inner)) {
				return true
			}
		}
		return false
	}

	nodePattern := e.parseNodePattern(ctx, pattern)
	if nodePattern.variable == "" && len(nodePattern.labels) == 0 && len(nodePattern.properties) == 0 {
		return false
	}
	nodes, err := e.loadPatternNodes(ctx, nodePattern.labels, nodePattern.properties)
	if err != nil {
		return false
	}
	for _, node := range nodes {
		inner := outer
		inner.nodes = make(map[string]*storage.Node, len(outer.nodes)+1)
		for name, outerNode := range outer.nodes {
			inner.nodes[name] = outerNode
		}
		if nodePattern.variable != "" {
			inner.nodes[nodePattern.variable] = node
		}
		if innerWhere == "" || e.evaluateRowPredicate(ctx, innerWhere, pipelineRowFromPathContext(inner)) {
			return true
		}
	}
	return false
}

func pipelineRowFromPathContext(path PathContext) map[string]interface{} {
	row := make(map[string]interface{}, len(path.nodes)+len(path.rels))
	for name, node := range path.nodes {
		row[name] = node
	}
	for name, relationship := range path.rels {
		row[name] = relationship
	}
	return row
}
