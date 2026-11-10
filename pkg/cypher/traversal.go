// Package cypher provides graph traversal operations for NornicDB.
// This file implements relationship pattern matching, variable-length paths,
// and shortest path algorithms for Neo4j-compatible traversal queries.

package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
	usedEdges        map[storage.EntityIdentity]bool
	nodeProperties   []string                         // nil requires full entities; otherwise only demanded fields
	nodeCache        map[storage.NodeID]*storage.Node // Cache for batch-fetched nodes
	limit            int                              // OPTIMIZATION: Early termination limit (0 = no limit)
	resultCount      int                              // Count of results found so far
	temporalViewport TemporalViewport
	temporalChecker  temporalCurrentNodeChecker
	// cancelCtx is the required originating statement context. Every production
	// initializer supplies it; private zero-value traversal states are invalid.
	// DFS entry and sibling expansion retain its cancellation and ownership.
	cancelCtx            context.Context
	cancelDone           <-chan struct{}
	cancelParentDone     <-chan struct{}
	cancelInitialized    bool
	cancellationObserved bool
	// memory is the traversal's account in the statement's memory scope,
	// resolved once from cancelCtx (#1343).
	memory         memoryAccount
	memoryResolved bool
	// endNodeID, when set, is the only node a path may end at (a shortestPath
	// pair, #863).
	endNodeID storage.NodeID
	// deepest is the greatest depth the search reached.
	deepest int
	// endpointsNeedOnlyExist is set when nothing in the statement reads the
	// nodes the traversal reaches (traversalEndpointsNeedOnlyExist).
	endpointsNeedOnlyExist bool
	// endpointsNeedOnlyLabels is set when the nodes the traversal reaches
	// need only their labels checked: a labelled, property-less endpoint
	// with no path variable exposing it (traversalEndpointsNeedOnlyLabels).
	// The traversal retains ID + labels after the canonical visibility read.
	endpointsNeedOnlyLabels bool
	// relationshipsNeedOnlyHeaders is set when nothing in the statement reads
	// the relationships' properties (traversalRelationshipsNeedOnlyHeaders).
	relationshipsNeedOnlyHeaders bool
	// repeatable lets a path repeat relationships: a walk, under MATCH
	// REPEATABLE ELEMENTS (repeatable_elements.go).
	repeatable bool
	// pinnedRelationship, when set, is the only relationship the path's first
	// step may traverse. Its start nodes are that relationship's own endpoints
	// (relationshipIDSeedEndpointNodes), so the step lists no adjacency.
	pinnedRelationship *storage.Edge
}

// traversalEndpointsNeedOnlyExist reports whether the nodes a traversal
// reaches are needed only to exist and be visible: the end node is anonymous
// with no labels or property map, and no path variable exposes the nodes.
// The traversal checks canonical query visibility before retaining a header.
func traversalEndpointsNeedOnlyExist(match *TraversalMatch) bool {
	if match.IntermediateNode {
		return false
	}
	return !match.IsChained && match.PathVariable == "" && match.EndNode.variable == "" &&
		len(match.EndNode.labels) == 0 && len(match.EndNode.properties) == 0
}

// traversalEndpointsNeedOnlyLabels reports whether the nodes a traversal
// reaches need only their labels checked: a labelled endpoint with no inline
// property map, no path variable exposing the node, and either an anonymous
// endpoint variable or a named one whose properties no later clause reads
// (match.UnusedPropertyVariables). The traversal retains ID + labels after
// checking canonical query visibility (#1110).
func traversalEndpointsNeedOnlyLabels(match *TraversalMatch) bool {
	if match.IntermediateNode {
		return false
	}
	if match.IsChained || match.PathVariable != "" ||
		len(match.EndNode.labels) == 0 || len(match.EndNode.properties) > 0 {
		return false
	}
	if match.EndNode.variable == "" {
		return true
	}
	return match.UnusedPropertyVariables[match.EndNode.variable]
}

// traversalRelationshipsNeedOnlyHeaders reports whether a traversal needs
// its relationships only for their ID, type and endpoints: the relationship
// is anonymous with no property map and no path variable exposes it. The
// traversal then lists them through storage.EdgeHeaderReader, which can
// answer without reading the relationship records.
func traversalRelationshipsNeedOnlyHeaders(match *TraversalMatch) bool {
	return !match.IsChained && match.PathVariable == "" && (match.Relationship.Variable == "" || match.UnusedPropertyVariables[match.Relationship.Variable]) && len(match.Relationship.Properties) == 0
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
			edges, answered, err = reader.OutgoingEdgeHeaders(ctx.cancelCtx, nodeID)
		} else {
			edges, answered, err = reader.IncomingEdgeHeaders(ctx.cancelCtx, nodeID)
		}
		if err != nil {
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
		edges, err = e.storage.GetOutgoingEdges(ctx.cancelCtx, nodeID)
	} else {
		edges, err = e.storage.GetIncomingEdges(ctx.cancelCtx, nodeID)
	}
	if err != nil {
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
func (e *StorageExecutor) parseRelationshipPattern(ctx context.Context, pattern string) (*RelationshipPattern, error) {
	return readRelationshipPattern(ctx, pattern, e.parsePropertyMap)
}

// relationshipArrows classifies the existing endpoint markers together so a
// two-headed relationship retains the same both-direction semantics everywhere.
func relationshipArrows(pattern string) (direction string, incoming, outgoing bool) {
	incoming = strings.HasPrefix(pattern, "<-")
	outgoing = strings.HasSuffix(pattern, "->")
	switch {
	case incoming && outgoing:
		return "both", incoming, outgoing
	case incoming:
		return "incoming", incoming, outgoing
	case outgoing:
		return "outgoing", incoming, outgoing
	default:
		return "both", incoming, outgoing
	}
}

func readRelationshipPattern(ctx context.Context, pattern string, properties patternPropertyReader) (*RelationshipPattern, error) {
	direction, hasIncomingArrow, hasOutgoingArrow := relationshipArrows(pattern)
	result := &RelationshipPattern{Direction: direction, MinHops: 1, MaxHops: 1, Properties: make(map[string]interface{})}
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

	// Extract [r:TYPE {props}] part, read outside backticks: a quoted type
	// may hold any character, a * or : included (#879, #907).
	if strings.HasPrefix(pattern, "[") && strings.HasSuffix(pattern, "]") {
		declaration := relationshipDeclarationOf(pattern[1 : len(pattern)-1])

		// Variable length: [*], [*2], [*1..3], [*2..], [*..5].
		if declaration.hasLength {
			result.VariableLength = true
			spec := declaration.length
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
		}

		// Property maps are valid with or without a relationship type, e.g.
		// [r {name: 'value'}] and [r:TYPE {name: 'value'}].
		if declaration.properties != "" {
			var err error
			result.Properties, err = properties(ctx, declaration.properties, true)
			if err != nil {
				return nil, err
			}
		}
		result.Variable = declaration.variable
		result.Types = declaration.types
	}

	return result, nil
}

// executeMatchWithRelationshipsWithPathSeeded is the single traversal entry
// with an additional endSeedNodes parameter. When a pipeline row already binds
// the pattern's end-node variable (but not its start-node variable), the
// caller passes that node here so the traversal starts from the known
// endpoint and walks backwards, instead of expanding the pattern over every
// relationship in the store and joining the bound value afterward. seedNodes
// (start-side) takes priority when both are supplied. relationship, when the
// caller's own id / elementId lookup supplies it, is the one relationship the
// pattern may walk: the traversal then starts from that relationship's own
// endpoints and lists no adjacency.
func (e *StorageExecutor) executeMatchWithRelationshipsWithPathSeeded(ctx context.Context, pattern string, whereClause string, returnItems []returnItem, seedNodes []*storage.Node, endSeedNodes []*storage.Node, relationship *storage.Edge, pathVariable string, readTail []string, earlyLimit int) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	collector := newTraversalResultCollector(ctx, result)
	defer collector.close()

	// Parse the pattern: (a:Label)-[r:TYPE]->(b:Label)
	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil {
		if err := getExpressionFailure(ctx); err != nil {
			return nil, err
		}
		return result, localizedError(localization.CypherMatchingTraversalPatternInvalid(pattern), nil)
	}
	if !matches.IsChained && len(seedNodes) > 0 && len(endSeedNodes) == 1 {
		matches.boundEndNodeID = storage.NodeID(endSeedNodes[0].IdentityKey())
	}
	returnItems = expandTraversalWildcardReturnItems(returnItems, matches, pathVariable)
	for _, item := range returnItems {
		result.Columns = append(result.Columns, item.column())
	}

	// Store the path variable for path functions (relationships(path), nodes(path), length(path))
	if pathVariable != "" {
		matches.PathVariable = pathVariable
	}
	// Record which pattern node variables' properties no later clause reads,
	// so a named labelled endpoint can load by ID + labels only (#1110). The
	// pipeline supplies the read-only clauses after this MATCH (readTail);
	// direct callers supply the RETURN items themselves.
	if readTail != nil {
		matches.UnusedPropertyVariables = e.traversalUnusedPropertyVariablesForTail(matches, whereClause, readTail)
		if !matches.IsChained && matches.PathVariable == "" {
			matches.endpointProperties = traversalSourceProperties(matches.EndNode, whereClause, readTail, "")
		}
	} else {
		matches.UnusedPropertyVariables = e.traversalUnusedPropertyVariables(matches, returnItems, whereClause)
	}
	if earlyLimit == 0 {
		return result, nil
	}
	repeatedEndpoint := matches.StartNode.variable != "" && matches.StartNode.variable == matches.EndNode.variable
	if earlyLimit > 0 && whereClause == "" && !repeatedEndpoint {
		matches.TraversalLimit = earlyLimit
	}

	// A plain relationship count is answered from the counters when they can
	// (relationshipCountFromStore decides), instead of materializing paths.
	if whereClause == "" && pathVariable == "" && len(returnItems) == 1 {
		if count, ok, err := e.relationshipCountFromStore(ctx, matches, returnItems[0]); ok {
			if err != nil {
				return nil, err
			}
			if err := collector.appendRow([]interface{}{count}); err != nil {
				return nil, err
			}
			return result, nil
		}
	}

	// A relationship the WHERE pins by id / elementId is the only relationship
	// this pattern may walk, and its own endpoints are the start nodes, so the
	// traversal lists no adjacency. The relationship's type, direction, inline
	// properties, endpoint admission and the WHERE keep their existing owners.
	if relationship != nil && !matches.IsChained {
		matches.pinnedRelationship = relationship
		if len(seedNodes) == 0 && len(endSeedNodes) == 0 {
			seedNodes = e.relationshipIDSeedEndpointNodes(ctx, relationship, matches)
			if len(seedNodes) == 0 {
				// Endpoint admission rejected every endpoint: nothing matches, and
				// the traversal must not fall back to a store-wide scan.
				return result, nil
			}
		}
	}

	// OPTIMIZATION: If WHERE clause filters by id(startNode) = value, filter start nodes before traversal
	// This avoids traversing from all nodes when we only need one specific node
	var optimizedStartNodes []*storage.Node
	usedPropertyIndex := false
	// startDecided records that the start node's candidates were decided,
	// possibly as none: an index seek or the start-property pre-scan
	// answered. An empty answer means no path starts anywhere; scanning the
	// start label again would only find the same nothing.
	startDecided := false
	// A bound end node with an unbound start is where a non-chained pattern
	// starts (traverseFromEndSeeds): searching the start side first, for
	// instance for every node whose property the WHERE requires to be non-null,
	// only finds a population the bound end reaches by its own relationships.
	endBound := len(seedNodes) == 0 && len(endSeedNodes) > 0 && !matches.IsChained
	// Inline preparation without WHERE must not hydrate a common posting
	// merely to discard it after choosing the opposite endpoint. Existing
	// posting counts can prove a zero/single candidate seek before reading it.
	eagerInlineStart := false
	if len(seedNodes) == 0 && !endBound && whereClause == "" && !matches.IsChained && len(matches.StartNode.properties) > 0 {
		seek, planned, err := e.planPatternPropertySeek(ctx, matches.StartNode, e.getStorage(ctx))
		if err != nil {
			return nil, err
		}
		if planned {
			count, err := seek.estimate(ctx, 1)
			if err != nil {
				return nil, err
			}
			eagerInlineStart = count <= 1
		}
	}
	if len(seedNodes) > 0 {
		optimizedStartNodes = seedNodes
		usedPropertyIndex = true
	} else if !endBound && (whereClause != "" || eagerInlineStart) {
		// The start node's candidates come from the one node-pattern access
		// decision (collectPipelineIndexedNodeCandidates): an id seek, or the
		// most selective of the WHERE's and the start pattern's own indexed
		// predicates (NOR-414). The whole WHERE is applied to the paths
		// afterwards, so the seed may over-fetch but never under-fetch.
		if matches.StartNode.variable != "" || len(matches.StartNode.properties) > 0 {
			if nodes, _, used, idxErr := e.collectPipelineIndexedNodeCandidates(ctx, matches.StartNode, whereClause, pipelineMatchPhysicalHint{}); idxErr != nil {
				return nil, idxErr
			} else if used {
				optimizedStartNodes = nodes
				usedPropertyIndex = true
				startDecided = true
			}
			// Fallback optimization: even when no property index exists, pre-prune start
			// nodes for simple start-variable predicates before traversing relationships.
			// This preserves semantics while avoiding expensive traversal from irrelevant
			// start nodes in patterns like:
			//   MATCH (n:Label)-[:R]->(m) WHERE n.prop = 'x' RETURN ...
			if !usedPropertyIndex {
				if nodes, used, scanErr := e.tryCollectNodesFromStartPropertyScan(ctx, matches.StartNode, whereClause); scanErr == nil && used {
					optimizedStartNodes = nodes
					startDecided = true
				}
			}
		}

	}

	// A chain's paths are filtered by the WHERE below; its conjuncts also
	// prune each path prefix as soon as they can be decided on it.
	matches.pruneWhere = whereClause

	// Only unbound, single-segment traversals choose their orientation here.
	// Index availability alone says nothing about adjacency expansion cost.
	if len(seedNodes) == 0 && len(endSeedNodes) == 0 && !matches.IsChained && matches.pinnedRelationship == nil {
		endNodes, selected, err := e.chooseTraversalEndSeeds(ctx, matches, optimizedStartNodes, startDecided, whereClause)
		if err != nil {
			return nil, err
		}
		if selected {
			endSeedNodes = endNodes
			optimizedStartNodes, startDecided = nil, len(endNodes) == 0
		}
	}

	// Execute traversal with optimized start nodes if available
	var paths []PathResult
	if endSeededPaths, endSeeded := e.traverseFromEndSeeds(ctx, matches, seedNodes, endSeedNodes, whereClause); endSeeded {
		// Final even when empty: a bound end node with no qualifying path must
		// not fall through to the store-wide scan below.
		paths = endSeededPaths
	} else if startDecided && len(optimizedStartNodes) == 0 {
		// The start node's access decision found no candidate: no path.
		paths = nil
	} else if len(optimizedStartNodes) > 0 {
		// Use optimized start nodes for non-chained traversal. Chained traversal uses
		// segment-aware expansion; feeding it through traverseGraphSequential can break
		// semantics because it ignores segment topology.
		if matches.IsChained && len(matches.Segments) > 1 {
			paths = e.traverseChainedGraph(ctx, matches, optimizedStartNodes)
		} else {
			viewport, _ := TemporalViewportFromContext(ctx)
			checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
			optimizedStartNodes = e.admitTraversalStartNodes(ctx, matches.StartNode, optimizedStartNodes)
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
	if repeatedEndpoint {
		filtered := paths[:0]
		for _, path := range paths {
			if len(path.Nodes) > 0 && path.Nodes[0].Identity() == path.Nodes[len(path.Nodes)-1].Identity() {
				filtered = append(filtered, path)
			}
		}
		paths = filtered
	}
	// Aggregates are the row pipeline's: its states (pipelineAggregateState)
	// group by the grouping values and aggregate every item on its own, as
	// every other MATCH ... RETURN does (NOR-70).
	for _, item := range returnItems {
		if containsAggregateFunc(item.expr) {
			rows := make([]traversalOptRow, 0, len(paths))
			for _, path := range paths {
				pathContext := e.buildPathContext(path, matches)
				rows = append(rows, traversalOptRow{values: e.pathContextValues(ctx, pathContext)})
			}
			aggregated, err := e.aggregateTraversalOptionalRows(ctx, rows, returnItems)
			if err != nil {
				return nil, err
			}
			if err := collector.appendRows(aggregated); err != nil {
				return nil, err
			}
			return result, nil
		}
	}

	// Build result rows (non-aggregation)
	if err := collector.appendPathRows(e, ctx, paths, matches, returnItems, earlyLimit); err != nil {
		return nil, err
	}

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

// traversalResultCollector is the one owner for rows returned by every
// traversal route, including count, grouping and the end-seeded fast path.
type traversalResultCollector struct {
	result *ExecuteResult
	memory memoryAccount
}

func newTraversalResultCollector(ctx context.Context, result *ExecuteResult) traversalResultCollector {
	return traversalResultCollector{result: result, memory: memoryAccount{scope: memoryScopeFromContext(ctx)}}
}

func (c *traversalResultCollector) close() { c.memory.close() }

func (c *traversalResultCollector) appendRow(row []interface{}) error {
	if c.memory.scope != nil {
		if err := c.memory.grow(estimateResultRowBytes(row, c.memory.scope.knownLists())); err != nil {
			return err
		}
	}
	c.result.Rows = append(c.result.Rows, row)
	return nil
}

func (c *traversalResultCollector) appendRows(rows [][]interface{}) error {
	for _, row := range rows {
		if err := c.appendRow(row); err != nil {
			return err
		}
	}
	return nil
}

func (c *traversalResultCollector) appendPathRows(e *StorageExecutor, ctx context.Context, paths []PathResult, matches *TraversalMatch, returnItems []returnItem, rowLimit int) error {
	for _, path := range paths {
		if rowLimit >= 0 && len(c.result.Rows) >= rowLimit {
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
		if err := c.appendRow(row); err != nil {
			return err
		}
	}
	return nil
}

type traversalSeedSide uint8

const (
	traversalStartSeed traversalSeedSide = iota
	traversalEndSeed
)

func (e *StorageExecutor) tryExecuteTraversalSeedOrderLimit(ctx context.Context, pattern string, whereClause string, returnItems []returnItem, pathVariable string, orderExpr string, limit int, seedSide traversalSeedSide) (*ExecuteResult, bool, error) {
	if limit <= 0 || strings.TrimSpace(orderExpr) == "" {
		return nil, false, nil
	}

	matches := e.parseTraversalPattern(ctx, pattern)
	if matches == nil || (seedSide == traversalEndSeed && matches.IsChained) {
		return nil, false, nil
	}
	if pathVariable != "" {
		matches.PathVariable = pathVariable
	}
	if seedSide == traversalStartSeed {
		if !matches.IsChained && strings.TrimSpace(matches.StartNode.variable) == "" {
			return nil, false, nil
		}
		seedNodes, used, err := e.tryCollectTraversalStartSeedOrderNodes(ctx, matches.StartNode, whereClause, orderExpr, limit)
		if err != nil {
			return nil, true, err
		}
		if !used {
			return nil, false, nil
		}
		var result *ExecuteResult
		for _, seed := range seedNodes {
			if err := ctx.Err(); err != nil {
				return nil, true, err
			}
			part, err := e.executeMatchWithRelationshipsWithPathSeeded(ctx, pattern, whereClause, returnItems, []*storage.Node{seed}, nil, nil, pathVariable, nil, -1)
			if err != nil {
				return nil, true, err
			}
			if result == nil {
				result = part
			} else {
				result.Rows = append(result.Rows, part.Rows...)
			}
			if len(result.Rows) >= limit {
				break
			}
		}
		if result == nil {
			// Preserve the existing empty-index behavior and result columns.
			result, err = e.executeMatchWithRelationshipsWithPathSeeded(ctx, pattern, whereClause, returnItems, seedNodes, nil, nil, pathVariable, nil, -1)
			if err != nil {
				return nil, true, err
			}
		}
		// A bounded seed window is conclusive only after the relationship
		// pattern has produced enough rows, or the index window is exhausted.
		// Otherwise later seeds may be the first ones with qualifying paths.
		if len(result.Rows) < limit && len(seedNodes) >= topKSeedLimit(limit) {
			return nil, false, nil
		}
		if len(result.Rows) > limit {
			clear(result.Rows[limit:])
			result.Rows = result.Rows[:limit]
		}
		e.markTraversalStartSeedTopKUsed()
		return result, true, nil
	}
	if strings.TrimSpace(matches.EndNode.variable) == "" || len(matches.EndNode.labels) == 0 {
		return nil, false, nil
	}

	orderSpecs, _ := e.parseNodeOrderSpecs(ctx, orderExpr, matches.EndNode.variable)
	if len(orderSpecs) == 0 {
		return nil, false, nil
	}

	seedWhere := e.extractTraversalSeedWhereClause(ctx, whereClause, matches.EndNode.variable, matches.StartNode.variable)
	if strings.TrimSpace(seedWhere) == "" && strings.TrimSpace(whereClause) != "" {
		return nil, false, nil
	}

	seedLimit := topKSeedLimit(limit)
	seedNodes, used, err := e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, matches.EndNode, seedWhere, orderExpr, seedLimit, indexOrderGeneral)
	if err != nil {
		return nil, true, err
	}
	if !used {
		if seedNodes, used, err = e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, matches.EndNode, seedWhere, orderExpr, seedLimit, indexOrderExplicitNotNull); err != nil {
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
				collector := newTraversalResultCollector(ctx, result)
				defer collector.close()
				if err := collector.appendPathRows(e, ctx, paths, matches, returnItems, limit); err != nil {
					return nil, true, err
				}
				e.markTraversalEndSeedTopKUsed()
				return result, true, nil
			}
		}
	}

	if len(paths) < limit && len(seedNodes) >= seedLimit {
		return nil, false, nil
	}

	collector := newTraversalResultCollector(ctx, result)
	defer collector.close()
	if err := collector.appendPathRows(e, ctx, paths, matches, returnItems, limit); err != nil {
		return nil, true, err
	}
	e.markTraversalEndSeedTopKUsed()
	return result, true, nil
}

func (e *StorageExecutor) extractTraversalSeedWhereClause(ctx context.Context, whereClause string, seedVar string, blockedVars ...string) string {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" {
		return ""
	}
	parts := splitTopLevelAndConjuncts(ctx, clause)
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
	// The read tail's endpoint projection names the original end node's
	// properties; the reversed walk loads the original start node as its end,
	// so it loads complete endpoints.
	reversed.endpointProperties = nil
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

	seedNodes, used, err := e.tryCollectNodesFromPropertyIndexOrderLimit(ctx, nodePattern, whereClause, orderExpr, topKSeedLimit(limit), indexOrderGeneral)
	if err != nil || !used {
		return nil, used, err
	}

	return seedNodes, true, nil
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
	// Variable references alone do not establish a start-node predicate:
	// graph predicates and producers can reference only the start variable,
	// but must run once per matched row, never during seed pruning as well.
	// The shared complete plan admits only simple, side-effect-free operands.
	plan := planRowPredicate(whereClause)
	if plan == nil || !plan.complete {
		return nil, false, nil
	}
	if !forEachSemanticExpressionReference(whereClause, func(variable, _ string, _ bool, _ int) bool {
		return variable == nodePattern.variable
	}) {
		return nil, false, nil
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

// isLengthPathExpr checks if an expression is length(path) for some path variable
func isLengthPathExpr(expr string) bool {
	return matchFuncStartAndSuffix(expr, "length") && strings.Contains(lowerASCII(expr), "path")
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
	// boundEndNodeID filters endpoints before they count toward TraversalLimit.
	boundEndNodeID storage.NodeID
	// pinnedRelationship is the one relationship a WHERE id() / elementId()
	// equality names. findPaths walks it from its own endpoints instead of
	// listing a start node's adjacency (relationshipIDSeedEdge).
	pinnedRelationship *storage.Edge
	// UnusedPropertyVariables lists pattern node variables whose properties no
	// later clause (RETURN, WHERE) reads, so the traversal may load a named
	// labelled endpoint by ID + labels alone. nil means "not analysed": every
	// variable is treated as property-read (the safe fallback). Computed by
	// the planner in executeMatchWithRelationshipsWithPathSeeded.
	UnusedPropertyVariables map[string]bool
	// endpointProperties is the read-only tail's projection for a single
	// segment's cached nodes. nil preserves complete entities for unknown
	// tails, path values, writes and intermediate chain bindings.
	endpointProperties []string
	// IntermediateNode marks a chained segment whose end node is an
	// intermediate binding of the chain rather than the pattern's final
	// endpoint. Its properties may be read by a later segment (a zero-length
	// variable relationship binds the next segment's start to it), so the
	// traversal must not load it labels-only or exist-only.
	IntermediateNode bool
	// pruneWhere is the WHERE the caller applies to the chain's complete paths
	// (filterPathsByWhere). traverseChainedGraph also applies each of its
	// conjuncts as soon as a path prefix binds every variable it names
	// (chainPrefixPrune), so a prefix that fails it is not extended.
	pruneWhere string
}

// TraversalSegment represents one segment in a chained pattern
type TraversalSegment struct {
	FromNode     nodePatternInfo
	ToNode       nodePatternInfo
	Relationship RelationshipPattern
}

// parseTraversalPattern parses (a:Label)-[r:TYPE]->(b:Label) style patterns
// Also handles chained patterns like (a)<-[:R1]-(b)-[:R2]->(c)
// A pattern with a quantified path pattern (((x)-->(y))+) is none: only
// the clause pipeline runs one (quantified_path_pattern.go).
// Uses a state machine instead of regex to properly handle parentheses in property values
func (e *StorageExecutor) parseTraversalPattern(ctx context.Context, pattern string) *TraversalMatch {
	return e.readTraversalPattern(ctx, pattern, e.parsePropertyMap)
}

// Structural admission uses the same string reader without evaluating values.
func (e *StorageExecutor) parseTraversalPatternStructure(ctx context.Context, pattern string) *TraversalMatch {
	return e.readTraversalPattern(ctx, pattern, patternPropertyKeys)
}

func (e *StorageExecutor) readTraversalPattern(ctx context.Context, pattern string, properties patternPropertyReader) *TraversalMatch {
	if hasQuantifiedGroup(pattern) {
		return nil
	}
	pattern = normalizeAnonymousTraversalRelationships(pattern)
	// First check if this is a chained pattern (has multiple relationship segments)
	if e.isChainedPattern(pattern) {
		return e.readChainedTraversalPattern(ctx, pattern, properties)
	}
	return e.readTraversalPatternStateMachine(ctx, pattern, properties)
}

func normalizeAnonymousTraversalRelationships(pattern string) string {
	var normalized strings.Builder
	normalized.Grow(len(pattern) + 8)
	for index := 0; index < len(pattern); {
		if end, quoted := skipCypherLexicalToken(pattern, index); quoted {
			normalized.WriteString(pattern[index:end])
			index = end
			continue
		}
		character := pattern[index]
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
	count := 0
	forEachRelationshipSource(pattern, func(_, _, _, _ int, _, _ bool) bool {
		count++
		return count < 2
	})
	return count >= 2
}

// parseChainedTraversalPattern parses multi-segment patterns like (a)<-[:R1]-(b)-[:R2]->(c)
func (e *StorageExecutor) parseChainedTraversalPattern(ctx context.Context, pattern string) *TraversalMatch {
	return e.readChainedTraversalPattern(ctx, pattern, e.parsePropertyMap)
}

func (e *StorageExecutor) readChainedTraversalPattern(ctx context.Context, pattern string, properties patternPropertyReader) *TraversalMatch {
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

	for i := 0; i < len(pattern); i++ {
		if end, quoted := skipCypherLexicalToken(pattern, i); quoted {
			if inNode {
				currentNode.WriteString(pattern[i:end])
			} else if inRel {
				currentRel.WriteString(pattern[i:end])
			}
			i = end - 1
			continue
		}
		c := pattern[i]

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
			// A bracket inside a node pattern belongs to a property value
			// (list, subscript or slice), not to the next relationship.
			if inNode {
				currentNode.WriteByte(c)
				continue
			}
			if bracketDepth == 0 {
				inRel = true
			}
			bracketDepth++
			if inRel {
				currentRel.WriteByte(c)
			}
		case ']':
			if inNode {
				currentNode.WriteByte(c)
				continue
			}
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
		node, err := readNodePattern(ctx, nodeStr, properties)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		allNodes = append(allNodes, node)
	}

	// Build segments
	for i := 0; i < len(parts)-1; i++ {
		relStr := parts[i].relStr
		if relStr == "" {
			continue
		}

		relationship, err := readRelationshipPattern(ctx, relStr, properties)
		if err != nil {
			recordExpressionFailure(ctx, err)
			return nil
		}
		segment := TraversalSegment{
			FromNode:     allNodes[i],
			ToNode:       allNodes[i+1],
			Relationship: *relationship,
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
	return e.readTraversalPatternStateMachine(ctx, pattern, e.parsePropertyMap)
}

func (e *StorageExecutor) readTraversalPatternStateMachine(ctx context.Context, pattern string, properties patternPropertyReader) *TraversalMatch {
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

	start, err := readNodePattern(ctx, startNodeStr, properties)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	end, err := readNodePattern(ctx, endNodeStr, properties)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	relationship, err := readRelationshipPattern(ctx, relStr, properties)
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	return &TraversalMatch{StartNode: start, Relationship: *relationship, EndNode: end}
}

func findMatchingBracket(s string, startIdx int) int {
	return cyphertext.FindMatchingDelimiter(s, startIdx, '[', ']', cyphertext.OpeningDelimiter)
}

// findMatchingParen finds the index of the closing paren that matches the
// opening paren at startIdx. It shares the comment- and quote-aware matcher
// so a ')' inside a string, backtick name or comment never closes early.
func findMatchingParen(s string, startIdx int) int {
	return cyphertext.FindMatchingDelimiter(s, startIdx, '(', ')', cyphertext.OpeningDelimiter)
}

// parseNodePatternFromString parses n:Label {props} from a string
func (e *StorageExecutor) parseNodePatternFromString(ctx context.Context, s string) (nodePatternInfo, error) {
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

	startNodes, seeded, err := e.relationshipTypeSeedNodes(ctx, match)
	if !seeded && err == nil {
		startNodes, _, err = e.collectPipelineInitialNodeCandidates(ctx, match.StartNode, "", pipelineMatchPhysicalHint{limit: -1, earlyLimit: -1})
	}
	if err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}

	startNodes = e.admitTraversalStartNodes(ctx, match.StartNode, startNodes)

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

// relationshipTypeSeedNodes is the start nodes of a one-hop match whose start
// node has no label or property and whose relationship has types, such as
// ()-[r:T {id: $id}]->(): the endpoints the direction starts from, of the
// relationships of those types, read from the type index. A label-less scan
// would read and decode every node, for a relationship among a few. seeded is
// false for any other match, which collectPipelineInitialNodeCandidates seeds.
// The traversal still checks the whole pattern from each start node.
func (e *StorageExecutor) relationshipTypeSeedNodes(ctx context.Context, match *TraversalMatch) (nodes []*storage.Node, seeded bool, err error) {
	relationship := match.Relationship
	if planTraversalScan(match).access != traversalTypeScan {
		return nil, false, nil
	}
	store := e.getStorage(ctx)
	seen := make(map[storage.NodeID]bool)
	add := func(id storage.NodeID) {
		if seen[id] {
			return
		}
		seen[id] = true
		if node, getErr := e.newRelationshipEndpointReader(store).readRetained(ctx, id, nil); getErr == nil && node != nil {
			nodes = append(nodes, node)
		}
	}
	for _, relationshipType := range relationship.Types {
		edges, err := store.GetEdgesByType(ctx, relationshipType)
		if err != nil {
			return nil, true, err
		}
		for _, edge := range edges {
			if edge == nil {
				continue
			}
			switch relationship.Direction {
			case "outgoing":
				if id, ok := edge.StartIdentity().NodeLookupID(store); ok {
					add(id)
				}
			case "incoming":
				if id, ok := edge.EndIdentity().NodeLookupID(store); ok {
					add(id)
				}
			default:
				if id, ok := edge.StartIdentity().NodeLookupID(store); ok {
					add(id)
				}
				if id, ok := edge.EndIdentity().NodeLookupID(store); ok {
					add(id)
				}
			}
		}
	}
	return nodes, true, nil
}

func (e *StorageExecutor) traverseGraphWithStreamingStartNodes(
	ctx context.Context,
	match *TraversalMatch,
	viewport TemporalViewport,
	checker temporalCurrentNodeChecker,
) ([]PathResult, bool, error) {
	store := e.getStorage(ctx)
	labels := nodeScanLabels(ctx, store, match.StartNode.labels)
	var stream func(func(*storage.Node) error) error
	if e.queryNodeReader(store) != nil {
		stream = func(visit func(*storage.Node) error) error {
			return streamQueryNodes(ctx, store, labels, visit)
		}
	} else if len(labels) > 0 {
		reader, ok := store.(storage.ProjectedLabelNodeReader)
		if !ok {
			return nil, false, nil
		}
		stream = func(visit func(*storage.Node) error) error {
			return reader.StreamNodesByLabelProjected(ctx, labels[0], nil, visit)
		}
	} else {
		reader, ok := storage.ResolveStreamingEngine(store)
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
			endNodeID:                    match.boundEndNodeID,
			relTypes:                     match.Relationship.Types,
			relTypeSet:                   buildRelTypeSet(match.Relationship.Types),
			relProperties:                match.Relationship.Properties,
			direction:                    match.Relationship.Direction,
			minHops:                      match.Relationship.MinHops,
			maxHops:                      match.Relationship.MaxHops,
			usedEdges:                    make(map[storage.EntityIdentity]bool),
			nodeCache:                    make(map[storage.NodeID]*storage.Node),
			limit:                        ctxLimit,
			temporalViewport:             viewport,
			temporalChecker:              checker,
			cancelCtx:                    ctx,
			repeatable:                   repeatableElements(ctx),
			endpointsNeedOnlyExist:       traversalEndpointsNeedOnlyExist(match),
			nodeProperties:               match.endpointProperties,
			endpointsNeedOnlyLabels:      traversalEndpointsNeedOnlyLabels(match),
			relationshipsNeedOnlyHeaders: traversalRelationshipsNeedOnlyHeaders(match),
			pinnedRelationship:           match.pinnedRelationship,
		}

		paths := e.findPaths(traversalCtx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode, nil)
		traversalCtx.memory.close()
		results = append(results, paths...)
		if remaining > 0 {
			remaining -= len(paths)
		}
		if ctx.Err() != nil {
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
				if ctx.Err() != nil {
					break
				}
				// Each goroutine gets its own traversal state (no shared state)
				traversalCtx := &TraversalContext{
					startNode:                    startNode,
					endNodeID:                    match.boundEndNodeID,
					relTypes:                     match.Relationship.Types,
					relTypeSet:                   buildRelTypeSet(match.Relationship.Types),
					relProperties:                match.Relationship.Properties,
					direction:                    match.Relationship.Direction,
					minHops:                      match.Relationship.MinHops,
					maxHops:                      match.Relationship.MaxHops,
					usedEdges:                    make(map[storage.EntityIdentity]bool),
					nodeCache:                    make(map[storage.NodeID]*storage.Node),
					temporalViewport:             viewport,
					temporalChecker:              checker,
					cancelCtx:                    ctx,
					repeatable:                   repeatableElements(ctx),
					endpointsNeedOnlyExist:       traversalEndpointsNeedOnlyExist(match),
					nodeProperties:               match.endpointProperties,
					endpointsNeedOnlyLabels:      traversalEndpointsNeedOnlyLabels(match),
					relationshipsNeedOnlyHeaders: traversalRelationshipsNeedOnlyHeaders(match),
					pinnedRelationship:           match.pinnedRelationship,
				}

				paths := e.findPaths(traversalCtx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode, nil)
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
		// A path variable exposes the chain's nodes and relationships, and the
		// planner's per-variable read analysis applies to every segment's
		// endpoint, so propagate both into each segment's traversal flags.
		PathVariable:            match.PathVariable,
		UnusedPropertyVariables: match.UnusedPropertyVariables,
		// The first segment's end node is an intermediate binding: a later
		// zero-length segment can bind it to a named variable whose
		// properties are read, so it must load fully.
		IntermediateNode: true,
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
	prune := e.newChainPrefixPrune(ctx, match)
	kept := currentPaths[:0]
	for _, path := range currentPaths {
		path.SegmentLengths = []int{len(path.Relationships)}
		if repeats.consistent(path, 1) {
			kept = append(kept, path)
		}
	}
	currentPaths = prune.keep(ctx, e, kept, 1)

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
				StartNode:               seg.FromNode,
				EndNode:                 seg.ToNode,
				Relationship:            seg.Relationship,
				PathVariable:            match.PathVariable,
				UnusedPropertyVariables: match.UnusedPropertyVariables,
				// Only the final segment's end node is the pattern's endpoint;
				// every earlier one is an intermediate binding.
				IntermediateNode: segIdx < len(match.Segments)-1,
			}

			// Traverse from the last node
			segPaths := e.traverseFromNode(ctx, lastNode, segMatch)

			// Join paths: combine current path with each segment path
			for _, segPath := range segPaths {
				if !repeatableElements(ctx) && pathResultsReuseRelationship(path, segPath) {
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

		currentPaths = prune.keep(ctx, e, extendedPaths, segIdx+1)
	}

	return currentPaths
}

// chainPrefixPrune applies a chain's WHERE (TraversalMatch.pruneWhere) to its
// path prefixes: after k segments, the conjuncts whose every variable the
// first k segments bind, which no later segment can change. A prefix that
// fails one is not extended: its complete paths would fail the same conjunct
// in the WHERE the caller still applies to them. Conjuncts that name the path
// variable, a variable bound only later or outside the pattern, or no
// variable, or that hold a subquery expression, or that are not
// deterministic, wait for that WHERE.
type chainPrefixPrune struct {
	levels []chainPrefixLevel // levels[k-1]: after k segments
}

type chainPrefixLevel struct {
	match *TraversalMatch // the chain's first k segments
	where *preparedRowPredicate
}

func (e *StorageExecutor) newChainPrefixPrune(ctx context.Context, match *TraversalMatch) *chainPrefixPrune {
	if match.pruneWhere == "" || len(match.Segments) < 2 || len(match.IntermediateNodes) < len(match.Segments)-1 {
		return nil
	}
	conjuncts := splitTopLevelAndConjuncts(ctx, strings.TrimSpace(match.pruneWhere))
	decided := make([]bool, len(conjuncts))
	bound := map[string]bool{}
	bind := func(name string) {
		if name != "" {
			bound[name] = true
		}
	}
	bind(match.Segments[0].FromNode.variable)
	prune := &chainPrefixPrune{levels: make([]chainPrefixLevel, len(match.Segments)-1)}
	found := false
	for k := 1; k < len(match.Segments); k++ {
		segment := match.Segments[k-1]
		bind(segment.Relationship.Variable)
		bind(segment.ToNode.variable)
		var parts []string
		for i, conjunct := range conjuncts {
			if decided[i] || !chainPrefixDecides(ctx, conjunct, bound, match.PathVariable) {
				continue
			}
			decided[i] = true
			parts = append(parts, "("+conjunct+")")
		}
		if len(parts) == 0 {
			continue
		}
		found = true
		prune.levels[k-1] = chainPrefixLevel{match: chainPrefixMatch(match, k), where: prepareRowPredicate(strings.Join(parts, " AND "))}
	}
	if !found {
		return nil
	}
	return prune
}

// chainPrefixDecides reports whether conjunct can be decided on a path prefix
// that binds bound.
func chainPrefixDecides(ctx context.Context, conjunct string, bound map[string]bool, pathVariable string) bool {
	lower := strings.ToLower(conjunct)
	if strings.Contains(lower, "rand(") || strings.Contains(lower, "randomuuid(") || referencesVariable(conjunct, pathVariable) {
		return false
	}
	// A subquery expression (EXISTS / COUNT / COLLECT { … }) reads outer
	// variables in its body, which expressionFreeVariables does not list: a
	// conjunct that holds one waits for the final WHERE, where every variable
	// its body may read is bound.
	if mayContainSubqueryExpression(conjunct) && len(findSubqueryExpressions(conjunct)) > 0 {
		return false
	}
	names := expressionFreeVariables(ctx, conjunct)
	if len(names) == 0 {
		return false
	}
	for _, name := range names {
		if name == pathVariable || !bound[name] {
			return false
		}
	}
	return true
}

// chainPrefixMatch is match's first k segments as a pattern of their own, so
// buildPathContext binds a k-segment prefix's variables.
func chainPrefixMatch(match *TraversalMatch, k int) *TraversalMatch {
	segments := match.Segments[:k]
	prefix := &TraversalMatch{
		StartNode:    match.StartNode,
		EndNode:      segments[k-1].ToNode,
		Relationship: segments[k-1].Relationship,
		Segments:     segments,
	}
	if k > 1 {
		prefix.IsChained = true
		prefix.IntermediateNodes = match.IntermediateNodes[:k-1]
	}
	return prefix
}

// keep returns the paths of k segments whose prefix passes the conjuncts
// decided after k segments.
func (p *chainPrefixPrune) keep(ctx context.Context, e *StorageExecutor, paths []PathResult, k int) []PathResult {
	if p == nil || k < 1 || k > len(p.levels) || p.levels[k-1].where == nil {
		return paths
	}
	level := p.levels[k-1]
	kept := paths[:0]
	for _, path := range paths {
		values := e.pathContextValues(ctx, e.buildPathContext(path, level.match))
		bindParameterRow(ctx, values)
		if level.where.holds(e, ctx, values) {
			kept = append(kept, path)
		}
	}
	return kept
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
	return chainPositionNode(path, position).Identity() == chainPositionNode(path, repeats[position]).Identity()
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
			if leftRelationship != nil && rightRelationship != nil && leftRelationship.Identity() == rightRelationship.Identity() {
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
	ctx.endNodeID = match.boundEndNodeID
	// Single-segment traversals (including each segment of a chain) share the
	// planner's endpoint/relationship read analysis so the header and
	// labels-only fast paths apply to chained patterns too (#1110).
	ctx.endpointsNeedOnlyExist = traversalEndpointsNeedOnlyExist(match)
	ctx.nodeProperties = match.endpointProperties
	ctx.endpointsNeedOnlyLabels = traversalEndpointsNeedOnlyLabels(match)
	ctx.relationshipsNeedOnlyHeaders = traversalRelationshipsNeedOnlyHeaders(match)
	ctx.pinnedRelationship = match.pinnedRelationship
	return e.findPaths(ctx, startNode, []*storage.Node{startNode}, []*storage.Edge{}, 0, &match.EndNode, nil)
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
		usedEdges:     make(map[storage.EntityIdentity]bool),
		nodeCache:     make(map[storage.NodeID]*storage.Node),
		cancelCtx:     traversalCtx,
		repeatable:    repeatableElements(traversalCtx),
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
	if nextNode == nil {
		properties := ctx.nodeProperties
		if ctx.temporalViewport.Enabled() {
			properties = nil
		}
		if (ctx.endpointsNeedOnlyLabels || ctx.endpointsNeedOnlyExist) && !ctx.temporalViewport.Enabled() {
			properties = []string{}
		}
		var err error
		nextNode, err = e.newRelationshipEndpointReader(e.storage).readRetained(ctx.cancelCtx, nextNodeID, properties)
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

// findPaths appends matching paths in DFS order to the traversal-owned result
// accumulator. Retained paths copy their node/edge slices once at acceptance;
// recursive frames do not allocate and copy intermediate result collections.
func (e *StorageExecutor) findPaths(
	ctx *TraversalContext,
	currentNode *storage.Node,
	pathNodes []*storage.Node,
	pathEdges []*storage.Edge,
	depth int,
	endPattern *nodePatternInfo,
	results []PathResult,
) []PathResult {

	// Every frame and sibling expansion must observe cancellation. Sampling
	// recursive entries alone leaves callers expanding the remaining siblings
	// after a cancelled child returns.
	if ctx.traversalCancelled() {
		return results
	}

	// OPTIMIZATION: Early termination if limit reached
	if ctx.limit > 0 && ctx.resultCount >= ctx.limit {
		return results
	}

	if depth > ctx.deepest {
		ctx.deepest = depth
	}

	// Check if current path meets minimum length and endpoint requirements
	if depth >= ctx.minHops && (ctx.endNodeID == "" || storage.NodeID(currentNode.IdentityKey()) == ctx.endNodeID) {
		if e.matchesEndPattern(currentNode, endPattern) {
			// A path the expansion keeps counts against the statement's
			// memory limit; going over cancels the statement (#1343).
			if err := ctx.memoryAccount().grow(estimatePathResultBytes(len(pathNodes), len(pathEdges))); err != nil {
				return results
			}
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

	var edges []*storage.Edge
	if ctx.pinnedRelationship != nil && depth == 0 {
		// A pinned relationship is the one step this path may take. Its
		// endpoints are the start nodes, so no adjacency is listed and no
		// source translation is needed to select it.
		edges = []*storage.Edge{ctx.pinnedRelationship}
	} else {
		// Translate the native source once at the storage boundary. Local IDs
		// alone cannot select adjacency in a composite with duplicate IDs.
		lookupID, validSource := currentNode.Identity().NodeLookupID(e.storage)
		if !validSource {
			return results
		}
		// Get edges based on direction
		switch ctx.direction {
		case "outgoing":
			edges = e.traversalEdges(ctx, lookupID, true)
		case "incoming":
			edges = e.traversalEdges(ctx, lookupID, false)
		case "both":
			var err error
			edges, err = undirectedIncidentEdges(ctx.cancelCtx, e.storage, lookupID)
			if err != nil {
				recordExpressionFailure(ctx.cancelCtx, err)
				return results
			}
		}
	}

	// Traverse each edge
	for _, edge := range edges {
		if ctx.traversalCancelled() {
			return results
		}
		// A Cypher path may revisit a node, but it cannot reuse a relationship.
		// This also prevents an undirected expansion from walking the same edge
		// immediately back in the opposite direction.
		if ctx.usedEdges[edge.Identity()] && !ctx.repeatable {
			continue
		}
		if ctx.pinnedRelationship != nil && depth == 0 {
			current := currentNode.Identity()
			if ctx.direction == "outgoing" && edge.StartIdentity() != current {
				continue
			}
			if ctx.direction == "incoming" && edge.EndIdentity() != current {
				continue
			}
			if ctx.direction == "both" && edge.StartIdentity() != current && edge.EndIdentity() != current {
				continue
			}
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
		nextIdentity := edge.StartIdentity()
		if ctx.direction == "outgoing" || (ctx.direction == "both" && edge.StartIdentity() == currentNode.Identity()) {
			nextIdentity = edge.EndIdentity()
		}
		// At the final permitted hop, an already-known endpoint can reject
		// the relationship before any node hydration. Earlier hops must keep
		// intermediates, including ones that are not the bound endpoint.
		if ctx.endNodeID != "" && depth+1 == ctx.maxHops && storage.NodeID(nextIdentity.Key()) != ctx.endNodeID {
			continue
		}
		nextNodeID, validSource := nextIdentity.NodeLookupID(e.storage)
		if !validSource {
			continue
		}

		nextNode, ok := e.loadTraversalEndpointNode(ctx, nextNodeID)
		if !ok {
			continue
		}

		ctx.usedEdges[edge.Identity()] = true

		// Reuse the DFS working path across siblings. Retained results above
		// already copy their node/relationship slices, so copying each working
		// prefix as well allocates twice for every successful branch.
		nextNodesLen, ok := util.SafeIntAdd(len(pathNodes), 1)
		if !ok {
			delete(ctx.usedEdges, edge.Identity())
			continue
		}
		nextEdgesLen, ok := util.SafeIntAdd(len(pathEdges), 1)
		if !ok {
			delete(ctx.usedEdges, edge.Identity())
			continue
		}
		pathNodes = append(pathNodes, nextNode)
		pathEdges = append(pathEdges, edge)
		results = e.findPaths(ctx, nextNode, pathNodes, pathEdges, depth+1, endPattern, results)
		pathNodes[nextNodesLen-1] = nil
		pathEdges[nextEdgesLen-1] = nil
		pathNodes = pathNodes[:nextNodesLen-1]
		pathEdges = pathEdges[:nextEdgesLen-1]

		// Only the active path owns marks. Keeping false entries retains every
		// visited sibling identity and grows this map with fanout rather than depth.
		delete(ctx.usedEdges, edge.Identity())
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

// Binding insertion owns lazy allocation: an anonymous node, unnamed
// relationship or absent path needs no binding map. The same insertion rule
// applies when correlated WHERE evaluation adds outer entities.
func (path *PathContext) bindNode(name string, node *storage.Node) {
	if path.nodes == nil {
		path.nodes = make(map[string]*storage.Node)
	}
	path.nodes[name] = node
}

func (path *PathContext) bindRelationship(name string, edge *storage.Edge) {
	if path.rels == nil {
		path.rels = make(map[string]*storage.Edge)
	}
	path.rels[name] = edge
}

func (path *PathContext) bindPath(name string, value *PathResult) {
	if path.paths == nil {
		path.paths = make(map[string]*PathResult)
	}
	path.paths[name] = value
}

// pathContextValues returns the path's variables as row values: nodes and
// relationships as themselves, named paths as the pipeline's path maps.
func (e *StorageExecutor) pathContextValues(ctx context.Context, pathCtx PathContext) map[string]interface{} {
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
			values[name] = e.pathContextRowValue(ctx, path)
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
func (e *StorageExecutor) pathContextRowValue(ctx context.Context, path *PathResult) interface{} {
	if relationshipOnlyPath(path) {
		return relationshipListValue(path.Relationships)
	}
	return e.pathToMapContext(ctx, *path)
}

// buildPathContext creates a context for evaluating expressions over a path
func (e *StorageExecutor) buildPathContext(path PathResult, match *TraversalMatch) PathContext {
	ctx := PathContext{
		pathLength:   path.Length,        // Store path length for length(path) function
		allPathEdges: path.Relationships, // Store all edges for relationships(path)
		allPathNodes: path.Nodes,         // Store all nodes for nodes(path)
	}

	// Map the path variable if present (for path functions like relationships(path), nodes(path))
	if match.PathVariable != "" {
		pathCopy := path // Make a copy to store pointer
		ctx.bindPath(match.PathVariable, &pathCopy)
	}

	// Map start node
	if match.StartNode.variable != "" && len(path.Nodes) > 0 {
		ctx.bindNode(match.StartNode.variable, path.Nodes[0])
	}

	// Map end node
	if match.EndNode.variable != "" && len(path.Nodes) > 0 {
		ctx.bindNode(match.EndNode.variable, path.Nodes[len(path.Nodes)-1])
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
					ctx.bindNode(intermediateInfo.variable, path.Nodes[pathIdx])
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
					ctx.bindPath(seg.Relationship.Variable, &PathResult{Relationships: relationships})
				} else if relationshipOffset < segmentEnd {
					ctx.bindRelationship(seg.Relationship.Variable, path.Relationships[relationshipOffset])
				}
			}
			relationshipOffset = segmentEnd
		}
	} else {
		// Map relationship (single segment)
		if match.Relationship.Variable != "" {
			if match.Relationship.VariableLength {
				relationships := append([]*storage.Edge{}, path.Relationships...)
				ctx.bindPath(match.Relationship.Variable, &PathResult{Relationships: relationships})
			} else if len(path.Relationships) > 0 {
				ctx.bindRelationship(match.Relationship.Variable, path.Relationships[0])
			}
		}
	}

	return ctx
}

// getRelType gets the type of a relationship - used for type(r) function
func (e *StorageExecutor) getRelType(ctx context.Context, relID storage.EdgeID) string {
	edge, err := e.storage.GetEdge(ctx, relID)
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
	where := prepareRowPredicate(strings.TrimSpace(whereClause))
	for _, path := range paths {
		context := e.buildPathContext(path, matches)
		for name, value := range extra {
			switch v := value.(type) {
			case *storage.Node:
				if v != nil {
					context.bindNode(name, v)
				}
			case *storage.Edge:
				if v != nil {
					context.bindRelationship(name, v)
				}
			}
		}
		values := e.pathContextValues(ctx, context)
		bindParameterRow(ctx, values)
		if where.holds(e, ctx, values) {
			filtered = append(filtered, path)
		}
	}
	return filtered
}

// evaluateWhereOnPath evaluates a WHERE condition against a path context.
// Handles conditions like: i.name = 'value', e.score < 90, etc.
func (e *StorageExecutor) evaluateWhereOnPath(ctx context.Context, whereClause string, pathCtx PathContext) bool {
	values := e.pathContextValues(ctx, pathCtx)
	bindParameterRow(ctx, values)
	return e.evaluateRowPredicate(ctx, strings.TrimSpace(whereClause), values)
}
