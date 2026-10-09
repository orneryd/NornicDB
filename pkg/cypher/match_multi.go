package cypher

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// countKeywordOccurrences counts how many times a keyword appears in the query
// using word boundary detection. Excludes occurrences inside labels (after ':')
func countKeywordOccurrences(upper, keyword string) int {
	count := 0
	idx := 0
	for {
		found := strings.Index(upper[idx:], keyword)
		if found == -1 {
			break
		}
		// Check word boundary before
		pos := idx + found
		// Must have space/newline/tab before, NOT ':' (which would indicate a label)
		beforeOk := pos == 0 || (upper[pos-1] == ' ' || upper[pos-1] == '\n' || upper[pos-1] == '\t')
		// Check word boundary after
		afterPos := pos + len(keyword)
		afterOk := afterPos >= len(upper) || (upper[afterPos] == ' ' || upper[afterPos] == '(' || upper[afterPos] == '\n' || upper[afterPos] == '\t')

		if beforeOk && afterOk {
			count++
		}
		idx = pos + len(keyword)
	}
	return count
}

// lastKeywordIndexBefore returns the last occurrence of keyword before endIdx.
// It uses keyword-aware scanning and returns -1 when not found.
func lastKeywordIndexBefore(query, keyword string, endIdx int) int {
	if endIdx <= 0 {
		return -1
	}
	if endIdx > len(query) {
		endIdx = len(query)
	}
	segment := query[:endIdx]
	pos := -1
	search := 0
	for {
		rel := findKeywordIndex(segment[search:], keyword)
		if rel < 0 {
			break
		}
		found := search + rel
		pos = found
		search = found + len(keyword)
		if search >= len(segment) {
			break
		}
	}
	return pos
}

// splitMatchClauses splits the query into individual MATCH clause patterns
func splitMatchClauses(cypher string, whereIdx, returnIdx int) []string {
	var clauses []string

	// Find the end of MATCH patterns (before WHERE or RETURN)
	endIdx := returnIdx
	if whereIdx > 0 && whereIdx < returnIdx {
		endIdx = whereIdx
	}

	matchPart := cypher[5:endIdx] // Skip first "MATCH"

	// Split by subsequent MATCH keywords
	parts := strings.Split(upperASCII(matchPart), "MATCH")
	offset := 5 // Start after first MATCH

	for i, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		// Find the actual length in original case
		pattern := strings.TrimSpace(cypher[offset : offset+len(p)])
		clauses = append(clauses, pattern)
		offset += len(p)
		if i < len(parts)-1 {
			offset += 5 // Skip "MATCH"
		}
	}

	// Fix: Re-split using findKeywordIndex for accuracy
	clauses = clauses[:0]
	start := 5 // After first MATCH
	searchStart := start
	for {
		nextMatch := findKeywordIndex(cypher[searchStart:], "MATCH")
		if nextMatch == -1 || searchStart+nextMatch >= endIdx {
			// No more MATCH - take everything to end
			clauses = append(clauses, strings.TrimSpace(cypher[start:endIdx]))
			break
		}
		pos := searchStart + nextMatch
		clauses = append(clauses, strings.TrimSpace(cypher[start:pos]))
		start = pos + 5 // Skip "MATCH"
		searchStart = pos + 5
	}

	return clauses
}

// binding represents variable bindings from multiple MATCH clauses.
//
// NOTE: this stays map[string]*storage.Node (node-only) on purpose. A wide
// range of existing tests (binding_where_compile*_test.go,
// coverage_binding_where_test.go, coverage_lift_test.go,
// internal_branch_coverage_boost_test.go, binding_where_benchmark_test.go,
// ...) construct and index `binding{...}` directly, so widening its value
// type would ripple into all of them for no benefit. Relationship variables
// bound by a MATCH clause (e.g. "rel" in `(s)-[rel]->(x)`) are tracked in a
// parallel `map[string]*storage.Edge` — see executeFirstMatch/
// executeChainedMatch below — that travels index-aligned alongside a
// []binding slice rather than living inside binding itself.
type binding map[string]*storage.Node
type relationshipBinding map[string]interface{}

// executeChainedMatch executes a subsequent MATCH against existing bindings.
// existingRelBindings must be index-aligned with existingBindings (as
// returned by executeFirstMatch/executeChainedMatch); it may be nil when no
// relationship variable has been bound by an earlier clause. The returned
// relationship-binding slice is index-aligned with the returned bindings and
// carries forward any relationship bound earlier in the chain plus any
// relationship variable bound by this clause's own pattern.
func (e *StorageExecutor) executeChainedMatch(ctx context.Context, pattern string, existingBindings []binding, existingRelBindings []relationshipBinding) ([]binding, []relationshipBinding) {
	var newBindings []binding
	var newRelBindings []relationshipBinding
	// Bracketed (-[r]->) and bare (-->, <--, --) relationships both make a
	// traversal; only a lone node pattern is matched as a node.
	isRelationshipPattern := strings.Contains(pattern, "-[") || strings.Contains(pattern, "]-") || containsRelExistencePattern(pattern)
	var matches *TraversalMatch
	if isRelationshipPattern {
		matches = e.parseTraversalPattern(ctx, pattern)
		if matches == nil {
			return newBindings, newRelBindings
		}
	}

	for idx, existing := range existingBindings {
		var existingRels relationshipBinding
		if idx < len(existingRelBindings) {
			existingRels = existingRelBindings[idx]
		}

		// Check for relationship pattern
		if isRelationshipPattern {
			// Check if any bound variables are referenced
			boundStartNode := existing[matches.StartNode.variable]
			boundEndNode := existing[matches.EndNode.variable]

			var paths []PathResult
			if boundStartNode != nil {
				if matches.IsChained && len(matches.Segments) > 1 {
					paths = e.traverseChainedGraph(ctx, matches, []*storage.Node{boundStartNode})
				} else {
					paths = e.traverseFromNode(ctx, boundStartNode, matches)
				}
			} else {
				paths = e.traverseGraph(ctx, matches)
			}
			for _, path := range paths {
				if len(path.Nodes) == 0 {
					continue
				}
				startNode := path.Nodes[0]
				endNode := path.Nodes[len(path.Nodes)-1]

				// Check if path matches any bound variables
				startMatches := boundStartNode == nil || startNode.ID == boundStartNode.ID
				endMatches := boundEndNode == nil || endNode.ID == boundEndNode.ID

				if startMatches && endMatches {
					pathContext := e.buildPathContext(path, matches)
					b, ok := mergeNodeBindingsChecked(existing, pathContext.nodes)
					if !ok {
						continue
					}
					mergedRels, ok := mergeRelationshipBindingsChecked(existingRels, relationshipBindingsFromPathContext(pathContext))
					if !ok {
						continue
					}
					newBindings = append(newBindings, b)
					// Carry forward any relationship bound by an earlier
					// clause plus the relationship variable (if any) bound
					// by this clause's pattern, rejecting rows that would
					// rebind an already-bound relationship variable to a
					// different edge.
					newRelBindings = append(newRelBindings, mergedRels)
				}
			}
		} else {
			// Simple node pattern
			nodePattern := e.parseNodePattern(ctx, pattern)

			// Check if variable is already bound
			if boundNode := existing[nodePattern.variable]; boundNode != nil {
				// Variable is bound, just propagate
				newBindings = append(newBindings, existing)
				newRelBindings = append(newRelBindings, existingRels)
				continue
			}

			nodes, _ := e.loadPatternNodes(ctx, nodePattern.labels, nodePattern.properties)

			for _, node := range nodes {
				b := make(binding)
				for k, v := range existing {
					b[k] = v
				}
				b[nodePattern.variable] = node
				newBindings = append(newBindings, b)
				newRelBindings = append(newRelBindings, existingRels)
			}
		}
	}

	return newBindings, newRelBindings
}

func mergeNodeBindingsChecked(existing binding, current map[string]*storage.Node) (binding, bool) {
	merged := make(binding, len(existing)+len(current))
	for name, node := range existing {
		merged[name] = node
	}
	for name, node := range current {
		if previous := merged[name]; previous != nil && node != nil && previous.ID != node.ID {
			return nil, false
		}
		if merged[name] == nil {
			merged[name] = node
		}
	}
	return merged, true
}

// mergeRelBindingsChecked merges two relationship-binding maps (e.g. one
// carried forward from earlier MATCH clauses and one bound by the current
// clause). Overlapping keys must point at the same edge ID; otherwise the row
// combination is invalid and the merge fails. Returns nil when both are empty
// so callers can treat "no relationship bound yet" and "empty map"
// identically.
func mergeRelBindingsChecked(existing, current map[string]*storage.Edge) (map[string]*storage.Edge, bool) {
	if len(existing) == 0 && len(current) == 0 {
		return nil, true
	}
	merged := make(map[string]*storage.Edge, len(existing)+len(current))
	for k, v := range existing {
		merged[k] = v
	}
	for k, v := range current {
		prev := merged[k]
		if prev != nil && v != nil && prev.ID != v.ID {
			return nil, false
		}
		if prev == nil {
			merged[k] = v
		}
	}
	return merged, true
}

func relationshipBindingsFromPathContext(pathContext PathContext) relationshipBinding {
	bindings := make(relationshipBinding, len(pathContext.rels)+len(pathContext.paths))
	for name, relationship := range pathContext.rels {
		bindings[name] = relationship
	}
	for name, path := range pathContext.paths {
		if path == nil || path.Nodes != nil {
			continue
		}
		bindings[name] = append([]*storage.Edge{}, path.Relationships...)
	}
	if len(bindings) == 0 {
		return nil
	}
	return bindings
}

func mergeRelationshipBindingsChecked(existing, current relationshipBinding) (relationshipBinding, bool) {
	if len(existing) == 0 && len(current) == 0 {
		return nil, true
	}
	merged := make(relationshipBinding, len(existing)+len(current))
	for name, value := range existing {
		merged[name] = value
	}
	for name, value := range current {
		if previous, exists := merged[name]; exists && !sameRelationshipBinding(previous, value) {
			return nil, false
		}
		merged[name] = value
	}
	return merged, true
}

func sameRelationshipBinding(left, right interface{}) bool {
	leftEdge, leftIsEdge := left.(*storage.Edge)
	rightEdge, rightIsEdge := right.(*storage.Edge)
	if leftIsEdge || rightIsEdge {
		return leftIsEdge && rightIsEdge && leftEdge != nil && rightEdge != nil && leftEdge.ID == rightEdge.ID
	}
	leftList, leftIsList := left.([]*storage.Edge)
	rightList, rightIsList := right.([]*storage.Edge)
	if !leftIsList || !rightIsList || len(leftList) != len(rightList) {
		return false
	}
	for index := range leftList {
		if leftList[index] == nil || rightList[index] == nil || leftList[index].ID != rightList[index].ID {
			return false
		}
	}
	return true
}

func edgeRelationshipBindings(bindings relationshipBinding) map[string]*storage.Edge {
	result := make(map[string]*storage.Edge)
	for name, value := range bindings {
		if relationship, ok := value.(*storage.Edge); ok {
			result[name] = relationship
		}
	}
	return result
}

// filterBindingsByWhere filters bindings based on WHERE clause. Kept
// unchanged (node-only) because several existing tests
// (binding_where_benchmark_test.go, migration_query_shapes_test.go) call it
// directly with plain []binding for queries that bind no relationship
// variable. See filterBindingsByWhereWithRels for the relationship-aware
// variant used by executeMultiMatch.
func (e *StorageExecutor) filterBindingsByWhere(ctx context.Context, bindings []binding, whereClause string, params map[string]interface{}) []binding {
	compiled := e.newBindingFilterPredicate(ctx, whereClause, params)
	result := make([]binding, 0, len(bindings))

	for _, b := range bindings {
		if compiled.matches(b, params) {
			result = append(result, b)
		}
	}

	return result
}

// filterBindingsByWhereWithRels filters (binding, relationship-binding) row
// pairs by a WHERE clause that may reference a relationship variable's
// properties (e.g. "rel.evidence_source = $e"). The returned slices stay
// index-aligned with each other.
//
// This reuses the existing node-only WHERE compiler
// (binding_where_compile.go) unchanged by presenting each bound relationship
// as a read-only property view alongside the row's real node bindings for
// the duration of the WHERE check only — see bindingWithRelView. The real
// *storage.Edge values (needed for RETURN/DELETE) are returned separately
// and never replaced by the view.
func (e *StorageExecutor) filterBindingsByWhereWithRels(ctx context.Context, bindings []binding, relBindings []relationshipBinding, whereClause string, params map[string]interface{}) ([]binding, []relationshipBinding) {
	compiled := e.newBindingFilterPredicate(ctx, whereClause, params)
	resultBindings := make([]binding, 0, len(bindings))
	resultRels := make([]relationshipBinding, 0, len(bindings))

	for i, b := range bindings {
		var rels relationshipBinding
		if i < len(relBindings) {
			rels = relBindings[i]
		}
		if compiled.matchesRelationships(b, rels, params) {
			resultBindings = append(resultBindings, b)
			resultRels = append(resultRels, rels)
		}
	}

	return resultBindings, resultRels
}

// bindingWithRelView returns a binding row for WHERE-clause evaluation that
// additionally exposes each bound relationship variable as a read-only,
// node-shaped property view (ID + Properties only, no labels/edges). The
// binding-where evaluator (binding_where_compile.go) only ever reads
// `.Properties[prop]` or `.ID` off a bound variable via getBindingNodeValue,
// so this view lets relationship property predicates (e.g.
// "rel.evidence_source = $e") evaluate correctly without teaching every
// WHERE-compiler function in binding_where_compile.go about a second
// bound-value kind. Returns b unchanged when there is nothing to add.
func bindingWithRelView(b binding, rels map[string]*storage.Edge) binding {
	if len(rels) == 0 {
		return b
	}
	view := make(binding, len(b)+len(rels))
	for k, v := range b {
		view[k] = v
	}
	for k, edge := range rels {
		if edge == nil {
			continue
		}
		view[k] = &storage.Node{ID: storage.NodeID(edge.ID), Properties: edge.Properties}
	}
	return view
}

// evaluateBindingWhere evaluates WHERE clause against a binding
func (e *StorageExecutor) evaluateBindingWhere(ctx context.Context, b binding, whereClause string, params map[string]interface{}) bool {
	return e.evaluateBindingWhereGeneric(ctx, b, whereClause, params)
}

func (e *StorageExecutor) resolveWhereValue(ctx context.Context, raw string, params map[string]interface{}) interface{} {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "$") {
		name := strings.TrimSpace(strings.TrimPrefix(s, "$"))
		if params != nil {
			if v, ok := params[name]; ok {
				return v
			}
		}
	}
	return e.parseValue(ctx, s)
}

// resolveBindingItem resolves a return item against a binding. rels carries
// any relationship variable(s) bound for this specific row (index-aligned
// counterpart produced by executeFirstMatch/executeChainedMatch); pass nil
// when the row binds no relationship variable.
func (e *StorageExecutor) resolveBindingItemWithRelationships(ctx context.Context, item returnItem, b binding, rels relationshipBinding) interface{} {
	expr := strings.TrimSpace(item.expr)
	if expr == "" {
		return nil
	}
	return e.resolveBindingExprWithRelationships(ctx, expr, b, rels)
}

func (e *StorageExecutor) resolveBindingItem(ctx context.Context, item returnItem, b binding, rels map[string]*storage.Edge) interface{} {
	return e.resolveBindingItemWithRelationships(ctx, item, b, relationshipBindingFromEdges(rels))
}

func (e *StorageExecutor) resolveBindingExpr(ctx context.Context, expr string, b binding, rels map[string]*storage.Edge) interface{} {
	return e.resolveBindingExprWithRelationships(ctx, expr, b, relationshipBindingFromEdges(rels))
}

func (e *StorageExecutor) resolveBindingExprWithRelationships(ctx context.Context, expr string, b binding, rels relationshipBinding) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}

	// elementId(var) — check node bindings first, then relationship bindings.
	if strings.HasPrefix(lowerASCII(expr), "elementid(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[len("elementId(") : len(expr)-1])
		if node := b[inner]; node != nil {
			return storage.NodeElementID(e.executionDatabaseName(ctx), node.ID)
		}
		if edge, ok := rels[inner].(*storage.Edge); ok && edge != nil {
			return storage.RelationshipElementID(e.executionDatabaseName(ctx), edge.ID)
		}
		return nil
	}

	// Reuse shared COALESCE evaluator used in MATCH row projection paths.
	if strings.HasPrefix(upperASCII(expr), "COALESCE(") && strings.HasSuffix(expr, ")") {
		return e.evaluateCoalesceInContext(expr, b, edgeRelationshipBindings(rels), nil)
	}

	// Literal value
	if strings.HasPrefix(expr, "'") || strings.HasPrefix(expr, "\"") ||
		strings.EqualFold(expr, "true") || strings.EqualFold(expr, "false") ||
		strings.EqualFold(expr, "null") || isNumericLiteral(expr) {
		return e.parseValue(ctx, expr)
	}

	// Property access: var.prop — check node bindings first, then
	// relationship bindings (e.g. "rel.evidence_source").
	if dotIdx := strings.Index(expr, "."); dotIdx > 0 && isSimpleIdentifierOrProperty(expr) {
		varName := expr[:dotIdx]
		propName := expr[dotIdx+1:]
		if node := b[varName]; node != nil {
			if val, ok := getBindingNodeValue(node, propName); ok {
				return val
			}
			return nil
		}
		if edge, ok := rels[varName].(*storage.Edge); ok && edge != nil {
			return edge.Properties[propName]
		}
		return nil
	}

	// Node variable
	if node := b[expr]; node != nil {
		return node
	}

	// BUG FIX: relationship variable (e.g. bare "rel" in `RETURN rel` or
	// `DELETE rel`). Previously unreachable — `binding` never stored
	// relationships, so this always fell through to the generic evaluator
	// below with a nil rels map and produced nil. Returning the real
	// *storage.Edge here (rather than a synthetic node) matters because
	// DELETE's classifyDeleteTargetValue and Neo4j-compat RETURN both
	// distinguish edges from nodes by Go type.
	if relationship, exists := rels[expr]; exists {
		return relationship
	}

	// Fallback to the common expression evaluator used in other MATCH/RETURN
	// paths. It already accepts a relationship map (previously always passed
	// nil here), so expressions like count(rel) / collect(rel.prop) resolve
	// correctly once rels is populated.
	if val := e.evaluateExpressionWithContext(ctx, expr, b, edgeRelationshipBindings(rels)); val != nil {
		return val
	}

	return nil
}

func relationshipBindingFromEdges(edges map[string]*storage.Edge) relationshipBinding {
	bindings := make(relationshipBinding, len(edges))
	for name, edge := range edges {
		bindings[name] = edge
	}
	return bindings
}

func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// collectNodesWithStreaming efficiently collects nodes from storage using streaming when possible.
// This avoids loading all nodes into memory, which is critical for performance with large datasets.
//
// Parameters:
//   - ctx: Context for cancellation
//   - labels: Optional label filter (only nodes with this label)
//   - properties: Optional property filters
//   - limit: Maximum number of nodes to collect (-1 for unlimited)
//
// Returns collected nodes or error.
func (e *StorageExecutor) collectNodesWithStreaming(
	ctx context.Context,
	labels []string,
	properties map[string]interface{},
	whereVariable string,
	whereClause string,
	limit int,
) ([]*storage.Node, error) {
	return e.collectNodesWithStreamingProjection(ctx, labels, properties, whereVariable, whereClause, limit, nil)
}

// collectNodesWithStreamingProjection is collectNodesWithStreaming whose
// label scan reads only the projection's user properties; nil reads whole
// nodes. The caller guarantees nothing else in the statement uses the
// returned nodes' other properties.
func (e *StorageExecutor) collectNodesWithStreamingProjection(
	ctx context.Context,
	labels []string,
	properties map[string]interface{},
	whereVariable string,
	whereClause string,
	limit int,
	projection []string,
) ([]*storage.Node, error) {
	capacity := 0
	if limit > 0 {
		capacity = limit
	}
	collected := make([]*storage.Node, 0, capacity)
	if err := e.visitNodesWithStreamingProjection(ctx, labels, properties, whereVariable, whereClause, limit, projection, &collected, nil); err != nil {
		return nil, err
	}
	return collected, nil
}

// visitNodesWithStreamingProjection is collectNodesWithStreamingProjection
// appending each node to *collected, or handing it to visit when visit is
// set, as the scan reads it, in the same order. visit returning
// storage.ErrIterationStopped ends the scan without error; any other error
// ends it with that error.
func (e *StorageExecutor) visitNodesWithStreamingProjection(
	ctx context.Context,
	labels []string,
	properties map[string]interface{},
	whereVariable string,
	whereClause string,
	limit int,
	projection []string,
	collected *[]*storage.Node,
	visit func(*storage.Node) error,
) error {
	store := e.getStorage(ctx)
	viewport, hasViewport := TemporalViewportFromContext(ctx)
	checker, canCheckViewport := store.(temporalCurrentNodeChecker)
	hideSystemNodes := shouldHideSystemNodes(store)
	visited := 0
	var whereFilter FilterFunc
	if strings.TrimSpace(whereClause) != "" {
		whereFilter = e.compileNodeWhereFilter(ctx, whereVariable, whereClause)
	}
	collect := func(node *storage.Node) error {
		if node == nil || (hideSystemNodes && isSystemNode(node)) {
			return nil
		}
		if len(labels) > 0 && !mergeNodeHasLabels(node, labels) {
			return nil
		}
		if len(properties) > 0 && !e.nodeMatchesProps(node, properties) {
			return nil
		}
		if whereFilter != nil && !whereFilter(node) {
			return nil
		}
		if hasViewport && canCheckViewport {
			visible, err := checker.IsCurrentTemporalNode(node, viewport.AsOf)
			if err != nil {
				return err
			}
			if !visible {
				return nil
			}
		}
		if visit == nil {
			*collected = append(*collected, node)
		} else if err := visit(node); err != nil {
			return err
		}
		visited++
		if limit > 0 && visited >= limit {
			return storage.ErrIterationStopped
		}
		return nil
	}
	// A materialized fallback's nodes go to visit as a list.
	visitAll := func(nodes []*storage.Node) error {
		if visit == nil {
			*collected = append(*collected, nodes...)
			return nil
		}
		return visitNodeList(nodes, visit)
	}

	// A label-indexed stream is the primary physical scan for every labelled
	// MATCH. It preserves the shared row pipeline while keeping work
	// proportional to the matching label, applies residual predicates before
	// materialising rows, and lets LIMIT stop the storage iterator early.
	if len(labels) > 0 {
		if reader, ok := store.(storage.ProjectedLabelNodeReader); ok {
			err := reader.StreamNodesByLabelProjected(labels[0], projection, collect)
			if err == nil || err == storage.ErrIterationStopped {
				return nil
			}
			return err
		}
	}

	// Compatibility path for stores that expose label IDs but not label-node
	// streaming. This still avoids a full graph scan for bounded simple reads.
	if limit > 0 && len(labels) == 1 && len(properties) == 0 && strings.TrimSpace(whereClause) == "" {
		ids, err := storage.NodeIDsByLabel(store, labels[0], limit)
		if err != nil {
			return err
		}
		filtered := make([]*storage.Node, 0, util.SafePreallocCap(len(ids)))
		for _, id := range ids {
			node, getErr := store.GetNode(id)
			if getErr != nil {
				return getErr
			}
			if node == nil {
				continue
			}
			if hideSystemNodes && isSystemNode(node) {
				continue
			}
			if hasViewport && canCheckViewport {
				visible, err := checker.IsCurrentTemporalNode(node, viewport.AsOf)
				if err != nil {
					return err
				}
				if !visible {
					continue
				}
			}
			filtered = append(filtered, node)
			if len(filtered) >= limit {
				break
			}
		}
		return visitAll(filtered)
	}

	var nodes []*storage.Node
	var err error

	// A label-less property match reads every node (#824): decode only the
	// properties a match must have, let the engine skip a node that fails them
	// before decoding the rest of it, and read the whole node only for a
	// match. A required string is compared with the stored bytes, so a node
	// without it is skipped undecoded (#857). The node then passes the same
	// filters as on the full scan, including the whole WHERE. The required
	// properties are the pattern's and the WHERE's top-level equalities on
	// the variable (WHERE n.id = $id, #857), which every match satisfies.
	var required map[string]interface{}
	if len(labels) == 0 {
		required = e.labellessScanRequiredProperties(ctx, properties, whereVariable, whereClause)
	}
	if len(required) > 0 {
		keys := make([]string, 0, len(required))
		var stringEquals map[string]string
		for key, value := range required {
			keys = append(keys, key)
			if text, ok := value.(string); ok {
				if stringEquals == nil {
					stringEquals = make(map[string]string, len(required))
				}
				stringEquals[key] = text
			}
		}
		matches := func(props map[string]interface{}) bool {
			return nodePropertiesMatch(&storage.Node{Properties: props}, required)
		}
		err := store.StreamNodesWithOptions(ctx, storage.StreamNodesOptions{Projection: keys, ApplyDecayFilter: true, PropertyFilter: matches, PropertyStringEquals: stringEquals}, func(projected *storage.Node) error {
			if projected == nil || !matches(projected.Properties) {
				return nil
			}
			node, err := store.GetNode(projected.ID)
			if errors.Is(err, storage.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			return collect(node)
		})
		if err != nil && err != storage.ErrIterationStopped {
			return err
		}
		return nil
	}

	// Streaming is the shared scan primitive for the converged executor. Apply
	// every residual filter in the visitor so both bounded and unbounded scans
	// avoid the storage APIs that first materialize the complete population.
	// LIMIT additionally stops storage iteration as soon as enough qualifying
	// nodes have been produced.
	if streamer, ok := store.(storage.StreamingEngine); ok {
		err = streamer.StreamNodes(ctx, collect)
		if err == storage.ErrIterationStopped {
			err = nil
		}
		return err
	}

	// Compatibility fallback for storage implementations without StreamingEngine.
	if len(labels) > 0 {
		nodes, err = store.GetNodesByLabel(labels[0])
	} else {
		nodes, err = store.AllNodes()
	}
	if err != nil {
		return err
	}

	// Filter out system nodes (labels starting with _)
	filteredNodes := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if len(labels) > 0 && !mergeNodeHasLabels(node, labels) {
			continue
		}
		if !hideSystemNodes || !isSystemNode(node) {
			filteredNodes = append(filteredNodes, node)
		}
	}
	nodes = filteredNodes
	if hasViewport && canCheckViewport {
		nodes, err = filterNodesByTemporalViewport(nodes, viewport, checker)
		if err != nil {
			return err
		}
	}

	// Apply property filters
	if len(properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, properties)
	}
	if strings.TrimSpace(whereClause) != "" {
		nodes = e.filterNodes(ctx, nodes, whereVariable, whereClause)
	}
	if limit > 0 && len(nodes) > limit {
		nodes = nodes[:limit]
	}

	return visitAll(nodes)
}

// visitNodeList hands nodes to visit in order. visit returning
// storage.ErrIterationStopped ends the visit without error; any other error
// ends it with that error.
func visitNodeList(nodes []*storage.Node, visit func(*storage.Node) error) error {
	for _, node := range nodes {
		if err := visit(node); err != nil {
			if err == storage.ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// labellessScanRequiredProperties returns the property values every node a
// label-less scan may match must have: the pattern's properties and the
// WHERE's top-level equalities between variable.property and a constant
// (parameter, literal or bound value; parseSimpleIndexedEquality). Over-
// approximating the WHERE is safe because the caller still evaluates it on
// every node the scan returns.
func (e *StorageExecutor) labellessScanRequiredProperties(ctx context.Context, properties map[string]interface{}, variable, whereClause string) map[string]interface{} {
	clause := unwrapOuterParens(strings.TrimSpace(whereClause))
	if clause == "" || strings.TrimSpace(variable) == "" {
		return properties
	}
	required := make(map[string]interface{}, len(properties)+1)
	for key, value := range properties {
		required[key] = value
	}
	for _, conjunct := range splitTopLevelAndConjuncts(clause) {
		property, value, ok := e.parseSimpleIndexedEquality(ctx, variable, unwrapOuterParens(strings.TrimSpace(conjunct)))
		if !ok {
			continue
		}
		if _, exists := required[property]; !exists {
			required[property] = value
		}
	}
	return required
}

func shouldHideSystemNodes(engine storage.Engine) bool {
	// Allow system nodes to be queried when the active database is system.
	// For all other databases, hide internal nodes (labels starting with "_")
	// to avoid leaking metadata into normal user queries.
	if namespaced, ok := engine.(*storage.NamespacedEngine); ok {
		return namespaced.Namespace() != "system"
	}
	return true
}

func isSystemNode(node *storage.Node) bool {
	if node == nil {
		return false
	}
	for _, label := range node.Labels {
		if strings.HasPrefix(label, "_") {
			return true
		}
	}
	return false
}

type cartesianInConstraint struct {
	prop      string
	allowed   map[string]struct{}
	hasValues bool
}

type cartesianEqConstraint struct {
	leftVar   string
	leftProp  string
	rightVar  string
	rightProp string
	// offset is the integer offset in leftVar.leftProp = rightVar.rightProp
	// + offset, parsed from joins like b.id = a.id + 1 (#692). 0 means a
	// plain property equality.
	offset int64
}

func (e *StorageExecutor) applyCartesianWherePushdown(
	ctx context.Context,
	patternMatches []struct {
		variable string
		nodes    []*storage.Node
	},
	whereClause string,
) []struct {
	variable string
	nodes    []*storage.Node
} {
	if len(patternMatches) < 2 || strings.TrimSpace(whereClause) == "" {
		return patternMatches
	}

	varIndex := make(map[string]int, len(patternMatches))
	for i, pm := range patternMatches {
		if pm.variable != "" {
			varIndex[pm.variable] = i
		}
	}

	inConstraints := map[string]cartesianInConstraint{}
	nullConstraints := map[string]cartesianNullConstraint{}
	eqConstraints := make([]cartesianEqConstraint, 0, 2)
	for _, term := range splitTopLevelAndConjuncts(whereClause) {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		if v, p, vals, ok := parseCartesianInListTerm(term); ok {
			c := inConstraints[v]
			if c.prop == "" {
				c.prop = p
				c.allowed = make(map[string]struct{}, len(vals))
				for _, raw := range vals {
					c.allowed[cartesianValueKey(raw)] = struct{}{}
				}
				c.hasValues = true
			}
			inConstraints[v] = c
			continue
		}
		if v, p, expectNotNull, ok := parseCartesianNullTerm(term); ok {
			key := v + "|" + p
			c := nullConstraints[key]
			if c.hasValue && c.expectNotNull != expectNotNull {
				c.conflict = true
			} else {
				c.expectNotNull = expectNotNull
				c.hasValue = true
			}
			nullConstraints[key] = c
			continue
		}
		if lv, lp, rv, rp, offset, ok := parseCartesianVarPropEqualityTerm(term); ok {
			eqConstraints = append(eqConstraints, cartesianEqConstraint{
				leftVar:   lv,
				leftProp:  lp,
				rightVar:  rv,
				rightProp: rp,
				offset:    offset,
			})
			continue
		}
		if variable, ok := parseCartesianSingleNodeComparisonTerm(term); ok {
			idx, exists := varIndex[variable]
			if !exists {
				continue
			}
			predicate, supported := e.getCompiledSimpleWhere(ctx, variable, term)
			if !supported {
				continue
			}
			filtered := make([]*storage.Node, 0, len(patternMatches[idx].nodes))
			for _, node := range patternMatches[idx].nodes {
				if node != nil && predicate(node) {
					filtered = append(filtered, node)
				}
			}
			patternMatches[idx].nodes = filtered
		}
	}

	applyCartesianNullConstraints(patternMatches, varIndex, nullConstraints)

	// Apply direct IN constraints.
	for v, c := range inConstraints {
		if !c.hasValues {
			continue
		}
		idx, ok := varIndex[v]
		if !ok {
			continue
		}
		patternMatches[idx].nodes = filterNodesByAllowedPropSet(patternMatches[idx].nodes, c.prop, c.allowed)
	}

	// Propagate equality constraints both directions until stable.
	for changed := true; changed; {
		changed = false
		for _, c := range eqConstraints {
			leftIdx, lok := varIndex[c.leftVar]
			rightIdx, rok := varIndex[c.rightVar]
			if !lok || !rok {
				continue
			}
			leftAllowed := collectPropValues(patternMatches[leftIdx].nodes, c.leftProp)
			rightAllowed := collectPropValues(patternMatches[rightIdx].nodes, c.rightProp)
			if len(leftAllowed) == 0 || len(rightAllowed) == 0 {
				continue
			}
			// leftVar.leftProp = rightVar.rightProp + offset:
			//   allowed right values = left values shifted by -offset
			//   allowed left values  = right values shifted by +offset
			rightCandidates := leftAllowed
			leftCandidates := rightAllowed
			if c.offset != 0 {
				rightCandidates = shiftCartesianIntKeySet(leftAllowed, -c.offset)
				leftCandidates = shiftCartesianIntKeySet(rightAllowed, c.offset)
				if len(rightCandidates) == 0 || len(leftCandidates) == 0 {
					continue
				}
			}
			if filtered := filterNodesByAllowedPropSet(patternMatches[rightIdx].nodes, c.rightProp, rightCandidates); len(filtered) != len(patternMatches[rightIdx].nodes) {
				patternMatches[rightIdx].nodes = filtered
				changed = true
			}
			if filtered := filterNodesByAllowedPropSet(patternMatches[leftIdx].nodes, c.leftProp, leftCandidates); len(filtered) != len(patternMatches[leftIdx].nodes) {
				patternMatches[leftIdx].nodes = filtered
				changed = true
			}
		}
	}
	return patternMatches
}

// shiftCartesianIntKeySet shifts the integer values in a cartesian value-key
// set by offset, dropping keys that are not integers. The shifted set names
// the property values that may pair with the original set under an
// lv.lp = rv.rp + offset constraint (#692).
func shiftCartesianIntKeySet(set map[string]struct{}, offset int64) map[string]struct{} {
	out := make(map[string]struct{}, len(set))
	for key := range set {
		var digits string
		switch {
		case strings.HasPrefix(key, "i64:"):
			digits = key[len("i64:"):]
		case strings.HasPrefix(key, "i:"):
			digits = key[len("i:"):]
		default:
			continue
		}
		value, err := strconv.ParseInt(digits, 10, 64)
		if err != nil {
			continue
		}
		out[fmt.Sprintf("i64:%d", value+offset)] = struct{}{}
	}
	return out
}

func applyCartesianNullConstraints(
	patternMatches []struct {
		variable string
		nodes    []*storage.Node
	},
	varIndex map[string]int,
	nullConstraints map[string]cartesianNullConstraint,
) {
	for key, constraint := range nullConstraints {
		parts := strings.SplitN(key, "|", 2)
		if len(parts) != 2 {
			continue
		}
		idx, ok := varIndex[parts[0]]
		if !ok {
			continue
		}
		if constraint.conflict {
			patternMatches[idx].nodes = patternMatches[idx].nodes[:0]
			continue
		}
		patternMatches[idx].nodes = filterNodesByNullConstraint(patternMatches[idx].nodes, parts[1], constraint.expectNotNull)
	}
}

func parseCartesianNullTerm(term string) (string, string, bool, bool) {
	upper := upperASCII(strings.TrimSpace(term))
	if strings.HasSuffix(upper, " IS NOT NULL") {
		expr := strings.TrimSpace(term[:len(term)-len(" IS NOT NULL")])
		v, p, ok := parseCartesianVarProp(expr)
		if !ok {
			return "", "", false, false
		}
		return v, p, true, true
	}
	if strings.HasSuffix(upper, " IS NULL") {
		expr := strings.TrimSpace(term[:len(term)-len(" IS NULL")])
		v, p, ok := parseCartesianVarProp(expr)
		if !ok {
			return "", "", false, false
		}
		return v, p, false, true
	}
	return "", "", false, false
}

func parseCartesianVarProp(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	dot := strings.IndexByte(expr, '.')
	if dot <= 0 || dot >= len(expr)-1 {
		return "", "", false
	}
	v := strings.TrimSpace(expr[:dot])
	p := strings.TrimSpace(expr[dot+1:])
	property, validProperty := isOneSymbolicName(p)
	if !isSimpleIdentifierCartesian(v) || !validProperty {
		return "", "", false
	}
	return v, property, true
}

func parseCartesianSingleNodeComparisonTerm(term string) (string, bool) {
	clause := strings.TrimSpace(term)
	scan, comparison := scanComparisonChain(clause)
	if !comparison || scan.count != 1 {
		return "", false
	}
	span := scan.operator(0)
	operator := clause[span.offset : span.offset+span.length]
	switch operator {
	case "=", "<>", "!=", ">", ">=", "<", "<=", "=~":
	default:
		return "", false
	}
	variable, _, ok := parseCartesianVarProp(clause[:span.offset])
	if !ok {
		return "", false
	}
	if _, ok := parseLiteralValue(clause[span.offset+span.length:]); !ok {
		return "", false
	}
	return variable, true
}

func parseCartesianInListTerm(term string) (string, string, []interface{}, bool) {
	upper := upperASCII(term)
	idx := strings.Index(upper, " IN ")
	if idx <= 0 || idx+4 >= len(term) {
		return "", "", nil, false
	}
	lhs := strings.TrimSpace(term[:idx])
	rhs := strings.TrimSpace(term[idx+4:])
	v, p, ok := parseCartesianVarProp(lhs)
	if !ok {
		return "", "", nil, false
	}
	if !(strings.HasPrefix(rhs, "[") && strings.HasSuffix(rhs, "]")) {
		return "", "", nil, false
	}
	inner := strings.TrimSpace(rhs[1 : len(rhs)-1])
	if inner == "" {
		return v, p, []interface{}{}, true
	}
	items := splitTopLevelCommaKeepEmpty(inner)
	values := make([]interface{}, 0, len(items))
	for _, raw := range items {
		lit, ok := parseLiteralValue(strings.TrimSpace(raw))
		if !ok {
			return "", "", nil, false
		}
		values = append(values, lit)
	}
	return v, p, values, true
}

func isSimpleIdentifierCartesian(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if isIdentStartByte(ch) {
			continue
		}
		if i > 0 && ch >= '0' && ch <= '9' {
			continue
		}
		return false
	}
	return true
}

func parseCartesianVarPropEqualityTerm(term string) (string, string, string, string, int64, bool) {
	idx := strings.Index(term, "=")
	if idx <= 0 || idx+1 >= len(term) {
		return "", "", "", "", 0, false
	}
	lhs := strings.TrimSpace(term[:idx])
	rhs := strings.TrimSpace(term[idx+1:])
	if lv, lp, lok := parseCartesianVarProp(lhs); lok {
		if rv, rp, rok := parseCartesianVarProp(rhs); rok {
			return lv, lp, rv, rp, 0, true
		}
		// b.id = a.id + 1: the right side is an offset expression.
		if rv, rp, offset, rok := parseCartesianVarPropOffset(rhs); rok {
			return lv, lp, rv, rp, offset, true
		}
	}
	// a.id + 1 = b.id normalizes to b.id = a.id + 1.
	if lv, lp, offset, lok := parseCartesianVarPropOffset(lhs); lok {
		if rv, rp, rok := parseCartesianVarProp(rhs); rok {
			return rv, rp, lv, lp, offset, true
		}
	}
	return "", "", "", "", 0, false
}

// parseCartesianVarPropOffset parses <var>.<prop> + <int> or
// <var>.<prop> - <int> with an integer literal offset, independent of
// whitespace around the operator (b.id=a.id+1 parses the same as
// b.id = a.id + 1, #692).
func parseCartesianVarPropOffset(expr string) (string, string, int64, bool) {
	expr = strings.TrimSpace(expr)
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch != '+' && ch != '-' {
			continue
		}
		left := strings.TrimSpace(expr[:i])
		literal := strings.TrimSpace(expr[i+1:])
		variable, prop, ok := parseCartesianVarProp(left)
		if !ok {
			continue
		}
		offset, err := strconv.ParseInt(literal, 10, 64)
		if err != nil {
			return "", "", 0, false
		}
		if ch == '-' {
			offset = -offset
		}
		return variable, prop, offset, true
	}
	return "", "", 0, false
}

// addInt64Offset adds offset to value exactly, reporting false when the sum
// overflows int64.
func addInt64Offset(value, offset int64) (int64, bool) {
	sum := value + offset
	if (offset > 0 && sum < value) || (offset < 0 && sum > value) {
		return 0, false
	}
	return sum, true
}

// cartesianShiftValueKey shifts an integer property value by offset exactly
// (no float64 rounding above 2^53, #692), keeping its stored type in the
// key. It reports false when the value is not an integer or the shift
// overflows.
func cartesianShiftValueKey(v interface{}, offset int64) (string, bool) {
	var base int64
	switch x := v.(type) {
	case int:
		base = int64(x)
	case int64:
		base = x
	default:
		return "", false
	}
	sum, ok := addInt64Offset(base, offset)
	if !ok {
		return "", false
	}
	switch v.(type) {
	case int:
		return cartesianValueKey(int(sum)), true
	default:
		return cartesianValueKey(sum), true
	}
}

// cartesianOffsetValuesEqual reports whether left == right + offset.
// Integer values compare exactly (no float64 rounding above 2^53, #692);
// non-integer values compare through float64, and non-numeric values never
// satisfy an offset join.
func cartesianOffsetValuesEqual(left, right interface{}, offset int64) bool {
	if li, lok := int64OfValue(left); lok {
		if ri, rok := int64OfValue(right); rok {
			sum, ok := addInt64Offset(ri, offset)
			return ok && li == sum
		}
	}
	lNum, lNumeric := toFloat64(left)
	rNum, rNumeric := toFloat64(right)
	return lNumeric && rNumeric && lNum == rNum+float64(offset)
}

// int64OfValue reports an integer value exactly (int or int64).
func int64OfValue(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	default:
		return 0, false
	}
}

func cartesianValueKey(v interface{}) string {
	return cypherEquivalenceKey(v)
}

func collectPropValues(nodes []*storage.Node, prop string) map[string]struct{} {
	out := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n == nil || n.Properties == nil {
			continue
		}
		val, ok := n.Properties[prop]
		if !ok {
			continue
		}
		out[cartesianValueKey(val)] = struct{}{}
	}
	return out
}

func filterNodesByAllowedPropSet(nodes []*storage.Node, prop string, allowed map[string]struct{}) []*storage.Node {
	if len(nodes) == 0 || len(allowed) == 0 {
		return nodes
	}
	out := make([]*storage.Node, 0, len(nodes))
	for _, n := range nodes {
		if n == nil || n.Properties == nil {
			continue
		}
		val, ok := n.Properties[prop]
		if !ok {
			continue
		}
		if _, keep := allowed[cartesianValueKey(val)]; keep {
			out = append(out, n)
		}
	}
	return out
}

type cartesianNullConstraint struct {
	expectNotNull bool
	hasValue      bool
	conflict      bool
}

func filterNodesByNullConstraint(nodes []*storage.Node, prop string, expectNotNull bool) []*storage.Node {
	if len(nodes) == 0 {
		return nodes
	}
	out := make([]*storage.Node, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		val, exists := n.Properties[prop]
		isNull := !exists || val == nil
		if expectNotNull {
			if !isNull {
				out = append(out, n)
			}
			continue
		}
		if isNull {
			out = append(out, n)
		}
	}
	return out
}

// evaluateWhereForContext evaluates a WHERE clause against a node context
func (e *StorageExecutor) evaluateWhereForContext(ctx context.Context, whereClause string, nodes map[string]*storage.Node) bool {
	if strings.TrimSpace(whereClause) == "" {
		return true
	}
	predicate := e.newBindingFilterPredicate(ctx, whereClause, nil)
	return predicate.matches(binding(nodes), nil)
}

// evaluateBoundRelationshipPattern evaluates a WHERE pattern against the
// current bindings using the same parser and traversal implementation as a
// MATCH clause. The second return value distinguishes a valid pattern that did
// not match from an expression that is not a relationship pattern.
func (e *StorageExecutor) evaluateBoundRelationshipPattern(ctx context.Context, clause string, nodes map[string]*storage.Node) (bool, bool) {
	match, recognized := e.parseBoundRelationshipPattern(ctx, clause)
	if !recognized {
		return false, false
	}
	return e.evaluateParsedBoundRelationshipPattern(ctx, match, nodes), true
}

func (e *StorageExecutor) parseBoundRelationshipPattern(ctx context.Context, clause string) (*TraversalMatch, bool) {
	pattern := strings.TrimSpace(clause)
	if !strings.HasPrefix(pattern, "(") || !strings.HasSuffix(pattern, ")") ||
		(!strings.Contains(pattern, "-[") && !strings.Contains(pattern, "]-") && !strings.Contains(pattern, "--")) {
		return nil, false
	}

	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || len(match.Segments) == 0 && match.Relationship.MinHops == 0 && !match.Relationship.VariableLength {
		return nil, false
	}
	return match, true
}

func (e *StorageExecutor) evaluateParsedBoundRelationshipPattern(ctx context.Context, match *TraversalMatch, nodes map[string]*storage.Node) bool {
	if match.StartNode.variable != "" && nodes[match.StartNode.variable] == nil ||
		match.EndNode.variable != "" && nodes[match.EndNode.variable] == nil {
		return false
	}
	for _, intermediate := range match.IntermediateNodes {
		if intermediate.variable != "" && nodes[intermediate.variable] == nil {
			return false
		}
	}
	if !match.IsChained && match.Relationship.MinHops == 1 && match.Relationship.MaxHops == 1 {
		return e.evaluateBoundOneHopPattern(ctx, match, nodes)
	}

	var paths []PathResult
	if start := nodes[match.StartNode.variable]; start != nil {
		if match.IsChained && len(match.Segments) > 1 {
			paths = e.traverseChainedGraph(ctx, match, []*storage.Node{start})
		} else {
			paths = e.traverseFromNode(ctx, start, match)
		}
	} else if end := nodes[match.EndNode.variable]; end != nil && !match.IsChained {
		reversed := reverseTraversalMatch(match)
		if reversed == nil {
			return false
		}
		for _, reversedPath := range e.traverseFromNode(ctx, end, reversed) {
			paths = append(paths, reversePathResult(reversedPath))
		}
	} else {
		paths = e.traverseGraph(ctx, match)
	}

	existing := binding(nodes)
	for _, path := range paths {
		pathContext := e.buildPathContext(path, match)
		if _, compatible := mergeNodeBindingsChecked(existing, pathContext.nodes); compatible {
			return true
		}
	}
	return false
}

func (e *StorageExecutor) evaluateBoundOneHopPattern(ctx context.Context, match *TraversalMatch, nodes map[string]*storage.Node) bool {
	start := nodes[match.StartNode.variable]
	if start == nil {
		end := nodes[match.EndNode.variable]
		if end == nil {
			return false
		}
		reversed := reverseTraversalMatch(match)
		return reversed != nil && e.evaluateBoundOneHopPattern(ctx, reversed, nodes)
	}
	if !pipelineNodeMatchesPattern(start, match.StartNode) {
		return false
	}
	viewport, _ := TemporalViewportFromContext(ctx)
	checker, _ := e.getStorage(ctx).(temporalCurrentNodeChecker)
	paths := e.traverseGraphSequential(ctx, match, []*storage.Node{start}, viewport, checker)
	end, bound := nodes[match.EndNode.variable]
	for _, path := range paths {
		if len(path.Nodes) == 0 {
			continue
		}
		if !bound || end != nil && path.Nodes[len(path.Nodes)-1].ID == end.ID {
			return true
		}
	}
	return false
}

// executeCreate handles CREATE queries.
