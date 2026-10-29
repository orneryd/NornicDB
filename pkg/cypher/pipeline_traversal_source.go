package cypher

import (
	"context"
	"errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// pipelineTraversalSource feeds fixed-length relationship MATCH rows directly
// to the next operator. In particular, aggregation and Bolt projection must
// not first collect a graph-sized PathResult or binding-map slice.
func (e *StorageExecutor) pipelineTraversalSource(ctx context.Context, input pipelineRowSource, clause string, hint pipelineMatchPhysicalHint) (pipelineRowSource, bool, error) {
	if materialize, _ := ctx.Value(traversalMaterializeKey{}).(bool); materialize {
		return nil, false, nil
	}
	// Keep the existing bounded/top-k physical operators for LIMIT and ORDER.
	if hint.readTail == nil || hint.orderExpr != "" || hint.earlyLimit >= 0 {
		return nil, false, nil
	}
	for _, tail := range hint.readTail {
		if topLevelKeywordIndex(tail, "ORDER BY") >= 0 {
			return nil, false, nil
		}
	}
	pattern := pipelineClauseBody(clause, "MATCH")
	where := ""
	if index := topLevelKeywordIndex(pattern, "WHERE"); index >= 0 {
		where = strings.TrimSpace(pattern[index+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:index])
	}
	if !containsRelationshipPattern(pattern) || len(splitTopLevelComma(pattern)) != 1 {
		return nil, false, nil
	}
	if viewport, _ := TemporalViewportFromContext(ctx); viewport.Enabled() {
		return nil, false, nil
	}
	// Admission has no input row. Inspect syntax through the shared reader;
	// the scan planner retains the existing property-seeded access paths.
	shape := e.parseTraversalPatternStructure(ctx, pattern)
	if shape == nil || shape.IsChained || shape.Relationship.VariableLength || shape.Relationship.MinHops != 1 || shape.Relationship.MaxHops != 1 {
		return nil, false, nil
	}
	// Selected paths have their own dispatcher, even when their text contains
	// a fixed relationship segment. A preliminary parse of inline properties
	// would evaluate row-bound expressions before the input row exists.
	if strings.Contains(where, pathSelectorFunction) || strings.Contains(where, repeatableElementsFunction) {
		return nil, false, nil
	}
	if planPipelineTraversalScan(shape, where, nil).access == traversalSeededAccess {
		return nil, false, nil
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	predicate := prepareRowPredicate(where)
	store := e.getStorage(ctx)
	endpoints := e.newRelationshipEndpointReader(store)
	return func(yield func(pipelineRow) bool) bool {
		valid := true
		endpointValues := &endpointPropertyCache{}
		current := make(pipelineRow)
		completed := input(func(row pipelineRow) bool {
			// Validate the common MATCH contract before choosing either a scan
			// or its runtime seeded continuation, as the materializing dispatcher does.
			if err := validateBoundPatternNodes([]pipelineRow{row}, pattern); err != nil {
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
			plan := planPipelineTraversalScan(shape, where, row)
			if plan.access == traversalSeededAccess {
				rows, _, err := e.pipelineApplySeededTraversalMatch(ctx, []pipelineRow{row}, clause, hint)
				if err != nil {
					recordExpressionFailure(ctx, err)
					valid = false
					return false
				}
				for _, selected := range rows {
					if !yield(selected) {
						return false
					}
				}
				return true
			}
			rowCtx := withValueBindings(ctx, row)
			match := e.parseTraversalPattern(rowCtx, pattern)
			if match == nil {
				valid = false
				return false
			}
			match.PathVariable = pathVariable
			match.UnusedPropertyVariables = e.traversalUnusedPropertyVariablesForTail(match, where, hint.readTail)
			startProperties := traversalSourceProperties(match.StartNode, where, hint.readTail, pathVariable)
			endProperties := traversalSourceProperties(match.EndNode, where, hint.readTail, pathVariable)
			// Only metadata-only nodes enter this fixed-size cache; arbitrary
			// property bodies are never retained between rows.
			headers := make(map[storage.NodeID]*storage.Node)
			load := func(id storage.NodeID, properties []string) (*storage.Node, error) {
				if properties != nil && len(properties) == 0 && headers[id] != nil {
					return headers[id], nil
				}
				node, err := endpoints.readRetained(id, properties)
				if err == nil && node != nil && properties != nil && len(properties) == 0 {
					if len(headers) == 1024 {
						clear(headers)
					}
					headers[id] = node
				}
				return node, err
			}
			bind := func(name string, value interface{}) bool {
				if name == "" {
					return true
				}
				if previous, exists := current[name]; exists {
					return pipelineBindingValuesEqual(previous, value)
				}
				current[name] = value
				return true
			}
			emit := func(edge *storage.Edge, startID, endID storage.NodeID) error {
				clear(current)
				for name, value := range row {
					current[name] = value
				}
				current[endpointPropertyCacheKey] = endpointValues
				// Check bound identities before any property read.
				for _, endpoint := range []struct {
					variable string
					id       storage.NodeID
				}{{match.StartNode.variable, startID}, {match.EndNode.variable, endID}} {
					if bound, ok := pipelineBoundNode(row, endpoint.variable); ok {
						id, valid := e.identityLookup(store).NodeLookupID(bound.Identity())
						if !valid || id != endpoint.id {
							return nil
						}
					}
				}
				start, err := load(startID, startProperties)
				if err != nil || start == nil || !e.matchesEndPattern(start, &match.StartNode) {
					return nil
				}
				end, err := load(endID, endProperties)
				if err != nil || end == nil || !e.matchesEndPattern(end, &match.EndNode) {
					return nil
				}
				if !bind(match.StartNode.variable, start) || !bind(match.EndNode.variable, end) || !bind(match.Relationship.Variable, edge) {
					return nil
				}
				if pathVariable != "" {
					path := PathResult{Nodes: []*storage.Node{start, end}, Relationships: []*storage.Edge{edge}, Length: 1}
					if !bind(pathVariable, e.pathContextRowValue(&path)) {
						return nil
					}
				}
				if where != "" && !predicate.holds(e, rowCtx, current) {
					return getExpressionFailure(ctx)
				}
				if !yield(current) {
					return storage.ErrIterationStopped
				}
				return nil
			}
			err := plan.visitEdges(rowCtx, e, match, func(edge *storage.Edge, start, end storage.NodeID) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if len(match.Relationship.Types) > 0 && !containsString(match.Relationship.Types, edge.Type) || !e.edgeMatchesProps(edge, match.Relationship.Properties) {
					return nil
				}
				return emit(edge, start, end)
			})
			if err != nil {
				if !errors.Is(err, storage.ErrIterationStopped) {
					recordExpressionFailure(ctx, err)
					valid = false
				}
				return false
			}
			return true
		})
		return completed && valid
	}, true, nil
}

func traversalSourceProperties(node nodePatternInfo, where string, tail []string, pathVariable string) []string {
	if pathVariable != "" {
		return nil
	}
	properties := pipelineNodePredicateProperties(node.variable, where, node.properties)
	if properties == nil || node.variable == "" {
		return properties
	}
	for _, text := range tail {
		if !traversalVariablePropertiesRead(text, node.variable) {
			continue
		}
		used := pipelineNodePredicateProperties(node.variable, text, nil)
		if used == nil {
			return nil
		}
		properties = append(properties, used...)
	}
	return properties
}
