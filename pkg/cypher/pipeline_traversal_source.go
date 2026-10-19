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
	pattern := pipelineClauseBody(clause, "MATCH")
	where := ""
	if index := topLevelKeywordIndex(pattern, "WHERE"); index >= 0 {
		where = strings.TrimSpace(pattern[index+len("WHERE"):])
		pattern = strings.TrimSpace(pattern[:index])
	}
	if !containsRelationshipPattern(pattern) || len(splitTopLevelComma(pattern)) != 1 || hint.readTail == nil || hint.orderExpr != "" {
		return nil, false, nil
	}
	if viewport, _ := TemporalViewportFromContext(ctx); viewport.Enabled() {
		return nil, false, nil
	}
	shape := e.parseTraversalPattern(ctx, pattern)
	if shape == nil || shape.IsChained || shape.Relationship.VariableLength || shape.Relationship.MinHops != 1 || shape.Relationship.MaxHops != 1 {
		return nil, false, nil
	}
	pathVariable := extractPathAssignmentVariable(pattern)
	predicate := prepareRowPredicate(where)
	store := e.getStorage(ctx)
	return func(yield func(pipelineRow) bool) bool {
		valid := true
		emitted := 0
		limited := hint.earlyLimit == 0
		current := make(pipelineRow)
		completed := input(func(row pipelineRow) bool {
			if limited {
				return false
			}
			rowCtx := withValueBindings(ctx, row)
			match := e.parseTraversalPattern(rowCtx, pattern)
			if err := validateBoundPatternNodes([]pipelineRow{row}, pattern); err != nil {
				recordExpressionFailure(ctx, err)
				valid = false
				return false
			}
			startProperties := traversalSourceProperties(match.StartNode, where, hint.readTail, pathVariable)
			endProperties := traversalSourceProperties(match.EndNode, where, hint.readTail, pathVariable)
			// Only metadata-only nodes enter this fixed-size cache; arbitrary
			// property bodies are never retained between rows.
			headers := make(map[storage.NodeID]*storage.Node)
			load := func(id storage.NodeID, properties []string) (*storage.Node, error) {
				if properties != nil {
					if len(properties) == 0 && headers[id] != nil {
						return headers[id], nil
					}
					if reader, ok := store.(storage.ProjectedNodeReader); ok {
						node, err := reader.GetNodeProjected(id, properties)
						if err == nil && node != nil && len(properties) == 0 {
							if len(headers) == 1024 {
								clear(headers)
							}
							headers[id] = node
						}
						return node, err
					}
				}
				return e.getQueryNode(store, id)
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
				// Check bound identities before any property read.
				for _, endpoint := range []struct {
					variable string
					id       storage.NodeID
				}{{match.StartNode.variable, startID}, {match.EndNode.variable, endID}} {
					if bound, ok := pipelineBoundNode(row, endpoint.variable); ok && bound.ID != endpoint.id {
						return nil
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
				emitted++
				if hint.earlyLimit > 0 && emitted >= hint.earlyLimit {
					limited = true
					return storage.ErrIterationStopped
				}
				return nil
			}
			edgeType := ""
			if len(match.Relationship.Types) == 1 {
				edgeType = match.Relationship.Types[0]
			}
			err := storage.StreamEdgesByType(rowCtx, store, edgeType, func(edge *storage.Edge) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if len(match.Relationship.Types) > 1 && !containsString(match.Relationship.Types, edge.Type) || !e.edgeMatchesProps(edge, match.Relationship.Properties) {
					return nil
				}
				if match.Relationship.Direction == "incoming" {
					return emit(edge, edge.EndNode, edge.StartNode)
				}
				if err := emit(edge, edge.StartNode, edge.EndNode); err != nil {
					return err
				}
				if match.Relationship.Direction == "both" && edge.StartNode != edge.EndNode {
					return emit(edge, edge.EndNode, edge.StartNode)
				}
				return nil
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
		return (completed || limited) && valid
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
		used := pipelineNodePredicateProperties(node.variable, text, nil)
		if used == nil {
			return nil
		}
		properties = append(properties, used...)
	}
	return properties
}
