package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type relationshipSeek struct {
	name, relType string
	properties    []string
	values        []interface{}
}

// A seek is only a different source of candidates for the existing single-hop
// MATCH. Pattern parsing, value evaluation, endpoint readers, predicates and
// projection retain their existing owners.
func (e *StorageExecutor) relationshipPropertySeek(ctx context.Context, match *TraversalMatch, where string, unknown map[string]bool) (relationshipSeek, bool) {
	var empty relationshipSeek
	if match == nil || match.IsChained || match.Relationship.VariableLength || match.Relationship.MinHops != 1 || match.Relationship.MaxHops != 1 || len(match.Relationship.Types) != 1 {
		return empty, false
	}
	if viewport, _ := TemporalViewportFromContext(ctx); viewport.Enabled() {
		return empty, false
	}
	reader, ok := e.getStorage(ctx).(storage.RelationshipPropertySeeker)
	if !ok || !reader.RelationshipPropertySeeksAvailable() {
		return empty, false
	}
	rel := match.Relationship
	values := make(map[string]interface{}, len(rel.Properties))
	for key, value := range rel.Properties {
		values[key] = value
	}
	if rel.Variable != "" {
		for _, conjunct := range splitTopLevelAndConjuncts(where) {
			shape := parseSimpleIndexedEqualityShape(rel.Variable, conjunct)
			if !shape.ok {
				continue
			}
			if _, exists := values[shape.property]; exists {
				continue
			}
			if value, ok := e.indexSeekConstant(ctx, shape.constant); ok {
				values[shape.property] = value
			} else if relationshipSeekIncomingExpression(shape.constant, unknown) {
				// EXPLAIN knows an incoming row's variables, not their values.
				values[shape.property] = "<incoming value>"
			}
		}
	}
	for _, index := range e.getStorage(ctx).GetSchema().RelationshipEqualityIndexes(rel.Types[0]) {
		key := make([]interface{}, len(index.Properties))
		complete := true
		for i, property := range index.Properties {
			var exists bool
			key[i], exists = values[property]
			if !exists {
				complete = false
				break
			}
		}
		if complete && storage.RelationshipIndexValuesSupported(key) {
			return relationshipSeek{name: index.Name, relType: rel.Types[0], properties: index.Properties, values: key}, true
		}
	}
	return empty, false
}

func (e *StorageExecutor) tryRelationshipPropertySeek(ctx context.Context, match *TraversalMatch, where string, startSeeds, endSeeds []*storage.Node) ([]PathResult, bool, error) {
	seek, ok := e.relationshipPropertySeek(ctx, match, where, nil)
	if !ok {
		return nil, false, nil
	}
	reader := e.getStorage(ctx).(storage.RelationshipPropertySeeker)
	edges, answered, err := reader.SeekRelationships(ctx, seek.relType, seek.properties, seek.values)
	if err != nil || !answered {
		return nil, answered, err
	}
	traversal := e.newTraversalContext(ctx, nil, &match.Relationship)
	traversal.endpointsNeedOnlyExist = traversalEndpointsNeedOnlyExist(match)
	traversal.endpointsNeedOnlyLabels = traversalEndpointsNeedOnlyLabels(match)
	defer traversal.memory.close()
	seedAllows := func(seeds []*storage.Node, id storage.NodeID) bool {
		if len(seeds) == 0 {
			return true
		}
		for _, node := range seeds {
			if node.ID == id {
				return true
			}
		}
		return false
	}
	var paths []PathResult
	residual := make(map[string]interface{}, len(seek.properties))
	for i, p := range seek.properties {
		residual[p] = seek.values[i]
	}
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		if !e.edgeMatchesProps(edge, match.Relationship.Properties) {
			continue
		}
		// WHERE-derived key values are residual predicates too: the equality
		// key can group numerics more broadly than Cypher's exact comparison.
		if !e.edgeMatchesProps(edge, residual) {
			continue
		}
		orientations := [][2]storage.NodeID{{edge.StartNode, edge.EndNode}}
		switch match.Relationship.Direction {
		case "incoming":
			orientations[0] = [2]storage.NodeID{edge.EndNode, edge.StartNode}
		case "both":
			if edge.StartNode != edge.EndNode {
				orientations = append(orientations, [2]storage.NodeID{edge.EndNode, edge.StartNode})
			}
		}
		for _, endpoints := range orientations {
			if !seedAllows(startSeeds, endpoints[0]) || !seedAllows(endSeeds, endpoints[1]) {
				continue
			}
			if match.boundEndNodeID != "" && endpoints[1] != match.boundEndNodeID {
				continue
			}
			start, ok := e.loadTraversalEndpointNode(traversal, endpoints[0])
			if !ok || !e.matchesEndPattern(start, &match.StartNode) {
				continue
			}
			end, ok := e.loadTraversalEndpointNode(traversal, endpoints[1])
			if !ok || !e.matchesEndPattern(end, &match.EndNode) {
				continue
			}
			if err := traversal.memoryRetention().reservePath(2, 1); err != nil {
				return nil, true, err
			}
			paths = append(paths, PathResult{Nodes: []*storage.Node{start, end}, Relationships: []*storage.Edge{edge}, Length: 1})
		}
	}
	if where != "" {
		paths = e.filterPathsByWhere(ctx, paths, match, where, nil)
	}
	if match.TraversalLimit > 0 && len(paths) > match.TraversalLimit {
		paths = paths[:match.TraversalLimit]
	}
	recordRelationshipSeek(ctx, match.Relationship.Variable, seek, int64(len(edges)), int64(len(paths)))
	return paths, true, nil
}

func relationshipSeekIncomingExpression(expr string, unknown map[string]bool) bool {
	variables := expressionFreeVariables(expr)
	if len(variables) == 0 {
		return false
	}
	for _, variable := range variables {
		if !unknown[variable] {
			return false
		}
	}
	return true
}
