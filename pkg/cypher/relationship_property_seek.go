package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type relationshipSeek struct {
	name, relType string
	properties    []string
	values        []interface{}
	estimatedRows int64
}

type relationshipSeekPredicateKey struct{}
type relationshipSeekPredicate struct{ pattern, where string }

func withRelationshipSeekPredicate(ctx context.Context, pattern, where string) context.Context {
	return context.WithValue(ctx, relationshipSeekPredicateKey{}, relationshipSeekPredicate{pattern: pattern, where: where})
}

func (run relationshipAccessExecution) tryID(ctx context.Context, match *TraversalMatch, startSeeds, endSeeds []*storage.Node) ([]PathResult, bool, error) {
	access := run.access.id
	if access == nil {
		return nil, false, nil
	}
	e := run.executor
	predicate := run.physicalWhere
	if predicate == "" {
		predicate = run.where
	}
	variable, lookupID, empty, ok := e.idEqualityLookup(ctx, predicate, "5")
	if !ok || variable != match.Relationship.Variable {
		return nil, false, nil
	}
	if empty {
		recordRelationshipIDAccess(ctx, run.pattern, run.where, match.Relationship.Variable, access, 0, 0, 0)
		return nil, true, nil
	}
	edge, err := e.getStorage(ctx).GetEdge(storage.EdgeID(lookupID))
	if err != nil || edge == nil {
		recordRelationshipIDAccess(ctx, run.pattern, run.where, match.Relationship.Variable, access, 1, 0, 0)
		return nil, true, nil
	}
	if !e.edgeTypeMatches(edge.Type, match.Relationship.Types) || !e.edgeMatchesProps(edge, match.Relationship.Properties) {
		recordRelationshipIDAccess(ctx, run.pattern, run.where, match.Relationship.Variable, access, 1, 1, 0)
		return nil, true, nil
	}
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
	orientations := [][2]storage.NodeID{{edge.StartNode, edge.EndNode}}
	switch match.Relationship.Direction {
	case "incoming":
		orientations[0] = [2]storage.NodeID{edge.EndNode, edge.StartNode}
	case "both":
		if edge.StartNode != edge.EndNode {
			orientations = append(orientations, [2]storage.NodeID{edge.EndNode, edge.StartNode})
		}
	}
	traversal := e.newTraversalContext(ctx, nil, &match.Relationship)
	var paths []PathResult
	for _, endpoints := range orientations {
		if !seedAllows(startSeeds, endpoints[0]) || !seedAllows(endSeeds, endpoints[1]) {
			continue
		}
		if match.boundEndNodeID != "" && endpoints[1] != match.boundEndNodeID {
			continue
		}
		start, loaded := e.loadTraversalEndpointNode(traversal, endpoints[0])
		if !loaded || !e.matchesEndPattern(start, &match.StartNode) {
			continue
		}
		end, loaded := e.loadTraversalEndpointNode(traversal, endpoints[1])
		if !loaded || !e.matchesEndPattern(end, &match.EndNode) {
			continue
		}
		paths = append(paths, PathResult{Nodes: []*storage.Node{start, end}, Relationships: []*storage.Edge{edge}, Length: 1})
	}
	if predicate != "" {
		paths = e.filterPathsByWhere(ctx, paths, match, predicate, nil)
	}
	if match.TraversalLimit > 0 && len(paths) > match.TraversalLimit {
		paths = paths[:match.TraversalLimit]
	}
	recordRelationshipIDAccess(ctx, run.pattern, run.where, match.Relationship.Variable, access, 1, 1, int64(len(paths)))
	return paths, true, nil
}

func (run relationshipAccessExecution) trySeek(ctx context.Context, match *TraversalMatch, startSeeds, endSeeds []*storage.Node) ([]PathResult, bool, error) {
	e, pattern, seekWhere := run.executor, run.pattern, run.where
	seek, ok := run.access.resolve(ctx, e, match.Relationship.Properties)
	if !ok {
		recordRelationshipSeekFallback(ctx, pattern, seekWhere, match.Relationship.Variable)
		return nil, false, nil
	}
	reader := e.getStorage(ctx).(storage.RelationshipPropertySeeker)
	edges, answered, err := reader.SeekRelationships(ctx, seek.relType, seek.properties, seek.values)
	if err != nil || !answered {
		if !answered {
			recordRelationshipSeekFallback(ctx, pattern, seekWhere, match.Relationship.Variable)
		}
		return nil, answered, err
	}
	traversal := e.newTraversalContext(ctx, nil, &match.Relationship)
	// The traversal's projection hints describe only the far endpoint. A
	// relationship seek loads both ends here, so use full query-node reads.
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
			paths = append(paths, PathResult{Nodes: []*storage.Node{start, end}, Relationships: []*storage.Edge{edge}, Length: 1})
		}
	}
	if run.physicalWhere != "" {
		paths = e.filterPathsByWhere(ctx, paths, match, run.physicalWhere, nil)
	}
	if match.TraversalLimit > 0 && len(paths) > match.TraversalLimit {
		paths = paths[:match.TraversalLimit]
	}
	recordRelationshipSeek(ctx, pattern, seekWhere, match.Relationship.Variable, seek, int64(len(edges)), int64(len(paths)))
	return paths, true, nil
}
