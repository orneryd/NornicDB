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

func (e *StorageExecutor) tryRelationshipPropertySeek(ctx context.Context, pattern string, match *TraversalMatch, where string, startSeeds, endSeeds []*storage.Node) ([]PathResult, bool, error) {
	seekWhere := where
	if original, ok := ctx.Value(relationshipSeekPredicateKey{}).(relationshipSeekPredicate); ok && original.pattern == pattern {
		// The pipeline keeps outer-binding predicates for its joined-row filter.
		// They can still supply an equality key evaluated in this incoming row.
		seekWhere = original.where
	}
	seek, ok := e.relationshipAccessFor(ctx, pattern, match, seekWhere).resolve(ctx, e, match.Relationship.Properties)
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
	if where != "" {
		paths = e.filterPathsByWhere(ctx, paths, match, where, nil)
	}
	if match.TraversalLimit > 0 && len(paths) > match.TraversalLimit {
		paths = paths[:match.TraversalLimit]
	}
	recordRelationshipSeek(ctx, pattern, seekWhere, match.Relationship.Variable, seek, int64(len(edges)), int64(len(paths)))
	return paths, true, nil
}
