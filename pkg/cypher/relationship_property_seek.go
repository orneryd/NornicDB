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

// A relationship row shares one frame for its values and original syntax.
// The identity is immutable text from the bounded syntax cache, never access
// authority. Every read still resolves the actual admitted program's template.
type relationshipRowContext struct {
	context.Context
	bindings map[string]interface{}
	identity *relationshipAccessIdentity

	// Runtime values belong to this reached row and admitted source. Seed
	// discovery and traversal share one parse; no value enters syntax caches.
	statement admittedRelationshipStatement
	executor  *StorageExecutor
	traversal *TraversalMatch
}

type relationshipRowIdentityKey struct{}

func (row *relationshipRowContext) Value(key interface{}) interface{} {
	switch key.(type) {
	case valueBindingsKey:
		return row.bindings
	case relationshipRowIdentityKey:
		return row.identity
	default:
		return row.Context.Value(key)
	}
}

func (e *StorageExecutor) withRelationshipRow(statement admittedRelationshipStatement, ctx context.Context, bindings map[string]interface{}, pattern, where string) context.Context {
	return &relationshipRowContext{Context: ctx, bindings: bindings, identity: relationshipAccessIdentityFor(pattern, where), statement: statement, executor: e}
}

func (run relationshipAccessExecution) tryID(ctx context.Context, match *TraversalMatch, startSeeds, endSeeds []*storage.Node) ([]PathResult, bool, error) {
	access := run.access.id
	if access == nil {
		return nil, false, nil
	}
	e := run.executor
	predicate := run.physicalWhere()
	if predicate == "" {
		predicate = run.where()
	}
	variable, lookupID, empty, ok := e.idEqualityLookup(run.statement(), ctx, predicate, "5")
	if !ok || variable != match.Relationship.Variable {
		return nil, false, nil
	}
	if empty {
		recordRelationshipIDAccess(ctx, run.pattern(), run.where(), match.Relationship.Variable, access, 0, 0, 0)
		return nil, true, nil
	}
	edge, err := e.getStorage(ctx).GetEdge(storage.EdgeID(lookupID))
	if err != nil || edge == nil {
		recordRelationshipIDAccess(ctx, run.pattern(), run.where(), match.Relationship.Variable, access, 1, 0, 0)
		return nil, true, nil
	}
	if !matchesRelationshipTypes(edge, match.Relationship.Types) || !e.edgeMatchesProps(edge, match.Relationship.Properties) {
		recordRelationshipIDAccess(ctx, run.pattern(), run.where(), match.Relationship.Variable, access, 1, 1, 0)
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
	// The logical predicate supplies a lookup key, while the physical predicate
	// contains only terms evaluable with this path's local bindings. The pipeline
	// evaluates the original predicate after joining the incoming row, as for RANGE.
	if run.physicalWhere() != "" {
		paths = e.filterPathsByWhere(run.statement(), ctx, paths, match, run.physicalWhere(), nil)
	}
	if match.TraversalLimit > 0 && len(paths) > match.TraversalLimit {
		paths = paths[:match.TraversalLimit]
	}
	recordRelationshipIDAccess(ctx, run.pattern(), run.where(), match.Relationship.Variable, access, 1, 1, int64(len(paths)))
	return paths, true, nil
}

func (run relationshipAccessExecution) trySeek(ctx context.Context, match *TraversalMatch, startSeeds, endSeeds []*storage.Node) ([]PathResult, bool, error) {
	e, pattern, seekWhere := run.executor, run.pattern(), run.where()
	seek, ok := run.access.resolve(run.statement(), ctx, e, match.Relationship.Properties)
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
	if run.physicalWhere() != "" {
		paths = e.filterPathsByWhere(run.statement(), ctx, paths, match, run.physicalWhere(), nil)
	}
	if match.TraversalLimit > 0 && len(paths) > match.TraversalLimit {
		paths = paths[:match.TraversalLimit]
	}
	recordRelationshipSeek(ctx, pattern, seekWhere, match.Relationship.Variable, seek, int64(len(edges)), int64(len(paths)))
	return paths, true, nil
}
