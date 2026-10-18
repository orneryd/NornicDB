package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type relationshipSeek struct {
	name, relType string
	properties    []string
	values        []interface{}
	runtimeKey    bool // EXPLAIN: supported equality key is resolved per incoming row.
	estimatedRows int64
}

type relationshipSeekPredicateKey struct{}
type relationshipSeekPredicate struct{ pattern, where string }

func withRelationshipSeekPredicate(ctx context.Context, pattern, where string) context.Context {
	return context.WithValue(ctx, relationshipSeekPredicateKey{}, relationshipSeekPredicate{pattern: pattern, where: where})
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
	var unresolved map[string]bool
	for key, value := range rel.Properties {
		values[key] = value
	}
	if rel.Variable != "" {
		for _, conjunct := range splitTopLevelAndConjuncts(where) {
			shape := parseSimpleIndexedEqualityShape(rel.Variable, conjunct)
			if !shape.ok || !relationshipSeekExactProperty(rel.Variable, shape.property, conjunct) {
				continue
			}
			if _, exists := values[shape.property]; exists {
				continue
			}
			if value, ok := e.indexSeekConstant(ctx, shape.constant); ok {
				values[shape.property] = value
			} else if relationshipSeekIncomingExpression(shape.constant, unknown) {
				// The semantic scope owner supplies incoming names. Keep a
				// deferred key explicit, rather than inventing a scalar value.
				if unresolved == nil {
					unresolved = make(map[string]bool)
				}
				unresolved[shape.property] = true
			}
		}
	}
	for _, index := range e.getStorage(ctx).GetSchema().RelationshipEqualityIndexes(rel.Types[0]) {
		key := make([]interface{}, len(index.Properties))
		complete := true
		runtimeKey := false
		for i, property := range index.Properties {
			var exists bool
			key[i], exists = values[property]
			if unresolved[property] {
				runtimeKey = true
				continue
			}
			if !exists {
				complete = false
				break
			}
		}
		supportedValues := key
		if runtimeKey {
			supportedValues = make([]interface{}, 0, len(key))
			for i, property := range index.Properties {
				if !unresolved[property] {
					supportedValues = append(supportedValues, key[i])
				}
			}
		}
		if complete && (len(supportedValues) == 0 || storage.RelationshipIndexValuesSupported(supportedValues)) {
			return relationshipSeek{name: index.Name, relType: rel.Types[0], properties: index.Properties, values: key, runtimeKey: runtimeKey, estimatedRows: 10}, true
		}
	}
	return empty, false
}

func (e *StorageExecutor) tryRelationshipPropertySeek(ctx context.Context, pattern string, match *TraversalMatch, where string, startSeeds, endSeeds []*storage.Node) ([]PathResult, bool, error) {
	seekWhere := where
	if original, ok := ctx.Value(relationshipSeekPredicateKey{}).(relationshipSeekPredicate); ok && original.pattern == pattern {
		// The pipeline keeps outer-binding predicates for its joined-row filter.
		// They can still supply an equality key evaluated in this incoming row.
		seekWhere = original.where
	}
	seek, ok := e.relationshipPropertySeek(ctx, match, seekWhere, nil)
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
	if where != "" {
		paths = e.filterPathsByWhere(ctx, paths, match, where, nil)
	}
	if match.TraversalLimit > 0 && len(paths) > match.TraversalLimit {
		paths = paths[:match.TraversalLimit]
	}
	recordRelationshipSeek(ctx, pattern, seekWhere, match.Relationship.Variable, seek, int64(len(edges)), int64(len(paths)))
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

// The generic node equality shape reader folds variable case. Relationship
// candidate ownership is exact: node R and relationship r are distinct bindings.
func relationshipSeekExactProperty(variable, property, predicate string) bool {
	clause := unwrapOuterParens(strings.TrimSpace(predicate))
	equals := topLevelEqualsIndex(clause)
	if equals < 0 {
		return false
	}
	for _, side := range []string{clause[:equals], clause[equals+1:]} {
		name, rest, ok := parseIdentifierToken(strings.TrimSpace(side))
		if !ok || symbolicNameValue(name) != variable {
			continue
		}
		rest = strings.TrimSpace(rest)
		if !strings.HasPrefix(rest, ".") {
			continue
		}
		name, tail, ok := parseIdentifierToken(strings.TrimSpace(rest[1:]))
		if ok && strings.TrimSpace(tail) == "" && symbolicNameValue(name) == property {
			return true
		}
	}
	return false
}
