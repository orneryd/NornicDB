package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// boundExpandCount is boundChainCount for one relationship of length one,
// (a)-[r]-(b), as Neo4j plans [NOT] EXISTS over it: SemiApply over Expand
// (Into when b is bound too). The relationships are read once from the end
// that has fewer of them, a candidate's far end is compared by ID before
// any node is read, a bound end is the row's own node (no node read), the
// WHERE's row is built once for every candidate, and EXISTS stops at the
// first match. It gives the rows the traversal below gives the same body;
// expanded is false for any other chain, and under a temporal viewport,
// which the traversal handles.
func (e *StorageExecutor) boundExpandCount(ctx context.Context, matches *TraversalMatch, seed *storage.Node, where string, values map[string]interface{}, first bool) (count int64, expanded bool) {
	rel := matches.Relationship
	if matches.IsChained || rel.VariableLength || rel.MinHops != 1 || rel.MaxHops != 1 {
		return 0, false
	}
	if viewport, ok := TemporalViewportFromContext(ctx); ok && viewport.Enabled() {
		return 0, false
	}
	if !e.pipelineNodeMatchesPattern(seed, matches.StartNode) {
		return 0, true
	}
	// chainVariablesBindEntities admitted only entities of the right kind:
	// a bound null matches no path.
	var end *storage.Node
	if name := matches.EndNode.variable; name != "" {
		if value, bound := values[name]; bound {
			if end, _ = value.(*storage.Node); end == nil {
				return 0, true
			}
		}
	}
	var boundRel *storage.Edge
	if name := rel.Variable; name != "" {
		if value, bound := values[name]; bound {
			if boundRel, _ = value.(*storage.Edge); boundRel == nil {
				return 0, true
			}
		}
	}
	// With both ends bound, expand from the end with fewer relationships in
	// the direction it sees them: the same relationships either way.
	from, target := seed, storage.NodeID("")
	if end != nil {
		target = end.ID
		reversed := reverseRelationshipDirection(rel.Direction)
		if end.ID != seed.ID && e.relationshipDegree(end.ID, reversed) < e.relationshipDegree(seed.ID, rel.Direction) {
			from, target, rel.Direction = end, seed.ID, reversed
		}
	}
	traversal := e.newTraversalContext(ctx, from, &rel)
	traversal.endpointsNeedOnlyExist = traversalEndpointsNeedOnlyExist(matches)
	traversal.endpointsNeedOnlyLabels = traversalEndpointsNeedOnlyLabels(matches)
	traversal.relationshipsNeedOnlyHeaders = traversalRelationshipsNeedOnlyHeaders(matches)
	var edges []*storage.Edge
	switch rel.Direction {
	case "outgoing":
		edges = e.traversalEdges(traversal, from.ID, true)
	case "incoming":
		edges = e.traversalEdges(traversal, from.ID, false)
	default:
		var err error
		if edges, err = undirectedIncidentEdges(e.storage, from.ID); err != nil {
			recordExpressionFailure(ctx, err)
			return 0, true
		}
	}
	var row map[string]interface{}
	for index, edge := range edges {
		if index&bfsCancelCheckMask == 0 && traversal.traversalCancelled() {
			return count, true
		}
		if len(rel.Types) > 0 {
			if len(rel.Types) == 1 {
				if edge.Type != rel.Types[0] {
					continue
				}
			} else if _, ok := traversal.relTypeSet[edge.Type]; !ok {
				continue
			}
		}
		if boundRel != nil && boundRel.ID != edge.ID {
			continue
		}
		if len(rel.Properties) > 0 && !e.edgeMatchesProps(edge, rel.Properties) {
			continue
		}
		far := edge.StartNode
		if rel.Direction == "outgoing" || (rel.Direction == "both" && edge.StartNode == from.ID) {
			far = edge.EndNode
		}
		endNode := end
		if target != "" {
			if far != target {
				continue
			}
		} else {
			loaded, ok := e.loadTraversalEndpointNode(traversal, far)
			if !ok {
				continue
			}
			endNode = loaded
		}
		if !e.matchesEndPattern(endNode, &matches.EndNode) {
			continue
		}
		if where != "" {
			// The WHERE sees the row's values with the relationship's bindings.
			if row == nil {
				row = make(map[string]interface{}, len(values)+4)
				for name, value := range values {
					row[name] = value
				}
				row[matches.StartNode.variable] = seed
			}
			if name := matches.EndNode.variable; name != "" {
				row[name] = endNode
			}
			if rel.Variable != "" {
				row[rel.Variable] = edge
			}
			if !e.evaluateRowPredicate(ctx, where, row) {
				continue
			}
		}
		count++
		if first {
			return count, true
		}
	}
	return count, true
}

// reverseRelationshipDirection is a relationship pattern's direction read
// from its other end.
func reverseRelationshipDirection(direction string) string {
	switch direction {
	case "outgoing":
		return "incoming"
	case "incoming":
		return "outgoing"
	}
	return direction
}

// relationshipDegree is how many relationships a node has in direction: an
// estimate to choose the cheaper end of an expansion by, never a result.
func (e *StorageExecutor) relationshipDegree(id storage.NodeID, direction string) int {
	switch direction {
	case "outgoing":
		return e.storage.GetOutDegree(id)
	case "incoming":
		return e.storage.GetInDegree(id)
	}
	return e.storage.GetOutDegree(id) + e.storage.GetInDegree(id)
}

