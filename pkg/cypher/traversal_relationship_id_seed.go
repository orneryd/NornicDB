package cypher

import (
	"errors"
	"context"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipIDSeedNodes is the start nodes of a one-hop match whose WHERE
// pins its relationship by id(r) or elementId(r) (idEqualityLookup), such as
// MATCH ()-[r]->() WHERE elementId(r) = $id: that
// relationship's endpoint in the pattern's direction (outgoing: its start,
// incoming: its end, undirected: both), read directly instead of starting from
// every node. The traversal still checks the whole pattern and WHERE from
// each start node. none is true when no such relationship can match (it
// doesn't exist, or has another type); ok is false for any other shape.
func (e *StorageExecutor) relationshipIDSeedNodes(ctx context.Context, pattern, whereClause string) (nodes []*storage.Node, none, ok bool) {
	variable, lookupID, empty, ok := e.idEqualityLookup(ctx, whereClause, "5")
	if !ok {
		return nil, false, false
	}
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || match.IsChained {
		return nil, false, false
	}
	relationship := match.Relationship
	if relationship.Variable != variable || relationship.VariableLength || relationship.MinHops != 1 || relationship.MaxHops != 1 {
		return nil, false, false
	}
	if empty {
		return nil, true, true
	}
	store := e.getStorage(ctx)
	edge, err := store.GetEdge(storage.EdgeID(lookupID))
	if err != nil && !errors.Is(err, storage.ErrNotFound) { recordExpressionFailure(ctx, err); return nil, true, true }
	if edge == nil {
		return nil, true, true
	}
	if len(relationship.Types) > 0 && !slices.Contains(relationship.Types, edge.Type) {
		return nil, true, true
	}
	endpoints := []storage.NodeID{edge.StartNode}
	switch relationship.Direction {
	case "incoming":
		endpoints = []storage.NodeID{edge.EndNode}
	case "outgoing":
	default:
		if edge.EndNode != edge.StartNode {
			endpoints = append(endpoints, edge.EndNode)
		}
	}
	// The traversal trusts its seeds: each must match the start node's own
	// labels and properties, as a bound start node does.
	for _, id := range endpoints {
		node, getErr := e.newRelationshipEndpointReader(store).readRetained(id, nil)
		if getErr != nil && !errors.Is(getErr, storage.ErrNotFound) { recordExpressionFailure(ctx, getErr); return nil, true, true }
		if node != nil && e.matchesEndPattern(node, &match.StartNode) {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) == 0 {
		return nil, true, true
	}
	return nodes, false, true
}
