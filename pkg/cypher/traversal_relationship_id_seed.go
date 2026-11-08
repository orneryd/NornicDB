package cypher

import (
	"context"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipIDSeedEdge resolves a one-hop match whose WHERE pins its
// relationship by id(r) or elementId(r) (idEqualityLookup), such as
// MATCH ()-[r]->() WHERE elementId(r) = $id, to that relationship. The
// traversal then starts from the relationship's own endpoints
// (relationshipIDSeedEndpointNodes) and walks only it, instead of listing the
// start node's whole adjacency. none is true when no such relationship can
// match (it doesn't exist, or has another type); ok is false for any other
// shape.
func (e *StorageExecutor) relationshipIDSeedEdge(ctx context.Context, pattern, whereClause string) (edge *storage.Edge, none, ok bool) {
	if viewport, active := TemporalViewportFromContext(ctx); active && viewport.Enabled() {
		return nil, false, false
	}
	match := e.parseTraversalPattern(ctx, pattern)
	if match == nil || match.IsChained {
		return nil, false, false
	}
	relationship := match.Relationship
	if relationship.VariableLength || relationship.MinHops != 1 || relationship.MaxHops != 1 {
		return nil, false, false
	}
	var lookupID string
	var empty, matched bool
	for _, term := range splitTopLevelAndConjuncts(ctx, whereClause) {
		variable, id, none, eligible := e.idEqualityLookup(ctx, unwrapOuterParens(strings.TrimSpace(term)), "5")
		if eligible && variable == relationship.Variable {
			lookupID, empty, matched = id, none, true
			break
		}
	}
	if !matched {
		return nil, false, false
	}
	if empty {
		return nil, true, true
	}
	edge, err := e.getStorage(ctx).GetEdge(ctx, storage.EdgeID(lookupID))
	if err != nil || edge == nil {
		return nil, true, true
	}
	if len(relationship.Types) > 0 && !slices.Contains(relationship.Types, edge.Type) {
		return nil, true, true
	}
	return edge, false, true
}

// relationshipIDSeedEndpointNodes is the start nodes of a traversal whose
// relationship is pinned (relationshipIDSeedEdge): that relationship's endpoint
// in the pattern's direction (outgoing: its start, incoming: its end,
// undirected: both), read directly instead of starting from every node. The
// traversal trusts its seeds: each must match the pattern's start node labels
// and properties, as a bound start node does. No matching endpoint means the
// pattern matches nothing.
func (e *StorageExecutor) relationshipIDSeedEndpointNodes(ctx context.Context, edge *storage.Edge, match *TraversalMatch) []*storage.Node {
	endpoints := []storage.EntityIdentity{edge.StartIdentity()}
	switch match.Relationship.Direction {
	case "incoming":
		endpoints = []storage.EntityIdentity{edge.EndIdentity()}
	case "outgoing":
	default:
		if edge.EndNode != edge.StartNode {
			endpoints = append(endpoints, edge.EndIdentity())
		}
	}
	var nodes []*storage.Node
	for _, identity := range endpoints {
		id, valid := identity.NodeLookupID(e.getStorage(ctx))
		if !valid {
			continue
		}
		if node, err := e.newRelationshipEndpointReader(e.getStorage(ctx)).readRetained(ctx, id, nil); err == nil && node != nil && e.matchesEndPattern(node, &match.StartNode) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}
