package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// undirectedIncidentEdges returns each incident relationship once. Storage
// correctly exposes a self-relationship as both incoming and outgoing, but an
// undirected Cypher expansion has only one orientation for that relationship.
func undirectedIncidentEdges(store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, error) {
	outgoing, incoming, err := incidentEdgeLists(store, nodeID)
	if err != nil {
		return nil, err
	}
	return combineIncidentEdges(context.Background(), nodeID, outgoing, incoming)
}

func combineIncidentEdges[K ~string](ctx context.Context, _ K, outgoing, incoming []*storage.Edge) ([]*storage.Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	edges := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
	for index, edge := range outgoing {
		if index&bfsCancelCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		edges = append(edges, edge)
	}
	for index, edge := range incoming {
		if index&bfsCancelCheckMask == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// An incident self-relationship occurs in both lists. Compare its
		// native endpoint identities, not an engine-domain lookup argument.
		if edge != nil && edge.StartIdentity() == edge.EndIdentity() {
			continue
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

// incidentEdgeLists returns a node's outgoing and incoming relationships
// (a self-relationship is in both). A store that reads both directions at
// once (storage.AdjacentEdgesEngine) is read with one call.
func incidentEdgeLists(store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	return incidentEdgeListsContext(context.Background(), store, nodeID, "both")
}

// incidentEdgeListsContext is the single storage reader for selected paths.
// Directed reads avoid loading the other direction; both directions use one
// adjacency call when supported. Cancellation and read errors reach the caller.
func incidentEdgeListsContext(ctx context.Context, store storage.Engine, nodeID storage.NodeID, direction string) ([]*storage.Edge, []*storage.Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if direction == "outgoing" {
		edges, err := store.GetOutgoingEdges(nodeID)
		if err == nil {
			err = ctx.Err()
		}
		return edges, nil, err
	}
	if direction == "incoming" {
		edges, err := store.GetIncomingEdges(nodeID)
		if err == nil {
			err = ctx.Err()
		}
		return nil, edges, err
	}
	if adjacent, ok := store.(storage.AdjacentEdgesEngine); ok {
		out, in, err := adjacent.GetAdjacentEdges(nodeID)
		if err == nil {
			err = ctx.Err()
		}
		return out, in, err
	}
	outgoing, err := store.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	incoming, err := store.GetIncomingEdges(nodeID)
	if err != nil {
		return nil, nil, err
	}
	return outgoing, incoming, ctx.Err()
}
