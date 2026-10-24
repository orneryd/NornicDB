package cypher

import (
	"context"
	"iter"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// adjacentEdges is the one reader of a node's relationships for a pass over
// them: one storage read in direction ("outgoing", "incoming" or "both"; the
// read is the storage boundary), then each relationship through the
// cancellation owner's check (walk), outgoing first. Both directions yield a
// self-relationship twice, as storage holds it; undirectedEdges yields it once.
func adjacentEdges(walk *materializedRowCheckpoint, store storage.Engine, nodeID storage.NodeID, direction string) (iter.Seq[*storage.Edge], error) {
	outgoing, incoming, err := incidentEdgeListsContext(walk.Context(), store, nodeID, direction)
	if err != nil {
		return nil, err
	}
	return func(yield func(*storage.Edge) bool) {
		for _, edge := range observedItems(walk, outgoing) {
			if !yield(edge) {
				return
			}
		}
		for _, edge := range observedItems(walk, incoming) {
			if !yield(edge) {
				return
			}
		}
	}, nil
}

// undirectedEdges is adjacentEdges in both directions with each relationship
// once: a self-relationship is one undirected relationship.
func undirectedEdges(walk *materializedRowCheckpoint, store storage.Engine, nodeID storage.NodeID) (iter.Seq[*storage.Edge], error) {
	outgoing, incoming, err := incidentEdgeListsContext(walk.Context(), store, nodeID, "both")
	if err != nil {
		return nil, err
	}
	return func(yield func(*storage.Edge) bool) {
		for _, edge := range observedItems(walk, outgoing) {
			if !yield(edge) {
				return
			}
		}
		for _, edge := range observedItems(walk, incoming) {
			if edge != nil && edge.StartNode == nodeID && edge.EndNode == nodeID {
				continue
			}
			if !yield(edge) {
				return
			}
		}
	}, nil
}

// undirectedIncidentEdges returns each incident relationship once. Storage
// correctly exposes a self-relationship as both incoming and outgoing, but an
// undirected Cypher expansion has only one orientation for that relationship.
func undirectedIncidentEdges(walk *materializedRowCheckpoint, store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, error) {
	outgoing, incoming, err := incidentEdgeListsContext(walk.Context(), store, nodeID, "both")
	if err != nil {
		return nil, err
	}
	return combineIncidentEdges(walk, nodeID, outgoing, incoming), nil
}

// combineIncidentEdges is outgoing and incoming as one undirected list, a
// self-relationship once, through the owner's check (walk).
func combineIncidentEdges(walk *materializedRowCheckpoint, nodeID storage.NodeID, outgoing, incoming []*storage.Edge) []*storage.Edge {
	edges := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
	for _, edge := range observedItems(walk, outgoing) {
		edges = append(edges, edge)
	}
	for _, edge := range observedItems(walk, incoming) {
		if edge != nil && edge.StartNode == nodeID && edge.EndNode == nodeID {
			continue
		}
		edges = append(edges, edge)
	}
	return edges
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
