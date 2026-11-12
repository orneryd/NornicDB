package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// undirectedIncidentEdges returns each incident relationship once. Storage
// correctly exposes a self-relationship as both incoming and outgoing, but an
// undirected Cypher expansion has only one orientation for that relationship:
// the both-direction adjacent-edge request reports it once. The request selects
// the pattern's types and detail; the caller's context reaches the storage
// read, so a cancelled traversal stops instead of waiting for an unaffected
// read.
func undirectedIncidentEdges(ctx context.Context, store storage.Engine, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	request.Direction = storage.AdjacentBoth
	edges, err := store.ReadAdjacentEdges(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return edges, nil
}

func combineIncidentEdges(ctx context.Context, nodeID storage.NodeID, outgoing, incoming []*storage.Edge) ([]*storage.Edge, error) {
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
		if edge != nil && edge.StartIdentity() == edge.EndIdentity() {
			continue
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

// incidentEdgeListsContext is the single storage reader for selected paths.
// A directed read avoids loading the other direction; both directions use one
// adjacent-edge request and are split by endpoint, so a self-relationship stays
// in both lists as storage's own directional reads report it. Cancellation and
// read errors reach the caller.
func incidentEdgeListsContext(ctx context.Context, store storage.Engine, request storage.AdjacentEdgeRead, direction string) ([]*storage.Edge, []*storage.Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if direction == "outgoing" {
		request.Direction = storage.AdjacentOutgoing
		edges, err := store.ReadAdjacentEdges(ctx, request)
		if err == nil {
			err = ctx.Err()
		}
		return edges, nil, err
	}
	if direction == "incoming" {
		request.Direction = storage.AdjacentIncoming
		edges, err := store.ReadAdjacentEdges(ctx, request)
		if err == nil {
			err = ctx.Err()
		}
		return nil, edges, err
	}
	request.Direction = storage.AdjacentBoth
	edges, err := store.ReadAdjacentEdges(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	var outgoing, incoming []*storage.Edge
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if edge.StartNode == request.Node {
			outgoing = append(outgoing, edge)
		}
		if edge.EndNode == request.Node {
			incoming = append(incoming, edge)
		}
	}
	return outgoing, incoming, nil
}
