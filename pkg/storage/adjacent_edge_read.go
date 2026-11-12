package storage

import "context"

// AdjacentEdgeDirection selects which incident relationships an
// AdjacentEdgeRead lists.
type AdjacentEdgeDirection uint8

const (
	// AdjacentOutgoing lists the relationships whose start node is the
	// request's node.
	AdjacentOutgoing AdjacentEdgeDirection = iota
	// AdjacentIncoming lists the relationships whose end node is the
	// request's node.
	AdjacentIncoming
	// AdjacentBoth lists every relationship incident to the request's node
	// once: the outgoing ones first, then the incoming ones, with a
	// self-relationship appearing only in the outgoing half. It is the
	// undirected expansion's one-orientation contract.
	AdjacentBoth
	// AdjacentBetween lists the relationships directed from the request's
	// node to its Other.
	AdjacentBetween
)

// AdjacentEdgeRead is the one request for a node's adjacent relationships: the
// node, the direction (with the far endpoint of a between read), the
// relationship types it may list, and whether the caller needs only headers.
//
// Types nil or empty lists every type. A non-empty Types set is a filter the
// reader applies before it hydrates a relationship: the current adjacency
// index value carries the relationship's type, so an entry of another type is
// rejected without reading its record. Headers reports that the caller needs
// each relationship only for its ID, type and endpoints, so a reader may
// answer without reading the relationship's record or its properties.
//
// The pinned readers take the same request. A pinned adjacency entry carries
// only its tombstone bit, so a pinned reader validates the current header
// against the relationship's MVCC head version and resolves the historical
// relationship when that header is stale; a current type is never taken as
// evidence of a historical type.
type AdjacentEdgeRead struct {
	Direction AdjacentEdgeDirection
	Node      NodeID
	Other     NodeID
	Types     []string
	Headers   bool
}

// validate rejects a request that names no node, or a between request without
// its other endpoint.
func (r AdjacentEdgeRead) validate() error {
	switch r.Direction {
	case AdjacentOutgoing, AdjacentIncoming, AdjacentBoth:
		if r.Node == "" {
			return ErrInvalidID
		}
		return nil
	case AdjacentBetween:
		if r.Node == "" || r.Other == "" {
			return ErrInvalidID
		}
		return nil
	default:
		return ErrInvalidData
	}
}

// adjacentTypesAdmit reports whether edgeType is one of types. nil or empty
// types admit every type.
func adjacentTypesAdmit(types []string, edgeType string) bool {
	if len(types) == 0 {
		return true
	}
	if len(types) == 1 {
		return edgeType == types[0]
	}
	for _, candidate := range types {
		if edgeType == candidate {
			return true
		}
	}
	return false
}

// admits reports whether edge is one this request lists: attached at the
// request's node in its direction (and at Other for a between read) and of a
// requested type.
func (r AdjacentEdgeRead) admits(edge *Edge) bool {
	if edge == nil || !adjacentTypesAdmit(r.Types, edge.Type) {
		return false
	}
	switch r.Direction {
	case AdjacentOutgoing:
		return edge.StartNode == r.Node
	case AdjacentIncoming:
		return edge.EndNode == r.Node
	case AdjacentBetween:
		return edge.StartNode == r.Node && edge.EndNode == r.Other
	case AdjacentBoth:
		return edge.StartNode == r.Node || edge.EndNode == r.Node
	default:
		return false
	}
}

// header is the relationship in the detail the request asks for: the
// relationship itself for a full read, or a shallow copy whose properties are
// absent otherwise. The caller of a header read must not read properties.
func (r AdjacentEdgeRead) header(edge *Edge) *Edge {
	if edge == nil || !r.Headers {
		return edge
	}
	header := *edge
	header.Properties = nil
	return &header
}

// halves splits a both-direction request into its two directional requests.
func (r AdjacentEdgeRead) halves() (outgoing, incoming AdjacentEdgeRead) {
	outgoing, incoming = r, r
	outgoing.Direction, incoming.Direction = AdjacentOutgoing, AdjacentIncoming
	return outgoing, incoming
}

// readBothAdjacentEdges answers a both-direction request from one direction at
// a time through read. It is the shared answer for readers whose native
// adjacency is directional, so every engine keeps the same ordering and the
// same self-relationship contract.
func readBothAdjacentEdges(ctx context.Context, request AdjacentEdgeRead, read func(AdjacentEdgeRead) ([]*Edge, error)) ([]*Edge, error) {
	outgoingRequest, incomingRequest := request.halves()
	outgoing, err := read(outgoingRequest)
	if err != nil {
		return nil, err
	}
	incoming, err := read(incomingRequest)
	if err != nil {
		return nil, err
	}
	return combineAdjacentEdgeHalves(ctx, request.Node, outgoing, incoming)
}

// combineAdjacentEdgeHalves is the one owner of a both-direction result: the
// outgoing relationships first, then the incoming ones, a self-relationship
// once. The caller's context is observed while the halves are copied.
func combineAdjacentEdgeHalves(ctx context.Context, nodeID NodeID, outgoing, incoming []*Edge) ([]*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(incoming) == 0 {
		return outgoing, nil
	}
	edges := make([]*Edge, 0, len(outgoing)+len(incoming))
	for index, edge := range outgoing {
		if index&bulkReadCancellationInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		edges = append(edges, edge)
	}
	for index, edge := range incoming {
		if index&bulkReadCancellationInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if edge != nil && edge.StartNode == edge.EndNode && edge.StartNode == nodeID {
			continue
		}
		edges = append(edges, edge)
	}
	return edges, nil
}
