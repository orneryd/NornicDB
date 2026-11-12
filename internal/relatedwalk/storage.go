package relatedwalk

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// StorageSource adapts a storage engine to Source.
//
// For every node it reports outgoing edges before incoming edges, in the
// engine's own adjacency order. The reported neighbor is the endpoint opposite
// the expanded node, and the reported direction follows the list the engine
// returned the edge in, which is the derivation every related-node consumer
// used before this package owned the walk.
//
// Both directions are always attempted and every row a read returned is reported,
// even when the engine also returned an error for that list: the pre-merge MCP
// and server readers passed the error aside and walked the rows, so an ordinary
// adjacency read failure keeps the neighbors the engine did return instead of
// dropping a direction. A row without a usable endpoint (a nil edge or an empty
// endpoint) is reported with an empty neighbor id so the walk still counts it for
// the cancellation poll. The returned error is the first read failure; the walk
// itself keeps the reported neighbors and stops at its next cancellation poll.
type StorageSource struct {
	Engine storage.Engine
}

// EachIncidentEdge implements Source.
func (s StorageSource) EachIncidentEdge(ctx context.Context, nodeID string, visit StepVisitor) error {
	if s.Engine == nil {
		return nil
	}

	var readErr error
	out, err := s.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: storage.NodeID(nodeID)})
	if err != nil {
		readErr = err
	}
	for _, edge := range out {
		neighbor, relationship := incidentEndpoint(edge, nodeID)
		if !visit.Step(neighbor, relationship, Outgoing) {
			return readErr
		}
	}

	in, err := s.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: storage.NodeID(nodeID)})
	if err != nil && readErr == nil {
		readErr = err
	}
	for _, edge := range in {
		neighbor, relationship := incidentEndpoint(edge, nodeID)
		if !visit.Step(neighbor, relationship, Incoming) {
			return readErr
		}
	}
	return readErr
}

// incidentEndpoint returns the edge endpoint opposite the expanded node and the
// edge type. A nil edge or a missing endpoint yields an empty neighbor id, which
// the walk examines and skips.
func incidentEndpoint(edge *storage.Edge, nodeID string) (neighbor, relationship string) {
	if edge == nil {
		return "", ""
	}
	if string(edge.StartNode) == nodeID {
		return string(edge.EndNode), edge.Type
	}
	return string(edge.StartNode), edge.Type
}
