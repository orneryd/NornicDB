package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// undirectedIncidentEdges returns each incident relationship once. Storage
// correctly exposes a self-relationship as both incoming and outgoing, but an
// undirected Cypher expansion has only one orientation for that relationship.
// A store that reads both directions at once (storage.AdjacentEdgesEngine)
// is read with one call.
func undirectedIncidentEdges(store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, error) {
	var outgoing, incoming []*storage.Edge
	var err error
	if adjacent, ok := store.(storage.AdjacentEdgesEngine); ok {
		if outgoing, incoming, err = adjacent.GetAdjacentEdges(nodeID); err != nil {
			return nil, err
		}
	} else {
		if outgoing, err = store.GetOutgoingEdges(nodeID); err != nil {
			return nil, err
		}
		if incoming, err = store.GetIncomingEdges(nodeID); err != nil {
			return nil, err
		}
	}
	edges := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
	edges = append(edges, outgoing...)
	for _, edge := range incoming {
		if edge != nil && edge.StartNode == nodeID && edge.EndNode == nodeID {
			continue
		}
		edges = append(edges, edge)
	}
	return edges, nil
}
