package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// undirectedIncidentEdges returns each incident relationship once. Storage
// correctly exposes a self-relationship as both incoming and outgoing, but an
// undirected Cypher expansion has only one orientation for that relationship.
func undirectedIncidentEdges(store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, error) {
	outgoing, incoming, err := incidentEdgeLists(store, nodeID)
	if err != nil {
		return nil, err
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

// incidentEdgeLists returns a node's outgoing and incoming relationships
// (a self-relationship is in both). A store that reads both directions at
// once (storage.AdjacentEdgesEngine) is read with one call.
func incidentEdgeLists(store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	if adjacent, ok := store.(storage.AdjacentEdgesEngine); ok {
		return adjacent.GetAdjacentEdges(nodeID)
	}
	outgoing, err := store.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, nil, err
	}
	incoming, err := store.GetIncomingEdges(nodeID)
	if err != nil {
		return nil, nil, err
	}
	return outgoing, incoming, nil
}
