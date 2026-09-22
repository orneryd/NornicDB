package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// undirectedIncidentEdges returns each incident relationship once. Storage
// correctly exposes a self-relationship as both incoming and outgoing, but an
// undirected Cypher expansion has only one orientation for that relationship.
func undirectedIncidentEdges(store storage.Engine, nodeID storage.NodeID) ([]*storage.Edge, error) {
	outgoing, err := store.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	incoming, err := store.GetIncomingEdges(nodeID)
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
