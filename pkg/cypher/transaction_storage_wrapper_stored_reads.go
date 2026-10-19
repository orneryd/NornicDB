package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// The transaction wrapper owns a semantic view. Its stored capability must
// preserve that view instead of unwrapping to committed storage.
var _ storage.StoredEntityReader = (*transactionStorageWrapper)(nil)

func (w *transactionStorageWrapper) GetStoredNode(id storage.NodeID) (*storage.Node, error) {
	node, err := w.tx.GetStoredNode(w.prefixNodeID(id))
	if err != nil || w.namespace == "" {
		return node, err
	}
	return w.toUserNode(node), nil
}

func (w *transactionStorageWrapper) GetStoredEdge(id storage.EdgeID) (*storage.Edge, error) {
	edge, err := w.tx.GetStoredEdge(w.prefixEdgeID(id))
	if err != nil || w.namespace == "" {
		return edge, err
	}
	return w.toUserEdge(edge), nil
}

func (w *transactionStorageWrapper) GetStoredOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	edges, err := w.tx.GetStoredOutgoingEdges(w.prefixNodeID(id))
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetStoredIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	edges, err := w.tx.GetStoredIncomingEdges(w.prefixNodeID(id))
	if err != nil || w.namespace == "" {
		return edges, err
	}
	return w.toUserEdges(edges), nil
}

func (w *transactionStorageWrapper) GetStoredNodesByLabel(label string) ([]*storage.Node, error) {
	nodes, err := w.tx.GetStoredNodesByLabel(label)
	if err != nil || w.namespace == "" {
		return nodes, err
	}
	return w.toUserNamespacedNodes(nodes), nil
}

func (w *transactionStorageWrapper) StreamStoredNodesByLabelProjected(label string, properties []string, visit func(*storage.Node) error) error {
	return w.streamNodesByLabelProjected(label, properties, true, visit)
}

func (w *transactionStorageWrapper) StreamStoredEdgesByType(ctx context.Context, edgeType string, visit func(*storage.Edge) error) error {
	return w.streamEdgesByType(ctx, edgeType, true, visit)
}
